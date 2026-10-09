// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourcehelper "k8s.io/component-helpers/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/SlinkyProject/slurm-bridge/internal/controller/node/slurmcontrol"
	nodeutils "github.com/SlinkyProject/slurm-bridge/internal/controller/node/utils"
	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

const (
	reasonSlurmGRESCompatible        = "SlurmGRESCompatible"
	reasonIncompatibleSlurmGRES      = "IncompatibleSlurmGRES"
	reasonSlurmGRESVerificationError = "SlurmGRESVerificationError"

	reasonSlurmResourcesFit               = "SlurmResourcesFit"
	reasonSlurmResourcesExceedAllocatable = "SlurmResourcesExceedAllocatable"
	reasonSlurmResourcesVerificationError = "SlurmResourcesVerificationError"
)

func (r *NodeReconciler) setSlurmGRESCompatibilityCondition(
	ctx context.Context,
	node *corev1.Node,
	status corev1.ConditionStatus,
	reason string,
	message string,
) error {
	return r.patchNodeCondition(ctx, node, wellknown.NodeConditionSlurmGRESCompatible, status, reason, message)
}

func (r *NodeReconciler) patchNodeCondition(
	ctx context.Context,
	node *corev1.Node,
	conditionType corev1.NodeConditionType,
	status corev1.ConditionStatus,
	reason string,
	message string,
) error {
	condition := findNodeCondition(node.Status.Conditions, conditionType)
	if condition != nil && condition.Status == status && condition.Reason == reason && condition.Message == message {
		return nil
	}

	now := metav1.Now()
	updated := corev1.NodeCondition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
	}
	if condition != nil && condition.Status == status {
		updated.LastTransitionTime = condition.LastTransitionTime
	}
	// Conditions merge by type under a strategic merge patch, so the cached
	// node is a sufficient base: the patch only touches this condition.
	patched := node.DeepCopy()
	setNodeCondition(&patched.Status.Conditions, updated)
	if err := r.Status().Patch(ctx, patched, client.StrategicMergeFrom(node)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("patching %s condition on Kubernetes node %q: %w", conditionType, node.Name, err)
	}
	return nil
}

func (r *NodeReconciler) clearSlurmGRESCompatibilityCondition(ctx context.Context, node *corev1.Node) error {
	return r.clearNodeCondition(ctx, node, wellknown.NodeConditionSlurmGRESCompatible)
}

func (r *NodeReconciler) clearNodeCondition(ctx context.Context, node *corev1.Node, conditionType corev1.NodeConditionType) error {
	if findNodeCondition(node.Status.Conditions, conditionType) == nil {
		return nil
	}
	patched := node.DeepCopy()
	patched.Status.Conditions = slices.DeleteFunc(patched.Status.Conditions, func(c corev1.NodeCondition) bool {
		return c.Type == conditionType
	})
	if err := r.Status().Patch(ctx, patched, client.StrategicMergeFrom(node)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("clearing %s condition on Kubernetes node %q: %w", conditionType, node.Name, err)
	}
	return nil
}

func (r *NodeReconciler) recordSlurmGRESCompatibilityError(ctx context.Context, node *corev1.Node, err error) error {
	var incompatibleGRES *slurmcontrol.IncompatibleGRESConfigurationError
	if errors.As(err, &incompatibleGRES) {
		conditionErr := r.setSlurmGRESCompatibilityCondition(
			ctx,
			node,
			corev1.ConditionFalse,
			reasonIncompatibleSlurmGRES,
			err.Error(),
		)
		return errors.Join(err, conditionErr)
	}

	conditionErr := r.setSlurmGRESCompatibilityCondition(
		ctx,
		node,
		corev1.ConditionUnknown,
		reasonSlurmGRESVerificationError,
		fmt.Sprintf("Could not verify Slurm GRES compatibility: %v", err),
	)
	return errors.Join(err, conditionErr)
}

func (r *NodeReconciler) recordIncompatibleSlurmGRESError(ctx context.Context, node *corev1.Node, err error) error {
	var incompatibleGRES *slurmcontrol.IncompatibleGRESConfigurationError
	if !errors.As(err, &incompatibleGRES) {
		return err
	}
	// A labeled node that is also slurmd-registered may already carry the
	// compatibility feature from an earlier, compatible reconcile. Strip it
	// so Slurm stops placing bridge jobs on the node while it is incompatible.
	// This is a no-op for missing Slurm nodes.
	disableErr := r.slurmControl.DisableNodeGRESCompatibility(ctx, node)
	return errors.Join(r.recordSlurmGRESCompatibilityError(ctx, node, err), disableErr)
}

// syncSlurmResourcesFitCondition reports whether the CPUs and memory Slurm can
// allocate on a co-resident node fit in its Allocatable minus the requests of
// pods slurm-bridge does not schedule. Otherwise a pod can get stuck after Slurm
// allocates its job, or kubelet can evict under memory pressure.
func (r *NodeReconciler) syncSlurmResourcesFitCondition(ctx context.Context, node *corev1.Node) error {
	verificationError := func(err error) error {
		message := fmt.Sprintf("Could not verify that Slurm's CPUs and memory fit: %v", err)
		return errors.Join(err, r.patchNodeCondition(ctx, node, wellknown.NodeConditionSlurmResourcesFit,
			corev1.ConditionUnknown, reasonSlurmResourcesVerificationError, message))
	}
	slurmCPUs, slurmMemoryMB, err := r.slurmControl.GetNodeSchedulableResources(ctx, node)
	if err != nil {
		return verificationError(err)
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.MatchingFields{nodeutils.IndexFieldPodNodeName: node.Name}); err != nil {
		return verificationError(err)
	}

	availableMilliCPU := node.Status.Allocatable.Cpu().MilliValue()
	availableMemory := node.Status.Allocatable.Memory().Value()
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.SchedulerName == r.SchedulerName ||
			pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
		availableMilliCPU -= requests.Cpu().MilliValue()
		availableMemory -= requests.Memory().Value()
	}

	if int64(slurmCPUs)*1000 > availableMilliCPU || slurmMemoryMB*1024*1024 > availableMemory {
		message := fmt.Sprintf("Slurm schedules %d CPUs and %d MiB; Kubernetes has %s CPUs and %d MiB available after other pods' requests.",
			slurmCPUs, slurmMemoryMB, strconv.FormatFloat(float64(availableMilliCPU)/1000, 'f', -1, 64), availableMemory/(1024*1024))
		return r.patchNodeCondition(ctx, node, wellknown.NodeConditionSlurmResourcesFit,
			corev1.ConditionFalse, reasonSlurmResourcesExceedAllocatable, message)
	}
	return r.patchNodeCondition(ctx, node, wellknown.NodeConditionSlurmResourcesFit,
		corev1.ConditionTrue, reasonSlurmResourcesFit, "Slurm's schedulable CPUs and memory fit in Kubernetes Allocatable after other pods' requests.")
}

func findNodeCondition(conditions []corev1.NodeCondition, conditionType corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func setNodeCondition(conditions *[]corev1.NodeCondition, condition corev1.NodeCondition) {
	for i := range *conditions {
		if (*conditions)[i].Type == condition.Type {
			(*conditions)[i] = condition
			return
		}
	}
	*conditions = append(*conditions, condition)
}
