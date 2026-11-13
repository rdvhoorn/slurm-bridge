// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package registration

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/SlinkyProject/slurm-bridge/internal/utils/slurmjobir"
	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

const registrationEventReminder = 5 * time.Minute

type registrationEvent struct {
	eventType, reason, message string
}

// Allocation keys serialize these calls and include object UIDs and LWS replica
// identities. Expiry permits reminders without retaining completed workloads.
func (r *Reconciler) registrationEvent(key Request, eventType, reason, message string) {
	if r.eventRecorder == nil {
		return
	}
	ref := &corev1.ObjectReference{APIVersion: "v1", Kind: key.Kind, Namespace: key.Namespace, Name: key.Name, UID: key.UID}
	if key.Kind != "Pod" {
		typeMeta, err := r.typeMeta(key)
		if err != nil {
			return
		}
		ref.APIVersion = typeMeta.APIVersion
	}
	if key.ReplicaHash != "" {
		message = fmt.Sprintf("Replica %s: %s", key.ReplicaHash, message)
	}
	event := registrationEvent{eventType, reason, message}
	if previous, found := r.eventHistory.Get(key); found && previous == event {
		return
	}
	r.eventHistory.Set(key, event, registrationEventReminder)
	r.eventRecorder.Eventf(ref, eventType, reason, "%s", message)
}

// memPerNodeEvents warns on each Pod, once per submission, when a mem-per-node
// annotation was below the Pods' memory and the Slurm job reserves the Pods'
// memory instead.
func (r *Reconciler) memPerNodeEvents(ir *slurmjobir.SlurmJobIR) {
	if r.eventRecorder == nil {
		return
	}
	for _, component := range ir.Components {
		if component.MemPerNodeRaisedFrom == nil || component.JobInfo.MemPerNode == nil {
			continue
		}
		for _, pod := range component.Pods.Items {
			ref := &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}
			r.eventRecorder.Eventf(ref, corev1.EventTypeWarning, "MemPerNodeRaised",
				"%s annotation (%d MiB) is below the memory this Pod may use; the Slurm job reserves %d MiB instead",
				wellknown.AnnotationMemPerNode, *component.MemPerNodeRaisedFrom, *component.JobInfo.MemPerNode)
		}
	}
}

func (r *Reconciler) clearRegistrationEvent(key Request) {
	if r.eventHistory != nil {
		r.eventHistory.Delete(key)
	}
}

func (r *Reconciler) registrationReadEvent(key Request, err error) (reconcile.Result, error) {
	result, retryErr := registrationReadResult(err)
	if err != nil {
		if retryErr == nil {
			r.registrationEvent(key, corev1.EventTypeNormal, "WaitingForMembers", err.Error())
		} else {
			r.registrationEvent(key, corev1.EventTypeWarning, "SlurmRegistrationFailed", err.Error())
		}
	}
	return result, retryErr
}
