// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/e2e-framework/pkg/types"

	bridgeconfig "github.com/SlinkyProject/slurm-bridge/internal/config"
	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

const (
	coResidentFeature = "Co-resident sharing"
	// The Slurm Bridge chart is installed into the Slurm namespace.
	slurmBridgeConfigMap = "slurm-bridge-config"
	// Every native job and pod in this feature reserves this much memory.
	coResidentMemoryMiB = 100
	// How long a job must keep its state to count as running beside, or
	// waiting for, another job.
	coResidentHoldTime = 10 * time.Second
)

// testHybridCoResidentSharing checks nodeSharing: coResident. Bridge pods and
// native Slurm jobs share a hybrid node core by core, and Slurm makes either
// one wait when the node has no room left.
func testHybridCoResidentSharing(coResident bool) types.Feature {
	return features.New(coResidentFeature).
		WithLabel(slurmNodeModeLabel, string(slurmNodeModeHybrid)).
		Setup(func(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
			if !coResident {
				t.Skipf("requires %s=%s", slurmNodeSharingEnvironment, bridgeconfig.NodeSharingCoResident)
			}
			// Fail clearly if the cluster was not created for co-resident sharing.
			crClient, err := getControllerRuntimeClient(config)
			if err != nil {
				t.Fatal(err)
			}
			configMap := &corev1.ConfigMap{}
			if err := crClient.Get(ctx, client.ObjectKey{Namespace: slurmNamespace, Name: slurmBridgeConfigMap}, configMap); err != nil {
				t.Fatalf("get Slurm Bridge config: %v", err)
			}
			bridgeConfig, err := bridgeconfig.Unmarshal([]byte(configMap.Data["config.yaml"]))
			if err != nil {
				t.Fatalf("parse Slurm Bridge config: %v", err)
			}
			if bridgeConfig.NodeSharing != bridgeconfig.NodeSharingCoResident {
				t.Fatalf("Slurm Bridge runs with nodeSharing %q; create the cluster with %s=%s",
					bridgeConfig.NodeSharing, slurmNodeSharingEnvironment, bridgeconfig.NodeSharingCoResident)
			}
			return ctx
		}).
		Assess("native job and pod run on one node at once and Slurm counts both", assessCoResidentPair).
		Assess("job that does not fit waits for the other to end, in both orders", assessCoResidentWait).
		Assess("exclusive pod waits for the whole node", assessCoResidentExclusive).
		Assess("pod without a memory limit is rejected", assessCoResidentAdmission).
		Assess("hybrid workers report whether Slurm's resources fit", assessCoResidentSizing).
		Feature()
}

func assessCoResidentPair(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
	r := newCoResidentRun(ctx, t, config, "pair")
	nativeID := r.submitNativeJob(ctx, t, 1)
	r.waitForJobRunning(ctx, t, nativeID)
	pod := r.waitForPodRunning(ctx, t, r.createPod(ctx, t, 1, nil))
	bridgeID := pod.Labels[slurmJobIDLabel]
	t.Logf("native job %s and pod %s (job %s) run on %s", nativeID, pod.Name, bridgeID, r.node.Name)

	// Slurm may round each job up to whole cores, so expect what it allocated.
	wantCPUs := r.jobCPUs(ctx, t, nativeID) + r.jobCPUs(ctx, t, bridgeID)
	wantMemory := 2 * coResidentMemoryMiB
	if err := holdsFor(ctx, func(ctx context.Context) error {
		if err := r.expectJobState(ctx, nativeID, "RUNNING"); err != nil {
			return err
		}
		if err := r.expectPodPhase(ctx, pod, corev1.PodRunning); err != nil {
			return err
		}
		node, err := r.showNode(ctx, r.node.Name)
		if err != nil {
			return err
		}
		switch {
		case node.baseState() != "MIXED":
			return fmt.Errorf("node %s is %s, want MIXED", node.Name, node.State)
		case node.AllocCPUs > node.CPUs || node.AllocMemory > node.RealMemory:
			return fmt.Errorf("node %s allocates %d CPUs and %d MiB, more than its %d CPUs and %d MiB",
				node.Name, node.AllocCPUs, node.AllocMemory, node.CPUs, node.RealMemory)
		case node.AllocCPUs != wantCPUs || node.AllocMemory != wantMemory:
			return fmt.Errorf("node %s allocates %d CPUs and %d MiB, want %d CPUs and %d MiB for native job %s and pod job %s",
				node.Name, node.AllocCPUs, node.AllocMemory, wantCPUs, wantMemory, nativeID, bridgeID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ctx
}

func assessCoResidentWait(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
	r := newCoResidentRun(ctx, t, config, "wait")
	if r.node.CPUs < 2*r.node.ThreadsPerCore {
		t.Fatalf("node %s has %d CPUs and %d threads per core; this check needs two cores",
			r.node.Name, r.node.CPUs, r.node.ThreadsPerCore)
	}

	// The pod waits: a native job leaves one core free, and the pod asks for
	// one CPU more than Slurm has left.
	nativeID := r.submitNativeJob(ctx, t, r.node.CPUs-r.node.ThreadsPerCore)
	r.waitForJobRunning(ctx, t, nativeID)
	podCPUs := r.freeCPUs(ctx, t) + 1
	pod := r.createPod(ctx, t, podCPUs, nil)
	bridgeID := pod.Labels[slurmJobIDLabel]
	if err := holdsFor(ctx, func(ctx context.Context) error {
		if err := r.expectJobState(ctx, nativeID, "RUNNING"); err != nil {
			return err
		}
		if err := r.expectJobState(ctx, bridgeID, "PENDING"); err != nil {
			return err
		}
		return r.expectPodPhase(ctx, pod, corev1.PodPending)
	}); err != nil {
		t.Fatalf("pod needing %d CPUs did not wait for native job %s: %v", podCPUs, nativeID, err)
	}
	t.Logf("pod %s (job %s, %d CPUs) waits while native job %s runs on %s", pod.Name, bridgeID, podCPUs, nativeID, r.node.Name)

	r.cancelJob(ctx, t, nativeID)
	pod = r.waitForPodRunning(ctx, t, pod)
	if err := waitForSlurmJobGone(ctx, config, r.crClient, nativeID); err != nil {
		t.Fatalf("native job %s did not end: %v", nativeID, err)
	}
	t.Logf("pod %s started on %s after native job %s ended", pod.Name, r.node.Name, nativeID)

	// The native job waits: it asks for one CPU more than the pod left free.
	nativeCPUs := r.freeCPUs(ctx, t) + 1
	nativeID = r.submitNativeJob(ctx, t, nativeCPUs)
	if err := holdsFor(ctx, func(ctx context.Context) error {
		if err := r.expectPodPhase(ctx, pod, corev1.PodRunning); err != nil {
			return err
		}
		return r.expectJobState(ctx, nativeID, "PENDING")
	}); err != nil {
		t.Fatalf("native job needing %d CPUs did not wait for pod %s: %v", nativeCPUs, pod.Name, err)
	}
	t.Logf("native job %s (%d CPUs) waits while pod %s runs on %s", nativeID, nativeCPUs, pod.Name, r.node.Name)

	if _, err := execInPod(ctx, config, pod, "touch", "/tmp/finish"); err != nil {
		t.Fatalf("finish pod %s: %v", pod.Name, err)
	}
	if _, err := waitForPod(ctx, r.crClient, client.ObjectKeyFromObject(pod), func(p *corev1.Pod) bool {
		return p.Status.Phase == corev1.PodSucceeded
	}); err != nil {
		t.Fatalf("pod %s did not finish: %v", pod.Name, err)
	}
	r.waitForJobRunning(ctx, t, nativeID)
	t.Logf("native job %s started on %s after pod %s finished", nativeID, r.node.Name, pod.Name)
	return ctx
}

func assessCoResidentExclusive(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
	r := newCoResidentRun(ctx, t, config, "exclusive")
	nativeID := r.submitNativeJob(ctx, t, 1)
	r.waitForJobRunning(ctx, t, nativeID)
	pod := r.createPod(ctx, t, 1, map[string]string{wellknown.AnnotationExclusive: "true"})
	bridgeID := pod.Labels[slurmJobIDLabel]
	if err := holdsFor(ctx, func(ctx context.Context) error {
		if err := r.expectJobState(ctx, nativeID, "RUNNING"); err != nil {
			return err
		}
		if err := r.expectJobState(ctx, bridgeID, "PENDING"); err != nil {
			return err
		}
		return r.expectPodPhase(ctx, pod, corev1.PodPending)
	}); err != nil {
		t.Fatalf("exclusive pod did not wait for native job %s: %v", nativeID, err)
	}
	t.Logf("exclusive pod %s (job %s) waits while native job %s runs on %s", pod.Name, bridgeID, nativeID, r.node.Name)

	r.cancelJob(ctx, t, nativeID)
	pod = r.waitForPodRunning(ctx, t, pod)
	node, err := r.showNode(ctx, r.node.Name)
	if err != nil {
		t.Fatal(err)
	}
	if node.AllocCPUs != node.CPUs {
		t.Fatalf("exclusive pod %s holds %d of %d CPUs on %s, want all", pod.Name, node.AllocCPUs, node.CPUs, node.Name)
	}
	t.Logf("exclusive pod %s holds all %d CPUs of %s", pod.Name, node.CPUs, node.Name)
	return ctx
}

func assessCoResidentAdmission(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
	crClient, err := getControllerRuntimeClient(config)
	if err != nil {
		t.Fatal(err)
	}
	pod := slurmTestPod(slurmBridgeNamespace, envconf.RandomName("coresident-no-memory-limit", 40),
		[]string{"sh", "-c", "sleep 300"})
	delete(pod.Spec.Containers[0].Resources.Limits, corev1.ResourceMemory)
	t.Cleanup(func() {
		captureReleaseSignalDiagnostics(t, coResidentFeature+" admission", slurmBridgeNamespace, slurmNamespace, slinkyNamespace)
		if !e2eCleanupEnabled(t) {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), slurmCleanupTimeout)
		defer cancel()
		// Only finds a pod if admission wrongly accepted it.
		deletePodAndAssertCleanup(cleanupCtx, t, config, crClient, pod)
	})

	err = crClient.Create(ctx, pod)
	if err == nil {
		t.Fatalf("pod %s without a memory limit was admitted", pod.Name)
	}
	if !strings.Contains(err.Error(), "co-resident node sharing requires a memory limit") {
		t.Fatalf("pod %s was rejected for another reason: %v", pod.Name, err)
	}
	if err := crClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("rejected pod %s exists: %v", pod.Name, err)
	}
	return ctx
}

// coResidentRun tracks what one assessment creates on its node, so that its
// cleanup removes exactly that.
type coResidentRun struct {
	name       string
	config     *envconf.Config
	crClient   client.Client
	controller *corev1.Pod
	// node is the idle worker the run uses, as Slurm saw it when picked.
	node slurmNode
	// partition holds only node, so the bridge's Slurm jobs land there.
	partition string
	pods      []*corev1.Pod
	nativeIDs []string
}

// assessCoResidentSizing checks that every hybrid worker reports whether the
// CPUs and memory Slurm can allocate fit in Kubernetes. Kind workers reserve
// nothing for the system, so the condition is usually False there; the check
// is that it is set and quotes Slurm's own numbers.
func assessCoResidentSizing(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
	crClient, err := getControllerRuntimeClient(config)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := getSlurmControllerPod(ctx, crClient)
	if err != nil {
		t.Fatal(err)
	}
	r := &coResidentRun{config: config, crClient: crClient, controller: controller}
	workers := &corev1.NodeList{}
	if err := crClient.List(ctx, workers, client.MatchingLabels{slurmBridgeWorkerLabel: "worker"}); err != nil {
		t.Fatalf("list bridge workers: %v", err)
	}
	for i := range workers.Items {
		name := workers.Items[i].Name
		var condition corev1.NodeCondition
		if err := wait.For(func(ctx context.Context) (bool, error) {
			node := &corev1.Node{}
			if err := crClient.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
				return false, err
			}
			for _, c := range node.Status.Conditions {
				if c.Type == wellknown.NodeConditionSlurmResourcesFit {
					condition = c
					return true, nil
				}
			}
			return false, nil
		}, wait.WithContext(ctx), wait.WithTimeout(slurmBridgeReadinessTimeout), wait.WithInterval(2*time.Second)); err != nil {
			t.Fatalf("node %s has no %s condition: %v", name, wellknown.NodeConditionSlurmResourcesFit, err)
		}
		slurmNode, err := r.showNode(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("node %s: %s %s: %s", name, condition.Status, condition.Reason, condition.Message)
		switch condition.Status {
		case corev1.ConditionTrue:
		case corev1.ConditionFalse:
			if want := fmt.Sprintf("Slurm schedules %d CPUs and ", slurmNode.CPUs); !strings.Contains(condition.Message, want) {
				t.Errorf("node %s condition message %q does not contain %q", name, condition.Message, want)
			}
		default:
			t.Errorf("node %s condition is %s: %s", name, condition.Status, condition.Message)
		}
	}
	return ctx
}

// newCoResidentRun picks an idle hybrid worker and registers the run's cleanup.
func newCoResidentRun(ctx context.Context, t *testing.T, config *envconf.Config, name string) *coResidentRun {
	t.Helper()
	crClient, err := getControllerRuntimeClient(config)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := getSlurmControllerPod(ctx, crClient)
	if err != nil {
		t.Fatal(err)
	}
	r := &coResidentRun{name: name, config: config, crClient: crClient, controller: controller}
	t.Cleanup(func() { r.cleanup(t) })
	r.node = r.idleNode(ctx, t)
	t.Logf("using node %s: %d CPUs, %d threads per core, %d MiB",
		r.node.Name, r.node.CPUs, r.node.ThreadsPerCore, r.node.RealMemory)
	return r
}

func (r *coResidentRun) cleanup(t *testing.T) {
	captureReleaseSignalDiagnostics(t, coResidentFeature+" "+r.name, slurmBridgeNamespace, slurmNamespace, slinkyNamespace)
	if !e2eCleanupEnabled(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), slurmCleanupTimeout)
	defer cancel()
	for _, pod := range r.pods {
		deletePodAndAssertCleanup(ctx, t, r.config, r.crClient, pod)
	}
	// scancel fails for jobs that already ended, so only cancel active ones.
	active, err := execInPod(ctx, r.config, r.controller, "squeue", "--noheader", "--format=%i")
	if err != nil {
		t.Errorf("list Slurm jobs: %v", err)
	}
	for _, jobID := range r.nativeIDs {
		if !slices.Contains(strings.Fields(active), jobID) {
			continue
		}
		if _, err := execInPod(ctx, r.config, r.controller, "scancel", jobID); err != nil {
			t.Errorf("cancel native job %s: %v", jobID, err)
		}
	}
	// Leave the node idle for the next assessment, and the partition empty.
	for _, jobID := range r.nativeIDs {
		if err := waitForSlurmJobGone(ctx, r.config, r.crClient, jobID); err != nil {
			t.Errorf("native job %s did not end: %v", jobID, err)
		}
	}
	if r.partition != "" {
		if _, err := execInPod(ctx, r.config, r.controller, "scontrol", "delete", "PartitionName="+r.partition); err != nil {
			t.Errorf("delete test partition %s: %v", r.partition, err)
		}
	}
}

// idleNode waits until a hybrid worker has no Slurm jobs and returns it.
func (r *coResidentRun) idleNode(ctx context.Context, t *testing.T) slurmNode {
	t.Helper()
	workers := &corev1.NodeList{}
	if err := r.crClient.List(ctx, workers, client.MatchingLabels{slurmBridgeWorkerLabel: "worker"}); err != nil {
		t.Fatalf("list bridge workers: %v", err)
	}
	names := make([]string, 0, len(workers.Items))
	for i := range workers.Items {
		names = append(names, workers.Items[i].Name)
	}
	slices.Sort(names)

	var idle slurmNode
	var states []string
	if err := wait.For(func(ctx context.Context) (bool, error) {
		states = states[:0]
		for _, name := range names {
			node, err := r.showNode(ctx, name)
			if err != nil {
				return false, err
			}
			if node.idle() {
				idle = node
				return true, nil
			}
			states = append(states, node.Name+"="+node.State)
		}
		return false, nil
	}, wait.WithContext(ctx), wait.WithTimeout(slurmBridgeReadinessTimeout), wait.WithInterval(2*time.Second)); err != nil {
		t.Fatalf("no idle hybrid worker: %v; last states: %v", err, states)
	}
	return idle
}

func (r *coResidentRun) showNode(ctx context.Context, name string) (slurmNode, error) {
	output, err := execInPod(ctx, r.config, r.controller, "scontrol", "show", "node", name, "--oneliner")
	if err != nil {
		return slurmNode{}, fmt.Errorf("query Slurm node %s: %w", name, err)
	}
	return parseSlurmNode(output)
}

// freeCPUs returns how many CPUs Slurm can still allocate on the run's node.
func (r *coResidentRun) freeCPUs(ctx context.Context, t *testing.T) int {
	t.Helper()
	node, err := r.showNode(ctx, r.node.Name)
	if err != nil {
		t.Fatal(err)
	}
	return node.CPUs - node.AllocCPUs
}

// submitNativeJob submits a native Slurm job that holds cpus CPUs on the run's
// node until it is canceled.
func (r *coResidentRun) submitNativeJob(ctx context.Context, t *testing.T, cpus int) string {
	t.Helper()
	output, err := execInPod(ctx, r.config, r.controller, "sbatch", "--parsable",
		"--job-name=coresident-"+r.name+"-native", "--partition="+slurmBridgePartition,
		"--nodelist="+r.node.Name, "--nodes=1", "--ntasks=1",
		"--cpus-per-task="+strconv.Itoa(cpus), fmt.Sprintf("--mem=%dM", coResidentMemoryMiB),
		"--time=5", "--chdir=/tmp", "--output=/dev/null", "--wrap=sleep 300")
	if err != nil {
		t.Fatalf("submit native job: %v", err)
	}
	jobID, _, _ := strings.Cut(strings.TrimSpace(output), ";")
	if jobID == "" {
		t.Fatal("sbatch returned no job ID")
	}
	r.nativeIDs = append(r.nativeIDs, jobID)
	return jobID
}

func (r *coResidentRun) cancelJob(ctx context.Context, t *testing.T, jobID string) {
	t.Helper()
	if _, err := execInPod(ctx, r.config, r.controller, "scancel", jobID); err != nil {
		t.Fatalf("cancel native job %s: %v", jobID, err)
	}
}

// createPod creates a bridge pod with cpus CPUs, waits for its Slurm job and
// returns the pod. The bridge scheduler doesn't follow Kubernetes node
// selectors, so the job is sent to a partition that holds only the run's node.
func (r *coResidentRun) createPod(ctx context.Context, t *testing.T, cpus int, annotations map[string]string) *corev1.Pod {
	t.Helper()
	pod := slurmTestPod(slurmBridgeNamespace, envconf.RandomName("coresident-"+r.name, 40),
		[]string{"sh", "-c", "while [ ! -e /tmp/finish ]; do sleep 1; done"})
	pod.Spec.Containers[0].Resources = slurmTestResources(strconv.Itoa(cpus), fmt.Sprintf("%dMi", coResidentMemoryMiB))
	if r.partition == "" {
		if _, err := execInPod(ctx, r.config, r.controller, "scontrol", "create",
			"PartitionName="+pod.Name, "Nodes="+r.node.Name, "Default=NO", "State=UP"); err != nil {
			t.Fatalf("create single-node test partition: %v", err)
		}
		r.partition = pod.Name
	}
	pod.Annotations = map[string]string{
		wellknown.AnnotationJobName:   pod.Name,
		wellknown.AnnotationPartition: r.partition,
	}
	maps.Copy(pod.Annotations, annotations)
	if err := r.crClient.Create(ctx, pod); err != nil {
		t.Fatalf("create pod %s: %v", pod.Name, err)
	}
	r.pods = append(r.pods, pod)
	submitted, err := waitForPod(ctx, r.crClient, client.ObjectKeyFromObject(pod), func(p *corev1.Pod) bool {
		return p.Labels[slurmJobIDLabel] != ""
	})
	if err != nil {
		t.Fatalf("bridge did not submit a Slurm job for pod %s: %v", pod.Name, err)
	}
	return submitted
}

// waitForPodRunning waits until pod runs its Slurm job on the run's node.
func (r *coResidentRun) waitForPodRunning(ctx context.Context, t *testing.T, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	running, err := waitForPod(ctx, r.crClient, client.ObjectKeyFromObject(pod), func(p *corev1.Pod) bool {
		return p.Status.Phase == corev1.PodRunning && podHasSlurmAllocation(p)
	})
	if err != nil {
		t.Fatalf("pod %s did not start: %v", pod.Name, err)
	}
	if running.Spec.NodeName != r.node.Name || running.Labels[slurmJobIDLabel] != pod.Labels[slurmJobIDLabel] {
		t.Fatalf("pod %s runs job %s on %s, want job %s on %s", pod.Name,
			running.Labels[slurmJobIDLabel], running.Spec.NodeName, pod.Labels[slurmJobIDLabel], r.node.Name)
	}
	return running
}

// waitForJobRunning waits until Slurm job jobID runs on the run's node.
func (r *coResidentRun) waitForJobRunning(ctx context.Context, t *testing.T, jobID string) {
	t.Helper()
	var output string
	if err := wait.For(func(ctx context.Context) (bool, error) {
		var err error
		output, err = querySlurmJob(ctx, r.config, r.crClient, jobID)
		if err != nil {
			return false, err
		}
		state, err := slurmJobField(output, "JobState")
		if err == nil && state != "PENDING" && state != "RUNNING" {
			return false, fmt.Errorf("Slurm job %s reached %s: %s", jobID, state, output)
		}
		return state == "RUNNING", err
	}, wait.WithContext(ctx), wait.WithTimeout(slurmBridgeReadinessTimeout), wait.WithInterval(time.Second)); err != nil {
		t.Fatalf("Slurm job %s did not start: %v", jobID, err)
	}
	if node, err := slurmJobNodeList(output); err != nil || node != r.node.Name {
		t.Fatalf("Slurm job %s runs on %q, want %q: %v", jobID, node, r.node.Name, err)
	}
}

// jobCPUs returns how many CPUs Slurm allocated to a running job.
func (r *coResidentRun) jobCPUs(ctx context.Context, t *testing.T, jobID string) int {
	t.Helper()
	output, err := querySlurmJob(ctx, r.config, r.crClient, jobID)
	if err != nil {
		t.Fatal(err)
	}
	value, err := slurmJobField(output, "NumCPUs")
	if err != nil {
		t.Fatal(err)
	}
	cpus, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("parse NumCPUs of Slurm job %s: %v", jobID, err)
	}
	return cpus
}

// expectJobState returns an error unless Slurm job jobID is in state want.
func (r *coResidentRun) expectJobState(ctx context.Context, jobID, want string) error {
	output, err := querySlurmJob(ctx, r.config, r.crClient, jobID)
	if err != nil {
		return err
	}
	if state, err := slurmJobField(output, "JobState"); err != nil || state != want {
		return fmt.Errorf("Slurm job %s is not %s: %s", jobID, want, output)
	}
	return nil
}

// expectPodPhase returns an error unless pod is in phase want. A pending pod
// must also not be bound to a node yet.
func (r *coResidentRun) expectPodPhase(ctx context.Context, pod *corev1.Pod, want corev1.PodPhase) error {
	current := &corev1.Pod{}
	if err := r.crClient.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
		return err
	}
	if current.Status.Phase != want || (want == corev1.PodPending && current.Spec.NodeName != "") {
		return fmt.Errorf("pod %s is %s on node %q, want %s", pod.Name, current.Status.Phase, current.Spec.NodeName, want)
	}
	return nil
}

// holdsFor checks condition every second and fails as soon as it returns an
// error. It succeeds once condition has held for coResidentHoldTime.
func holdsFor(ctx context.Context, condition func(context.Context) error) error {
	start := time.Now()
	return wait.For(func(ctx context.Context) (bool, error) {
		if err := condition(ctx); err != nil {
			return false, err
		}
		return time.Since(start) >= coResidentHoldTime, nil
	}, wait.WithContext(ctx), wait.WithTimeout(slurmBridgeReadinessTimeout), wait.WithInterval(time.Second))
}

// slurmNode is the part of `scontrol show node` the co-resident checks use.
// CPU counts are Slurm's, so with ThreadsPerCore above 1 they count threads.
type slurmNode struct {
	Name  string
	State string
	// CPUs is CPUEfctv: the CPUs Slurm can allocate to jobs.
	CPUs           int
	ThreadsPerCore int
	AllocCPUs      int
	// RealMemory and AllocMemory are in MiB.
	RealMemory  int
	AllocMemory int
}

func parseSlurmNode(output string) (slurmNode, error) {
	var node slurmNode
	var err error
	if node.Name, err = slurmJobField(output, "NodeName"); err != nil {
		return slurmNode{}, err
	}
	if node.State, err = slurmJobField(output, "State"); err != nil {
		return slurmNode{}, err
	}
	for name, value := range map[string]*int{
		"CPUEfctv":       &node.CPUs,
		"ThreadsPerCore": &node.ThreadsPerCore,
		"CPUAlloc":       &node.AllocCPUs,
		"RealMemory":     &node.RealMemory,
		"AllocMem":       &node.AllocMemory,
	} {
		field, err := slurmJobField(output, name)
		if err != nil {
			return slurmNode{}, err
		}
		if *value, err = strconv.Atoi(field); err != nil {
			return slurmNode{}, fmt.Errorf("parse %s of Slurm node %s: %w", name, node.Name, err)
		}
	}
	return node, nil
}

// baseState returns the node state without flags such as DYNAMIC_NORM.
func (n slurmNode) baseState() string {
	base, _, _ := strings.Cut(n.State, "+")
	return base
}

// idle reports whether the node has no jobs and can start one right away.
// Hybrid workers are dynamic nodes, so DYNAMIC_NORM is the only flag allowed.
func (n slurmNode) idle() bool {
	return n.State == "IDLE" || n.State == "IDLE+DYNAMIC_NORM"
}

func TestParseSlurmNode(t *testing.T) {
	t.Parallel()

	output := "NodeName=worker-1 Arch=x86_64 CoresPerSocket=4 CPUAlloc=3 CPUEfctv=8 CPUTot=8 " +
		"RealMemory=7900 AllocMem=200 FreeMem=6000 Sockets=1 State=MIXED+DYNAMIC_NORM ThreadsPerCore=2 " +
		"CfgTRES=cpu=8,mem=7900M,billing=8 AllocTRES=cpu=3,mem=200M\n"
	got, err := parseSlurmNode(output)
	if err != nil {
		t.Fatalf("parseSlurmNode() error = %v", err)
	}
	want := slurmNode{
		Name:           "worker-1",
		State:          "MIXED+DYNAMIC_NORM",
		CPUs:           8,
		ThreadsPerCore: 2,
		AllocCPUs:      3,
		RealMemory:     7900,
		AllocMemory:    200,
	}
	if got != want {
		t.Fatalf("parseSlurmNode() = %+v, want %+v", got, want)
	}
	if got.baseState() != "MIXED" || got.idle() {
		t.Fatalf("node in state %s: baseState() = %q, idle() = %v", got.State, got.baseState(), got.idle())
	}
	if _, err := parseSlurmNode("NodeName=worker-1 State=IDLE"); err == nil {
		t.Fatal("parseSlurmNode() accepted output without CPU counts")
	}
}
