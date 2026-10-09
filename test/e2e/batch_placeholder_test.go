// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/e2e-framework/pkg/types"
	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/SlinkyProject/slurm-bridge/internal/utils"
	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

const (
	slurmPlaceholderEnvironment = "SLURM_PLACEHOLDER"
	slurmPlaceholderExternal    = slurmPlaceholder("external")
	slurmPlaceholderBatch       = slurmPlaceholder("batch")
	slurmPlaceholderLabel       = "slurm-placeholder"
	// batchHoldMax is the epilog HOLD_MAX set by hack/kind.sh, and
	// batchMaxGracePeriodSeconds the admission cap set by the skaffold profile.
	batchHoldMax               = 120 * time.Second
	batchMaxGracePeriodSeconds = 60
	// batchHoldSkew allows for Slurm's whole-second StartTime, the poll interval,
	// and kubelet deleting the pod object just after its sandbox stops.
	batchHoldSkew    = 5 * time.Second
	batchHoldTimeout = 5 * time.Minute
)

type slurmPlaceholder string

func parseSlurmPlaceholder(value string) (slurmPlaceholder, error) {
	placeholder := slurmPlaceholder(value)
	switch placeholder {
	case slurmPlaceholderExternal, slurmPlaceholderBatch:
		return placeholder, nil
	default:
		return "", fmt.Errorf("%s must be one of %q or %q, got %q",
			slurmPlaceholderEnvironment, slurmPlaceholderExternal, slurmPlaceholderBatch, value)
	}
}

func parseSlurmPlaceholderFromEnvironment() (slurmPlaceholder, error) {
	value := os.Getenv(slurmPlaceholderEnvironment)
	if value == "" {
		value = string(slurmPlaceholderExternal)
	}
	return parseSlurmPlaceholder(value)
}

// slurmNodeReason returns the Reason of a one-line `scontrol show node`,
// without the trailing "[user@time]".
func slurmNodeReason(output string) string {
	_, reason, found := strings.Cut(output, " Reason=")
	if !found {
		return ""
	}
	reason, _, _ = strings.Cut(reason, " [")
	return strings.TrimSpace(reason)
}

// slowStopPod takes its full grace period to stop: busybox sh ignores SIGTERM
// as PID 1, so kubelet kills it only when the grace period ends.
func slowStopPod(name, partition string) *corev1.Pod {
	pod := slurmTestPod(slurmBridgeNamespace, name, []string{"sh", "-c", "while true; do sleep 1; done"})
	pod.Annotations = map[string]string{
		wellknown.AnnotationJobName:   name,
		wellknown.AnnotationPartition: partition,
	}
	pod.Spec.TerminationGracePeriodSeconds = ptr.To[int64](batchMaxGracePeriodSeconds)
	pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{
		Exec: &corev1.ExecAction{Command: []string{"sleep", "45"}},
	}}
	return pod
}

func testBatchPlaceholderHold(placeholder slurmPlaceholder) types.Feature {
	const featureName = "Batch placeholder hold"
	return features.New(featureName).
		WithLabel(slurmNodeModeLabel, string(slurmNodeModeHybrid)).
		WithLabel(slurmPlaceholderLabel, string(slurmPlaceholderBatch)).
		Setup(func(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
			if placeholder != slurmPlaceholderBatch {
				t.Skipf("requires %s=%s", slurmPlaceholderEnvironment, slurmPlaceholderBatch)
			}
			return ctx
		}).
		Assess("native work waits for a slow pod shutdown", func(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
			h := newBatchHoldTest(ctx, t, config)
			defer h.captureDiagnostics(featureName)
			partition := h.createPartition(ctx, "hold-slow", h.idleNodes(ctx, 1))
			pod := h.createBridgePods(ctx, slowStopPod(envconf.RandomName("hold-slow", 40), partition))[0]
			pod = h.waitForPod(ctx, pod, func(p *corev1.Pod) bool {
				return p.Status.Phase == corev1.PodRunning && podHasSlurmAllocation(p)
			})
			h.assertHeldUntilPodsGone(ctx, pod.Labels[slurmJobIDLabel], pod)
			return ctx
		}).
		Assess("native work waits for a pod whose image never pulls", func(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
			h := newBatchHoldTest(ctx, t, config)
			defer h.captureDiagnostics(featureName)
			partition := h.createPartition(ctx, "hold-pull", h.idleNodes(ctx, 1))
			name := envconf.RandomName("hold-pull", 40)
			pod := slurmTestPod(slurmBridgeNamespace, name, []string{"true"})
			pod.Annotations = map[string]string{wellknown.AnnotationPartition: partition}
			pod.Spec.Containers[0].Image = "registry.invalid/slurm-bridge-e2e/never:1"
			pod = h.createBridgePods(ctx, pod)[0]
			// A pull error means kubelet has created the pod's sandbox.
			pod = h.waitForPod(ctx, pod, func(p *corev1.Pod) bool {
				for _, status := range p.Status.ContainerStatuses {
					if waiting := status.State.Waiting; waiting != nil &&
						(waiting.Reason == "ErrImagePull" || waiting.Reason == "ImagePullBackOff") {
						return podHasSlurmAllocation(p)
					}
				}
				return false
			})
			h.assertHeldUntilPodsGone(ctx, pod.Labels[slurmJobIDLabel], pod)
			return ctx
		}).
		Assess("both nodes of a two-node placeholder wait for their own pod", func(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
			h := newBatchHoldTest(ctx, t, config)
			defer h.captureDiagnostics(featureName)
			partition := h.createPartition(ctx, "hold-gang", h.idleNodes(ctx, 2))
			name := envconf.RandomName("hold-gang", 40)
			podGroup := &schedv1alpha1.PodGroup{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: slurmBridgeNamespace},
				Spec: schedv1alpha1.PodGroupSpec{
					MinMember: 2,
					MinResources: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(testCPU),
						corev1.ResourceMemory: resource.MustParse(testMemory),
					},
				},
			}
			if err := h.crClient.Create(ctx, podGroup); err != nil {
				t.Fatalf("create PodGroup: %v", err)
			}
			t.Cleanup(func() {
				if e2eCleanupEnabled(t) {
					deleteObject(t, context.Background(), h.crClient, podGroup)
				}
			})
			pods := []*corev1.Pod{slowStopPod(name+"-0", partition), slowStopPod(name+"-1", partition)}
			for _, pod := range pods {
				pod.Labels = map[string]string{schedv1alpha1.PodGroupLabel: name}
			}
			pods = h.createBridgePods(ctx, pods...)
			for i := range pods {
				pods[i] = h.waitForPod(ctx, pods[i], func(p *corev1.Pod) bool {
					return p.Status.Phase == corev1.PodRunning && podHasSlurmAllocation(p)
				})
			}
			jobID := pods[0].Labels[slurmJobIDLabel]
			if pods[1].Labels[slurmJobIDLabel] != jobID || pods[0].Spec.NodeName == pods[1].Spec.NodeName {
				t.Fatalf("PodGroup pods do not share one two-node Slurm job: %s on %s, %s on %s",
					jobID, pods[0].Spec.NodeName, pods[1].Labels[slurmJobIDLabel], pods[1].Spec.NodeName)
			}
			h.assertHeldUntilPodsGone(ctx, jobID, pods...)
			return ctx
		}).
		Assess("the epilog drains the node when a pod outlives HOLD_MAX", func(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
			h := newBatchHoldTest(ctx, t, config)
			defer h.captureDiagnostics(featureName)
			node := h.idleNodes(ctx, 1)[0]
			partition := h.createPartition(ctx, "hold-drain", []string{node})
			// A placeholder made by hand, so that no bridge deletes the pod for it.
			jobID := h.sbatch(ctx, "--partition="+partition, "--nodelist="+node,
				"--constraint="+wellknown.SlurmFeatureGRESCompatible, "--no-requeue",
				"--chdir=/tmp", "--output=/dev/null", "--wrap=exec sleep infinity")
			if err := h.waitForJobState(ctx, jobID, "RUNNING"); err != nil {
				t.Fatalf("hand-made placeholder did not start: %v", err)
			}
			// The pod is not managed by the bridge, ignores SIGTERM, and carries
			// the job ID label that the epilog looks for.
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      envconf.RandomName("hold-drain", 40),
					Namespace: metav1.NamespaceDefault,
					Labels:    map[string]string{slurmJobIDLabel: jobID},
				},
				Spec: corev1.PodSpec{
					NodeName:                      node,
					RestartPolicy:                 corev1.RestartPolicyNever,
					TerminationGracePeriodSeconds: ptr.To[int64](600),
					Tolerations:                   []corev1.Toleration{*utils.NewTolerationNodeBridged(slurmBridgeScheduler)},
					Containers: []corev1.Container{{
						Name:    "worker",
						Image:   testContainerImage,
						Command: []string{"sh", "-c", "trap '' TERM; while true; do sleep 1; done"},
					}},
				},
			}
			if err := h.crClient.Create(ctx, pod); err != nil {
				t.Fatalf("create unmanaged pod: %v", err)
			}
			t.Cleanup(func() {
				if !e2eCleanupEnabled(t) {
					return
				}
				cleanupCtx, cancel := context.WithTimeout(context.Background(), slurmCleanupTimeout)
				defer cancel()
				if err := h.crClient.Delete(cleanupCtx, pod, client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
					t.Errorf("force-delete pod %s: %v", pod.Name, err)
				}
				state, err := h.nodeState(cleanupCtx, node)
				if err != nil {
					t.Errorf("query node %s: %v", node, err)
					return
				}
				if strings.Contains(state, "DRAIN") {
					if _, err := execInPod(cleanupCtx, config, h.controller,
						"scontrol", "update", "nodename="+node, "state=resume"); err != nil {
						t.Errorf("resume node %s: %v", node, err)
					}
				}
			})
			h.waitForPod(ctx, pod, func(p *corev1.Pod) bool { return p.Status.Phase == corev1.PodRunning })
			if _, err := execInPod(ctx, config, h.controller, "scancel", jobID); err != nil {
				t.Fatalf("cancel hand-made placeholder: %v", err)
			}
			cancelledAt := time.Now()
			if err := h.crClient.Delete(ctx, pod); err != nil {
				t.Fatalf("delete unmanaged pod: %v", err)
			}
			wantReason := "epilog-bridge-hold: job " + jobID + ":"
			var output string
			if err := wait.For(func(ctx context.Context) (bool, error) {
				var err error
				output, err = execInPod(ctx, config, h.controller, "scontrol", "show", "node", node, "--oneliner")
				if err != nil {
					return false, err
				}
				return strings.Contains(slurmNodeStates(output)[node], "DRAIN") &&
					strings.HasPrefix(slurmNodeReason(output), wantReason), nil
			}, wait.WithContext(ctx), wait.WithTimeout(batchHoldMax+time.Minute), wait.WithInterval(2*time.Second)); err != nil {
				t.Fatalf("node %s was not drained with reason %q: %v; last observation: %s", node, wantReason, err, output)
			}
			t.Logf("node %s drained %s after scancel: %s", node, time.Since(cancelledAt).Round(time.Second), slurmNodeReason(output))
			return ctx
		}).
		Assess("admission rejects a grace period above the cap", func(ctx context.Context, t *testing.T, config *envconf.Config) context.Context {
			crClient, err := getControllerRuntimeClient(config)
			if err != nil {
				t.Fatal(err)
			}
			pod := slurmTestPod(slurmBridgeNamespace, envconf.RandomName("hold-grace", 40), []string{"true"})
			pod.Spec.TerminationGracePeriodSeconds = ptr.To[int64](batchMaxGracePeriodSeconds + 1)
			err = crClient.Create(ctx, pod)
			if err == nil {
				deleteObject(t, ctx, crClient, pod)
				t.Fatalf("admission accepted terminationGracePeriodSeconds=%d", batchMaxGracePeriodSeconds+1)
			}
			if !strings.Contains(err.Error(), "terminationGracePeriodSeconds must not exceed "+strconv.Itoa(batchMaxGracePeriodSeconds)) {
				t.Fatalf("unexpected admission error: %v", err)
			}
			return ctx
		}).
		Feature()
}

// batchHoldTest holds what each batch placeholder assessment needs. Its
// helpers register their own cleanup with t.Cleanup.
type batchHoldTest struct {
	t          *testing.T
	config     *envconf.Config
	crClient   client.Client
	controller *corev1.Pod
}

func newBatchHoldTest(ctx context.Context, t *testing.T, config *envconf.Config) *batchHoldTest {
	t.Helper()
	crClient, err := getControllerRuntimeClient(config)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := getSlurmControllerPod(ctx, crClient)
	if err != nil {
		t.Fatal(err)
	}
	return &batchHoldTest{t: t, config: config, crClient: crClient, controller: controller}
}

// captureDiagnostics is deferred by each assessment so that it runs before
// the cleanups remove the evidence.
func (h *batchHoldTest) captureDiagnostics(featureName string) {
	captureReleaseSignalDiagnostics(h.t, featureName, slurmBridgeNamespace, slurmNamespace, slinkyNamespace)
}

// idleNodes returns count idle nodes of the bridge partition.
func (h *batchHoldTest) idleNodes(ctx context.Context, count int) []string {
	h.t.Helper()
	output, err := execInPod(ctx, h.config, h.controller, "sinfo", "--noheader", "--Node",
		"--partition="+slurmBridgePartition, "--states=idle", "--format=%N")
	if err != nil {
		h.t.Fatalf("list idle nodes: %v", err)
	}
	nodes := slices.Compact(slices.Sorted(slices.Values(strings.Fields(output))))
	if len(nodes) < count {
		h.t.Fatalf("found %d idle nodes in partition %s, want %d: %v", len(nodes), slurmBridgePartition, count, nodes)
	}
	return nodes[:count]
}

// createPartition pins jobs to nodes; the bridge scheduler does not honor
// Kubernetes node selectors.
func (h *batchHoldTest) createPartition(ctx context.Context, prefix string, nodes []string) string {
	h.t.Helper()
	name := envconf.RandomName(prefix, 30)
	if _, err := execInPod(ctx, h.config, h.controller, "scontrol", "create",
		"PartitionName="+name, "Nodes="+strings.Join(nodes, ","), "Default=NO", "State=UP"); err != nil {
		h.t.Fatalf("create test partition: %v", err)
	}
	h.t.Cleanup(func() {
		if !e2eCleanupEnabled(h.t) {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), slurmCleanupTimeout)
		defer cancel()
		if _, err := execInPod(cleanupCtx, h.config, h.controller, "scontrol", "delete", "PartitionName="+name); err != nil {
			h.t.Errorf("delete test partition: %v", err)
		}
	})
	return name
}

func (h *batchHoldTest) createBridgePods(ctx context.Context, pods ...*corev1.Pod) []*corev1.Pod {
	h.t.Helper()
	for _, pod := range pods {
		if err := h.crClient.Create(ctx, pod); err != nil {
			h.t.Fatalf("create pod %s: %v", pod.Name, err)
		}
	}
	h.t.Cleanup(func() {
		if !e2eCleanupEnabled(h.t) {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), slurmCleanupTimeout)
		defer cancel()
		deletePodsAndAssertCleanup(cleanupCtx, h.t, h.config, h.crClient, pods...)
	})
	return pods
}

func (h *batchHoldTest) waitForPod(ctx context.Context, pod *corev1.Pod, predicate func(*corev1.Pod) bool) *corev1.Pod {
	h.t.Helper()
	observed, err := waitForPod(ctx, h.crClient, client.ObjectKeyFromObject(pod), predicate)
	if err != nil {
		h.t.Fatalf("pod %s did not become ready for the test: %v", pod.Name, err)
	}
	return observed
}

// sbatch submits a native job and cancels it at cleanup. Finished jobs make
// scancel fail, so that is only logged.
func (h *batchHoldTest) sbatch(ctx context.Context, args ...string) string {
	h.t.Helper()
	output, err := execInPod(ctx, h.config, h.controller, append([]string{"sbatch", "--parsable"}, args...)...)
	if err != nil {
		h.t.Fatalf("submit native job: %v", err)
	}
	jobID, _, _ := strings.Cut(strings.TrimSpace(output), ";")
	if jobID == "" {
		h.t.Fatalf("sbatch returned no job ID: %q", output)
	}
	h.t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), slurmCleanupTimeout)
		defer cancel()
		if _, err := execInPod(cleanupCtx, h.config, h.controller,
			"scancel", jobID); err != nil {
			h.t.Logf("cancel native job %s: %v", jobID, err)
		}
	})
	return jobID
}

func (h *batchHoldTest) jobState(ctx context.Context, jobID string) (string, error) {
	output, err := querySlurmJob(ctx, h.config, h.crClient, jobID)
	if err != nil {
		return "", err
	}
	return slurmJobField(output, "JobState")
}

func (h *batchHoldTest) waitForJobState(ctx context.Context, jobID, want string) error {
	return wait.For(func(ctx context.Context) (bool, error) {
		state, err := h.jobState(ctx, jobID)
		return state == want, err
	}, wait.WithContext(ctx), wait.WithTimeout(slurmBridgeReadinessTimeout), wait.WithInterval(time.Second))
}

func (h *batchHoldTest) nodeState(ctx context.Context, node string) (string, error) {
	output, err := execInPod(ctx, h.config, h.controller, "scontrol", "show", "node", node, "--oneliner")
	if err != nil {
		return "", err
	}
	return slurmNodeStates(output)[node], nil
}

// jobStartTime converts a job's StartTime, which Slurm prints in
// slurmctld's local time zone, to a time.Time.
func (h *batchHoldTest) jobStartTime(ctx context.Context, output string) (time.Time, error) {
	start, err := slurmJobField(output, "StartTime")
	if err != nil {
		return time.Time{}, err
	}
	epoch, err := execInPod(ctx, h.config, h.controller, "date", "--date="+start, "+%s")
	if err != nil {
		return time.Time{}, err
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(epoch), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse StartTime %s: %w", start, err)
	}
	return time.Unix(seconds, 0), nil
}

// assertHeldUntilPodsGone queues a native job on each pod's node, cancels the
// placeholder, and checks that each node stays COMPLETING, with its native
// job pending, until that node's pod is gone.
func (h *batchHoldTest) assertHeldUntilPodsGone(ctx context.Context, jobID string, pods ...*corev1.Pod) {
	t := h.t
	t.Helper()
	natives := map[string]string{}
	for _, pod := range pods {
		node := pod.Spec.NodeName
		natives[node] = h.sbatch(ctx, "--job-name="+pod.Name+"-native", "--partition="+slurmBridgePartition,
			"--nodelist="+node, "--nodes=1", "--ntasks=1", "--cpus-per-task=1", "--mem=100M",
			"--time=5", "--chdir=/tmp", "--output=/dev/null", "--wrap=true")
		// The placeholder allocates the node exclusively.
		if state, err := h.jobState(ctx, natives[node]); err != nil || state != "PENDING" {
			t.Fatalf("native job %s on %s is %q, want PENDING behind placeholder %s: %v", natives[node], node, state, jobID, err)
		}
	}

	if _, err := execInPod(ctx, h.config, h.controller, "scancel", jobID); err != nil {
		t.Fatalf("cancel placeholder %s: %v", jobID, err)
	}
	cancelledAt := time.Now()
	podGone := map[string]time.Time{}
	held := map[string]bool{}
	started := map[string]bool{}
	if err := wait.For(func(ctx context.Context) (bool, error) {
		for _, pod := range pods {
			node := pod.Spec.NodeName
			if _, gone := podGone[node]; !gone {
				err := h.crClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
				if apierrors.IsNotFound(err) {
					podGone[node] = time.Now()
					t.Logf("pod %s on %s gone %s after scancel", pod.Name, node, time.Since(cancelledAt).Round(time.Second))
				} else if err != nil {
					return false, err
				}
			}
			if _, gone := podGone[node]; !gone && !held[node] {
				state, err := h.nodeState(ctx, node)
				if err != nil {
					return false, err
				}
				if strings.Contains(state, "COMPLETING") {
					held[node] = true
					t.Logf("node %s is %s while pod %s still exists", node, state, pod.Name)
				}
			}
			if started[node] {
				continue
			}
			state, err := h.jobState(ctx, natives[node])
			if err != nil {
				return false, err
			}
			switch state {
			case "PENDING":
			case "RUNNING", "COMPLETING", "COMPLETED":
				started[node] = true
				t.Logf("native job %s on %s is %s %s after scancel", natives[node], node, state, time.Since(cancelledAt).Round(time.Second))
			default:
				return false, fmt.Errorf("native job %s on %s reached %s", natives[node], node, state)
			}
		}
		return len(podGone) == len(pods) && len(started) == len(pods), nil
	}, wait.WithContext(ctx), wait.WithTimeout(batchHoldTimeout), wait.WithInterval(time.Second)); err != nil {
		t.Fatalf("placeholder %s: pods gone on %v, native jobs started on %v: %v", jobID, podGone, started, err)
	}

	for _, pod := range pods {
		node := pod.Spec.NodeName
		if !held[node] {
			t.Errorf("node %s was never COMPLETING while pod %s existed", node, pod.Name)
		}
		var output string
		if err := wait.For(func(ctx context.Context) (bool, error) {
			var err error
			output, err = querySlurmJob(ctx, h.config, h.crClient, natives[node])
			if err != nil {
				return false, err
			}
			state, err := slurmJobField(output, "JobState")
			return state == "COMPLETED", err
		}, wait.WithContext(ctx), wait.WithTimeout(slurmBridgeReadinessTimeout), wait.WithInterval(time.Second)); err != nil {
			t.Errorf("native job %s did not complete: %v; last observation: %s", natives[node], err, output)
			continue
		}
		if ran, err := slurmJobNodeList(output); err != nil || ran != node {
			t.Errorf("native job %s ran on %q, want %q: %v", natives[node], ran, node, err)
		}
		start, err := h.jobStartTime(ctx, output)
		if err != nil {
			t.Errorf("native job %s start time: %v", natives[node], err)
			continue
		}
		if start.Before(podGone[node].Add(-batchHoldSkew)) {
			t.Errorf("native job %s started on %s at %s, before pod %s was gone at %s",
				natives[node], node, start.Format(time.TimeOnly), pod.Name, podGone[node].Format(time.TimeOnly))
		}
	}
}

func TestParseSlurmPlaceholder(t *testing.T) {
	t.Setenv(slurmPlaceholderEnvironment, "")
	if got, err := parseSlurmPlaceholderFromEnvironment(); err != nil || got != slurmPlaceholderExternal {
		t.Fatalf("parseSlurmPlaceholderFromEnvironment() = (%q, %v), want %q", got, err, slurmPlaceholderExternal)
	}
	t.Setenv(slurmPlaceholderEnvironment, "batch")
	if got, err := parseSlurmPlaceholderFromEnvironment(); err != nil || got != slurmPlaceholderBatch {
		t.Fatalf("parseSlurmPlaceholderFromEnvironment() = (%q, %v), want %q", got, err, slurmPlaceholderBatch)
	}
	t.Setenv(slurmPlaceholderEnvironment, "Batch")
	if _, err := parseSlurmPlaceholderFromEnvironment(); err == nil {
		t.Fatal("parseSlurmPlaceholderFromEnvironment() accepted an invalid placeholder")
	}
}

func TestSlurmNodeReason(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		output string
		want   string
	}{
		{
			output: "NodeName=w4 State=IDLE+DRAIN Reason=epilog-bridge-hold: job 46: 1 pod sandbox(es) still ready after 20s [root@2026-10-09T09:41:06] Comment=x",
			want:   "epilog-bridge-hold: job 46: 1 pod sandbox(es) still ready after 20s",
		},
		{
			output: "NodeName=w4 State=IDLE+DRAIN Reason=Epilog error [slurm@2026-10-09T09:37:09]",
			want:   "Epilog error",
		},
		{
			output: "NodeName=w4 State=IDLE",
			want:   "",
		},
	} {
		if got := slurmNodeReason(tt.output); got != tt.want {
			t.Errorf("slurmNodeReason(%q) = %q, want %q", tt.output, got, tt.want)
		}
	}
}
