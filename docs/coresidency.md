# Co-residency: what this fork adds

This repo is a fork of [SlinkyProject/slurm-bridge](https://github.com/SlinkyProject/slurm-bridge).
This file is the only fork-specific documentation. The other files in `docs/` are
upstream's and describe upstream behaviour.

## Background in 30 seconds

slurm-bridge makes **Slurm the scheduler for Kubernetes pods**. For every pod
it submits a *placeholder* Slurm job. When Slurm allocates that job, the bridge
binds the pod to the node Slurm picked. On a *hybrid* node, both `slurmd`
(native Slurm jobs) and `kubelet` (pods) run.

**Upstream limitation:** a hybrid node runs either native Slurm jobs *or* bridge
pods, never both at once. They take turns.

**This fork:** lets them share a node at the same time, each on its own cores
and memory, with Slurm deciding placement for both. It is two independent
features, each behind a config flag. With both flags off, behaviour is exactly
upstream.

| Feature | Flag (Helm `schedulerConfig.*`) | Solves |
|---|---|---|
| Co-resident sharing | `nodeSharing: coResident` | Pods and native jobs on one node at once |
| Batch placeholders | `placeholder: batch` | Native jobs starting on a node before a pod has stopped |

## Feature 1: co-resident sharing

**What changes when `nodeSharing: coResident` is set:**

1. **Bridge jobs are non-exclusive by default** and are submitted *without* a
   `shared` value (upstream sends `shared=mcs`, which keeps native jobs off the
   node). With the partition set to `OverSubscribe=NO`, Slurm then packs pods
   and native jobs onto the node core by core. The annotation
   `slurmjob.slinky.slurm.net/exclusive: "true"` still asks for a whole node.
2. **MCS is turned off.** `mcsLabel` must be `""`; the scheduler refuses to
   start otherwise.
3. **Pods must have limits.** The admission webhook rejects a pod unless every
   container has a memory limit and either a CPU limit or a CPU DRA device
   (`dra.cpu`). Reason: Slurm reserves what the pod asks for, but only *limits*
   are enforced by the kernel. It also rejects the `cpu-per-task` and
   `mem-per-node` annotations, because the Slurm reservation must match the
   limits.
4. **Memory rounds up** to whole MiB when sizing the Slurm job (it used to round
   down, so a pod could slightly exceed its reservation).
5. **Startup warnings** if the partition allows oversubscription, or if
   `placeholder: batch` is not set.

**Optional extras:**

- `requireCPUDevice: true`: every container must use a CPU DRA device instead
  of a CPU limit. The DRA CPU driver then pins the pod to exactly the cores
  Slurm allocated. Without this, a CPU limit caps *how much* CPU a pod uses,
  not *which* cores, so pods can still cause noisy-neighbour effects.
- **`SlinkySlurmResourcesFit` node condition** (always on in co-resident mode):
  the node controller checks that Slurm's schedulable CPU and memory fit inside
  kubelet's Allocatable minus the requests of non-bridge pods (DaemonSets etc.).
  If not, the condition is `False` and its message gives both sides' numbers.
  Otherwise a pod can get stuck after Slurm already allocated its job.

## Feature 2: batch placeholders

**The problem (exists upstream too).** Upstream placeholders are Slurm
*external* jobs, which `slurmd` never launches. When Slurm ends one first
(`scancel`, preemption, time limit), the node becomes free *immediately*. A
native job can then start while the pod is still shutting down; the bridge
needs up to ~30 s to even notice. (If the *pod* finishes first, upstream is
already safe: the bridge cancels the job only after the pod has stopped.)

**The fix.** With `placeholder: batch`, the placeholder is an ordinary batch
job running `sleep infinity` (output to `/dev/null`, never requeued). When
Slurm ends a batch job, the node goes `COMPLETING` and runs the node epilog.
Slurm schedules nothing there until the epilog exits. The epilog
[`hack/epilog-bridge-hold.sh`](../hack/epilog-bridge-hold.sh):

1. Finds the pod's sandboxes on the node with `crictl`, by the
   `scheduler.slinky.slurm.net/slurm-jobid` label.
2. Waits until they have stopped. A placeholder whose pod has not appeared yet
   waits up to `NO_SHOW` (60 s). Non-bridge jobs exit immediately.
3. If it takes longer than `HOLD_MAX` (600 s), exits non-zero, so Slurm
   **drains the node** (the safe failure).

To keep step 3 rare, admission rejects pods with
`terminationGracePeriodSeconds` above `maxTerminationGracePeriodSeconds`
(default 300).

**Costs:** the whole node waits while a pod shuts down, not just its cores;
`slurmd` needs `crictl` and the containerd socket; the job's Slurm user must
exist on the node; if the bridge is down longer than `HOLD_MAX`, nodes drain.

## Node configuration you need

Co-resident mode only works if Slurm and kubelet agree on who owns what.

```conf
# slurm.conf
SelectTypeParameters=CR_Core_Memory     # Slurm must schedule memory
TaskPlugin=task/cgroup                  # confine native jobs to their cores
NodeName=... CpuSpecList=0-1            # reserved system cores
PartitionName=slurm-bridge ... OverSubscribe=NO
Epilog=/path/to/epilog-bridge-hold.sh   # batch placeholders only
PrologEpilogTimeout=720                 # must exceed HOLD_MAX
# cgroup.conf: ConstrainCores=yes, ConstrainRAMSpace=yes
```

Kubelet: CPU manager policy `none`; `reservedSystemCPUs` equal to Slurm's
`CpuSpecList`; `enforceNodeAllocatable: [pods]`; and size the node so the
`SlinkySlurmResourcesFit` condition is `True`.

The kind dev setup applies this through
[`hack/slurm-bridge-hybrid-coresident.yaml`](../hack/slurm-bridge-hybrid-coresident.yaml)
and [`hack/slurm-bridge-hybrid-batch.yaml`](../hack/slurm-bridge-hybrid-batch.yaml).

## What was tested

**Unit tests** (`make test`): job description per flag (no `shared`, batch
script, no requeue), memory rounding, config validation, every admission rule,
the resources-fit condition, the partition oversubscribe check, Helm chart
values.

**End-to-end in kind** (Kubernetes 1.37, Slurm 26.05, 6 hybrid nodes):

| Mode | Result |
|---|---|
| Flags off (upstream behaviour) | 115 tests, 0 failures |
| Co-resident + batch placeholders | 125 tests, 0 failures |

New e2e scenarios, in
[`test/e2e/coresidency_test.go`](../test/e2e/coresidency_test.go) and
[`test/e2e/batch_placeholder_test.go`](../test/e2e/batch_placeholder_test.go):

- A native job and a pod run on one node at once; Slurm accounts for both.
- A job that doesn't fit waits for the other to finish, in both orders.
- An exclusive pod waits for the whole node.
- A pod without a memory limit is rejected.
- Nodes report `SlinkySlurmResourcesFit` (it is `False` in kind, correctly:
  kind reserves nothing for the system).
- After `scancel`, native work waits for a pod with a slow shutdown, and for a
  pod whose image never pulls.
- A two-node placeholder holds both nodes.
- A pod that outlives `HOLD_MAX` drains its node.
- Admission rejects a grace period above the cap.

All existing upstream workloads (Jobs, JobSet, LeaderWorkerSet, PodGroups,
CompositePodGroups/hetjobs, DRA) also pass with both flags on.

Run them yourself:

```sh
make test
KIND_CLUSTER_NAME=slurm-bridge-coresident SLURM_NODE_MODE=hybrid \
  SLURM_NODE_SHARING=coResident SLURM_PLACEHOLDER=batch make kind-start test-e2e
```

## Still to test / open issues

kind can prove scheduling, accounting and the handoff, but **not real
isolation**: all kind nodes share one kernel and `slurmd` runs in a pod. On
real hybrid nodes, still to verify:

- **Isolation itself**: pods and native jobs really stay on their own cores and
  memory.
- **Reserved cores**: `CpuSpecList` matched with kubelet `reservedSystemCPUs`.
  (Couldn't be tested in kind: `CoreSpecCount` breaks kind's dynamic slurmd
  nodes.)
- **`requireCPUDevice`** with the DRA CPU driver, including init containers.

Known gaps:

- `cpu-per-task` / `mem-per-node` annotations placed on a JobSet,
  LeaderWorkerSet or PodGroup (instead of the pod) bypass the admission check.
- A containerized `slurmd` (slurm-operator NodeSets) is a BestEffort pod. Under
  memory pressure kubelet evicts it first, killing every native job on that
  node. Prefer host-installed `slurmd` on hybrid nodes.
- Without `requireCPUDevice`, pods can cause noisy-neighbour effects (cache,
  scheduling jitter) for native jobs.

Not started: an upstream PR for the sharing feature, and GPU/MIG sharing.

## How to read the code

Read in this order. Each step is small.

1. **Config flags:** `internal/config/config.go` (`NodeSharing`, `Placeholder`,
   `Validate`, `ValidateScheduler`).
2. **What gets sent to Slurm:** `buildJobDesc` and
   `sharedFromExclusiveAnnotation` in
   `internal/scheduler/plugins/slurmbridge/slurmcontrol/slurmcontrol.go`. This
   is the heart of both features (~30 lines each). Then see how the flags are
   wired in `internal/scheduler/plugins/slurmbridge/slurmbridge.go`.
3. **Admission rules:** `validateCoResidentLimits`,
   `validateCoResidentAnnotations` and the grace-period check in
   `internal/admission/admission.go`.
4. **The epilog:** `hack/epilog-bridge-hold.sh`.
5. **Sizing condition:** `syncSlurmResourcesFitCondition` in
   `internal/controller/node/node_condition.go`.
6. **Tests:** the two e2e files above. The `Assess(...)` names read as a spec.

To see each change in isolation, `git show` these commits:

| Commit | Change |
|---|---|
| `5b1b0f8` | Co-resident sharing in the scheduler |
| `fcf8757` | Admission requires limits |
| `0402f5c` | Memory rounds up |
| `acb8223` | Batch placeholders in the scheduler |
| `499100a` | Grace-period cap |
| `75857bd` | The hold epilog |
| `76f9ab8` | `SlinkySlurmResourcesFit` condition |
| `cb61aaf` | `requireCPUDevice` |
| `0e9ea07` | Warning when sharing runs without batch placeholders |
| `93f332b` | Unrelated bug fix found along the way: node controller requeue key |

Or see the whole fork at once: `git diff upstream/main...main -- ':!test'`.
