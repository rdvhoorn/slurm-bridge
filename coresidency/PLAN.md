# amphibian — plan

Let Kubernetes pods and native Slurm jobs run on the same node **at the same
time**, on separate resources, with Slurm deciding for both. Built as a small
patch on SchedMD Slinky
[`slurm-bridge`](https://github.com/SlinkyProject/slurm-bridge).

## 1. Goal

- Hybrid nodes run both slurmd and kubelet. Pods run natively under
  kubelet/containerd.
- slurm-bridge already makes Slurm the scheduler for bridge pods:
  - for each pod it submits a placeholder Slurm job;
  - once Slurm allocates that job, it binds the pod to the node;
  - it keeps both sides in sync.

  Slurm partitions, accounts, QOS and fair-share apply to pods too.
- What upstream lacks: pods and native jobs may only **take turns** on a node.
  amphibian lets them **share** it: a pod gets the CPUs and memory that native
  jobs aren't using
  ([#48](https://github.com/SlinkyProject/slurm-bridge/issues/48)).
- **Scope v0:** CPU and memory, Jobs and bare pods, on hybrid nodes.
- **Later:** CPU pinning, then static GPUs/MIG.
- **Out of scope:** long-running services, multiple clusters, external-mode
  nodes (no slurmd).

## 2. Why a patch on slurm-bridge

- **What blocks sharing is a policy choice.** Commit `06bd6d9` ("fix: enforce
  time-only sharing of nodes") submits non-exclusive bridge jobs with
  `shared=mcs`. That keeps native jobs off the node and makes `mcsLabel`
  mandatory. Before that commit, the two did share nodes (reported as a bug
  in [#53](https://github.com/SlinkyProject/slurm-bridge/issues/53)).
- **The hard part is enforcement, and upstream has already solved most of it**
  ([#41](https://github.com/SlinkyProject/slurm-bridge/issues/41)). Upstream
  already:
  - turns Slurm's core allocation into a DRA CPU claim, so dra-driver-cpu pins
    the pod to those cores
    ([#32](https://github.com/SlinkyProject/slurm-bridge/issues/32));
  - maps each Slurm GRES index to the matching DRA device.
- **Writing our own controller would repeat their work.** It would also mean
  finding their lifecycle bugs again: #43, #49, #16, #45, #51. And it would
  need the same delayed-release mechanism (§4.2) anyway.
- **Upstream has the handoff gap too.** When a placeholder is cancelled, its
  node can take native work before the pod has stopped. `docs/config.md`
  admits this. So our fix (§4.2) is useful upstream as well.
- **Risks:**
  - Upstream is heading toward nodes that switch between Slurm and
    Kubernetes, so future changes may assume that.
  - Minimum versions go up: Kubernetes ≥ 1.35, Slurm ≥ 25.11.
  - The batch placeholder (§4.2) moves away from upstream's `EXTERNAL_JOB`
    design, so upstream is less likely to accept that part.
- **Mitigations:**
  - two small patches, each behind its own flag;
  - e2e coverage;
  - the sharing patch can go upstream on its own.
- **When to revisit:**
  - the patches grow past about 300 lines outside tests;
  - a rebase costs more than about a day per upstream minor release;
  - upstream adds exclusivity that can't be turned off.

  If that happens, freeze on the last good tag and keep a hard fork (Apache-2.0
  allows it).

## 3. Repo layout

One repo: a fork of slurm-bridge.

- Upstream tree, plus the patch commits.
- `coresidency/`: this plan and our notes. It lives in its own directory, so
  upstream merges never conflict with it.
- Two independent patch series, kept apart from `coresidency/` commits:
  1. **sharing** (§4.1)
  2. **delayed release** (§4.2)

  An upstream PR is a cherry-pick of one series onto a clean branch.
- Track upstream by rebasing onto each release tag.

## 4. Changes

### 4.1 Sharing — flag `nodeSharing: coResident`

When it is set:

- **Submit non-exclusive bridge jobs with `Shared` left out**, not `mcs`.
  - Slurm's job `shared` field only accepts `none`, `oversubscribe`, `user`,
    `mcs` and `topo`. There is no "ok".
  - `oversubscribe` is the dangerous one: on a partition with
    `OverSubscribe=YES`, Slurm may stack jobs on the *same cores*.
  - With `Shared` left out and the partition set to `OverSubscribe=NO`,
    Slurm packs jobs onto the node one core at a time, which is what we want.
- **Make non-exclusive the default.** Upstream defaults bridge jobs to
  exclusive (whole node). `slurmjob.slinky.slurm.net/exclusive: "true"` still
  asks for a whole node.
- **Make `mcsLabel` optional.** `ValidateScheduler` no longer requires it,
  and an empty `McsLabel` is not sent. It warns if `placeholder: batch` is
  not set, since without it the handoff gap (§4.2) is open.
- **Round memory up.** The bridge sizes the Slurm job from max(request,
  limit) of the pod (`parsePodsCpuAndMemory`), and CPU already rounds up. But
  memory rounds *down* to MiB (`GetMemoryFromQuantity`), so a pod's limit can
  be slightly above what Slurm reserved.
- **Require limits.** Admission requires a memory limit, and either a CPU
  limit or `dra.cpu` (upstream rejects pods that set both). The limits are
  what the kernel enforces (CFS quota, `memory.max`). Without them a pod could
  use more than Slurm reserved for it. Guaranteed QoS is the easy way to
  satisfy this.

With the flag off, upstream behaviour is unchanged.

Where:
- `internal/config/config.go`
- `slurmcontrol.buildJobDesc` / `sharedFromExclusiveAnnotation`
- `internal/utils/slurmjobir`
- `internal/admission`

### 4.2 Delayed release — flag `placeholder: batch`

**Problem.** When a placeholder job ends, a native job must not start on that
node until the pod's containers are gone. Shutting a pod down can take a
while: grace period, preStop hooks. The pod controller also only notices that
a job has ended after up to ~30 s.

- Pod finishes or is deleted first: already safe. The bridge only cancels the
  job once every pod is terminal (`syncSlurm`).
- Slurm ends the job first (`scancel`, preemption, time limit): **not safe
  upstream.**

**Why upstream can't hold the node.** Upstream placeholders are
`EXTERNAL_JOB`s, which slurmd never launches. When one ends, slurmctld marks
its nodes idle at once and runs no epilog (`deallocate_nodes` in
`node_scheduler.c`). None of Slurm's delay settings apply:
- `KillWait` only covers processes slurmd started;
- `CompleteWait` only applies to completing jobs;
- `EpilogSlurmctld` runs after the resources are freed.

Polling faster from the bridge would only shrink the gap to seconds.

**Solution.** The placeholder becomes an ordinary batch job:
- script `exec sleep infinity`;
- output to `/dev/null`;
- `requeue: false`, so a node failure or preemption ends the job instead of
  sending it back to the queue.

When a job that slurmd launched ends, its node goes COMPLETING, and Slurm
schedules nothing on that node until the epilog exits. The epilog
(`hack/epilog-bridge-hold.sh`, part of the delayed-release series):

1. Find the pod's sandboxes with `crictl pods --label
   scheduler.slinky.slurm.net/slurm-jobid=$SLURM_JOB_ID`. The label is set
   when the job is submitted, before the sandbox exists.
2. If there are none and the job is not a placeholder, exit 0. A placeholder
   with no sandbox waits up to `NO_SHOW` (default 60 s) for one to appear,
   because Slurm can end the job before kubelet has created it.
3. Wait until none of the sandboxes is ready (`--state ready`). Kubelet only
   stops a sandbox once all its containers are dead, so this also covers
   image pulls and the gaps between init containers. It is a local check with
   no Kubernetes API calls.
4. If `HOLD_MAX` (default 10 min) passes first, exit non-zero. Slurm drains
   the node, which is the safe way to fail. `PrologEpilogTimeout` is set
   higher than `HOLD_MAX`, so a hung epilog ends the same way.

The wait lasts exactly as long as the pod takes to stop, which a fixed delay
couldn't do.

A placeholder is recognised by the `slurm_bridge_gres_compatible` constraint
that the bridge already adds to every job (`SLURM_JOB_CONSTRAINTS`). A native
job that requests it only waits `NO_SHOW` longer.

Admission rejects `terminationGracePeriodSeconds` above a configured maximum.
`HOLD_MAX` must exceed that maximum plus the ~30 s the bridge needs to notice,
so that a long grace period can't drain a node.

**Why it stays small.** The job type is set in one place (`buildJobDesc`).
Pod binding, the pod controller and the CPU/GPU mapping don't care which type
it is. The epilog is node configuration, not bridge code. The only other code
is the grace-period check.

**Costs:**
- While a pod shuts down, the whole node waits, not just the pod's cores.
- The job's user must exist on the node. If a batch launch fails, Slurm holds
  the job and drains the node.
- slurmd needs access to the CRI socket.
- If the bridge is down for longer than `HOLD_MAX`, nodes whose placeholder
  ends in that time are drained.

### 4.3 Node configuration

Documented and set in the e2e values. The only code is one check, at the end
of this section.

**Slurm:**
- `SelectTypeParameters=CR_Core_Memory`. Otherwise Slurm doesn't schedule
  memory at all.
- `TaskPlugin=task/cgroup` with `ConstrainCores=yes` and
  `ConstrainRAMSpace=yes`. This keeps native jobs on their own cores and
  memory.
- Partition `OverSubscribe=NO`.
- Every node in the bridge partition runs slurmd. A batch placeholder can't
  launch anywhere else.
- `Epilog=` the hold script, with `PrologEpilogTimeout` > `HOLD_MAX`.
- `PrologFlags=Alloc` is optional: on Slurm 26.05 every node of a
  multi-node placeholder ran the epilog without it (`notes/spike.md` §5).
- `CpuSpecList` set explicitly, so the reserved cores are known and can be
  matched on the Kubernetes side.

**Kubelet:**
- CPU manager policy `none`. The `static` policy would hand pods cores that
  Slurm gave to native jobs. dra-driver-cpu also expects `none`.
- `reservedSystemCPUs` = Slurm's `CpuSpecList`, so both sides reserve the
  same cores.
- **Sizing:** Slurm's schedulable CPU and memory (`RealMemory −
  MemSpecLimit`) must fit in kubelet Allocatable minus the requests of pods
  the bridge doesn't manage (DaemonSets, CNI, DRA drivers). Allocatable is
  capacity − kube-reserved − system-reserved − evictionHard.
  `enforceNodeAllocatable: [pods]`.
  - Kubelet's `memory.available` includes memory used by native jobs. Sized
    like this, it stays above the eviction threshold as long as those other
    pods stay within their requests.
  - Don't lower eviction thresholds instead: that hides a mismatch until the
    OOM killer shows up.
  - The bridge scheduler also runs `NodeResourcesFit`. If Kubernetes has less
    room than Slurm thinks it can give, a pod gets stuck after Slurm has
    allocated it.
- **slurmd in a pod** (slurm-operator NodeSets, kind): native jobs' cgroups
  nest under the slurmd pod, which is BestEffort. Under memory pressure
  kubelet evicts it first and every native job on the node dies with it.
  Prefer host-installed slurmd on hybrid nodes; with a containerized slurmd,
  this is an open risk (`notes/spike.md`, "Other observations").

**Check:** the node controller sets a node condition when Slurm's schedulable
CPU or memory is above Allocatable minus the requests of the other pods. It
goes next to the existing GRES-compatibility condition in
`node_condition.go`.

## 5. Isolation: what is enforced

- **Native jobs → pods:** Slurm's cgroups keep native jobs on their own cores
  and memory (§4.3).
- **Pods → native jobs, v0:** the CFS quota and memory limit cap *how much* a
  pod uses, but not *which* cores it runs on.
  - Total CPU and memory are never oversubscribed.
  - What remains is noisy-neighbour effects (cache, scheduling jitter).
    Acceptable for v0, and documented.
- **Pods → native jobs, milestone 5:** require CPU DRA.
  - Upstream turns Slurm's core bitmap into a DRA CPU claim, and
    dra-driver-cpu (`cpuDeviceMode: individual`) pins the pod to exactly
    those cores.
  - Containers without a claim (DaemonSets) still share every unclaimed CPU,
    native jobs' cores included. Check whether dra-driver-cpu can keep them
    on the reserved cores; otherwise this stays a documented noisy-neighbour
    effect.
- **Handoff:** covered in both directions.
  - When a pod ends, the bridge only cancels the job after its pods are
    terminal.
  - When Slurm ends the job, the node waits in the epilog until the pod has
    stopped (§4.2).
- **Not an isolation issue:** images are pulled only after Slurm allocates the
  job. That costs throughput, the same as in upstream's exclusive mode.
  Pre-pull images if it matters.

## 6. Testing

Use upstream's tooling. It is already agent-friendly:

```sh
SLURM_NODE_MODE=hybrid make kind-start test-e2e   # kind cluster with slurmd + kubelet on each worker
E2E_RUN='<regex>' E2E_CLEANUP=false make test-e2e  # run one feature, keep its workload for debugging
make test                                          # unit tests + envtest
```

- **`test/e2e/coresidency_test.go`**, modelled on `mcs_isolation_test.go` and
  the existing native-`sbatch` hybrid feature:
  - A native Slurm job and a bridge pod run on the same worker at the same
    time.
  - `scontrol show node` AllocTRES equals the native job plus the pod, and
    never exceeds the node's capacity.
  - A pod that doesn't fit stays pending until the native job ends, and the
    reverse.
  - `scancel` on a placeholder whose pod has a slow preStop (say 45 s), and on
    one whose pod is still pulling its image: a native job queued for that
    node starts only after the pod has stopped.
  - A two-node placeholder holds both nodes.
  - With `HOLD_MAX` shortened below a pod's grace period and a pod that
    ignores SIGTERM: the epilog exits non-zero and the node drains.
  - With the flags off, `mcs_isolation_test.go` still passes.
- **Hybrid values for co-resident mode**, next to
  `hack/slurm-bridge-hybrid.yaml`:
  - no MCS, both flags set;
  - the Slurm settings from §4.3, including the epilog;
  - a hostPath mount of the containerd socket for slurmd.
- **Unit tests:**
  - job description: no `Shared`, and a batch script without `EXTERNAL_JOB`
    and with requeue off;
  - memory round-up;
  - the admission rules (limits, grace period);
  - the sizing condition.
- **Limit of kind:** every node shares one kernel, slurmd runs in a pod, and
  the GPUs are fake. So kind proves scheduling, accounting and the handoff,
  but not real isolation. Do the enforcement checks (milestone 5) on real
  hybrid nodes with host-installed slurmd: a staging node at the site, or a
  small Vagrant/libvirt setup under `coresidency/` if no staging node is
  available.

## 7. Dependencies

Upstream's, unchanged:

- Kubernetes ≥ 1.35
- Slurm ≥ 25.11, with slurmrestd data_parser v0.0.44
- DRA feature gates
- For development: Go, Docker, kind, Helm, Skaffold

Added:

- `crictl` on hybrid nodes, for the epilog.
- For the enforcement phase: dra-driver-cpu with containerd NRI/CDI (already
  available as an upstream kind fixture).

## 8. Milestones

1. Fork slurm-bridge and move this plan into `coresidency/`. Get
   `SLURM_NODE_MODE=hybrid make kind-start test-e2e` green on unchanged
   upstream.
2. Spike in kind, with no Go code:
   - Show the gap on upstream: `scancel` a placeholder, and a native `srun`
     starts while the pod is still running.
   - Show the hold:
     - `sbatch` a `sleep infinity` job with the bridge's constraint, with an
       epilog that waits on a dummy pod;
     - `scancel` it, and check that the node stays COMPLETING and the native
       job waits;
     - do the same with a two-node job, and check that both nodes hold;
     - confirm that `SLURM_JOB_CONSTRAINTS` reaches the epilog.
   - Write `coresidency_test.go`. It should fail on upstream.
3. Sharing patch (§4.1). The sharing tests pass; with the flag off, the MCS
   test still passes.
4. Delayed-release patch (§4.2). The handoff tests pass.
5. Enforcement:
   - require CPU DRA;
   - add the sizing condition;
   - validate on real hybrid nodes.
6. Upstream PR for the sharing patch. Propose the delayed release separately.
   Update [#48](https://github.com/SlinkyProject/slurm-bridge/issues/48).
7. Static GPUs/MIG, using upstream's GRES↔DRA mapping.
