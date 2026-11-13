# Plan: native Slurm jobs and bridge Pods on one node, down to MIG instances

Baseline: upstream `main` at `60d4e94` (2026-11-11). This file lives on a side
branch of the fork and is not meant for upstream.

## Goal

A hybrid node (kubelet and `slurmd` on one host) runs native Slurm jobs and
slurm-bridge Pods at the same time. Slurm is the only allocator, and each
workload is confined to the cores, memory and devices Slurm gave it. On a GPU
split with MIG, a native job and a Pod can each hold their own MIG instances.

## Approach

No new mode. Upstream already has hybrid nodes, non-exclusive bridge jobs,
exact device identity through DRA, static MIG profiles and CPU pinning. The
goal is reached by closing the remaining gaps one pull request at a time. Each
pull request is useful to upstream on its own, leaves defaults unchanged, and
comes with tests.

Fork `main` stays identical to upstream. One branch per pull request, cut from
upstream `main`. One `integration` branch merges the in-flight branches for
testing on real nodes.

## Decisions

- **Mixing is permitted through the MCS label.** Native jobs in the mixed
  partition get the bridge's MCS label, for example through a `job_submit`
  plugin. The bridge keeps submitting `Shared=mcs`. No policy change is needed
  in the bridge.
- **Slurm-first release is handled by prevention.** Bridge jobs get no implicit
  time limit, and preemption and cancellation go through Pod deletion, so the
  Pod always stops before Slurm frees its resources. The batch placeholder
  with an epilog hold from the first attempt is kept as a later, strict option
  for sites that must also cover an administrator's `scancel`.

## Gaps

| ID | Gap                                                                                                                              |
| -- | -------------------------------------------------------------------------------------------------------------------------------- |
| G1 | Pod memory is rounded down to MiB for the Slurm job, and limits need not equal the reservation.                                  |
| G2 | Only CPU DRA pins a Pod to Slurm's cores. Other Pods float over cores Slurm gave to native jobs.                                 |
| G3 | Nothing checks that what Slurm hands out fits kubelet's allocatable after system Pods.                                           |
| G4 | On hybrid nodes only GRES names, types and counts are checked, not device order or identity. Mixed use could double-book a device. |
| G5 | The device-plugin GPU path (`nvidia.com/gpu`) lets kubelet pick the device, which is unsafe on a shared node.                    |
| G6 | When Slurm ends a bridge job first, its resources are free while the Pod still runs.                                             |
| G7 | Native jobs need the same MIG instances under the same GRES types and indices as the bridge. Nothing generates or verifies this. |
| G8 | Mixing is undocumented and untested, and kind cannot prove isolation.                                                            |

## Steps

Steps 1 to 3 are independent of each other. Step 4 needs all three. Step 5
needs 2 and 4.

### 0. Baseline

- Open an upstream issue with the goal, the gaps and this sequence.
- On stock upstream, run a native job carrying the bridge's MCS label next to a
  Pod with `exclusive: "false"` on one hybrid node. Repeat with MIG profiles on
  real hardware. Record what works and what breaks.

### 1. Exact accounting (G1, G2, G3)

| PR  | Change                                                                                           |
| --- | ------------------------------------------------------------------------------------------------ |
| 1.1 | `fix`: round Pod memory up to whole MiB                                                          |
| 1.2 | `fix`: pod-level limits for Jobs (from fork branch `fix/job-pod-level-limits`)                   |
| 1.3 | `feat(node)`: node condition for Slurm capacity fitting kubelet allocatable                      |
| 1.4 | `feat(admission)`: opt-in rule that memory limits equal the reservation and CPU is limited or pinned |
| 1.5 | `docs`: reserved cores shared by Slurm `CpuSpecList` and kubelet `reservedSystemCPUs`            |

### 2. Device identity on hybrid nodes (G4, G5, G7)

| PR  | Change                                                                                       |
| --- | -------------------------------------------------------------------------------------------- |
| 2.1 | `feat(node)`: emit the complete required `gres.conf` per node, in index order, including MIG |
| 2.2 | `feat(node)`: verify order and identity, and withhold `slurm_bridge_gres_compatible` on mismatch |
| 2.3 | `docs`: MIG on hybrid nodes, covering type naming and device files                           |
| 2.4 | `test(e2e)`: MIG-shaped devices in the kind hybrid profile                                   |
| 2.5 | `feat(admission)`: option to refuse the device-plugin GPU path                               |

2.2 is the hardest item: Slurm's REST API does not expose the device file
behind a GRES index, so it needs a node-local check or a hash of the applied
`gres.conf`. Choose after 2.1.

### 3. Safe hand-back (G6)

| PR  | Change                                                                                  |
| --- | --------------------------------------------------------------------------------------- |
| 3.1 | `feat`: route preemption and cancellation through Pod deletion; no implicit time limit  |
| 3.2 | `test(e2e)`: native work never starts on resources of a Pod that is still stopping      |

### 4. Supported mixing (G8)

| PR  | Change                                                                                           |
| --- | ------------------------------------------------------------------------------------------------ |
| 4.1 | `docs` and `test(e2e)`: recipe for native jobs and Pods on one node through the shared MCS label |
| 4.2 | `feat`: warnings for partition oversubscription, missing memory scheduling, device-plugin GPUs   |

Slurm settings the recipe requires: `SelectTypeParameters=CR_Core_Memory`,
`TaskPlugin=task/cgroup` with `ConstrainCores`, `ConstrainRAMSpace` and
`ConstrainDevices`, and partition `OverSubscribe=NO`.

### 5. MIG mixing on hardware

- Native job and Pod on different MIG instances of one GPU, in both start
  orders, with release in both orders.
- Check cores, memory and device cgroups, and that each side sees only its own
  MIG UUIDs.
- Update the upstream MIG guide and lift the "never at the same time"
  statements.

Out of scope: dynamic MIG, and changing a MIG layout without draining the node.

## To verify in step 0

- A native job can join the bridge's MCS label under `mcs/label` and share a
  node with a bridge job. Both decisions above rest on this.
- The GRES type names Slurm's NVML autodetection gives MIG instances, and
  whether device profile names can match them.
- Whether the NVML mock in kind can present MIG devices.
- Whether the old fork's fixes still apply to current upstream.

## Done means

- A native job and a Pod hold different MIG instances of one GPU on real
  hardware, each confined to its own cores, memory and devices.
- A node whose Slurm and DRA views disagree is excluded automatically.
- Upstream's docs no longer forbid simultaneous use on nodes that meet the
  stated requirements.
- The fork carries no patches that upstream does not have.
