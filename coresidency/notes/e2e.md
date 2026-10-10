# e2e results (M3–M5)

Run in kind v0.33.0, Kubernetes v1.37.0, Slurm 26.05.4, 9 workers with 6
hybrid slurmd nodes, on 2026-10-09 and 2026-10-10.

| Cluster mode | Built from | Suite | Result |
|---|---|---|---|
| upstream hybrid (flags off) | `main` before the patches | full | 105 tests, 0 failures |
| co-resident | `feat/co-resident-sharing` | full | green after giving two workloads limits |
| co-resident + sizing | `feat/co-resident-sizing` | `Co-resident sharing` | 5/5 steps |
| batch placeholder | `feat/batch-placeholder` | full | 113 tests, 0 failures |
| co-resident + batch | merged `main` | full | 118 tests; all pass after the hold-check fix below |
| upstream hybrid (flags off) | merged `main` | full | 111 tests, 0 failures |

Skips in every mode: the three `v1alpha2` PodGroup workloads (API not served)
and the NVIDIA GPU test (no `MOCK_NVML`). `Hybrid MCS isolation` skips in
co-resident mode, by design.

## Findings

- **Limits are enforced.** In co-resident mode admission rejects pods without
  limits. Two upstream e2e workloads (`Slurm-scheduled pod` and
  `Complete Kubernetes Job lifecycle`) had requests only; they now use
  `slurmTestResources`, so the full suite runs in every mode.
- **Non-exclusive by default works with every workload type.** JobSet,
  LeaderWorkerSet, PodGroup gangs and DRA features all pass in co-resident mode
  without changes.
- **The handoff check needs an exclusive native job.** In co-resident mode a
  one-CPU native job may legitimately start beside the pod. The hold check now
  queues `--exclusive` native jobs, which wait behind the placeholder and behind
  the `COMPLETING` hold in either mode.
- **Kind is undersized for co-resident mode, and the condition says so.** Kind
  nodes reserve nothing, so Slurm offers the whole host (8 CPUs, 23907 MiB)
  while Kubernetes has 7.7 CPUs and 23757 MiB left after DaemonSet requests.
  `SlinkySlurmResourcesFit` is `False` with exactly those numbers.
- **`CoreSpecCount` breaks dynamic slurmd nodes.** Adding `CoreSpecCount=1
  MemSpecLimit=1024` to the NodeSet's `extraConf` marked every node
  `INVALID_REG` ("CoreSpec differ"). Reverting needed `scontrol delete
  nodename=...` and a slurmd restart. Reserved cores have to be tested on real
  hybrid nodes with a static `slurm.conf`.
- **Partition `OverSubscribe` in slurmrestd v0.0.44:** `NO` is `jobs: 1`,
  `EXCLUSIVE` is `jobs: 0`, `YES:N` is `jobs: N`, `FORCE:N` is `jobs: N` with
  `flags: ["force"]`. The scheduler's startup warning relies on this.

## Not covered in kind

- `requireCPUDevice` with init containers and dra-driver-cpu.
- Reserved cores (`CpuSpecList`) matched with kubelet `reservedSystemCPUs`.
- Isolation itself: kind nodes share one kernel (PLAN §6).
