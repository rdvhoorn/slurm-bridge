# Baseline e2e on unchanged upstream (M1)

Upstream `main` at `0964dd6`, run on 2026-10-09.

```sh
KIND_CLUSTER_NAME=slurm-bridge-hybrid SLURM_NODE_MODE=hybrid make kind-start
KIND_CLUSTER_NAME=slurm-bridge-hybrid SLURM_NODE_MODE=hybrid make test-e2e
```

- kind v0.33.0, Kubernetes v1.37.0, 9 workers, 6 hybrid slurmd nodes.
- Result: `DONE 105 tests, 4 skipped in 254.885s`, no failures.
- Skipped, as expected on this setup: the three `v1alpha2` PodGroup
  workloads (API not served) and `NVIDIA DRA GPU allocated to container`.
- Hybrid-only features passed: `Native Slurm batch scheduling`,
  `Hybrid MCS isolation`, `Hybrid GRES compatibility condition`.

Host prerequisites: `hack/sysctl.sh` (inotify and key limits), and Docker
access for the user running kind.
