# Config

## Table of Contents

<!-- mdformat-toc start --slug=github --no-anchors --maxlevel=6 --minlevel=1 -->

- [Config](#config)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Partitions](#partitions)
  - [Nodes](#nodes)
    - [External Nodes](#external-nodes)
    - [Hybrid Nodes](#hybrid-nodes)
    - [Topology](#topology)
    - [Hybrid Workload Isolation](#hybrid-workload-isolation)
      - [Production label authorization](#production-label-authorization)
    - [Batch Placeholders](#batch-placeholders)

<!-- mdformat-toc end -->

## Overview

When using slurm-bridge, there are some configuration requirements and
considerations to be made.

## Partitions

Slurm bridge external jobs are submitted to a default partition (e.g.
`slurm-bridge`) in Slurm, or the one specified by
`slurmjob.slinky.slurm.net/partition` on the workload. Any partition where these
external jobs are submitted to should only consist of Slurm nodes that map to
Kubernetes nodes, whether they are nodes with co-located kubelet and slurmd or
Kubernetes nodes modeled as Slurm external nodes.

## Nodes

Slurm bridge supports two ways to make Kubernetes nodes available to Slurm:

- **External nodes**: `slurm-bridge` registers labeled Kubernetes nodes with
  Slurm as external Slurm nodes. No slurm-operator `Nodeset` is required for the
  bridge partition.
- **Hybrid nodes**: slurm-operator runs `slurmd` side-by-side with kubelet,
  typically using a DaemonSet-mode `Nodeset`. The `slurmd` pods register Slurm
  dynamic nodes.

In both modes, Slurm should only schedule bridge jobs to Slurm nodes that map
back to Kubernetes nodes.

### External Nodes

External nodes are Kubernetes nodes that `slurm-bridge` registers with Slurm
using `State=External`. The node controller watches for the
`scheduler.slinky.slurm.net/external-node` label and creates or removes the
matching Slurm node.

```yaml
apiVersion: v1
kind: Node
metadata:
  name: worker-1
  labels:
    scheduler.slinky.slurm.net/external-node: "true"
  annotations:
    scheduler.slinky.slurm.net/external-node-partitions: slurm-bridge
```

The `scheduler.slinky.slurm.net/external-node-partitions` annotation is a
comma-separated list of Slurm partitions. The bridge validates that each
partition exists, then registers the Slurm node with matching features. Slurm
`Nodeset` configuration can then map those features into partitions:

```conf
Nodeset=slurm-bridge Feature=slurm-bridge
PartitionName=slurm-bridge Nodes=slurm-bridge State=UP Default=NO
```

### Hybrid Nodes

Hybrid nodes run kubelet and `slurmd` on the same physical host. In this mode,
slurm-operator manages the Slurm nodes with a `Nodeset`, and bridge jobs run on
the Slurm nodes registered by those `slurmd` pods.

Hybrid nodes share capacity between workload managers over time. A physical node
must not run a native Slurm user workload and a Slurm-bridge-managed Kubernetes
user workload simultaneously. System components such as kubelet, `slurmd`, CNI,
device plugins, and monitoring DaemonSets are expected exceptions.

For example, a DaemonSet-mode `Nodeset` can place one `slurmd` pod on each
Kubernetes worker node selected for bridge scheduling:

```yaml
nodesets:
  slurm-bridge:
    enabled: true
    scalingMode: DaemonSet
    partition:
      enabled: true
    podSpec:
      nodeSelector:
        scheduler.slinky.slurm.net/slurm-bridge: worker
```

For DRA-backed devices, the bridge node controller records the stable device
inventory in the Slurm node's `Extra` field. The hybrid node's `gres.conf` must
provide matching `Name`, `Type`, and `Count` values. Other GRES entries are
allowed and remain available to native Slurm workloads. If the DRA-managed
entries do not match, the controller sets the Kubernetes node's
`SlinkySlurmGRESCompatible` condition to `False` with the required `gres.conf`
inventory. This condition reports compatibility to administrators; Slurm node
features control bridge-job placement.

The controller adds the `slurm_bridge_gres_compatible` Slurm feature to every
external node and to hybrid nodes whose GRES configuration has been verified.
Every Slurm job submitted by the bridge requires this feature. An incompatible
hybrid node therefore remains available to native Slurm jobs while bridge jobs
cannot be allocated to it. Other hybrid-node features and native job constraints
are unaffected.

### Topology

Slurm supports dynamic node topology with `topology.yaml`. The topology file
must be available to Slurm. Kubernetes nodes can be annotated with the Slurm
topology units they belong to.

```yaml
configFiles:
  topology.yaml: |
    ---
    - topology: topo-switch
      cluster_default: true
      tree:
        switches:
          - switch: sw_root
            children: s[1-2]
          - switch: s1
            nodes: slurm-node[1-2]
          - switch: s2
            nodes: slurm-node[3-4]
    - topology: topo-block
      cluster_default: false
      block:
        block_sizes:
          - 2
          - 4
        blocks:
          - block: b1
            nodes: slurm-node[1-2]
          - block: b2
            nodes: slurm-node[3-4]
```

Annotate the Kubernetes node with the topology units for the corresponding Slurm
node:

```yaml
apiVersion: v1
kind: Node
metadata:
  name: worker-1
  annotations:
    topology.slinky.slurm.net/spec: topo-switch:s1,topo-block:b1
```

External and hybrid modes use the same Kubernetes annotation, but different
controllers apply it to Slurm:

- In external mode, `slurm-bridge` reads `topology.slinky.slurm.net/spec` from
  the labeled Kubernetes node and reconciles the external Slurm node's dynamic
  topology.
- In hybrid mode, slurm-operator reads `topology.slinky.slurm.net/spec` from the
  Kubernetes node where the `Nodeset` pod is scheduled, copies it to the pod,
  and reconciles the `slurmd` pod's Slurm node topology.

Removing the annotation clears the dynamic topology in either mode. When using
multiple topologies, Slurm partitions can select a specific topology with the
partition `Topology` setting; otherwise Slurm uses the `cluster_default`
topology.

### Hybrid Workload Isolation

Bridge jobs receive exclusive whole-node Slurm allocations by default. Workloads
requesting `slurmjob.slinky.slurm.net/exclusive: "false"` are always submitted
with `Shared=mcs` and the configured `schedulerConfig.mcsLabel`. This allows
bridge-managed Kubernetes workloads in the same MCS category to share a node
without enabling unprotected sharing with native Slurm jobs.

Configure both the bridge and Slurm to enable [MCS] isolation:

```yaml
# slurm-bridge values.yaml
schedulerConfig:
  mcsLabel: kubernetes
```

```conf
# slurm.conf
MCSPlugin=mcs/label
MCSParameters=ondemand,ondemandselect
```

With `ondemandselect`, the bridge's `Shared=mcs` value activates MCS node
filtering for non-exclusive jobs. Native Slurm jobs with a different or empty
MCS label cannot share those nodes while a `kubernetes`-labeled allocation is
running.

#### Production label authorization

Slurm's `mcs/label` plugin controls category-based sharing but accepts arbitrary
labels; it does not authorize their use. Production clusters must reserve
`schedulerConfig.mcsLabel` at the `slurmctld` boundary with a server-side
[`job_submit` plugin][job-submit] or equivalent site policy, using a trusted
bridge identity and rejecting native submissions or modifications that request
the reserved label. Without that Slurm-side policy, a native user can
deliberately join the bridge's MCS category.

MCS only governs workloads represented by active Slurm allocations. Continue to
use the `slinky.slurm.net/managed-node` `NoExecute` taint and admission policy
to keep Kubernetes workloads that bypass slurm-bridge off hybrid nodes.
Operational or controller-driven cancellation must also keep a node unavailable
to native Slurm work until its Kubernetes pods have actually stopped.

[Batch placeholders](#batch-placeholders) close this gap when Slurm ends the job
first.

### Batch Placeholders

By default, the bridge submits each pod's placeholder as an external job, which
`slurmd` never launches. When Slurm ends such a job first, for example through
`scancel`, preemption, or its time limit, its nodes become idle at once and run
no epilog. Native Slurm work can then start while the pod is still shutting
down: the bridge needs up to about 30 seconds to notice that the job ended, and
the pod's grace period and `preStop` hooks come on top of that.

With `placeholder: batch`, the bridge submits the placeholder as an ordinary
batch job instead. It runs `sleep infinity`, discards its output, and is never
requeued. When Slurm ends it, its nodes go `COMPLETING` and run the node epilog
[`hack/epilog-bridge-hold.sh`][epilog-bridge-hold], which holds each node until
the pod's sandboxes on it have stopped. Admission rejects pods whose
`terminationGracePeriodSeconds` exceeds `maxTerminationGracePeriodSeconds`
(default 300), because the epilog only holds the node for a bounded time.

```yaml
# slurm-bridge values.yaml
schedulerConfig:
  placeholder: batch
  maxTerminationGracePeriodSeconds: 300
```

Install the epilog on every node in the bridge partition, either directly in
`slurm.conf`:

```conf
# slurm.conf
Epilog=/path/to/epilog-bridge-hold.sh
PrologEpilogTimeout=720
```

or, with the slurm-operator chart, as an `epilogScripts` entry, which `slurmd`
runs from its configless cache:

```sh
helm upgrade slurm oci://ghcr.io/slinkyproject/charts/slurm --reuse-values \
  --set-file 'epilogScripts.bridge-hold\.sh=hack/epilog-bridge-hold.sh' \
  --set 'controller.extraConfMap.PrologEpilogTimeout[0]=720'
```

- The epilog finds the pod's sandboxes with `crictl`, so `slurmd` needs `crictl`
  and access to the node's CRI socket (`CONTAINER_RUNTIME_ENDPOINT`, default
  `unix:///run/containerd/containerd.sock`).
- The epilog waits at most `HOLD_MAX` seconds (default 600) and then fails,
  which drains the node. `HOLD_MAX` must exceed
  `maxTerminationGracePeriodSeconds` plus about 30 seconds, and
  `PrologEpilogTimeout` must exceed `HOLD_MAX`.
- `slurmd` discards the epilog's output, so on failure the epilog records why in
  the node's drain reason (`epilog-bridge-hold: job <id>: ...`).
- Slurm runs the epilog with a minimal environment. To change `HOLD_MAX`,
  `NO_SHOW`, `CRICTL`, `SCONTROL`, or `CONTAINER_RUNTIME_ENDPOINT`, edit the
  defaults at the top of the script, or add an `export` line after the shebang.
- The epilog recognises a placeholder by its `slurm_bridge_gres_compatible`
  constraint and waits up to `NO_SHOW` seconds (default 60) for its pod sandbox
  to appear. It releases the nodes of other jobs at once.
- `PrologFlags=Alloc` is optional. Slurm 26.05 already runs the epilog on every
  node of a multi-node placeholder without it.

This has costs:

- While a pod shuts down, the whole node waits, not only the pod's cores.
- A placeholder that ends before its pod sandbox exists keeps the node
  `COMPLETING` for `NO_SHOW` seconds, even if no pod ever starts there.
- The batch job runs as its Slurm user (`SlurmUser` unless the
  `slurmjob.slinky.slurm.net/user-id` annotation is set), who must exist on the
  node. If the launch fails, Slurm holds the job and drains the node.
- If the bridge is down for longer than `HOLD_MAX`, nodes whose placeholder ends
  in that time are drained.

<!-- Links -->

[epilog-bridge-hold]: ../hack/epilog-bridge-hold.sh
[job-submit]: https://slurm.schedmd.com/job_submit_plugins.html
[mcs]: https://slurm.schedmd.com/mcs.html
