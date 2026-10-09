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
    - [Co-resident Nodes](#co-resident-nodes)

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

Hybrid nodes share capacity between workload managers over time, unless
[co-resident mode](#co-resident-nodes) is enabled. A physical node must not run
a native Slurm user workload and a Slurm-bridge-managed Kubernetes user workload
simultaneously. System components such as kubelet, `slurmd`, CNI, device
plugins, and monitoring DaemonSets are expected exceptions.

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

Unless [co-resident mode](#co-resident-nodes) is enabled, bridge jobs receive
exclusive whole-node Slurm allocations by default, and workloads requesting
`slurmjob.slinky.slurm.net/exclusive: "false"` are always submitted with
`Shared=mcs` and the configured `schedulerConfig.mcsLabel`. This allows
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

### Co-resident Nodes

Set `schedulerConfig.nodeSharing: coResident` to let bridge pods and native
Slurm jobs run on the same hybrid node at the same time, each on its own cores
and memory. Slurm still decides placement for both. In this mode:

- Bridge jobs are non-exclusive by default and are submitted without a `shared`
  value, so Slurm packs them beside native jobs one core at a time.
  `slurmjob.slinky.slurm.net/exclusive: "true"` still requests a whole node.
- MCS is not used. The scheduler refuses to start unless
  `schedulerConfig.mcsLabel` is `""`; also remove the MCS settings from
  `slurm.conf`.
- The admission webhook requires every container to be bounded by a memory
  limit, and by a CPU limit or a request for a core-bitmap CPU DeviceClass such
  as `dra.cpu`. A pod-level limit bounds all containers. The kernel enforces
  limits, not requests, so without them a pod could use more than Slurm reserved
  for it. Guaranteed QoS pods meet this requirement.
- The webhook rejects the `cpu-per-task` and `mem-per-node` annotations, since
  the Slurm reservation must match the limits. It can't see these annotations on
  an owning workload object, such as a JobSet, LeaderWorkerSet, or PodGroup,
  rather than on the pod template; don't use them there in this mode.
- Every partition that bridge jobs use must be `OverSubscribe=NO` or
  `OverSubscribe=EXCLUSIVE`. Bridge jobs leave sharing to the partition, so with
  `YES` or `FORCE` Slurm may run bridge pods and native jobs on the same cores.
  The scheduler logs a warning at startup if its default partition allows this.

```yaml
# slurm-bridge values.yaml
schedulerConfig:
  mcsLabel: ""
  nodeSharing: coResident
```

Slurm must schedule memory, confine native jobs to their cores and memory, and
never place two jobs on the same core. Reserve system cores explicitly:

```conf
# slurm.conf
SelectTypeParameters=CR_Core_Memory
TaskPlugin=task/cgroup
NodeName=... CpuSpecList=0-1
PartitionName=slurm-bridge ... OverSubscribe=NO
```

```conf
# cgroup.conf
ConstrainCores=yes
ConstrainRAMSpace=yes
```

Configure kubelet on each hybrid node to match:

- Use CPU manager policy `none`. The `static` policy would hand pods cores that
  Slurm allocated to native jobs.
- Set `reservedSystemCPUs` to the node's Slurm `CpuSpecList`, so both sides
  reserve the same cores.
- Slurm's schedulable CPU and memory (`RealMemory` minus `MemSpecLimit`) must
  fit in kubelet Allocatable minus the requests of pods that slurm-bridge does
  not manage, such as DaemonSets, CNI, and DRA drivers. Set
  `enforceNodeAllocatable: [pods]`. Otherwise a pod can get stuck after Slurm
  allocates its job. Kubelet counts memory used by native jobs towards eviction,
  so fix a sizing mismatch rather than lowering eviction thresholds.

Co-resident mode has these limitations:

- Slurm's cgroups keep native jobs on their own cores and memory, and total CPU
  and memory are never oversubscribed. A pod's CPU limit bounds how much CPU it
  uses, not which cores it runs on, so pods can still cause noisy-neighbour
  effects, such as cache contention and scheduling jitter, for native jobs.
- When a pod finishes, slurm-bridge cancels its Slurm job only after the pod has
  stopped. When Slurm ends the job first, for example at its time limit or on
  `scancel`, Slurm can start native work on the freed cores before the pod has
  stopped.

<!-- Links -->

[job-submit]: https://slurm.schedmd.com/job_submit_plugins.html
[mcs]: https://slurm.schedmd.com/mcs.html
