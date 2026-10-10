# Spike: delayed release in kind (M2)

Run on 2026-10-09 against `kind-slurm-bridge-hybrid` (upstream bridge,
hybrid mode, the M1 baseline cluster). No Go code. All times are UTC.

- Slurm 26.05.4 (slurmctld, slurmd, slurmrestd image
  `ghcr.io/slinkyproject/slurmrestd:26.05-ubuntu26.04`, data_parser
  v0.0.44); slurm chart `slurm-1.3.0-rc1` from slurm-operator `main`
  (`32e2184a`).
- crictl 1.36.0 and containerd 2.3.4 on the kind nodes.
- Scratch files (overlays, scripts):
  `/tmp/claude-1000/-home-robin-Documents-projects-slurm-bridge/ff07e853-79be-4173-8fd9-a38faa33988d/scratchpad/spike/`.

Shorthands used below:

```sh
K="kubectl --context kind-slurm-bridge-hybrid"
X="$K -n slurm exec slurm-controller-0 -c slurmctld --"   # runs as slurm(401)
W="$K -n slurm exec <slurmd pod on worker4> -c slurmd --"  # runs as root
```

Single-node tests use worker4 through a temporary partition
(`scontrol create PartitionName=spike-w4 Nodes=slurm-bridge-hybrid-worker4`).
The native job is always
`sbatch --partition=spike-w4 --nodelist=<node> --exclusive --wrap='sleep 300'`.

## Summary

| # | Question | Verdict |
|---|----------|---------|
| 1 | Gap on upstream | Confirmed. The native job ran 62 s alongside the pod. |
| 2 | Placeholder user | `slurm(401)`. It exists in the slurmd image, and batch jobs launch as it. |
| 3 | Epilog plumbing | Works. crictl and the socket mount fine, and every setting goes through the chart. The epilog runs as root with no `PATH`, and its stderr is discarded. |
| 4 | Single-node hold | Works with the real script. The node stays COMPLETING until the sandbox stops. NO_SHOW and the non-placeholder path behave as designed. |
| 5 | Two-node hold | Both nodes hold independently, **with or without `PrologFlags=Alloc`**. |
| 6 | Drain on timeout | Works. The drain reason is only `Epilog error`. |
| 7 | slurmrestd batch body | Launches. `environment` is required (error 2127). `mcs_label` is accepted under mcs/none. |
| 8 | Node sizing | Slurm and kubelet both see 8 CPUs and 23907 MiB. Nothing is reserved on either side. |

## 1. Gap on upstream

Setup: a bridge pod `q1-gap` in `slurm-bridge`. busybox, 1 CPU/100Mi
(requests = limits), `preStop: sleep 45`, `terminationGracePeriodSeconds:
60`, command `sh -c 'while true; do sleep 1; done'`. Its partition
annotation points at `spike-w4`, so it was placed on worker4 as job 27
(`EXTERNAL_JOB`, `Exclusive=NODE`, `MCS_label=kubernetes`, `Requeue=1`).

```sh
$X scancel 27
$X sbatch --parsable --partition=slurm-bridge \
  --nodelist=slurm-bridge-hybrid-worker4 --exclusive ... --wrap='sleep 300'  # -> 28
```

```
09:06:39 pod container started (job 27)
09:07:08 scancel 27; sbatch -> 28 PENDING, node idle
09:07:10 node=allocated  28 RUNNING   pod Running
09:07:12 event Killing "Stopping container worker"  (bridge deleted the pod)
09:08:12 pod Failed (container terminated at grace end)
09:08:14 pod gone
```

Verdict: **gap confirmed.** The node went idle at once, and the native job
started 2 s after `scancel`. It then ran for 62 s alongside the pod.

- The bridge deleted the pod 4 s after `scancel`. That was luck: the pod
  controller resyncs every 30 s (09:03:41, 09:04:11, … 09:07:11 in its log),
  so the worst case is about 30 s.
- busybox `sh` runs as PID 1 and ignores SIGTERM, so the pod always uses its
  full grace period (60 s here), not just the preStop (45 s). Keep this in
  mind when timing e2e tests. Use `trap 'exit 0' TERM` if a test needs the
  preStop to set the duration.

## 2. Placeholder user

```
$X scontrol show job 27 | grep UserId
   UserId=slurm(401) GroupId=slurm(401) MCS_label=kubernetes
$W getent passwd slurm
slurm:x:401:401::/home/slurm:/usr/sbin/nologin
```

The JWT in `slurm-bridge-token` carries `"sun":"slurm"`. A batch job as
`slurm` launches on slurmd:

```
$X sbatch --partition=spike-w4 --output=/tmp/q2-%j.out --wrap='id; ...'
JobState=COMPLETED ExitCode=0:0
uid=401(slurm) gid=401(slurm) groups=401(slurm)
```

Verdict: **OK.** The placeholder runs as `slurm`, which exists in the slinky
slurmd image. A nologin shell doesn't matter, because the job's `/bin/sh`
script is run directly. Jobs that use the `user-id` annotation would need
that uid in the image or in NSS (not tested).

## 3. Epilog plumbing in kind

### crictl and the CRI socket

On the kind node, `/usr/local/bin/crictl` is a static binary (v1.36.0), and
`/run/containerd/containerd.sock` exists as a `srw-rw---- root` socket. Both
were hostPath-mounted into the slurmd container (overlay in
[Settings](#settings-for-the-co-resident-kind-values)). Adding the volumes
rolled all six slurmd DaemonSet pods in about 60 s.

```
$W crictl --runtime-endpoint unix:///run/containerd/containerd.sock pods
POD ID         CREATED  STATE  NAME                             NAMESPACE
9b3c4fa23e125  27s ago  Ready  slurm-worker-slurm-bridge-npx9q  slurm
ccb9f2c29a6e1  6m ago   Ready  dra-example-driver-kubeletplugin-6hpst ...
$W crictl ... pods --label scheduler.slinky.slurm.net/slurm-jobid=31
daecaeb371adb  Less than a second ago  Ready  q3-crictl  slurm-bridge
$W crictl ... inspectp <id> | grep -A5 labels
  "io.kubernetes.pod.name": "q3-crictl",
  "scheduler.slinky.slurm.net/slurm-jobid": "31"
```

Pod labels appear as sandbox labels, so the label filter works for real
bridge pods.

- Without `/etc/crictl.yaml`, crictl writes
  `level=warning msg="Config \"/etc/crictl.yaml\" does not exist..."` to
  stderr on every call. `--config /dev/null` silences it (verified).

`--state ready` while the pod terminates (pod `q3-crictl`, 60 s grace,
deleted at 09:13:47):

```
09:13:48 all=1 ready=1
...       (ready for the whole preStop and grace period)
09:14:50 all=1 ready=0   (containers dead, sandbox NotReady)
09:15:30 all=0 ready=0   (sandbox removed)
```

The sandbox was also `Ready` while its image failed to pull (`ErrImagePull`,
`registry.invalid/nope:1`). It went not-ready within about 1 s of the pod's
deletion.

### Epilog environment

Test epilog `epilogScripts: {00-envlog.sh: ...}` dumped `id`, `pwd` and
`env` to `/tmp/epilog-$SLURM_JOB_ID.env`. The chart renders it as
`Epilog=epilog-00-envlog.sh`. Through configless, slurmd runs it as
`/var/spool/slurmd/conf-cache/epilog-00-envlog.sh`.

```
argv0=/var/spool/slurmd/conf-cache/epilog-00-envlog.sh cwd=/var/log/slurm
uid=0(root) gid=0(root)
SLURM_JOB_CONSTRAINTS=slurm_bridge_gres_compatible
SLURM_JOB_ID=30  SLURM_JOBID=30  SLURM_JOB_USER=slurm  SLURM_JOB_UID=401
SLURM_JOB_NODELIST=slurm-bridge-hybrid-worker4  SLURMD_NODENAME=...
SLURM_JOB_PARTITION=spike-w4  SLURM_JOB_EXCLUSIVE=NO  SLURM_JOB_STDOUT=/dev/null
SLURM_SCRIPT_CONTEXT=epilog_slurmd  SLURM_CONF=/var/spool/slurmd/conf-cache/slurm.conf
(no PATH, no HOME)
```

- It runs for batch jobs, as root.
- `SLURM_JOB_CONSTRAINTS` reaches it.
- There is **no `PATH`**. bash and dash fall back to a built-in default
  (`/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin` for bash
  in this image), and the `#!/usr/bin/env bash` shebang still resolves.
  `crictl`, `wc`, `grep` and `sleep` are all found.
- **stderr is discarded.** It is not in the slurmd log. stdout appears only
  at `SlurmdDebug=debug2`, as
  `prep/script: _run_subpath_command: epilog success rc:0 output:...`.
  At the default `info` level, slurmd logs only
  `epilog for JobId=N ran for S seconds` and, on failure,
  `error: JobId=N epilog failed status=1:0`.
- The slurmd log is a FIFO (`/var/log/slurm/slurmd.log`) drained by the
  `logfile` sidecar. Don't read it with `exec`, because that blocks and
  steals lines. Use `kubectl logs -c logfile`.

### slurm.conf settings through the chart

All of these went through `controller.extraConfMap` (or `configFiles` and
the NodeSet partition `configMap`) and showed up in `scontrol show config`:

```
PrologEpilogTimeout     = 720
PrologFlags             = Alloc
SelectTypeParameters    = CR_CORE_MEMORY
TaskPlugin              = task/cgroup
PartitionName=slurm-bridge Nodes=slurm-bridge OverSubscribe=NO
```

What each kind of change restarted:

| Change | slurmctld | slurmd |
|--------|-----------|--------|
| `extraConfMap`, `configFiles`, partition `configMap`, adding or removing an `epilogScripts` key (anything that changes `slurm.conf`) | pod recreated (about 30–60 s; wait for Ready) | not restarted; reconfigures in place (`Caught SIGHUP. Triggering reconfigure`, new process) and picks up `TaskPlugin` and `cgroup.conf` |
| slurmd `volumeMounts` and `podSpec.volumes` | — | DaemonSet pods rolled |
| Content of an existing `epilogScripts` key only | **nothing restarts** | **not refreshed** |

For a content-only change, the ConfigMap reaches the controller's
`/etc/slurm` after about 40–60 s (kubelet sync). slurmd's conf-cache only
updates after `scontrol reconfigure`, about 2 s later.

- Partitions made with `scontrol create` are lost whenever slurmctld
  restarts or reconfigures. Re-create them afterwards.
- Defaults before the change: `SelectTypeParameters=CR_CORE_MEMORY` (already
  the default), `TaskPlugin=(null)`, `PrologEpilogTimeout=65534`, partition
  `OverSubscribe=NO`, and `cgroup.conf` with only `CgroupPlugin=cgroup/v2`
  and `IgnoreSystemd=yes`.
- With `--reuse-values`, an empty list (`PrologFlags: []`) removes an
  `extraConfMap` key, but `epilogScripts: {key: null}` did *not* remove a
  script.

task/cgroup enforcement check (`-c1 --mem=100M`):

```
<slurmd-pod-cgroup>/system.slice/slurmstepd.scope/<job>  memory.max=104857600 cpuset=0,4
Cpus_allowed_list: 0,4
```

The job's cgroup sits **under the slurmd pod's cgroup**
(`kubelet-kubepods-besteffort-pod<uid>.slice/...`). The slurmd pod is
BestEffort (`resources: {}` in `hack/slurm-bridge-hybrid.yaml`). See
[Other observations](#other-observations).

## 4. Hold with a hand-made placeholder

The real `hack/epilog-bridge-hold.sh` from `feat/batch-placeholder` was
installed through `epilogScripts`. For evidence only, a header after the
shebang set `export HOLD_MAX=600 NO_SHOW=60`, appended stderr to
`/tmp/epilog-hold.log` and logged start and exit. The script body was
unchanged. The chart has no env setting for epilogs, which is why the
header is needed. The final smoke run at the end of this section used the
script unmodified via `--set-file`.

Placeholder: `sbatch --partition=spike-w4 --constraint=slurm_bridge_gres_compatible --nodelist=<n> --no-requeue --output=/dev/null --wrap='exec sleep infinity'`.
This gave `BatchFlag=1 Requeue=0`. The dummy pod `q4-dummy` (namespace
`default`) was bound with `nodeName` and tolerates the bridge taint. It has
the label `scheduler.slinky.slurm.net/slurm-jobid=<job>`, `preStop: sleep
45`, and 60 s grace. To emulate the bridge, it was deleted 3 s after
`scancel`.

```
09:19:37 scancel 34
09:19:38 sbatch native -> 35
09:19:38 node=completing  34:COMPLETING  35:PENDING
09:19:41 dummy pod deleted (deletionTimestamp 09:20:41)
...      node=completing  34:COMPLETING  35:PENDING   (every 2 s)
09:20:41 node=completing
09:20:43 node=allocated   35:RUNNING     pod gone
epilog log:
09:19:38 start job=34 constraints=slurm_bridge_gres_compatible
epilog-bridge-hold: job 34: waiting up to 600s for 1 pod sandbox(es) to stop
epilog-bridge-hold: job 34: pod sandboxes stopped, releasing the node
09:20:43 exit rc=0 job=34
slurmd: 09:20:43.615 Launching batch JobId=35
```

No sandbox, placeholder constraint (job 36):

```
09:21:14 scancel 36; native 37 PENDING, node completing
epilog-bridge-hold: job 36: waiting up to 60s for a pod sandbox
epilog-bridge-hold: job 36: no pod sandbox appeared, releasing the node
09:22:15 exit rc=0
09:22:16 37 RUNNING
```

Non-placeholder native job (job 35, no constraint):
`09:21:01 start job=35 constraints=` and `09:21:01 exit rc=0`.

Final smoke run, with the unmodified script installed via `--set-file`
(defaults, `PrologFlags` unset, MCS off). The dummy pod had 30 s grace:

```
09:53:12 scancel 53; 09:53:14 pod deleted
09:53:15 completing 53:COMPLETING 54:PENDING
09:53:46 54:RUNNING
```

Verdict: **all three paths behave as designed.**

## 5. Two-node hold

The placeholder is `sbatch -N2` on a temp partition with worker4 and
worker5 (`BatchHost=worker4`). Dummy pod `q5a` is on worker4 (grace 30,
preStop 20) and `q5b` on worker5 (grace 60, preStop 45). One native job is
queued per node, and both pods are deleted 3 s after `scancel`.

With `PrologFlags=Alloc` (job 38):

```
09:22:48 scancel 38; natives 39 (w4), 40 (w5) PENDING
09:22:52 worker4=completing worker5=completing
09:23:24 worker4=planned (w4 epilog exited 09:23:22)
09:23:26 39:RUNNING  worker5 still completing
09:23:54 40:RUNNING  (w5 epilog exited 09:23:54)
```

Without `PrologFlags` (`PrologFlags = (null)`, job 41):

```
09:25:35 scancel 41; natives 42 (w4), 43 (w5) PENDING
09:25:39 worker4=completing worker5=completing
09:26:12 42:RUNNING  (w4 epilog exited 09:26:09)
09:26:42 43:RUNNING  (w5 epilog exited 09:26:40)
```

In both runs the epilog log on worker5, the node that did *not* run the
batch script, shows `start job=N`, the wait and `releasing the node`.

Verdict: **both nodes hold, each for exactly as long as its own pod takes
to stop.** On Slurm 26.05.4 the node epilog runs on every allocated node of
a batch job, even without `PrologFlags=Alloc`. Alloc is harmless (no Prolog
is configured), but not needed for the hold.

## 6. Drain on timeout

The same placeholder setup, with the script header set to
`HOLD_MAX=20` and stderr not redirected. The dummy pod had 120 s grace and
a 1 s preStop, and ignores SIGTERM.

```
09:36:47 scancel 44; 09:36:50 pod deleted
09:36:50 node: completing
09:37:10 node: drained  Reason=Epilog error [slurm@2026-10-09T09:37:09]
slurmd:    epilog for JobId=44 ran for 21 seconds
           error: JobId=44 epilog failed status=1:0
slurmctld: error: job_epilog_complete: JobId=44 epilog error on
           slurm-bridge-hybrid-worker4, draining the node
           drain_nodes: node slurm-bridge-hybrid-worker4 state set to DRAIN
job 44:    JobState=CANCELLED ExitCode=0:15
```

The node went `IDLE+DRAIN`. The script's message
(`... still ready after 20s, leaving the node to be drained`) appears
nowhere.

A variant with
`scontrol update nodename="$SLURMD_NODENAME" state=drain reason="epilog-bridge-hold: job $SLURM_JOB_ID: $*"`
in `fail()` (job 46) gave:

```
Reason=epilog-bridge-hold: job 46: 1 pod sandbox(es) still ready after 20s [root@2026-10-09T09:41:06]
```

slurmctld's own `Epilog error` drain does not overwrite an existing reason.
`scontrol` is at `/usr/bin/scontrol` in the slurmd image and works from the
epilog, which runs as root with `SLURM_CONF` set.

Verdict: **works. The drain reason is `Epilog error` unless the script sets
its own.**

## 7. Batch job through slurmrestd

`kubectl port-forward svc/slurm-restapi 16820:6820`, then
`POST /slurm/v0.0.44/job/submit` with header `X-SLURM-USER-TOKEN: <SLURM_JWT>`
from `slurm/slurm-bridge-token`. The body mirrors `buildJobDesc` under
`placeholder: batch` (script `q7body.py` in scratch):

```json
{"job": {"admin_comment": "{\"pods\":[\"default/q7-full\"]}",
  "constraints": "slurm_bridge_gres_compatible",
  "current_working_directory": "/tmp",
  "environment": ["PATH=/usr/sbin:/usr/bin:/sbin:/bin"],
  "mcs_label": "kubernetes", "memory_per_node": {"set": true, "number": 100},
  "name": "q7-full", "nodes": "1", "partition": "spike-w4",
  "priority": {"set": false}, "requeue": false,
  "script": "#!/bin/sh\nexec sleep infinity\n", "shared": ["none"],
  "standard_output": "/dev/null", "tasks_per_node": 1,
  "time_limit": {"set": false}}}
```

There is no `flags` field, so no `EXTERNAL_JOB`.

| Variant | MCS config | Result |
|---------|------------|--------|
| full | `mcs/label`, `ondemand,ondemandselect` | job 48 RUNNING; `BatchFlag=1 Requeue=0 Exclusive=NODE MCS_label=kubernetes StdOut=/dev/null`; slurmd runs `slurmstepd: [48.batch]` and `sleep infinity` as slurm |
| no `environment` | `mcs/label` | rejected: `{"error_number": 2127, "error": "Environment is missing in job", "description": "Batch job submission failed", "source": "slurm_submit_batch_job()"}` |
| full | `mcs/none` (`MCSPlugin = (null)`) | accepted, job 50 RUNNING, `MCS_label=kubernetes` stored |
| `mcs_label: ""` | `mcs/none` | accepted (job 51) |
| no `mcs_label` | `mcs/none` | accepted (job 52) |

A real bridge pod (external job) under mcs/none was also scheduled normally
(job 55, `MCS_label=kubernetes`).

Verdict: **the batch body launches.** `environment` is required. Under
mcs/none, `mcs_label` is stored but not enforced, and never rejected.

## 8. Node sizing (report only)

```
$X scontrol show node slurm-bridge-hybrid-worker4
CPUTot=8 CPUEfctv=8 CoresPerSocket=4 Sockets=1 ThreadsPerCore=2
RealMemory=23907 CfgTRES=cpu=8,mem=23907M
(no MemSpecLimit, CoreSpecCount or CPUSpecList fields: unset)
slurmd: CPUSpecList=(null)
$K get node slurm-bridge-hybrid-worker4 -o jsonpath=...
capacity:    cpu=8 memory=24480924Ki (= 23907 MiB) pods=110
allocatable: cpu=8 memory=24480924Ki
```

Every kind worker reports the whole host (8 CPUs, 23907 MiB). Nothing is
reserved on either side, so allocatable equals capacity, and Slurm equals
kubelet exactly. With six hybrid nodes, the host is overcommitted sixfold.
The sizing check in §4.3 would pass trivially here, unless the e2e values
set `CpuSpecList`/`MemSpecLimit` and kubelet reservations explicitly.

## Other observations

- **The slurmd pod is BestEffort, and native jobs are charged to it.** The
  job cgroups nest under the slurmd pod's cgroup in
  `kubepods-besteffort`. Under node memory pressure, kubelet ranks
  BestEffort pods that exceed their requests first, so the slurmd pod would
  be evicted first and every native job on the node killed with it. §4.3's
  sizing argument should cover this: give slurmd requests, or state the
  risk.
- `scancel` of a placeholder whose pod has no sandbox adds `NO_SHOW` (60 s)
  of COMPLETING. The same happens to bridge jobs that end before kubelet
  starts the pod.
- The upstream `EXTERNAL_JOB` placeholder has `Requeue=1`, which has no
  effect on an external job.
- For e2e: changing only the epilog's content (for example, shortening
  `HOLD_MAX`) needs: helm upgrade, then waiting until
  `/etc/slurm/epilog-bridge-hold.sh` in slurmctld shows the new content,
  then `scontrol reconfigure`, then waiting for slurmd's conf-cache to
  match. Re-create any temp partitions after that.

## Settings for the co-resident kind values

`hack/slurm-bridge-coresident.yaml`, layered after
`slurm-bridge-common.yaml` and `slurm-bridge-hybrid.yaml`. It was rendered
with all three files (`helm template`) and applied live with
`--reuse-values`. The final smoke run in §4 ran on it.

```yaml
---
# Layer after slurm-bridge-common.yaml and slurm-bridge-hybrid.yaml.
# Install the epilog with:
#   --set-file 'epilogScripts.bridge-hold\.sh=hack/epilog-bridge-hold.sh'
configFiles:
  cgroup.conf: |
    CgroupPlugin=cgroup/v2
    IgnoreSystemd=yes
    ConstrainCores=yes
    ConstrainRAMSpace=yes
controller:
  extraConfMap:
    # No MCS: an empty list drops the key set by slurm-bridge-hybrid.yaml.
    MCSPlugin: []
    MCSParameters: []
    PrologEpilogTimeout:
      - 720
    SelectTypeParameters:
      - CR_Core_Memory
    TaskPlugin:
      - task/cgroup
    # Not needed on 26.05 (see §5); harmless if kept.
    # PrologFlags:
    #   - Alloc
nodesets:
  slurm-bridge:
    partition:
      configMap:
        OverSubscribe: "NO"
    slurmd:
      volumeMounts:
        - name: crictl
          mountPath: /usr/local/bin/crictl
          readOnly: true
        - name: containerd-sock
          mountPath: /run/containerd/containerd.sock
    podSpec:
      volumes:
        - name: crictl
          hostPath:
            path: /usr/local/bin/crictl
            type: File
        - name: containerd-sock
          hostPath:
            path: /run/containerd/containerd.sock
            type: Socket
```

Rendered `slurm.conf` additions: `Epilog=epilog-bridge-hold.sh`,
`PrologEpilogTimeout=720`, `SelectTypeParameters=CR_Core_Memory`,
`TaskPlugin=task/cgroup`, `PartitionName=slurm-bridge Nodes=slurm-bridge
OverSubscribe=NO`, and no `MCS*` lines.

In `kind.sh`, add the file (and the `--set-file`) to the first
`helm upgrade` in `slurm::configure_for_bridge`. The volume settings only
take effect once the NodeSet is enabled.

For the short-`HOLD_MAX` e2e test, install a variant with the header line
right after the shebang, then push it as described in
[Other observations](#other-observations):

```sh
{ head -1 hack/epilog-bridge-hold.sh; echo 'export HOLD_MAX=20';
  tail -n +2 hack/epilog-bridge-hold.sh; } > /tmp/hold-short.sh
helm upgrade slurm <chart> -n slurm --reuse-values \
  --set-file 'epilogScripts.bridge-hold\.sh=/tmp/hold-short.sh'
```

## Changes needed

In the batch series (`feat/batch-placeholder`):

1. **`PrologFlags=Alloc` is not required.** On 26.05.4 the epilog ran on
   the non-batch node without it (§5). Drop the "makes every node … run the
   epilog" bullet and the `PrologFlags=Alloc` line from `docs/config.md`
   (Batch Placeholders), and from PLAN §4.3. Or mark it optional.
2. **The docs' wrapper advice doesn't work with the slurm-operator chart.**
   Each `epilogScripts` key becomes its own `Epilog=` line and runs on its
   own. Arbitrary `configFiles` are not shipped to slurmd, so a wrapper
   can't `exec` the real script. Document what works: insert
   `export HOLD_MAX=... NO_SHOW=...` after the shebang (verified), or have
   the script source an optional env file.
3. **The `slurm.conf` example in the docs assumes a local file.**
   `Epilog=/etc/slurm/epilog-bridge-hold.sh` doesn't match configless
   slurm-operator. There the chart renders `Epilog=epilog-bridge-hold.sh`,
   which slurmd runs from `/var/spool/slurmd/conf-cache/`. Show the chart
   route (`epilogScripts` / `--set-file`) next to the raw `slurm.conf`.
4. **The epilog's failure reason is lost.** slurmd discards stderr, and the
   drain reason is just `Epilog error`. In `fail()`, call
   `scontrol update nodename="$SLURMD_NODENAME" state=drain reason="epilog-bridge-hold: job $SLURM_JOB_ID: $*"`
   (ignoring errors) before `exit 1`. This is verified to stick (§6).
   Optionally, also log to stdout, which slurmd logs at debug2.
5. **crictl warning noise.** Pass `--config /dev/null` with
   `--runtime-endpoint` (verified), or document `/etc/crictl.yaml`.
   Cosmetic, but it shows up on every poll once stderr is captured.
6. **Docs, the NO_SHOW cost.** A placeholder that ends before its sandbox
   exists keeps the node COMPLETING for `NO_SHOW` (60 s), even when no pod
   ever comes. Add this to the costs list.
7. `buildJobDesc` needs no change. `environment` is required (error 2127,
   `Environment is missing in job`), so the existing comment is right.
   `requeue:false` gives `Requeue=0`. `mcs_label` is accepted under
   mcs/none. Optionally, omit `mcs_label` when MCS is off, so jobs don't
   carry a label that means nothing.
8. **PLAN §4.3 / §6.** Add the BestEffort-slurmd eviction point from
   [Other observations](#other-observations), and the e2e note that a
   content-only epilog change needs `scontrol reconfigure`.
