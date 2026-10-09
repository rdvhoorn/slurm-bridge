#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
# SPDX-License-Identifier: Apache-2.0

# Slurm node epilog for slurm-bridge batch placeholders (placeholder: batch).
# Keeps the node COMPLETING until the pod sandboxes of the ending job have
# stopped on this node, so native work can't start while the pod shuts down.
# Exits non-zero, which makes Slurm drain the node, if that takes longer than
# HOLD_MAX seconds or if the container runtime can't be queried.
#
# Slurm runs epilogs with a minimal environment, so change a setting by editing
# its default below or by adding an export line after the shebang.

set -euo pipefail

CRICTL="${CRICTL:-crictl}"
SCONTROL="${SCONTROL:-scontrol}"
CONTAINER_RUNTIME_ENDPOINT="${CONTAINER_RUNTIME_ENDPOINT:-unix:///run/containerd/containerd.sock}"
NO_SHOW="${NO_SHOW:-60}"
HOLD_MAX="${HOLD_MAX:-600}"
INTERVAL="${INTERVAL:-2}"
LABEL="scheduler.slinky.slurm.net/slurm-jobid=${SLURM_JOB_ID:?}"
FEATURE="slurm_bridge_gres_compatible"
DEADLINE=$((SECONDS + HOLD_MAX))

log() {
	echo "epilog-bridge-hold: job $SLURM_JOB_ID: $*" >&2
}

# slurmd discards epilog output, so record the reason in the drain reason.
# Slurm keeps it when it drains the node for the failed epilog.
fail() {
	log "$*, leaving the node to be drained"
	"$SCONTROL" update nodename="${SLURMD_NODENAME:-}" state=drain \
		reason="epilog-bridge-hold: job $SLURM_JOB_ID: $*" || true
	exit 1
}

# The bridge adds FEATURE to the constraints of every job it submits, possibly
# combined with others (e.g. "slurm_bridge_gres_compatible&gpu").
is_placeholder() {
	grep -Eq "(^|[][&|,()*!])$FEATURE(\$|[][&|,()*!])" <<<"${SLURM_JOB_CONSTRAINTS:-}"
}

# count sets n to the number of this job's sandboxes on this node, filtered by
# any extra crictl arguments.
count() {
	local ids
	ids="$("$CRICTL" --config /dev/null --runtime-endpoint "$CONTAINER_RUNTIME_ENDPOINT" pods --quiet --label "$LABEL" "$@")" ||
		fail "crictl pods failed"
	n=$(wc -w <<<"$ids")
}

count
if ((n == 0)); then
	is_placeholder || exit 0
	# Slurm can end the job before kubelet has created the sandbox.
	log "waiting up to ${NO_SHOW}s for a pod sandbox"
	no_show=$((SECONDS + NO_SHOW))
	while ((n == 0)); do
		if ((SECONDS >= no_show)); then
			log "no pod sandbox appeared, releasing the node"
			exit 0
		fi
		sleep "$INTERVAL"
		count
	done
fi

# Kubelet only stops a sandbox once all of its containers are gone.
log "waiting up to ${HOLD_MAX}s for $n pod sandbox(es) to stop"
count --state ready
while ((n > 0)); do
	((SECONDS < DEADLINE)) || fail "$n pod sandbox(es) still ready after ${HOLD_MAX}s"
	sleep "$INTERVAL"
	count --state ready
done
log "pod sandboxes stopped, releasing the node"
