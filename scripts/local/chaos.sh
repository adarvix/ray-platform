#!/usr/bin/env bash
# Simulated Spot reclaim: kill the CPU worker pod while Ray Train is running, then require the
# RayJob to still SUCCEED (Ray Train restarts the worker group and resumes from checkpoint).
source "$(dirname "$0")/common.sh"
RUN_ID="${RUN_ID:-chaos-$(date +%H%M%S)}"
JOB="corpreg-$RUN_ID"

docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$ROOT/app" >/dev/null
log "Submitting $JOB"
python3 "$ROOT/k8s/render.py" --env local --image "$IMAGE" --run-id "$RUN_ID" | k apply -f -

log "Waiting for Ray Train to start and write its first checkpoint"
# Poll tightly: locally the whole training phase lasts ~15s and the cluster is deleted as soon
# as the job succeeds, so a coarse poll can miss the window entirely.
for _ in $(seq 1 900); do
  if k -n ray logs "job/$JOB" 2>/dev/null | grep -q "Checkpoint successfully created"; then break; fi
  sleep 1
done
VICTIM=$(k -n ray get pods -l "ray.io/group=cpu-workers,ray.io/cluster" -o name | head -1)
[[ -z "$VICTIM" ]] && { echo "no cpu worker pod found"; exit 1; }
log "Reclaiming $VICTIM (force delete, no graceful drain - worse than a real Spot notice)"
k -n ray delete "$VICTIM" --grace-period=0 --force

"$ROOT/scripts/ci/wait-rayjob.sh" "$JOB" ray "$CTX"
log "Evidence that training survived the reclaim:"
k -n ray logs "job/$JOB" 2>/dev/null | grep -E "Error count|Attempting to start training worker group|resuming training from checkpoint" \
  | sed -E 's/\x1b\[[0-9;]*m//g' | cut -c1-200 | tail -8
