#!/usr/bin/env bash
# Submit one pipeline run as a RayJob on the local cluster and wait for the verdict.
source "$(dirname "$0")/common.sh"
RUN_ID="${RUN_ID:-local-$(date +%Y%m%d-%H%M%S)}"

# Local clusters share the Docker image store and may garbage-collect unused images.
docker image inspect "$IMAGE" >/dev/null 2>&1 || { log "Image $IMAGE missing, rebuilding"; docker build -q -t "$IMAGE" "$ROOT/app" >/dev/null; }

log "Submitting RayJob for run_id=$RUN_ID"
python3 "$ROOT/k8s/render.py" --env local --image "$IMAGE" --run-id "$RUN_ID" | k apply -f -
"$ROOT/scripts/ci/wait-rayjob.sh" "corpreg-$RUN_ID" ray "$CTX"
