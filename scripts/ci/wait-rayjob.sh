#!/usr/bin/env bash
# Wait for a RayJob to reach a terminal state; print the driver's summary; exit non-zero on failure.
# Usage: wait-rayjob.sh <rayjob-name> <namespace> [kube-context]
set -euo pipefail
NAME="$1"; NS="$2"; CTX="${3:-}"
TIMEOUT="${RAYJOB_TIMEOUT_SECONDS:-5400}"
k() { if [[ -n "$CTX" ]]; then kubectl --context "$CTX" "$@"; else kubectl "$@"; fi; }

deadline=$(( $(date +%s) + TIMEOUT )); last=""
while :; do
  deploy=$(k -n "$NS" get rayjob "$NAME" -o jsonpath='{.status.jobDeploymentStatus}' 2>/dev/null || true)
  job=$(k -n "$NS" get rayjob "$NAME" -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)
  [[ "$deploy/$job" != "$last" ]] && echo "$(date +%T) rayjob/$NAME deployment=${deploy:-?} job=${job:-?}" && last="$deploy/$job"
  case "$deploy" in Complete|Failed|ValidationFailed) break ;; esac
  (( $(date +%s) > deadline )) && { echo "timed out after ${TIMEOUT}s"; deploy=Timeout; break; }
  sleep 10
done

# The submitter pod streams the driver's stdout (ray job logs --follow); show the run summary.
LOG_FILE=$(mktemp)
k -n "$NS" logs "job/$NAME" > "$LOG_FILE" 2>/dev/null || true
echo "---- run summary ----"
python3 "$(dirname "$0")/../summarize_run.py" "$LOG_FILE" 2>/dev/null || tail -40 "$LOG_FILE"
[[ "$deploy" == Complete && "$job" == SUCCEEDED ]] && { echo "RayJob $NAME SUCCEEDED"; exit 0; }
echo "RayJob $NAME did not succeed (deployment=$deploy job=$job)"; k -n "$NS" get events --sort-by=.lastTimestamp | tail -15
exit 1
