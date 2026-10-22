#!/usr/bin/env bash
# Submit the pipeline as a RayJob on EKS and block until it finishes.
# Needs: AWS creds (ci-deploy role), IMAGE, and the SSM-published settings.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
eval "$("$ROOT/scripts/ci/env-from-ssm.sh")"
: "${IMAGE:?}" "${AWS_REGION:?}"
RUN_ID="${RUN_ID:-ci-$(date +%Y%m%d%H%M%S)-${GIT_SHA:-manual}}"

aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION" >/dev/null
export AWS_REGION LAKE_BUCKET GLUE_DATABASE
python3 "$ROOT/k8s/render.py" --env aws --image "$IMAGE" --run-id "$RUN_ID" > rayjob.yaml
kubectl apply --server-side -f rayjob.yaml
JOB_NAME=$(python3 -c "import yaml;print(yaml.safe_load(open('rayjob.yaml'))['metadata']['name'])")
"$ROOT/scripts/ci/wait-rayjob.sh" "$JOB_NAME" ray
