#!/usr/bin/env bash
# Open the Ray dashboard of the most recent pipeline cluster through an authenticated tunnel.
#
# Access is layered (zero trust - no public endpoint exists):
#   1. IAM identity -> EKS access entry -> RBAC group corpreg:ray-users (pods/portforward in `ray` only)
#   2. kubectl port-forward rides the authenticated Kubernetes API connection
#   3. Ray token auth: the dashboard rejects requests without the token
#      (AWS: Secrets Manager, readable only by IAM principals allowed to; local: K8s Secret)
#
# Usage: scripts/dashboard.sh [aws|local]
set -euo pipefail
MODE="${1:-aws}"
if [[ "$MODE" == local ]]; then CTX_ARGS=(--context "${KUBE_CONTEXT:-orbstack}")
else CTX_ARGS=(--context "${KUBE_CONTEXT:-corpreg-${ENV_NAME:-dev}}"); fi  # never the current context
k() { kubectl ${CTX_ARGS[@]+"${CTX_ARGS[@]}"} -n ray "$@"; }

# Newest *running* head pod. (A RayJob's head Service outlives its cluster until the RayJob is
# deleted, so picking a Service can point at a cluster that no longer exists.)
HEAD=$(k get pods -l ray.io/node-type=head --field-selector=status.phase=Running \
  --sort-by=.metadata.creationTimestamp -o name | tail -1)
[[ -z "$HEAD" ]] && { echo "no running Ray cluster in namespace ray"; exit 1; }

if [[ "$MODE" == local ]]; then
  TOKEN=$(k get secret ray-dashboard-token -o jsonpath='{.data.auth_token}' | base64 -d)
else
  TOKEN=$(aws secretsmanager get-secret-value --secret-id "${PROJECT:-corpreg}-${ENV_NAME:-dev}/ray-dashboard-token" \
    --query SecretString --output text)
fi

echo "Ray dashboard: http://localhost:8265   (paste the token when prompted)"
# Don't print the token: terminals end up in scrollback, screen shares and recordings.
if command -v pbcopy >/dev/null; then printf '%s' "$TOKEN" | pbcopy; echo "token: copied to clipboard"
elif command -v xclip >/dev/null; then printf '%s' "$TOKEN" | xclip -selection clipboard; echo "token: copied to clipboard"
else echo "token: run   kubectl -n ray get secret ray-dashboard-token -o jsonpath='{.data.auth_token}' | base64 -d"; fi
echo "Grafana (AWS):  kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80"
echo "cluster: ${HEAD#pod/}"
k port-forward "$HEAD" 8265:8265
