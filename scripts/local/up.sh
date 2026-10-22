#!/usr/bin/env bash
# Local platform: KubeRay operator + SeaweedFS (S3) + Iceberg REST catalog + `ray` namespace.
source "$(dirname "$0")/common.sh"

# Built first: the create-lake-bucket Job below reuses this image.
log "Pipeline image $IMAGE"
docker build -q -t "$IMAGE" "$ROOT/app" >/dev/null
if [[ "$CTX" == kind-* ]]; then kind load docker-image "$IMAGE" --name "${CTX#kind-}"; fi

log "KubeRay operator $KUBERAY_VERSION (RayCronJob feature gate on)"
h upgrade --install kuberay-operator kuberay-operator \
  --repo https://ray-project.github.io/kuberay-helm/ --version "$KUBERAY_VERSION" \
  --namespace kuberay --create-namespace --wait \
  -f "$ROOT/k8s/local/kuberay-values.yaml"

log "Lakehouse stand-ins (SeaweedFS S3 + Iceberg REST catalog)"
k apply -f "$ROOT/k8s/local/lakehouse.yaml"
k -n lakehouse-local rollout status deploy/seaweedfs --timeout=180s
k -n lakehouse-local rollout status deploy/iceberg-rest --timeout=300s
k -n lakehouse-local wait --for=condition=complete job/create-lake-bucket --timeout=180s

log "ray namespace (restricted Pod Security, SA, autoscaler RBAC)"
k apply -f "$ROOT/k8s/local/ray-namespace.yaml"
if ! k -n ray get secret ray-dashboard-token >/dev/null 2>&1; then
  k -n ray create secret generic ray-dashboard-token --from-literal=auth_token="$(openssl rand -hex 24)"
fi

log "Local platform ready. Next: make local-run"
