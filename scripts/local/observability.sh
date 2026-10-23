#!/usr/bin/env bash
# Local observability: the same kube-prometheus-stack chart as EKS plus the platform's own
# PodMonitors, alert rules and Grafana dashboard (rendered from the platform-baseline chart).
# Run after `make local-up`; then `make local-run` and watch Prometheus / Grafana.
source "$(dirname "$0")/common.sh"
KPS_VERSION="${KPS_VERSION:-92.11.0}"

log "kube-prometheus-stack $KPS_VERSION (OCI chart; can take a few minutes on first pull)"
h upgrade --install kube-prometheus-stack oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack \
  --version "$KPS_VERSION" --namespace monitoring --create-namespace \
  -f "$ROOT/k8s/local/kube-prometheus-values.yaml" --wait --timeout 15m

log "KubeRay operator metrics (ServiceMonitor), as on EKS"
h upgrade kuberay-operator kuberay-operator --repo https://ray-project.github.io/kuberay-helm/ \
  --version "$KUBERAY_VERSION" --namespace kuberay --reuse-values --set metrics.serviceMonitor.enabled=true --wait

log "Platform PodMonitors, alert rules and Grafana dashboard"
helm template platform-baseline "$ROOT/infra/50-platform/charts/platform-baseline" \
  --show-only templates/observability.yaml | k apply -f -

cat <<EOF

Prometheus: kubectl --context $CTX -n monitoring port-forward svc/kube-prometheus-stack-prometheus 9090:9090
            http://localhost:9090/targets (podMonitor/ray/*) and /alerts (RayJobFailed, ...)
Grafana:    kubectl --context $CTX -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
            user admin, password: kubectl --context $CTX -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}' | base64 -d
            dashboard "Corporate Registry - Ray platform" (Karpenter panels stay empty locally)
EOF
