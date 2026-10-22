#!/usr/bin/env bash
# Remove everything the local demo created (only its own namespaces and release).
source "$(dirname "$0")/common.sh"
k delete rayjobs,raycronjobs --all -n ray --ignore-not-found --wait=true || true
k delete -f "$ROOT/k8s/local/ray-namespace.yaml" --ignore-not-found
k delete -f "$ROOT/k8s/local/lakehouse.yaml" --ignore-not-found
h uninstall kuberay-operator -n kuberay || true
k delete namespace kuberay --ignore-not-found
