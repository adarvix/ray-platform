#!/usr/bin/env bash
# Shared helpers for the local demo. Refuses to run against anything but a local cluster.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; export ROOT
export CTX="${KUBE_CONTEXT:-orbstack}"
KUBERAY_VERSION="${KUBERAY_VERSION:-1.7.1}"
IMAGE="${IMAGE:-corpreg:dev}"

case "$CTX" in
  orbstack|docker-desktop|minikube|kind-*|rancher-desktop) ;;
  *) echo "refusing to run local demo against kube context '$CTX' (not a local cluster)" >&2; exit 1 ;;
esac

k() { kubectl --context "$CTX" "$@"; }
h() { helm --kube-context "$CTX" "$@"; }
log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
