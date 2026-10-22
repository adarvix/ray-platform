#!/usr/bin/env bash
# Build the pipeline image (amd64 + arm64), scan it, push it to ECR tagged with the git SHA.
#   build-push.sh            build + scan + push   (needs AWS creds + ECR_REPOSITORY_URL)
#   build-push.sh --no-push  build + scan only     (pull requests)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SHA="${GIT_SHA:-$(git -C "$ROOT" rev-parse --short=12 HEAD)}"
TRIVY_VERSION="${TRIVY_VERSION:-0.76.1}"
TRIVY_CACHE="${TRIVY_CACHE:-$HOME/.cache/trivy}"   # persisted by CI cache: vuln + Java DBs are large
LOCAL_TAG="corpreg-pipeline:$SHA"

# 1. Build for the runner's arch and scan before anything leaves the runner.
docker build -t "$LOCAL_TAG" "$ROOT/app"
# The Ray base image is ~4 GB (incl. Java jars): the default 5m timeout is not enough.
mkdir -p "$TRIVY_CACHE"
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$TRIVY_CACHE:/root/.cache/trivy" \
  "aquasec/trivy:$TRIVY_VERSION" image --timeout 20m --skip-version-check \
  --scanners vuln --ignore-unfixed --severity CRITICAL --exit-code 1 --no-progress "$LOCAL_TAG"

[[ "${1:-}" == "--no-push" ]] && { echo "built and scanned $LOCAL_TAG (not pushed)"; exit 0; }

# 2. Multi-arch build + push: Karpenter may pick Graviton (arm64) Spot capacity.
: "${ECR_REPOSITORY_URL:?}"
REGISTRY="${ECR_REPOSITORY_URL%%/*}"
aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY"
docker buildx build --platform linux/amd64,linux/arm64 \
  --label "org.opencontainers.image.revision=$SHA" \
  -t "$ECR_REPOSITORY_URL:$SHA" --push "$ROOT/app"
echo "IMAGE=$ECR_REPOSITORY_URL:$SHA"
