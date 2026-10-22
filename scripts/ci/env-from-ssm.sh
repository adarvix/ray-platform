#!/usr/bin/env bash
# Print `export KEY=value` lines for deploy-time settings published by the Terraform layers.
# Usage: eval "$(scripts/ci/env-from-ssm.sh)"
# The deploy job never reads Terraform state: SSM is the contract between infra and app.
set -euo pipefail
PREFIX="/${PROJECT:-corpreg}/${ENV_NAME:-dev}"
get() { aws ssm get-parameter --name "$PREFIX/$1" --query Parameter.Value --output text; }
echo "export CLUSTER_NAME=$(get eks/cluster_name)"
echo "export LAKE_BUCKET=$(get data/lake_bucket)"
echo "export GLUE_DATABASE=$(get data/glue_database)"
echo "export ECR_REPOSITORY_URL=$(get data/ecr_repository_url)"
