#!/usr/bin/env bash
# GitLab -> AWS federation: exchange the job's OIDC id_token for short-lived role credentials.
# Usage (in a job with `id_tokens: { GITLAB_OIDC_TOKEN: ... }`): eval "$(scripts/ci/gitlab-aws-oidc.sh <role-arn>)"
set -euo pipefail
ROLE_ARN="${1:?role arn}"
read -r KEY SECRET TOKEN < <(aws sts assume-role-with-web-identity \
  --role-arn "$ROLE_ARN" --role-session-name "gitlab-${CI_PROJECT_ID:-local}-${CI_PIPELINE_ID:-0}" \
  --web-identity-token "${GITLAB_OIDC_TOKEN:?}" --duration-seconds 3600 \
  --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)
echo "export AWS_ACCESS_KEY_ID=$KEY AWS_SECRET_ACCESS_KEY=$SECRET AWS_SESSION_TOKEN=$TOKEN"
