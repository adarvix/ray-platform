#!/usr/bin/env bash
# Terraform wrapper used identically by GitHub Actions, GitLab CI and humans (via make).
#   tf.sh static            fmt + validate + mocked policy tests on every layer (no cloud access)
#   tf.sh plan  <layer>     plan one layer against remote state -> <layer>/plan.txt
#   tf.sh apply <layer>     apply one layer (CI: only from the reviewer-gated environment)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_NAME="${ENV_NAME:-dev}"
CMD="${1:?usage: tf.sh static|plan|apply [layer]}"
LAYERS=(10-network 20-eks 30-data 40-identity 50-platform)
export TF_IN_AUTOMATION=1 TF_INPUT=0

init() {
  local layer="$1"
  terraform -chdir="$ROOT/infra/$layer" init -reconfigure \
    -backend-config="$ROOT/infra/envs/$ENV_NAME/backend.hcl" \
    -backend-config="key=$ENV_NAME/$layer.tfstate" >/dev/null
}

varfile_args() {  # sets VAR_ARGS to (-var-file=...) when the env has a tfvars file for the layer
  local f="$ROOT/infra/envs/$ENV_NAME/$1.tfvars"
  VAR_ARGS=(); [[ -f "$f" ]] && VAR_ARGS=("-var-file=$f"); return 0
}

case "$CMD" in
  static)
    terraform fmt -check -recursive "$ROOT/infra"
    for layer in 00-bootstrap "${LAYERS[@]}"; do
      echo "--- validate $layer"
      terraform -chdir="$ROOT/infra/$layer" init -backend=false >/dev/null
      terraform -chdir="$ROOT/infra/$layer" validate -no-color
    done
    # Policy tests with mocked providers (no cloud access): IRSA trust pinning, CI role split,
    # lake guardrails, API endpoint CIDR guard.
    for dir in "$ROOT"/infra/*/tests "$ROOT"/infra/modules/*/tests; do
      [[ -d "$dir" ]] || continue
      mod="$(dirname "$dir")"; echo "--- terraform test ${mod#"$ROOT"/}"
      terraform -chdir="$mod" init -backend=false >/dev/null
      terraform -chdir="$mod" test -no-color
    done
    ;;
  plan)
    layer="${2:?layer}"; init "$layer"; varfile_args "$layer"
    # -detailed-exitcode: 0 = no changes, 2 = changes (not an error), 1 = error
    set +e
    terraform -chdir="$ROOT/infra/$layer" plan -lock-timeout=5m -out=tfplan ${VAR_ARGS[@]+"${VAR_ARGS[@]}"} -detailed-exitcode
    rc=$?; set -e
    [[ $rc == 1 ]] && exit 1
    terraform -chdir="$ROOT/infra/$layer" show -no-color tfplan > "$ROOT/infra/$layer/plan.txt"
    echo "plan for $layer: $([[ $rc == 2 ]] && echo CHANGES || echo no changes)"
    ;;
  apply)
    layer="${2:?layer}"; init "$layer"; varfile_args "$layer"
    terraform -chdir="$ROOT/infra/$layer" apply -lock-timeout=5m -auto-approve ${VAR_ARGS[@]+"${VAR_ARGS[@]}"}
    ;;
  *) echo "unknown command $CMD" >&2; exit 2 ;;
esac
