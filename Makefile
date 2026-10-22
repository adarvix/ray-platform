# One entry point for humans and CI. CI calls the same scripts these targets call.
SHELL := /bin/bash
ENV_NAME ?= dev
PY ?= .venv/bin/python
export ENV_NAME

.DEFAULT_GOAL := help
help: ## list targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

# ------------------------------------------------------------------ develop
venv: ## create .venv with pinned dev dependencies (Python 3.12)
	uv venv -p 3.12 .venv && uv pip install -p .venv -r requirements-dev.txt

test: ## unit + scheduling + manifest-policy tests
	$(PY) -m pytest -q

demo: ## full pipeline in one container (real Ray + Iceberg SQL catalog), run twice to show idempotency
	docker build -q -t corpreg:dev app >/dev/null
	@mkdir -p .demo/warehouse
	@for run in demo-1 demo-2; do \
	  echo "== $$run"; \
	  docker run --rm --shm-size=2g -v "$$PWD/.demo:/data" -e RUN_ID=$$run \
	    -e INPUT_URI=/data/raw -e ARTIFACT_URI=/data/artifacts \
	    -e PYICEBERG_CATALOG__REGISTRY__TYPE=sql \
	    -e PYICEBERG_CATALOG__REGISTRY__URI=sqlite:////data/warehouse/catalog.db \
	    -e PYICEBERG_CATALOG__REGISTRY__WAREHOUSE=file:///data/warehouse \
	    corpreg:dev python ray_pipeline.py --local --generate-sources > .demo/$$run.log 2>&1 \
	    && python3 scripts/summarize_run.py .demo/$$run.log || { tail -30 .demo/$$run.log; exit 1; }; \
	done

# ------------------------------------------------------------------ local Kubernetes (OrbStack/kind)
local-up: ## KubeRay + SeaweedFS(S3) + Iceberg REST catalog on the local cluster
	scripts/local/up.sh
local-run: ## submit the pipeline as a RayJob locally and wait for the verdict
	scripts/local/run.sh
local-chaos: ## simulated Spot reclaim: kill the worker mid-training, job must still SUCCEED
	scripts/local/chaos.sh
local-schedule: ## install the nightly RayCronJob locally
	$(PY) k8s/render.py --env local --image corpreg:dev --cron "0 2 * * *" | kubectl --context $${KUBE_CONTEXT:-orbstack} apply -f -
local-dashboard: ## port-forward the local Ray dashboard (token copied to clipboard)
	scripts/dashboard.sh local
local-down: ## remove everything local-up created
	scripts/local/down.sh

# ------------------------------------------------------------------ AWS
tf-static: ## terraform fmt + validate + mocked policy tests (terraform test), no AWS needed
	scripts/ci/tf.sh static
bootstrap: ## one-time: create the remote-state bucket (local state, run by a human)
	terraform -chdir=infra/00-bootstrap init && terraform -chdir=infra/00-bootstrap apply
plan: ## make plan LAYER=20-eks
	scripts/ci/tf.sh plan $(LAYER)
apply: ## make apply LAYER=20-eks
	scripts/ci/tf.sh apply $(LAYER)
apply-all: ## apply every layer in dependency order
	@for l in 10-network 20-eks 30-data 40-identity 50-platform; do scripts/ci/tf.sh apply $$l || exit 1; done
seed-data: ## upload the synthetic source CSVs to s3://<lake>/raw (upstream systems would do this)
	eval "$$(scripts/ci/env-from-ssm.sh)" && cd app && ../$(PY) -m corpreg.datagen --out s3://$$LAKE_BUCKET/raw
run: ## build+push image and run the pipeline RayJob on EKS (what CI does on merge)
	eval "$$(scripts/ci/env-from-ssm.sh)" && export GIT_SHA=$$(git rev-parse --short=12 HEAD) && \
	  IMAGE=$$ECR_REPOSITORY_URL:$$GIT_SHA && scripts/ci/build-push.sh && IMAGE=$$IMAGE scripts/ci/run-rayjob.sh
schedule: ## install the nightly RayCronJob on EKS
	eval "$$(scripts/ci/env-from-ssm.sh)" && export AWS_REGION LAKE_BUCKET GLUE_DATABASE && \
	  $(PY) k8s/render.py --env aws --image $$ECR_REPOSITORY_URL:$$(git rev-parse --short=12 HEAD) --cron "0 2 * * *" | kubectl apply -f -
dashboard: ## Ray dashboard on EKS via authenticated port-forward + Ray token
	scripts/dashboard.sh aws

.PHONY: help venv test demo local-up local-run local-chaos local-schedule local-dashboard local-down tf-static bootstrap plan apply apply-all seed-data run schedule dashboard
