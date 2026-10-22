# Design decisions

Each entry: what was chosen, why, what else was considered, and what would make me revisit it.

---

## D1. Terraform split into six layers, each with its own state, wired through SSM

**Chosen:** `00-bootstrap → 10-network → 20-eks → 30-data → 40-identity → 50-platform`. Each layer publishes its outputs to SSM Parameter Store (`/corpreg/<env>/<layer>/<key>`), and downstream layers read only those parameters.

**Why:**
- Blast radius: a Helm chart bump (layer 50) cannot plan a VPC change, and destroying the cluster (layer 20) cannot touch the lake (layer 30).
- Speed: plans stay small and fast.
- Ownership: layers can be owned by different people or teams.
- SSM instead of `terraform_remote_state`: with remote state, every consumer needs read access to the producer's whole state file, which can contain sensitive values. With SSM, the contract is explicit and small, and the deploy job reads it too, without any Terraform access.

**Alternatives:**
- One root module: simplest, but every change plans everything, and one bad apply can hit the whole stack.
- Terragrunt: good DRY tooling, but one more tool to justify in a take-home.
- `terraform_remote_state`: works, but couples consumers to the producer's state.

**Revisit when:** there are multiple environments or accounts. Then use Terragrunt or Terraform Stacks, plus one account per environment.

## D2. Karpenter for Ray workers, a small EKS managed node group for system/head

**Chosen:**
- A fixed on-demand managed node group (`system`) runs the Karpenter controller, the operators, monitoring and the Ray heads.
- Karpenter NodePools provide all Ray worker capacity:
  - `ray-worker-spot`, weight 100
  - `ray-worker-ondemand`, weight 10, as the fallback
  - `ray-resolver`, on-demand

**Why:**
- Spot diversification: Karpenter launches from many instance types, families, sizes and architectures with a price-capacity-optimized allocation. That lowers both the interruption rate and the chance of insufficient capacity.
- Automatic fallback: if Spot is unavailable, the weighted NodePools fall back to on-demand without anyone intervening.
- Interruption handling: Karpenter consumes the 2-minute Spot notice from an SQS queue and cordons and drains the node before reclaim.
- Scale to zero: Ray workers only exist while a job is running.

**Alternatives:**
- Cluster Autoscaler + Spot managed node groups: works, but it is slower, ties each node group to a narrow instance list, and needs a separate node group for the on-demand fallback.
- EKS Auto Mode: Karpenter managed by AWS, so less to operate. The cost is an extra per-instance fee and fewer knobs (for example, custom EC2NodeClass AMI pinning). A strong choice for a team without platform engineers.

**Revisit when:** the team doesn't want to run Karpenter. Then switch to EKS Auto Mode.

## D3. Placing `EntityResolver`: Ray label selector + Kubernetes nodeSelector/taint

**Chosen:** two layers, each enforcing its own boundary.
- Ray layer: the `resolver-workers` group carries the Ray node label `corpreg.io/pool=resolver`. The actor pool uses `label_selector={"corpreg.io/pool": "resolver"}`, so Ray can only place it on those nodes.
- Kubernetes layer: the same group's pods have `nodeSelector: corpreg.io/pool=resolver` and tolerate the `ray-resolver` NodePool's taint, so Kubernetes only places those pods on resolver nodes. Nothing else can land on those nodes.
- Proof: `tests/test_scheduling.py` runs a real 2-node Ray cluster and asserts that every resolver actor runs on the labelled node.

**Why labels and not custom resources (`resources={"resolver": 1}`)?** Custom resources also work and have been stable for years. But a custom resource is a *count*: `resolver: 1` limits concurrency per node by accident. A label states *where*, not *how many*, which is the actual requirement. With Ray 2.51+ and KubeRay 1.5.1+ the autoscaler also scales on label selectors. Label scheduling is newer, so it is covered by the scheduling test.

**Revisit when:** the resolver becomes a self-hosted LLM. Then add `num_gpus=1` and point the `ray-resolver` NodePool at GPU instances (g6). The selector logic stays the same.

## D4. Ephemeral RayJob per run (plus RayCronJob for the schedule), not a long-lived RayCluster

**Chosen:**
- Each run gets its own cluster. It is created on submit and deleted by `deletionStrategy` (immediately on success, after one hour on failure, so the head can be inspected).
- The schedule is KubeRay's native `RayCronJob`, with a time zone and deterministic child names that prevent duplicate firings.

**Why:**
- Isolation: no state leaks between runs, and a stuck run can't block the next one.
- Exact image: each run uses exactly the image CI built for it.
- Cost: zero worker cost between runs.
- No GCS fault tolerance needed: GCS FT (Redis or embedded storage) matters for long-lived clusters. For an ephemeral job, `backoffLimit` plus idempotent writes already handle head loss: the whole job is retried, and the retry is safe.

**Alternatives:**
- Long-lived RayCluster + `ray job submit`: faster start (no cluster spin-up), and right for interactive or notebook work. It needs GCS FT and capacity management.
- Kubernetes CronJob that creates RayJobs: works on any KubeRay version, but it is extra RBAC and code. `RayCronJob` is alpha, which is acceptable for a nightly batch job with a CI fallback.

**Revisit when:** start-up latency matters (sub-minute SLAs), or for interactive use. Then use a warm RayCluster.

## D5. Iceberg catalog: AWS Glue on AWS, Iceberg REST locally

**Chosen:** PyIceberg's Glue catalog. Locally, the Apache Iceberg REST catalog (SeaweedFS as the S3 store). Code is catalog-agnostic: `load_catalog("registry")` with `PYICEBERG_CATALOG__REGISTRY__*` env vars.

**Why:**
- Serverless and IAM-native: access control is plain IAM (the job role is scoped to one database).
- Readable by other engines: Athena, EMR, Spark and Trino can all read the tables. Terraform includes an Athena workgroup for SQL and time travel.
- Nothing to run: no catalog server to patch or secure.

**Alternatives:**
- S3 Tables: Managed Iceberg with automatic compaction, exposed through a REST endpoint. A strong choice for new builds. It's a small swap here (REST catalog config + SigV4).
- Nessie / Polaris: git-like branching and multi-engine RBAC, but more to operate.
- Hive Metastore: a legacy operational burden.

## D6. Transactional writes: PyIceberg `upsert` from the driver (not Spark)

**Chosen:**
- Golden records: `table.upsert(join_cols=["canonical_id"])` is one atomic Iceberg commit. Unchanged rows are skipped, so a replay produces zero updates and no new snapshot (verified in `make demo` and the tests).
- Concurrency: optimistic. PyIceberg already retries a bare conflict by rebuilding the commit on fresh metadata. What still reaches our code is retried by re-running the whole upsert against a refreshed table, which recomputes the diff:
  - `CommitFailedException`: retries exhausted.
  - `ValidationException`: a concurrent writer rewrote a data file this commit would replace. The first version missed this one and would have crashed instead of retrying. Found by `tests/test_concurrency.py`, which fails against the old code.
  - `CommitStateUnknownException` (REST): normally unsafe to retry blindly. It is safe here because the upsert is idempotent: if the first commit landed, the replay diffs to zero.
- Semantics: concurrent *incremental* writers (different entities) both land. A full-recompute writer is last-writer-wins by design: each run computes the full truth from its sources.
- Predictions: overwritten by the `run_id` partition, so they are idempotent per run.

**Why from the driver:** the golden table is *O(entities)* (about 1.5k rows here, millions in reality). That is small enough for a single writer. The heavy work (ingest, matching, training) is distributed. One writer also means one commit, so there is no need to coordinate several commits.

**Path to large, concurrent, incremental upserts:**
1. Ray Data `write_iceberg` for distributed append.
2. Or Spark/EMR Serverless `MERGE INTO`, with merge-on-read deletes for large tables.
3. Plus scheduled compaction and snapshot expiry (S3 Tables does this automatically).

## D7. Identity: IRSA for pods, OIDC federation for CI, EKS access entries for Kubernetes

**Chosen:**
- One IAM role per workload: its trust policy pins issuer, audience and `system:serviceaccount:<ns>:<sa>`.
  - `ray-job`: `raw/` read; `warehouse/` and `artifacts/` read/write; Glue on one database; one KMS key.
  - `ray-secrets`: two secrets.
  - `fluent-bit`: one log group.
- CI: assumes roles through GitHub or GitLab OIDC, with no stored keys:
  - Only PRs may plan.
  - Only `main` may deploy.
  - Only the reviewer-gated environment may apply.
- Kubernetes access: EKS access entries map CI and human roles to namespaced RBAC groups (`corpreg:ci-deployers`, `corpreg:ray-users`). Nobody except the apply role gets cluster-admin.
- IMDS locked: hop limit 1 on all nodes: pods cannot borrow the node role.

**Why IRSA and not EKS Pod Identity for Ray?** Pod Identity is AWS's newer default and scales better across clusters, since there is no per-cluster OIDC trust to manage. It is used here for Karpenter and EBS CSI. Ray's data plane, however, reads and writes S3 through **PyArrow's bundled AWS C++ SDK**, and that SDK's support for the Pod Identity credential provider depends on the bundled version. Web identity (IRSA) is supported everywhere. Moving Ray to Pod Identity is a one-line change once it's verified.

## D8. Secrets: Secrets Manager → External Secrets (namespaced) → env var on resolver pods only

**Chosen:**
- LLM key: lives in Secrets Manager. Terraform creates only the container; the value is set out-of-band, so it is never in state.
- Delivery: ESO syncs it to a K8s Secret through a *namespaced* SecretStore that authenticates as a dedicated SA. The Secret is mounted only into resolver pods. Tested in `tests/test_manifests.py`.
- Ray dashboard token: a Terraform ephemeral value, written through a write-only attribute, so it is never in state or plan output.

**Alternatives:**
- Secrets Store CSI driver: mounts as a file. Equally valid.
- Ray `runtime_env` env vars: rejected, because they are visible in the dashboard and job metadata.

## D9. Observability: kube-prometheus-stack + Fluent Bit → CloudWatch Logs

**Chosen:**
- Metrics:
  - PodMonitors scrape every Ray cluster in the `ray` namespace automatically.
  - ServiceMonitors cover KubeRay and Karpenter.
- Alerts: every metric name was checked against a live Ray 2.59 `/metrics` endpoint, KubeRay v1.7.1 source and the Karpenter v1.15 reference.
- Logs: a Fluent Bit DaemonSet that tolerates every taint ships all container stdout/stderr (head, workers, submitter) to CloudWatch, with Kubernetes metadata attached. The app logs JSON with `run_id`, so one Logs Insights query follows a run across pods.

**Alternatives:**
- Amazon Managed Prometheus + Managed Grafana: less to operate; the obvious production step.
- CloudWatch Container Insights add-on: easiest, but costlier per GB.
- Loki: cheaper at volume, but self-run.

**Known gap:** Ray's own per-process log files (raylet, GCS) live under `/tmp/ray` and are not on stdout. Today they are visible in the dashboard while the cluster lives. Next step: KubeRay's History Server (beta in v1.7), which keeps the dashboard and logs for finished ephemeral clusters, or a Fluent Bit sidecar via the operator's `workerSidecarContainers`.

## D10. Delivery: CI-agnostic scripts; separate app and infra release workflows

**Chosen:**
- Shared logic: all logic lives in `scripts/ci/*.sh` and `make`. `.github/workflows/*` and `.gitlab-ci.yml` are thin triggers over them.
- Separate pipelines:
  - `release` (app): build, scan, push, RayJob.
  - `infra` (Terraform): reviewer-gated, strictly sequential.

  An app merge cannot change IAM or the cluster.
- Supply chain:
  - Actions pinned by commit SHA.
  - ECR tags immutable; images tagged with the git SHA.
  - Trivy fails the build on fixable CRITICAL CVEs and on HIGH/CRITICAL IaC misconfigurations.
  - Accepted findings are listed in `.trivyignore.yaml`, each with a reason and an expiry date.

**Why both GitHub and GitLab:** the scripts make the CI system a detail. Most teams have one or the other, and switching between them is cheap.

## D11. Network: private nodes, default-deny NetworkPolicy, S3 gateway endpoint

**Chosen:**
- Nodes: private subnets only.
- Enforcement: VPC CNI native NetworkPolicy, with default-deny in `ray`. Allowed:
  - intra-namespace traffic (Ray's dynamic ports);
  - DNS;
  - Prometheus scraping;
  - KubeRay → head `:8265`;
  - egress on 443.
- S3 traffic: goes over the free gateway endpoint.
- Interface endpoints: STS, ECR, Logs, Secrets Manager and SQS sit behind a variable: about $7 per AZ per month each, so on in prod and optional in a sandbox.

**Next step:** FQDN egress allow-listing (an egress proxy or Cilium) so that "443 to anywhere" becomes "443 to the AWS APIs and the LLM provider only".
