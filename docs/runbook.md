# Runbook: failure modes and responses

## How failures are contained (by design)

| Failure | What happens automatically | Why it is safe |
|---|---|---|
| **Spot reclaim of a Ray worker** | Karpenter gets the 2-min notice from SQS, cordons and drains the node, and launches a replacement (Spot, or On-Demand fallback). Ray retries lost tasks; the actor pool restarts actors (`max_restarts=3`); Ray Train restarts the worker group (preemptions do not count against `FailureConfig(max_failures=3)`) and **resumes from the last checkpoint** (every 25 rounds, in S3). | Tasks are pure functions of their input blocks; checkpoints live on S3, not on the node. |
| **No Spot capacity** (insufficient capacity error) | The `ray-worker-ondemand` NodePool (weight 10) is used instead. Its CPU limit caps the cost. | Same pod spec, same label, different capacity type. |
| **Ray head lost** (rare: on-demand system node) | The RayJob fails, then `backoffLimit: 2` reruns the whole job on a fresh cluster. | Every write is idempotent: deterministic IDs + upsert that skips unchanged rows, and predictions overwritten by `run_id`. A rerun cannot duplicate or corrupt data. |
| **Iceberg commit conflict** (concurrent writer) | PyIceberg retries internally. `CommitFailedException`, `ValidationException` or `CommitStateUnknownException` then reaches us: refresh the table, re-run the upsert (recomputes the diff), jittered exponential backoff, 5 attempts. Verified in `tests/test_concurrency.py`. | Optimistic concurrency: our commit is rebuilt on the newer snapshot, never overwrites it; idempotent upsert makes even "commit state unknown" safe to retry. |
| **Bad input** (missing columns, < 1,000 rows) | The run fails before any write (`QualityGateError`, exit 2) and the RayJob is marked FAILED. | Fail closed: nothing published. |
| **Entity resolution quality drops** (precision < 0.95) | The run fails before the Iceberg write. | A false merge corrupts revenue labels downstream; a duplicate does not. |
| **Model worse than threshold** (R² < 0.5) | Registry data is already published (it is valid). The model is *not* promoted (`models/latest.json` unchanged) and predictions are *not* published. | Separate gates for data and model. |
| **OOM** (observed locally on a 2 GiB **head**) | Ray's memory monitor kills worker processes. Ray Train retries, and the RayJob `backoffLimit` reruns the job on a fresh cluster. Persistent OOM ends in a FAILED job and a red CI run. | Fails before commit; commits are atomic. Read the OOM message's node memory limit to find *which* pod: the head hosts the driver plus zero-CPU Ray Data/Train coordination tasks, so size head memory for the driver's working set. |
| **Job hangs** | `activeDeadlineSeconds: 7200` kills it; the `RayJobRunningTooLong` alert fires at 2 h. | Bounded cost. |
| **Bad deploy** (new image breaks) | The RayJob fails in CI (`wait-rayjob.sh` exits non-zero), so the release workflow goes red. The previous image is untouched (immutable tags). Roll back by re-running the release for the last good SHA. | One run = one image; nothing long-lived to roll back. |
| **Bad data published** | Iceberg time travel: `CALL system.rollback_to_snapshot(...)` in Athena/Spark, or query `FOR VERSION AS OF`. Every snapshot carries `run_id` in its summary, so the bad run's snapshot is easy to find. | Snapshots are immutable. |

## Alert → first actions

Commands below assume the EKS context from the README (`aws eks update-kubeconfig ... --alias corpreg-dev`, then `kubectl config use-context corpreg-dev`).

**RayJobFailed**
1. `kubectl -n ray get rayjob <name> -o yaml`: read `status.message` and `status.failed`.
2. Read the driver output: `kubectl -n ray logs job/<name>`, or in CloudWatch Logs Insights:
   ```
   fields @timestamp, kubernetes.pod_name, level, msg
   | filter run_id = "<run_id>" | sort @timestamp asc
   ```
3. Exit code 2 means a quality gate failed (`msg` says which). Anything else is an infrastructure or code error.
4. The failed cluster's head is kept for 1 h. Run `make dashboard` to inspect tasks, actors and logs.

**RayInfeasibleWork**: some task or actor asks for a label or resource that no worker group offers. Usually the resolver pool is misconfigured: `maxReplicas: 0`, label missing, or the NodePool is at its limit. Check `ray status` on the head and `kubectl get nodepools`.

**RayPodsPending**: `kubectl -n ray describe pod <pod>`.
- `Insufficient cpu/memory` with no new node: Karpenter. Check `kubectl -n kube-system logs deploy/karpenter`; look for insufficient capacity, NodePool limits, or `karpenter_cloudprovider_errors_total`.
- Account vCPU quota: new accounts start at 5 On-Demand and 5 Spot vCPUs. Karpenter logs `VcpuLimitExceeded` or `MaxSpotInstanceCountExceeded`; raise the quotas in Service Quotas.
- `exceeded quota`: the namespace `ResourceQuota`.

**Workers always On-Demand, never Spot**: Karpenter logs `AuthFailure.ServiceLinkedRoleCreationNotPermitted`. The account has never launched Spot, so `AWSServiceRoleForEC2Spot` is missing. Layer 50 creates it unless `create_spot_service_linked_role = false`. Jobs keep running on the On-Demand fallback meanwhile.

**RayObjectStoreSpilling**: the workload outgrew the object store. Raise `object-store-memory` and the `/dev/shm` `sizeLimit` together, keeping both under the pod memory limit. Or reduce Ray Data block size / concurrency.

**SpotInterruptionsElevated**: informational. Jobs still succeed but run longer. If it persists, widen `categories`/`cpus` in the `ray-worker-spot` NodePool (more pools = fewer correlated reclaims).

## Useful commands

```bash
make dashboard                                    # Ray dashboard (port-forward + token)
kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
kubectl -n ray get rayjobs,rayclusters,raycronjobs
kubectl get nodeclaims -L karpenter.sh/capacity-type,node.kubernetes.io/instance-type
aws athena start-query-execution --work-group corpreg-dev \
  --query-string 'SELECT * FROM corporate_registry."companies$snapshots"'
```
