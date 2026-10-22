# Terraform layers

Each layer has its own state (`<env>/<layer>.tfstate` in the bootstrap bucket, S3-native locking) and publishes outputs to SSM under `/corpreg/<env>/<layer>/*`. Downstream layers read SSM, never another layer's state ([why](../docs/decisions.md#d1-terraform-split-into-six-layers-each-with-its-own-state-wired-through-ssm)).

| Layer | Owns | Reads from SSM | Changes |
|---|---|---|---|
| `00-bootstrap` | State bucket (versioned, KMS, TLS-only), local state | n/a | Once |
| `10-network` | VPC, 3-AZ private/public subnets, NAT, S3 gateway endpoint, optional interface endpoints | n/a | Rarely |
| `20-eks` | EKS control plane, access-entry auth, add-ons (VPC CNI with NetworkPolicy, Pod Identity, EBS CSI), on-demand `system` node group | network | Upgrades |
| `30-data` | Lake bucket, KMS, Glue database (Iceberg), Athena workgroup, ECR, Secrets Manager secrets, log group | n/a | Rarely |
| `40-identity` | IRSA roles (ray-job, ray-secrets, fluent-bit), CI OIDC roles (plan / deploy / apply), EKS access entries | eks, data | When permissions change |
| `50-platform` | Karpenter (IAM, SQS, Helm, NodePools), KubeRay, External Secrets, kube-prometheus-stack, Fluent Bit, `platform-baseline` chart | eks, data, identity | Chart bumps, guardrails |

```bash
scripts/ci/tf.sh static            # fmt + validate all layers, no AWS needed
scripts/ci/tf.sh plan 20-eks       # uses envs/dev/backend.hcl + envs/dev/20-eks.tfvars if present
scripts/ci/tf.sh apply 20-eks
```

Apply order is `10 → 20 → 30 → 40 → 50`. Destroy in reverse.
