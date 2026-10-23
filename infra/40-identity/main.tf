# Layer 40 - identity. Every principal (pod or pipeline) gets its own role with only what it uses.
#
#   ray-job         (pod, IRSA)   read raw/, write warehouse/ + artifacts/, Glue on ONE database
#   ray-secrets     (pod, IRSA)   read ONE secret (used by External Secrets in the ray namespace)
#   fluent-bit      (pod, IRSA)   write to ONE log group
#   ci-plan         (OIDC)        read-only + state lock; PRs only
#   ci-deploy       (OIDC)        push ONE ECR repo + create RayJobs in ONE namespace; main only
#   ci-infra-apply  (OIDC)        terraform apply; protected environment with required reviewers
#
# No static AWS keys exist anywhere: pods use projected service-account tokens, CI uses OIDC.

data "aws_caller_identity" "current" {}

locals {
  name          = "${var.project}-${var.env}"
  ssm           = "/${var.project}/${var.env}"
  p             = { for k, v in data.aws_ssm_parameter.in : k => nonsensitive(v.value) }
  ray_namespace = "ray"
}

data "aws_ssm_parameter" "in" {
  for_each = toset([
    "eks/cluster_name", "eks/oidc_provider_arn", "eks/oidc_issuer",
    "data/lake_bucket_arn", "data/kms_key_arn", "data/glue_database", "data/ecr_repository_arn",
    "data/llm_secret_arn", "data/dashboard_secret_arn", "data/log_group_arn",
  ])
  name = "${local.ssm}/${each.key}"
}

# ============================== Workload identities (IRSA) ==============================

module "ray_job_role" {
  source            = "../modules/workload-role"
  name              = "${local.name}-ray-job"
  oidc_provider_arn = local.p["eks/oidc_provider_arn"]
  oidc_issuer       = local.p["eks/oidc_issuer"]
  namespace         = local.ray_namespace
  service_account   = "ray-job"
  policy_json = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Bucket-level, no prefix condition: PyArrow (Ray Train's checkpoint storage) calls
        # HeadBucket, which IAM evaluates as s3:ListBucket without an s3:prefix key, so a prefix
        # condition denies it (found on a real apply). The bucket is dedicated to this pipeline;
        # object reads/writes below stay scoped to raw/, warehouse/ and artifacts/.
        Sid      = "ListLakeBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.p["data/lake_bucket_arn"]
      },
      {
        Sid      = "ReadInputs"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${local.p["data/lake_bucket_arn"]}/raw/*"
      },
      {
        Sid    = "ReadWriteWarehouseAndArtifacts"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = [
          "${local.p["data/lake_bucket_arn"]}/warehouse/*",
          "${local.p["data/lake_bucket_arn"]}/artifacts/*",
        ]
      },
      {
        Sid    = "IcebergCatalogOneDatabase"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${local.p["data/glue_database"]}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${local.p["data/glue_database"]}/*",
        ]
      },
      {
        Sid      = "UseLakeKey"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = local.p["data/kms_key_arn"]
      },
    ]
  })
}

module "ray_secrets_role" {
  source            = "../modules/workload-role"
  name              = "${local.name}-ray-secrets"
  oidc_provider_arn = local.p["eks/oidc_provider_arn"]
  oidc_issuer       = local.p["eks/oidc_issuer"]
  namespace         = local.ray_namespace
  service_account   = "ray-secrets-reader"
  policy_json = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [local.p["data/llm_secret_arn"], local.p["data/dashboard_secret_arn"]]
      },
      {
        Effect    = "Allow"
        Action    = "kms:Decrypt"
        Resource  = local.p["data/kms_key_arn"]
        Condition = { StringEquals = { "kms:ViaService" = "secretsmanager.${var.region}.amazonaws.com" } }
      },
    ]
  })
}

module "fluent_bit_role" {
  source            = "../modules/workload-role"
  name              = "${local.name}-fluent-bit"
  oidc_provider_arn = local.p["eks/oidc_provider_arn"]
  oidc_issuer       = local.p["eks/oidc_issuer"]
  namespace         = "logging"
  service_account   = "fluent-bit"
  policy_json = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
      Resource = ["${local.p["data/log_group_arn"]}", "${local.p["data/log_group_arn"]}:*"]
    }]
  })
}

# ============================== CI identities (OIDC federation) ==============================

resource "aws_iam_openid_connect_provider" "github" {
  count          = var.github_repository == "" ? 0 : 1
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

resource "aws_iam_openid_connect_provider" "gitlab" {
  count          = var.gitlab_project_path == "" ? 0 : 1
  url            = var.gitlab_url
  client_id_list = [var.gitlab_url]
}

locals {
  gitlab_host = replace(var.gitlab_url, "https://", "")
  # Which OIDC subjects may assume each CI role. PRs can only plan; only main can deploy;
  # only the reviewer-gated environment can apply infrastructure.
  ci_subjects = {
    plan = {
      github = ["repo:${var.github_repository}:pull_request"]
      gitlab = ["project_path:${var.gitlab_project_path}:*"]
    }
    deploy = {
      github = ["repo:${var.github_repository}:ref:refs/heads/main"]
      gitlab = ["project_path:${var.gitlab_project_path}:ref_type:branch:ref:main"]
    }
    apply = {
      github = ["repo:${var.github_repository}:environment:infra-${var.env}"]
      gitlab = ["project_path:${var.gitlab_project_path}:ref_type:branch:ref:main"]
    }
  }
}

data "aws_iam_policy_document" "ci_trust" {
  for_each = local.ci_subjects
  dynamic "statement" {
    for_each = var.github_repository == "" ? [] : [1]
    content {
      actions = ["sts:AssumeRoleWithWebIdentity"]
      principals {
        type        = "Federated"
        identifiers = [aws_iam_openid_connect_provider.github[0].arn]
      }
      condition {
        test     = "StringEquals"
        variable = "token.actions.githubusercontent.com:aud"
        values   = ["sts.amazonaws.com"]
      }
      condition {
        test     = "StringLike"
        variable = "token.actions.githubusercontent.com:sub"
        values   = each.value.github
      }
    }
  }
  dynamic "statement" {
    for_each = var.gitlab_project_path == "" ? [] : [1]
    content {
      actions = ["sts:AssumeRoleWithWebIdentity"]
      principals {
        type        = "Federated"
        identifiers = [aws_iam_openid_connect_provider.gitlab[0].arn]
      }
      condition {
        test     = "StringEquals"
        variable = "${local.gitlab_host}:aud"
        values   = [var.gitlab_url]
      }
      condition {
        test     = "StringLike"
        variable = "${local.gitlab_host}:sub"
        values   = each.value.gitlab
      }
    }
  }
}

resource "aws_iam_role" "ci" {
  for_each             = (var.github_repository != "" || var.gitlab_project_path != "") ? local.ci_subjects : {}
  name                 = "${local.name}-ci-${each.key}"
  assume_role_policy   = data.aws_iam_policy_document.ci_trust[each.key].json
  max_session_duration = 3600
}

# plan: read-only everywhere + read/write of the state bucket (needed for the S3 lock file).
resource "aws_iam_role_policy_attachment" "ci_plan_readonly" {
  count      = contains(keys(aws_iam_role.ci), "plan") ? 1 : 0
  role       = aws_iam_role.ci["plan"].name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# ReadOnlyAccess would also let a pull request read lake data: explicitly deny it.
resource "aws_iam_role_policy" "ci_plan_deny_data" {
  count = contains(keys(aws_iam_role.ci), "plan") ? 1 : 0
  name  = "deny-data-plane"
  role  = aws_iam_role.ci["plan"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Deny", Action = "s3:GetObject", Resource = "${local.p["data/lake_bucket_arn"]}/*" },
      { Effect = "Deny", Action = "secretsmanager:GetSecretValue", Resource = "*" },
      { Effect = "Deny", Action = "athena:StartQueryExecution", Resource = "*" },
    ]
  })
}

resource "aws_iam_role_policy" "ci_state" {
  for_each = { for k, v in aws_iam_role.ci : k => v if k != "deploy" }
  name     = "terraform-state"
  role     = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${var.state_bucket}"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "arn:aws:s3:::${var.state_bucket}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:Encrypt"]
        Resource = var.state_kms_key_arn
      },
    ]
  })
}

# deploy: push one repository, describe the cluster. Kubernetes rights come from RBAC (below).
resource "aws_iam_role_policy" "ci_deploy" {
  count = contains(keys(aws_iam_role.ci), "deploy") ? 1 : 0
  name  = "deploy"
  role  = aws_iam_role.ci["deploy"].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "ecr:GetAuthorizationToken", Resource = "*" },
      {
        Effect = "Allow"
        Action = ["ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage", "ecr:DescribeImages",
        "ecr:DescribeImageScanFindings"]
        Resource = local.p["data/ecr_repository_arn"]
      },
      { Effect = "Allow", Action = "kms:GenerateDataKey", Resource = local.p["data/kms_key_arn"] },
      {
        Effect   = "Allow"
        Action   = "eks:DescribeCluster"
        Resource = "arn:aws:eks:${var.region}:${data.aws_caller_identity.current.account_id}:cluster/${local.p["eks/cluster_name"]}"
      },
      { Effect = "Allow", Action = "ssm:GetParameter", Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${local.ssm}/*" },
    ]
  })
}

# apply: broad by necessity (it creates IAM, VPCs, clusters). Constrained by WHO can assume it
# (reviewer-gated environment on main), not by what it can do. Next step: a permissions boundary.
resource "aws_iam_role_policy_attachment" "ci_apply_admin" {
  count      = contains(keys(aws_iam_role.ci), "apply") ? 1 : 0
  role       = aws_iam_role.ci["apply"].name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

# ============================== Kubernetes access (EKS access entries) ==============================
# IAM principal -> Kubernetes group. What each group may do is defined by namespaced RBAC in
# the platform-baseline chart (layer 50): ci-deployers manage RayJobs in `ray` only;
# ray-users may port-forward to the Ray dashboard and read logs in `ray` only.

resource "aws_eks_access_entry" "ci_deploy" {
  count             = contains(keys(aws_iam_role.ci), "deploy") ? 1 : 0
  cluster_name      = local.p["eks/cluster_name"]
  principal_arn     = aws_iam_role.ci["deploy"].arn
  kubernetes_groups = ["corpreg:ci-deployers"]
}

resource "aws_eks_access_entry" "ci_plan" {
  count         = contains(keys(aws_iam_role.ci), "plan") ? 1 : 0
  cluster_name  = local.p["eks/cluster_name"]
  principal_arn = aws_iam_role.ci["plan"].arn
}

resource "aws_eks_access_policy_association" "ci_plan" {
  count         = contains(keys(aws_iam_role.ci), "plan") ? 1 : 0
  cluster_name  = local.p["eks/cluster_name"]
  principal_arn = aws_eks_access_entry.ci_plan[0].principal_arn                     # ordering: the entry must exist first
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminViewPolicy" # helm state lives in Secrets
  access_scope { type = "cluster" }
}

resource "aws_eks_access_entry" "ci_apply" {
  count         = contains(keys(aws_iam_role.ci), "apply") ? 1 : 0
  cluster_name  = local.p["eks/cluster_name"]
  principal_arn = aws_iam_role.ci["apply"].arn
}

resource "aws_eks_access_policy_association" "ci_apply" {
  count         = contains(keys(aws_iam_role.ci), "apply") ? 1 : 0
  cluster_name  = local.p["eks/cluster_name"]
  principal_arn = aws_eks_access_entry.ci_apply[0].principal_arn # ordering: the entry must exist first
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope { type = "cluster" }
}

resource "aws_eks_access_entry" "ray_users" {
  for_each          = toset(var.ray_user_role_arns)
  cluster_name      = local.p["eks/cluster_name"]
  principal_arn     = each.value
  kubernetes_groups = ["corpreg:ray-users"]
}

resource "aws_ssm_parameter" "outputs" {
  for_each = merge(
    {
      ray_job_role_arn     = module.ray_job_role.arn
      ray_secrets_role_arn = module.ray_secrets_role.arn
      fluent_bit_role_arn  = module.fluent_bit_role.arn
    },
    { for k, v in aws_iam_role.ci : "ci_${k}_role_arn" => v.arn },
  )
  name  = "${local.ssm}/identity/${each.key}"
  type  = "String"
  value = each.value
}
