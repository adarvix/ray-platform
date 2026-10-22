# Layer 30 - data plane: lake bucket, Iceberg catalog (Glue), image registry, secrets, logs.
# Lifecycle is independent of the cluster: you can destroy/rebuild EKS without touching data.

data "aws_caller_identity" "current" {}

locals {
  name   = "${var.project}-${var.env}"
  ssm    = "/${var.project}/${var.env}"
  bucket = "${local.name}-lake-${data.aws_caller_identity.current.account_id}"
}

resource "aws_kms_key" "data" {
  description             = "${local.name} lake, ECR and secrets"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "data" {
  name          = "alias/${local.name}-data"
  target_key_id = aws_kms_key.data.key_id
}

# ---------- Lake bucket: raw/ (inputs) warehouse/ (Iceberg) artifacts/ (models, checkpoints) ----------
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = var.env != "prod"
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true # cuts KMS request cost for the many small Iceberg metadata files
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
      # No "deny PutObject without KMS header" statement: clients (PyArrow, PyIceberg) rely on
      # the bucket default above, and with IfExists semantics a missing header would be denied.
    ]
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-train-checkpoints"
    status = "Enabled"
    filter { prefix = "artifacts/ray-train/" }
    expiration { days = 30 }
  }
  rule {
    id     = "noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 30 }
    abort_incomplete_multipart_upload { days_after_initiation = 3 }
  }
}

# ---------- Iceberg catalog: Glue Data Catalog database ----------
# Glue is serverless, IAM-governed (no catalog server to run, patch or secure) and is read
# natively by Athena/EMR/Spark/Trino. The Ray job uses PyIceberg's Glue catalog.
resource "aws_glue_catalog_database" "registry" {
  name         = var.glue_database
  description  = "Corporate registry (Apache Iceberg tables)"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/${var.glue_database}.db"
}

# Ad-hoc SQL over the Iceberg tables (time travel included) without running any cluster.
resource "aws_athena_workgroup" "registry" {
  name          = local.name
  force_destroy = var.env != "prod"
  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.data.arn
      }
    }
  }
}

# ---------- Image registry ----------
resource "aws_ecr_repository" "pipeline" {
  name                 = "${var.project}/pipeline"
  image_tag_mutability = "IMMUTABLE" # a tag always means the same bits; deploys reference git SHA
  force_delete         = var.env != "prod"
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.data.arn
  }
}

resource "aws_ecr_lifecycle_policy" "pipeline" {
  repository = aws_ecr_repository.pipeline.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 30 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 30 }
      action       = { type = "expire" }
    }]
  })
}

# ---------- Secrets: the LLM API key ----------
# Terraform creates the container only. The value is set out-of-band
# (`aws secretsmanager put-secret-value`) so it never lands in state or CI logs.
resource "aws_secretsmanager_secret" "llm_api_key" {
  name                    = "${local.name}/llm-api-key"
  kms_key_id              = aws_kms_key.data.arn
  recovery_window_in_days = var.env == "prod" ? 30 : 0
}

# Ray dashboard/API token (Ray token auth). Generated as an *ephemeral* value and written via a
# write-only attribute: it exists in Secrets Manager but never in Terraform state or plan output.
ephemeral "random_password" "ray_dashboard_token" {
  count   = var.generate_dashboard_token ? 1 : 0
  length  = 48
  special = false
}

resource "aws_secretsmanager_secret" "ray_dashboard_token" {
  name                    = "${local.name}/ray-dashboard-token"
  kms_key_id              = aws_kms_key.data.arn
  recovery_window_in_days = var.env == "prod" ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "ray_dashboard_token" {
  count                    = var.generate_dashboard_token ? 1 : 0
  secret_id                = aws_secretsmanager_secret.ray_dashboard_token.id
  secret_string_wo         = ephemeral.random_password.ray_dashboard_token[0].result
  secret_string_wo_version = var.dashboard_token_version # bump to rotate
}

# ---------- Central log group (Fluent Bit ships every container's stdout/stderr here) ----------
resource "aws_cloudwatch_log_group" "containers" {
  name              = "/${var.project}/${var.env}/containers"
  retention_in_days = var.log_retention_days
}

resource "aws_ssm_parameter" "outputs" {
  for_each = {
    lake_bucket           = aws_s3_bucket.lake.bucket
    lake_bucket_arn       = aws_s3_bucket.lake.arn
    kms_key_arn           = aws_kms_key.data.arn
    glue_database         = aws_glue_catalog_database.registry.name
    ecr_repository_url    = aws_ecr_repository.pipeline.repository_url
    ecr_repository_arn    = aws_ecr_repository.pipeline.arn
    llm_secret_arn        = aws_secretsmanager_secret.llm_api_key.arn
    llm_secret_name       = aws_secretsmanager_secret.llm_api_key.name
    dashboard_secret_arn  = aws_secretsmanager_secret.ray_dashboard_token.arn
    dashboard_secret_name = aws_secretsmanager_secret.ray_dashboard_token.name
    log_group_name        = aws_cloudwatch_log_group.containers.name
    log_group_arn         = aws_cloudwatch_log_group.containers.arn
  }
  name  = "${local.ssm}/data/${each.key}"
  type  = "String"
  value = each.value
}
