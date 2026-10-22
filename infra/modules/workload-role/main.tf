# Reusable: an IAM role that exactly one Kubernetes ServiceAccount can assume via IRSA.
# The trust policy pins issuer + audience + namespace/serviceaccount, so a pod in any other
# namespace, or using any other SA, cannot obtain these credentials.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

variable "name" { type = string }
variable "oidc_provider_arn" { type = string }
variable "oidc_issuer" {
  type        = string
  description = "Issuer host/path without https://"
}
variable "namespace" { type = string }
variable "service_account" { type = string }
variable "policy_json" { type = string }

resource "aws_iam_role" "this" {
  name                 = var.name
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = var.oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.oidc_issuer}:aud" = "sts.amazonaws.com"
          "${var.oidc_issuer}:sub" = "system:serviceaccount:${var.namespace}:${var.service_account}"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "this" {
  name   = "${var.name}-inline"
  role   = aws_iam_role.this.id
  policy = var.policy_json
}

output "arn" { value = aws_iam_role.this.arn }
output "name" { value = aws_iam_role.this.name }
