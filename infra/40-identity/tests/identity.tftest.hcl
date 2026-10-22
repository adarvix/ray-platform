# CI identities exist only when a repository is explicitly configured, and only three of them.
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_ssm_parameter" {
    defaults = { value = "arn:aws:iam::123456789012:mock" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
}

override_data {
  target = data.aws_ssm_parameter.in["eks/cluster_name"]
  values = { value = "corpreg-dev" }
}

variables {
  state_bucket      = "corpreg-dev-tfstate-123456789012"
  state_kms_key_arn = "arn:aws:kms:ap-south-1:123456789012:key/mock"
}

run "no_ci_roles_unless_a_repository_is_configured" {
  command = plan

  assert {
    condition     = length(aws_iam_role.ci) == 0 && length(aws_iam_openid_connect_provider.github) == 0
    error_message = "zero trust default: no federated CI access unless explicitly enabled"
  }
}

run "github_gets_exactly_plan_deploy_apply" {
  command = plan
  variables {
    github_repository = "adarvix/ray-platform"
  }

  assert {
    condition     = toset(keys(aws_iam_role.ci)) == toset(["plan", "deploy", "apply"])
    error_message = "CI roles must be split by capability"
  }
  assert {
    condition     = length(aws_iam_role_policy.ci_plan_deny_data) == 1
    error_message = "the plan role must explicitly deny reading lake data and secrets"
  }
  assert {
    condition     = length(aws_eks_access_policy_association.ci_apply) == 1 && length(aws_eks_access_entry.ci_deploy) == 1
    error_message = "deploy maps to namespaced RBAC; only apply gets cluster-admin"
  }
  assert {
    condition     = aws_eks_access_entry.ci_deploy[0].kubernetes_groups == toset(["corpreg:ci-deployers"])
    error_message = "deploy role must map to the namespaced ci-deployers RBAC group"
  }
}
