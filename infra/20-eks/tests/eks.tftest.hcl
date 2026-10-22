# The Kubernetes API must never be exposed to the whole internet.
mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_data "aws_ssm_parameter" {
    defaults = { value = "subnet-a,subnet-b,subnet-c" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012", arn = "arn:aws:iam::123456789012:user/ci" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws", dns_suffix = "amazonaws.com" }
  }
  mock_data "aws_iam_session_context" {
    defaults = { issuer_arn = "arn:aws:iam::123456789012:role/ci" }
  }
}

run "rejects_world_open_api_endpoint" {
  command = plan
  variables {
    endpoint_public_access_cidrs = ["0.0.0.0/0"]
  }
  expect_failures = [var.endpoint_public_access_cidrs]
}

run "accepts_allow_listed_cidr" {
  command = plan
  variables {
    endpoint_public_access_cidrs = ["203.0.113.10/32"]
  }
  assert {
    condition     = module.eks.cluster_name == "corpreg-dev"
    error_message = "cluster name convention"
  }
}
