# The IRSA trust policy must pin issuer, audience AND the exact namespace/serviceaccount.
mock_provider "aws" {}

variables {
  name              = "test-ray-job"
  oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.ap-south-1.amazonaws.com/id/ABC"
  oidc_issuer       = "oidc.eks.ap-south-1.amazonaws.com/id/ABC"
  namespace         = "ray"
  service_account   = "ray-job"
  policy_json       = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
}

run "trust_is_pinned_to_one_service_account" {
  command = plan

  assert {
    condition     = jsondecode(aws_iam_role.this.assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.ap-south-1.amazonaws.com/id/ABC:sub"] == "system:serviceaccount:ray:ray-job"
    error_message = "trust policy must pin the exact namespace/serviceaccount subject"
  }
  assert {
    condition     = jsondecode(aws_iam_role.this.assume_role_policy).Statement[0].Condition.StringEquals["oidc.eks.ap-south-1.amazonaws.com/id/ABC:aud"] == "sts.amazonaws.com"
    error_message = "trust policy must pin the STS audience"
  }
  assert {
    condition     = jsondecode(aws_iam_role.this.assume_role_policy).Statement[0].Action == "sts:AssumeRoleWithWebIdentity"
    error_message = "only web identity federation may assume this role"
  }
  assert {
    condition     = aws_iam_role.this.max_session_duration == 3600
    error_message = "sessions must be short-lived"
  }
}
