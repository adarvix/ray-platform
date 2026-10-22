state_bucket      = "corpreg-dev-tfstate-ACCOUNT_ID"
state_kms_key_arn = "arn:aws:kms:ap-south-1:ACCOUNT_ID:key/KEY_ID"

# Enable one or both CI systems (empty string disables).
github_repository   = "OWNER/ray-platform"
gitlab_project_path = ""

# e.g. the SSO role your data scientists use; grants dashboard port-forward + log read in `ray`.
ray_user_role_arns = []
