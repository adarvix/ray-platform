output "ci_role_arns" { value = { for k, v in aws_iam_role.ci : k => v.arn } }

output "ray_job_role_arn" { value = module.ray_job_role.arn }
