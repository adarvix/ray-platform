output "lake_bucket" { value = aws_s3_bucket.lake.bucket }

output "ecr_repository_url" { value = aws_ecr_repository.pipeline.repository_url }

output "llm_secret_name" { value = aws_secretsmanager_secret.llm_api_key.name }
