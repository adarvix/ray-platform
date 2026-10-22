terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {} # bucket/region/use_lockfile from envs/<env>/backend.hcl; key passed by `make`
}
