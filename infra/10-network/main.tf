# Layer 10 - network. Changes rarely; blast radius is everything, so it has its own state.
terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {} # bucket/region/use_lockfile from envs/<env>/backend.hcl; key passed by `make`
}

provider "aws" {
  region = var.region
  default_tags { tags = { Project = var.project, Environment = var.env, ManagedBy = "terraform", Layer = "10-network" } }
}

locals {
  name         = "${var.project}-${var.env}"
  cluster_name = "${var.project}-${var.env}"
  azs          = slice(data.aws_availability_zones.available.names, 0, 3)
}

data "aws_availability_zones" "available" { state = "available" }

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.0"

  name = local.name
  cidr = var.vpc_cidr
  azs  = local.azs
  # /19 private subnets: Ray workers on Karpenter can burst to hundreds of pods (1 IP each with VPC CNI).
  private_subnets = [for i in range(3) : cidrsubnet(var.vpc_cidr, 3, i)]
  public_subnets  = [for i in range(3) : cidrsubnet(var.vpc_cidr, 8, 200 + i)]

  enable_nat_gateway = true
  # Sandbox: one NAT (~$32/mo) instead of one per AZ. Prod: false, so an AZ outage
  # does not take egress away from the other two AZs.
  single_nat_gateway   = var.single_nat_gateway
  enable_dns_hostnames = true
  enable_dns_support   = true

  public_subnet_tags = { "kubernetes.io/role/elb" = "1" }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
    "karpenter.sh/discovery"          = local.cluster_name # Karpenter launches nodes only here
  }
}

# S3 gateway endpoint is free and keeps data-plane traffic (Ray Data reads, Iceberg writes,
# checkpoints) off the NAT gateway: cheaper, and it never traverses the internet path.
module "endpoints" {
  source  = "terraform-aws-modules/vpc/aws//modules/vpc-endpoints"
  version = "~> 6.0"

  vpc_id                     = module.vpc.vpc_id
  create_security_group      = var.enable_interface_endpoints
  security_group_name_prefix = "${local.name}-vpce-"
  security_group_rules = {
    ingress_https = { description = "HTTPS from VPC", cidr_blocks = [module.vpc.vpc_cidr_block] }
  }

  endpoints = merge(
    {
      s3 = {
        service         = "s3"
        service_type    = "Gateway"
        route_table_ids = module.vpc.private_route_table_ids
      }
    },
    # Interface endpoints cost ~$7/AZ/month each; on in prod, optional in a sandbox.
    var.enable_interface_endpoints ? {
      for svc in ["sts", "ecr.api", "ecr.dkr", "logs", "secretsmanager", "sqs"] :
      replace(svc, ".", "_") => {
        service             = svc
        private_dns_enabled = true
        subnet_ids          = module.vpc.private_subnets
      }
    } : {}
  )
}

# Outputs are published to SSM so downstream layers read a small, explicit contract
# instead of reading this layer's state file (which they would otherwise need IAM access to).
resource "aws_ssm_parameter" "outputs" {
  for_each = {
    vpc_id             = module.vpc.vpc_id
    private_subnet_ids = join(",", module.vpc.private_subnets)
    vpc_cidr           = module.vpc.vpc_cidr_block
  }
  name  = "/${var.project}/${var.env}/network/${each.key}"
  type  = "String"
  value = each.value
}

output "vpc_id" { value = module.vpc.vpc_id }
output "private_subnet_ids" { value = module.vpc.private_subnets }
