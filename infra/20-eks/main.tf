# Layer 20 - EKS control plane + the always-on system node group.
# Worker capacity is NOT here: Karpenter (layer 50) provisions Ray worker nodes on demand.
terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags { tags = { Project = var.project, Environment = var.env, ManagedBy = "terraform", Layer = "20-eks" } }
}

locals {
  cluster_name = "${var.project}-${var.env}"
  ssm          = "/${var.project}/${var.env}"
}

data "aws_ssm_parameter" "vpc_id" { name = "${local.ssm}/network/vpc_id" }
data "aws_ssm_parameter" "private_subnet_ids" { name = "${local.ssm}/network/private_subnet_ids" }

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = local.cluster_name
  kubernetes_version = var.kubernetes_version

  vpc_id     = nonsensitive(data.aws_ssm_parameter.vpc_id.value)
  subnet_ids = split(",", nonsensitive(data.aws_ssm_parameter.private_subnet_ids.value))

  # Private endpoint for nodes; public endpoint only for allow-listed CIDRs (CI / admin VPN),
  # still IAM-authenticated. Prod: set public=false and run CI from in-VPC runners.
  endpoint_private_access      = true
  endpoint_public_access       = var.endpoint_public_access
  endpoint_public_access_cidrs = var.endpoint_public_access_cidrs

  # Access entries (EKS API) instead of the aws-auth ConfigMap: auditable, Terraform-managed.
  authentication_mode                      = "API"
  enable_cluster_creator_admin_permissions = true
  enable_irsa                              = true # OIDC provider for IRSA (Ray workloads)

  enabled_log_types = ["api", "audit", "authenticator"]
  # Envelope-encrypt Kubernetes Secrets with a module-managed KMS key (v21 default).

  addons = {
    coredns                = {}
    kube-proxy             = {}
    eks-pod-identity-agent = { before_compute = true } # Karpenter + EBS CSI use Pod Identity
    vpc-cni = {
      before_compute = true
      # Enforce Kubernetes NetworkPolicy natively in the VPC CNI (no Calico needed).
      configuration_values = jsonencode({ enableNetworkPolicy = "true" })
    }
    aws-ebs-csi-driver = {
      pod_identity_association = [{ role_arn = aws_iam_role.ebs_csi.arn, service_account = "ebs-csi-controller-sa" }]
    }
  }

  eks_managed_node_groups = {
    # On-demand, always-on: Karpenter controller, operators, monitoring, Ray heads.
    # Never Spot: losing a Ray head loses the job; losing Karpenter stops all scaling.
    system = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = var.system_instance_types
      capacity_type  = "ON_DEMAND"
      min_size       = 2
      max_size       = 4
      desired_size   = 2
      labels         = { "corpreg.io/pool" = "system" }
      metadata_options = {
        http_tokens                 = "required" # IMDSv2 only
        http_put_response_hop_limit = 1          # pods cannot reach node credentials -> must use IRSA
      }
      block_device_mappings = {
        root = { device_name = "/dev/xvda", ebs = { volume_size = 50, volume_type = "gp3", encrypted = true } }
      }
    }
  }

  node_security_group_tags = { "karpenter.sh/discovery" = local.cluster_name }
}

resource "aws_iam_role" "ebs_csi" {
  name = "${local.cluster_name}-ebs-csi"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

resource "aws_ssm_parameter" "outputs" {
  for_each = {
    cluster_name      = module.eks.cluster_name
    cluster_endpoint  = module.eks.cluster_endpoint
    oidc_provider_arn = module.eks.oidc_provider_arn
    oidc_issuer       = replace(module.eks.cluster_oidc_issuer_url, "https://", "")
    node_sg_id        = module.eks.node_security_group_id
  }
  name  = "${local.ssm}/eks/${each.key}"
  type  = "String"
  value = each.value
}

output "cluster_name" { value = module.eks.cluster_name }
output "configure_kubectl" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}
