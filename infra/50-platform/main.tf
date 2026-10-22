# Layer 50 - in-cluster platform ("paved road"): autoscaling, Ray operator, secrets, observability.
# Install order: Karpenter -> operators (CRDs) -> platform-baseline chart (CRs that use those CRDs).
terraform {
  required_version = ">= 1.10"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 6.0" }
    helm       = { source = "hashicorp/helm", version = "~> 3.0" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 3.0" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags { tags = { Project = var.project, Environment = var.env, ManagedBy = "terraform", Layer = "50-platform" } }
}

locals {
  ssm = "/${var.project}/${var.env}"
  p   = { for k, v in data.aws_ssm_parameter.in : k => nonsensitive(v.value) }
  # Platform components stay on the on-demand system node group.
  system_node_selector = { "corpreg.io/pool" = "system" }
}

data "aws_ssm_parameter" "in" {
  for_each = toset([
    "eks/cluster_name", "data/lake_bucket", "data/glue_database", "data/llm_secret_name",
    "data/dashboard_secret_name", "data/log_group_name",
    "identity/ray_job_role_arn", "identity/ray_secrets_role_arn", "identity/fluent_bit_role_arn",
  ])
  name = "${local.ssm}/${each.key}"
}

data "aws_eks_cluster" "this" { name = local.p["eks/cluster_name"] }

provider "helm" {
  kubernetes = {
    host                   = data.aws_eks_cluster.this.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", data.aws_eks_cluster.this.name, "--region", var.region]
    }
  }
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", data.aws_eks_cluster.this.name, "--region", var.region]
  }
}

# Default StorageClass (encrypted gp3) for Prometheus/Grafana volumes.
resource "kubernetes_storage_class_v1" "gp3" {
  metadata {
    name        = "gp3"
    annotations = { "storageclass.kubernetes.io/is-default-class" = "true" }
  }
  storage_provisioner    = "ebs.csi.aws.com"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true
  parameters             = { type = "gp3", encrypted = "true" }
}

# ---------------------------------------------------------------- Karpenter
# AWS side: controller role (Pod Identity), node role, SQS interruption queue + EventBridge
# rules for Spot interruption / rebalance / instance state changes.
module "karpenter" {
  source  = "terraform-aws-modules/eks/aws//modules/karpenter"
  version = "~> 21.0"

  cluster_name                    = local.p["eks/cluster_name"]
  namespace                       = "kube-system"
  node_iam_role_use_name_prefix   = false
  node_iam_role_name              = "${local.p["eks/cluster_name"]}-karpenter-node"
  create_pod_identity_association = true
  node_iam_role_additional_policies = {
    ssm = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" # break-glass via SSM, no SSH keys
  }
}

resource "helm_release" "karpenter" {
  name       = "karpenter"
  namespace  = "kube-system"
  repository = "oci://public.ecr.aws/karpenter"
  chart      = "karpenter"
  version    = var.karpenter_version
  wait       = true
  values = [yamlencode({
    replicas     = 2
    nodeSelector = local.system_node_selector
    settings = {
      clusterName       = local.p["eks/cluster_name"]
      interruptionQueue = module.karpenter.queue_name # 2-min Spot notice -> cordon + drain before reclaim
    }
    serviceMonitor = { enabled = true }
    controller     = { resources = { requests = { cpu = "250m", memory = "512Mi" }, limits = { memory = "1Gi" } } }
  })]
  depends_on = [module.karpenter]
}

# ---------------------------------------------------------------- KubeRay operator
resource "helm_release" "kuberay" {
  name             = "kuberay-operator"
  namespace        = "kuberay"
  create_namespace = true
  repository       = "https://ray-project.github.io/kuberay-helm/"
  chart            = "kuberay-operator"
  version          = var.kuberay_version
  wait             = true
  values = [yamlencode({
    nodeSelector = local.system_node_selector
    resources    = { requests = { cpu = "100m", memory = "256Mi" }, limits = { memory = "1Gi" } }
    metrics      = { enabled = true, serviceMonitor = { enabled = true } }
    # The chart's list replaces the default list, so every gate is stated explicitly.
    featureGates = [
      { name = "RayClusterStatusConditions", enabled = true },
      { name = "RayJobDeletionPolicy", enabled = true },
      { name = "RayMultiHostIndexing", enabled = true },
      { name = "RayServiceIncrementalUpgrade", enabled = true },
      { name = "RayCronJob", enabled = true }, # alpha: native schedule for the pipeline
      { name = "RayClusterMTLS", enabled = false },
      { name = "RayClusterNetworkPolicy", enabled = false }, # we ship our own NetworkPolicies
      { name = "RayClusterHistoryServer", enabled = false },
    ]
  })]
  depends_on = [helm_release.monitoring] # ServiceMonitor CRD must exist
}

# ---------------------------------------------------------------- External Secrets Operator
resource "helm_release" "external_secrets" {
  name             = "external-secrets"
  namespace        = "external-secrets"
  create_namespace = true
  repository       = "https://charts.external-secrets.io"
  chart            = "external-secrets"
  version          = var.external_secrets_version
  wait             = true
  values = [yamlencode({
    installCRDs    = true
    nodeSelector   = local.system_node_selector
    webhook        = { nodeSelector = local.system_node_selector }
    certController = { nodeSelector = local.system_node_selector }
    serviceMonitor = { enabled = true }
    # No AWS role on the controller itself: each namespace's SecretStore authenticates as
    # its own ServiceAccount (IRSA), so namespace A can never read namespace B's secrets.
  })]
  depends_on = [helm_release.monitoring]
}

# ---------------------------------------------------------------- Metrics: Prometheus + Grafana + Alertmanager
resource "helm_release" "monitoring" {
  name             = "kube-prometheus-stack"
  namespace        = "monitoring"
  create_namespace = true
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = var.kube_prometheus_stack_version
  timeout          = 900
  values = [yamlencode({
    prometheus = {
      prometheusSpec = {
        nodeSelector = local.system_node_selector
        # Discover PodMonitors/ServiceMonitors/rules from every namespace and release.
        podMonitorSelectorNilUsesHelmValues     = false
        serviceMonitorSelectorNilUsesHelmValues = false
        ruleSelectorNilUsesHelmValues           = false
        retention                               = "15d"
        resources                               = { requests = { cpu = "500m", memory = "2Gi" }, limits = { memory = "4Gi" } }
        storageSpec = {
          volumeClaimTemplate = { spec = {
            storageClassName = "gp3", accessModes = ["ReadWriteOnce"], resources = { requests = { storage = "50Gi" } }
          } }
        }
      }
    }
    alertmanager = { alertmanagerSpec = { nodeSelector = local.system_node_selector } }
    grafana = {
      nodeSelector = local.system_node_selector
      # Admin password is chart-generated into a K8s Secret (not in Terraform state).
      persistence = { enabled = true, storageClassName = "gp3", size = "10Gi" }
      sidecar     = { dashboards = { enabled = true, searchNamespace = "ALL" } }
      "grafana.ini" = {
        # Ray dashboard embeds Grafana panels in an iframe.
        security         = { allow_embedding = true }
        "auth.anonymous" = { enabled = false }
      }
    }
    kube-state-metrics = { nodeSelector = local.system_node_selector }
    prometheusOperator = { nodeSelector = local.system_node_selector }
    # node-exporter is a DaemonSet: it must also run on tainted Karpenter Ray nodes.
    prometheus-node-exporter = { tolerations = [{ operator = "Exists" }] }
  })]
  depends_on = [kubernetes_storage_class_v1.gp3, helm_release.karpenter]
}

# ---------------------------------------------------------------- Logs: Fluent Bit -> CloudWatch Logs
resource "helm_release" "fluent_bit" {
  name             = "fluent-bit"
  namespace        = "logging"
  create_namespace = true
  repository       = "https://fluent.github.io/helm-charts"
  chart            = "fluent-bit"
  version          = var.fluent_bit_version
  values = [yamlencode({
    serviceAccount = {
      create      = true
      name        = "fluent-bit"
      annotations = { "eks.amazonaws.com/role-arn" = local.p["identity/fluent_bit_role_arn"] }
    }
    tolerations    = [{ operator = "Exists" }] # every node, including Spot Ray workers
    serviceMonitor = { enabled = true }
    config = {
      inputs  = <<-EOT
        [INPUT]
            Name              tail
            Tag               kube.*
            Path              /var/log/containers/*.log
            multiline.parser  cri
            Mem_Buf_Limit     50MB
            Skip_Long_Lines   On
            Refresh_Interval  10
      EOT
      filters = <<-EOT
        [FILTER]
            Name                kubernetes
            Match               kube.*
            Merge_Log           On
            Keep_Log            Off
            Labels              On
            Annotations         Off
            K8S-Logging.Exclude On
      EOT
      # One stream per namespace/pod/container. JSON app logs are merged into fields, so
      # CloudWatch Logs Insights can filter by run_id across head, workers and submitter.
      outputs = <<-EOT
        [OUTPUT]
            Name                cloudwatch_logs
            Match               kube.*
            region              ${var.region}
            log_group_name      ${local.p["data/log_group_name"]}
            log_stream_prefix   fallback-
            log_stream_template $kubernetes['namespace_name'].$kubernetes['pod_name'].$kubernetes['container_name']
            auto_create_group   Off
            retry_limit         5
      EOT
    }
  })]
}

# ---------------------------------------------------------------- Baseline: our own CRs + guardrails
resource "helm_release" "platform_baseline" {
  name      = "platform-baseline"
  namespace = "kube-system"
  chart     = "${path.module}/charts/platform-baseline"
  values = [yamlencode({
    clusterName = local.p["eks/cluster_name"]
    karpenter   = { nodeRoleName = module.karpenter.node_iam_role_name }
    ray = {
      namespace       = "ray"
      jobRoleArn      = local.p["identity/ray_job_role_arn"]
      secretsRoleArn  = local.p["identity/ray_secrets_role_arn"]
      llmSecretName   = local.p["data/llm_secret_name"]
      dashboardSecret = local.p["data/dashboard_secret_name"]
      region          = var.region
    }
    quota = var.ray_namespace_quota
  })]
  depends_on = [helm_release.karpenter, helm_release.kuberay, helm_release.external_secrets, helm_release.monitoring]
}

output "grafana_password_cmd" {
  value = "kubectl -n monitoring get secret kube-prometheus-stack-grafana -o jsonpath='{.data.admin-password}' | base64 -d"
}
