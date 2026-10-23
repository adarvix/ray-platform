variable "project" {
  type    = string
  default = "corpreg"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

# Chart versions pinned (latest at time of writing); bumped deliberately via PR + plan.
variable "karpenter_version" {
  type    = string
  default = "1.15.0"
}

variable "kuberay_version" {
  type    = string
  default = "1.7.1"
}

variable "external_secrets_version" {
  type    = string
  default = "2.13.0"
}

variable "kube_prometheus_stack_version" {
  type    = string
  default = "92.11.0"
}

variable "fluent_bit_version" {
  type    = string
  default = "0.58.3"
}

variable "ray_namespace_quota" {
  description = "Hard cap on what Ray jobs may request: a runaway job cannot run up an unbounded bill."
  type        = object({ cpu = string, memory = string, pods = string })
  default     = { cpu = "128", memory = "512Gi", pods = "100" }
}

variable "create_spot_service_linked_role" {
  description = "Create AWSServiceRoleForEC2Spot. Needed once per account before the first Spot launch; set false if the account already has it."
  type        = bool
  default     = true
}
