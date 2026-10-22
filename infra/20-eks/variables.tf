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

variable "kubernetes_version" {
  type    = string
  default = "1.37" # latest EKS standard-support version at time of writing
}

variable "system_instance_types" {
  type    = list(string)
  default = ["m7i.large", "m6i.large"]
}

variable "endpoint_public_access" {
  type    = bool
  default = true
}

variable "endpoint_public_access_cidrs" {
  type        = list(string)
  description = "CIDRs allowed to reach the public API endpoint (admin IPs / CI egress). Never 0.0.0.0/0."
  validation {
    condition     = !contains(var.endpoint_public_access_cidrs, "0.0.0.0/0")
    error_message = "Refusing to expose the Kubernetes API to the whole internet."
  }
}
