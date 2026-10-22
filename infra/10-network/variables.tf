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

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "single_nat_gateway" {
  type        = bool
  default     = true
  description = "true = one shared NAT (sandbox cost). false = NAT per AZ (production resilience)."
}

variable "enable_interface_endpoints" {
  type        = bool
  default     = false
  description = "Private endpoints for STS/ECR/Logs/SecretsManager/SQS. Recommended for prod."
}
