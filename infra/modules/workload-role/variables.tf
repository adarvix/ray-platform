variable "name" { type = string }

variable "oidc_provider_arn" { type = string }

variable "oidc_issuer" {
  type        = string
  description = "Issuer host/path without https://"
}

variable "namespace" { type = string }

variable "service_account" { type = string }

variable "policy_json" { type = string }
