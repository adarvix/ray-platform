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

variable "glue_database" {
  type    = string
  default = "corporate_registry"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "dashboard_token_version" {
  type        = number
  default     = 1
  description = "Increment to rotate the Ray dashboard token."
}

variable "generate_dashboard_token" {
  type        = bool
  default     = true
  description = "Generate the Ray dashboard token. false = bring your own (put-secret-value out-of-band)."
}
