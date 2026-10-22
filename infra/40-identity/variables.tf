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

variable "state_bucket" {
  type        = string
  description = "Terraform state bucket (output of 00-bootstrap)."
}

variable "state_kms_key_arn" {
  type        = string
  description = "KMS key encrypting state (output of 00-bootstrap)."
}

variable "github_repository" {
  type        = string
  default     = ""
  description = "owner/repo allowed to federate via GitHub OIDC. Empty disables GitHub CI roles."
}

variable "gitlab_project_path" {
  type        = string
  default     = ""
  description = "group/project allowed to federate via GitLab OIDC. Empty disables GitLab CI roles."
}

variable "gitlab_url" {
  type    = string
  default = "https://gitlab.com"
}

variable "ray_user_role_arns" {
  type        = list(string)
  default     = []
  description = "IAM roles (e.g. SSO data-science role) allowed to port-forward to the Ray dashboard and read logs."
}
