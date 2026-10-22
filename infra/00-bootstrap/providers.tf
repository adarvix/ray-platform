provider "aws" {
  region = var.region
  default_tags { tags = { Project = var.project, Environment = var.env, ManagedBy = "terraform", Layer = "00-bootstrap" } }
}
