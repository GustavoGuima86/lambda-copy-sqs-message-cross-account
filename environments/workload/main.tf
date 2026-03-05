provider "aws" {
  region = var.region
}

module "workload_config" {
  source              = "../../modules/workload_iam"
  region              = var.region
  root_account_id     = var.root_account_id
}