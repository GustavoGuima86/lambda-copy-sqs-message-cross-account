provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

module "migrator_lambda" {
  source               = "../../modules/lambda_migrator"
  region               = var.region
  root_account_id      = data.aws_caller_identity.current.account_id
  workload_account_ids = var.workload_account_ids
}