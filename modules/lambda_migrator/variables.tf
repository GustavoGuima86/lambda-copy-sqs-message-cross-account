variable "region" {}
variable "root_account_id" {}
variable "workload_account_ids" {
  description = "List of workload account IDs the Lambda is allowed to assume a role in."
  type        = list(string)
}