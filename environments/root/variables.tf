variable "region" { default = "eu-central-1" }
variable "workload_account_ids" {
  description = "List of workload account IDs to allow the Lambda to migrate into (e.g. staging and prod)."
  type        = list(string)
}