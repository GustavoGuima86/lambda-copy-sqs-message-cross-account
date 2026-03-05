data "aws_caller_identity" "current" {}

# IAM role in the workload account that the root account's Lambda assumes
# to list and write to SQS queues during migration.
resource "aws_iam_role" "sqs_migration_workload_role" {
  name = "sqs-migration-workload-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowRootLambdaAssume"
      Effect = "Allow"
      Principal = {
        # Use the account root as principal so AWS doesn't validate the role's existence
        # at apply time (avoids chicken-and-egg when root is deployed after workload).
        # Access is still restricted: only the Lambda role has sts:AssumeRole on this
        # role in its identity policy, so no other principal in the account can assume it.
        AWS = "arn:aws:iam::${var.root_account_id}:root"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sqs_migration_workload_policy" {
  name = "sqs-migration-workload-policy"
  role = aws_iam_role.sqs_migration_workload_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # ListQueues and GetQueueUrl are account-level operations; resource must be "*".
        Sid    = "SQSListAccess"
        Effect = "Allow"
        Action = [
          "sqs:ListQueues",
          "sqs:GetQueueUrl",
        ]
        Resource = "*"
      },
      {
        Sid    = "SQSQueueAccess"
        Effect = "Allow"
        Action = [
          "sqs:GetQueueAttributes",
          "sqs:SendMessage",
          "sqs:SendMessageBatch",
        ]
        Resource = "arn:aws:sqs:${var.region}:${data.aws_caller_identity.current.account_id}:*"
      }
    ]
  })
}
