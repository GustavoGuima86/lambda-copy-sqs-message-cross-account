# SQS Migration Lambda

Migrates SQS messages from a **root account** to a **workload account** (staging or prod), matching queues by environment prefix (e.g. `staging-`, `prod-`).

---

## Architecture

```
Root Account                          Workload Account
┌─────────────────────────────┐       ┌──────────────────────────────────┐
│  Lambda (sqs-migration-     │  STS  │  Role (sqs-migration-workload-   │
│  lambda-role)               │──────▶│  role)                           │
│                             │       │                                  │
│  - Lists source queues      │       │  - Lists target queues           │
│  - Reads & deletes messages │       │  - Receives & writes messages    │
└─────────────────────────────┘       └──────────────────────────────────┘
```

The Lambda assumes `sqs-migration-workload-role` in the target account via STS. No long-lived cross-account credentials are needed.

---

## Prerequisites

- Terraform >= 1.3
- AWS CLI configured with credentials for **both** the root and workload accounts
- Two AWS accounts: root (where the Lambda lives) and workload (staging or prod)

---

## Deployment

### Step 1 — Deploy the workload IAM role (target account)

This creates the `sqs-migration-workload-role` that the Lambda will assume. Repeat for each workload account (staging and prod).

```bash
cd environments/workload

terraform init
terraform apply \
  -var="root_account_id=<ROOT_ACCOUNT_ID>" \
  -var="workload_account_id=<WORKLOAD_ACCOUNT_ID>" \
  -var="region=eu-central-1"
```

> **Note:** Run this against each workload account (staging, prod). Use the appropriate AWS profile or assume the target account role before running.

### Step 2 — Deploy the Lambda (root account)

This creates the Lambda function and its execution role (`sqs-migration-lambda-role`). The root account ID is automatically retrieved from the AWS provider context — no need to pass it explicitly.

```bash
cd environments/root

terraform init
terraform apply \
  -var='workload_account_ids=["<STAGING_ACCOUNT_ID>","<PROD_ACCOUNT_ID>"]' \
  -var="region=eu-central-1"
```

> **Important:** Step 1 must be completed for all workload accounts before Step 2, because the Lambda's IAM policy references the workload role ARNs.

---

## Invoking the Lambda

### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `environment` | string | Yes | Queue name prefix to target. Must be `staging` or `prod` (or any prefix used in your queue names). The Lambda searches for all queues starting with `<environment>-`. |
| `target_account_id` | string | Yes | 12-digit AWS account ID of the workload account to migrate queues into. |
| `mode` | string | No | `"move"` *(default)* — copies messages then deletes them from source. `"copy"` — copies messages and **leaves the source intact**. See deduplication notes below. |
| `dry_run` | boolean | No | `false` *(default)*. When `true`, **no messages are copied or moved**. The response contains queue discovery results and approximate message counts per matched queue so you can preview what would happen. |
| `blacklisted_queues` | list\<string\> | No | Exact queue names to skip entirely. Queues in this list are neither migrated nor reported in `only_in_source`/`only_in_target`. They appear in the `blacklisted` field of the response. |

### Deduplication in `copy` mode

When using `"mode": "copy"`, messages are **not** deleted from the source. To avoid duplicates in the target on repeated runs:

| Queue type | Guarantee | Detail |
|---|---|---|
| **FIFO** | Safe within 5 minutes | The original `MessageId` is used as `MessageDeduplicationId`. AWS deduplicates within a 5-minute window — re-running within that window is safe. |
| **FIFO with `ContentBasedDeduplication`** | Safe indefinitely (same body) | If the target FIFO queue has content-based deduplication enabled, AWS deduplicates by body hash with no time limit. |
| **Standard** | No guarantee | Standard queues have no deduplication mechanism. Re-running in `copy` mode will produce duplicate messages. |

> For a fully safe non-destructive migration, use FIFO queues with `ContentBasedDeduplication` enabled on the target, or run in `"move"` mode.

---

### JSON payload examples

**Dry run (preview only — nothing is moved):**
```json
{
  "environment": "staging",
  "target_account_id": "123456789012",
  "dry_run": true
}
```

**Move staging queues (default — source is drained):**
```json
{
  "environment": "staging",
  "target_account_id": "123456789012",
  "mode": "move"
}
```

**With a blacklist:**
```json
{
  "environment": "staging",
  "target_account_id": "123456789012",
  "mode": "move",
  "blacklisted_queues": [
    "staging-pe-v1-legacy-queue.fifo",
    "staging-pe-v1-test-queue.fifo"
  ]
}
```

**Copy staging queues (source is preserved):**
```json
{
  "environment": "staging",
  "target_account_id": "123456789012",
  "mode": "copy"
}
```

**Migrate prod queues:**
```json
{
  "environment": "prod",
  "target_account_id": "987654321098",
  "mode": "move"
}
```

### Invoke via AWS CLI

```bash
aws lambda invoke \
  --function-name sqs-migration-lambda \
  --payload '{"environment": "staging", "target_account_id": "123456789012"}' \
  --cli-binary-format raw-in-base64-out \
  response.json

cat response.json
```

---

## Lambda response

```json
{
  "statusCode": 200,
  "body": {
    "migrated": [
      { "queue": "staging-pe-v1-account-creation.fifo", "messages_migrated": 42 },
      { "queue": "staging-pe-v1-order-placed.fifo",     "messages_migrated": 7  }
    ],
    "migration_errors": [
      { "queue": "staging-pe-v1-payments.fifo", "error": "Access denied ..." }
    ],
    "only_in_source": [
      "staging-pe-v1-legacy-queue.fifo"
    ],
    "only_in_target": [
      "staging-pe-v1-new-queue.fifo"
    ]
  }
}
```

| Field | Description |
|---|---|
| `dry_run` | `true` when dry run mode was active — no messages were touched. |
| `migrated` | Queues successfully drained from source to target, with actual message count (`messages_copied`). In dry run mode: approximate count (`messages_approximate`) from `GetQueueAttributes`. |
| `migration_errors` | Queues that matched but failed during migration (or failed to retrieve count in dry run mode). |
| `only_in_source` | Queue names found in the root account but missing in the workload account (blacklisted queues excluded). |
| `only_in_target` | Queue names found in the workload account but missing in the root account (blacklisted queues excluded). |
| `blacklisted` | Queue names that were found in either account but skipped due to the `blacklisted_queues` input. |

---

## Project structure

```
environments/
  root/         — Terraform for the root account (Lambda + its IAM role)
  workload/     — Terraform for the workload account (cross-account IAM role)
modules/
  lambda_migrator/
    lambda.py   — Lambda function source code
    main.tf     — Lambda function resource + IAM execution role
    variables.tf
  workload_iam/
    main.tf     — IAM role in the workload account for cross-account access
    variables.tf
```
