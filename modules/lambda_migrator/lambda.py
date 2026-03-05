import os
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Must match the role name created by the workload_iam module.
WORKLOAD_ROLE_NAME = "sqs-migration-workload-role"


def _assume_workload_role(target_account_id: str) -> dict:
    """Assume the migration role in the target account and return temporary credentials."""
    sts = boto3.client("sts")
    role_arn = f"arn:aws:iam::{target_account_id}:role/{WORKLOAD_ROLE_NAME}"
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName="sqs-migration-session",
    )
    return response["Credentials"]


def _sqs_client(region: str, credentials: dict = None):
    if credentials:
        return boto3.client(
            "sqs",
            region_name=region,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
    return boto3.client("sqs", region_name=region)


def _list_queues_by_prefix(sqs, prefix: str) -> dict[str, str]:
    """Return {queue_name: queue_url} for all queues matching the given prefix."""
    queues: dict[str, str] = {}
    kwargs: dict = {"QueueNamePrefix": prefix}
    while True:
        resp = sqs.list_queues(**kwargs)
        for url in resp.get("QueueUrls", []):
            queues[url.split("/")[-1]] = url
        next_token = resp.get("NextToken")
        if not next_token:
            break
        kwargs["NextToken"] = next_token
    return queues


def _migrate_queue(sqs_src, sqs_dst, src_url: str, dst_url: str, delete_source: bool) -> int:
    """Copy messages from src_url to dst_url. Deletes from source only when delete_source=True."""
    total = 0
    is_fifo = src_url.endswith(".fifo")

    while True:
        resp = sqs_src.receive_message(
            QueueUrl=src_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        messages = resp.get("Messages", [])
        if not messages:
            break

        send_entries = []
        for msg in messages:
            entry: dict = {
                "Id": msg["MessageId"],
                "MessageBody": msg["Body"],
            }
            if msg.get("MessageAttributes"):
                entry["MessageAttributes"] = msg["MessageAttributes"]
            if is_fifo:
                # MessageGroupId is a required system attribute for FIFO queues.
                entry["MessageGroupId"] = msg.get("Attributes", {}).get(
                    "MessageGroupId", "default"
                )
                # Use the original MessageId as deduplication ID so re-runs are idempotent.
                entry["MessageDeduplicationId"] = msg["MessageId"]
            send_entries.append(entry)

        send_resp = sqs_dst.send_message_batch(QueueUrl=dst_url, Entries=send_entries)

        # Only delete messages that were successfully forwarded.
        successful_ids = {r["Id"] for r in send_resp.get("Successful", [])}
        delete_entries = [
            {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
            for m in messages
            if m["MessageId"] in successful_ids
        ]
        if delete_source and delete_entries:
            sqs_src.delete_message_batch(QueueUrl=src_url, Entries=delete_entries)

        total += len(delete_entries)

    return total


def lambda_handler(event, context):
    """
    Required event parameters:
      - environment       (str): Queue prefix to target, e.g. "staging" or "prod".
      - target_account_id (str): AWS account ID of the workload account to migrate into.

    Optional event parameters:
      - mode (str): "move" (default) — copy messages then delete them from source.
                    "copy"           — copy messages but leave source intact.

    Deduplication guarantees:
      - FIFO queues: MessageDeduplicationId (original MessageId) prevents duplicates
        within AWS's 5-minute deduplication window. Re-running in "copy" mode more
        than 5 minutes after a previous run will produce duplicates unless the target
        queue has ContentBasedDeduplication enabled.
      - Standard queues: no deduplication guarantee in "copy" mode across multiple runs.

    Returns:
      - migrated        : list of {queue, messages_copied} for matched queues.
      - migration_errors: list of {queue, error} for matched queues that failed.
      - only_in_source  : queue names present in source but absent in target.
      - only_in_target  : queue names present in target but absent in source.
    """
    environment = event.get("environment")
    target_account_id = event.get("target_account_id")
    mode = event.get("mode", "move")

    if mode not in ("move", "copy"):
        return {"statusCode": 400, "error": "'mode' must be 'move' or 'copy'"}

    if not environment or not target_account_id:
        return {
            "statusCode": 400,
            "error": "Missing required parameters: 'environment' and 'target_account_id'",
        }

    region = os.environ.get("AWS_REGION", "us-east-1")
    prefix = f"{environment}-"

    # --- Build SQS clients ---
    sqs_source = _sqs_client(region)
    try:
        workload_creds = _assume_workload_role(target_account_id)
    except Exception as exc:
        logger.error(f"Failed to assume workload role: {exc}")
        return {"statusCode": 500, "error": f"Could not assume workload role: {exc}"}
    sqs_target = _sqs_client(region, workload_creds)

    # --- Discover queues in both accounts ---
    logger.info(f"Listing source queues with prefix '{prefix}'")
    source_queues = _list_queues_by_prefix(sqs_source, prefix)
    logger.info(f"Source queues found: {sorted(source_queues)}")

    logger.info(f"Listing target queues with prefix '{prefix}'")
    target_queues = _list_queues_by_prefix(sqs_target, prefix)
    logger.info(f"Target queues found: {sorted(target_queues)}")

    # --- Compute match sets ---
    source_names = set(source_queues)
    target_names = set(target_queues)
    matched = sorted(source_names & target_names)
    only_in_source = sorted(source_names - target_names)
    only_in_target = sorted(target_names - source_names)

    # --- Migrate matched queues ---
    migrated = []
    migration_errors = []

    for name in matched:
        src_url = source_queues[name]
        dst_url = target_queues[name]
        try:
            count = _migrate_queue(sqs_source, sqs_target, src_url, dst_url, delete_source=(mode == "move"))
            logger.info(f"{'Moved' if mode == 'move' else 'Copied'} {count} messages: {name}")
            migrated.append({"queue": name, "messages_copied": count})
        except Exception as exc:
            logger.error(f"Error migrating {name}: {exc}")
            migration_errors.append({"queue": name, "error": str(exc)})

    return {
        "statusCode": 200,
        "body": {
            "migrated": migrated,
            "migration_errors": migration_errors,
            "only_in_source": only_in_source,
            "only_in_target": only_in_target,
        },
    }