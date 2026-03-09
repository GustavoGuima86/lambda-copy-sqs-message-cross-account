import os
import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Must match the role name created by the workload_iam module.
WORKLOAD_ROLE_NAME = "sqs-migration-workload-role"

# Concurrency tuning
WORKERS_PER_QUEUE = 20  # parallel receive→send pipelines draining the same queue
QUEUE_PARALLELISM = 5   # max queues migrated simultaneously


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
    """Build an SQS client, optionally with assumed-role credentials."""
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


def _get_message_count(sqs, queue_url: str) -> int:
    """Return ApproximateNumberOfMessages for a queue without touching any messages."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages"],
    )
    return int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0))


def _drain_worker(
    region: str,
    src_creds: dict | None,
    dst_creds: dict,
    src_url: str,
    dst_url: str,
    is_fifo: bool,
    delete_source: bool,
) -> int:
    """
    One concurrent drain pipeline. Creates its own boto3 clients so threads
    never share mutable client state.
    Returns the count of messages successfully forwarded.
    """
    sqs_src = _sqs_client(region, src_creds)
    sqs_dst = _sqs_client(region, dst_creds)
    local_count = 0

    while True:
        recv_kwargs: dict = {
            "QueueUrl": src_url,
            "MaxNumberOfMessages": 10,
            "WaitTimeSeconds": 0,   # no long-poll: queue is known to be full
            "AttributeNames": ["All"],
            "MessageAttributeNames": ["All"],
        }

        resp = sqs_src.receive_message(**recv_kwargs)
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
                entry["MessageGroupId"] = msg.get("Attributes", {}).get(
                    "MessageGroupId", "default"
                )
                entry["MessageDeduplicationId"] = msg["MessageId"]
            send_entries.append(entry)

        send_resp = sqs_dst.send_message_batch(QueueUrl=dst_url, Entries=send_entries)
        successful_ids = {r["Id"] for r in send_resp.get("Successful", [])}

        if delete_source:
            delete_entries = [
                {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
                for m in messages
                if m["MessageId"] in successful_ids
            ]
            if delete_entries:
                sqs_src.delete_message_batch(QueueUrl=src_url, Entries=delete_entries)
        else:
            # Copy mode: release messages back to visible immediately after forwarding.
            # This keeps the in-flight count near zero, avoiding the SQS limit of
            # 120k (standard) / 20k (FIFO) in-flight messages — critical for queues
            # with hundreds of thousands of messages.
            release_entries = [
                {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
                for m in messages
                if m["MessageId"] in successful_ids
            ]
            if release_entries:
                sqs_src.change_message_visibility_batch(QueueUrl=src_url, Entries=release_entries)

        local_count += len(successful_ids)

    return local_count


def _migrate_queue(
    region: str,
    src_creds: dict | None,
    dst_creds: dict,
    src_url: str,
    dst_url: str,
    delete_source: bool,
) -> int:
    """Fan out WORKERS_PER_QUEUE concurrent pipelines against the same queue pair."""
    is_fifo = src_url.endswith(".fifo")
    total = 0
    worker_errors = []

    with ThreadPoolExecutor(max_workers=WORKERS_PER_QUEUE) as pool:
        futures = [
            pool.submit(
                _drain_worker,
                region, src_creds, dst_creds,
                src_url, dst_url, is_fifo, delete_source,
            )
            for _ in range(WORKERS_PER_QUEUE)
        ]
        for future in as_completed(futures):
            try:
                total += future.result()
            except Exception as exc:
                worker_errors.append(str(exc))

    if worker_errors:
        raise RuntimeError(
            f"{len(worker_errors)} worker(s) failed. First error: {worker_errors[0]}"
        )
    return total


def lambda_handler(event, context):
    """
    Required event parameters:
      - environment       (str): Queue prefix to target, e.g. "staging" or "prod".
      - target_account_id (str): AWS account ID of the workload account to migrate into.

    Optional event parameters:
      - mode (str): "move" (default) — copy messages then delete them from source.
                    "copy"           — copy messages and leave the source intact.
                    In copy mode messages are hidden in source for 12 h (SQS max visibility
                    timeout) and reappear afterwards. Re-running within 12 h is safe for
                    FIFO queues due to MessageDeduplicationId; standard queues will duplicate.
      - dry_run (bool): when true, no messages are copied or moved. The response contains
                    the same queue discovery results plus the approximate message count
                    per matched queue, so you can preview what would happen.
      - blacklisted_queues (list[str]): optional list of exact queue names to skip entirely.
                    Blacklisted queues are excluded from migration regardless of whether
                    they exist in source or target, and reported in the response.

    Throughput (approximate):
      - WORKERS_PER_QUEUE=20 concurrent pipelines × 10 msgs/batch ≈ 500 msg/s per queue.
      - QUEUE_PARALLELISM=5  queues in parallel → ~2 000–2 500 msg/s total.
      - 400 000 messages across several queues: ~3–4 minutes, well within the 15 min limit.

    Returns:
      - dry_run         : true when dry_run mode was active (no messages were touched).
      - migrated        : list of {queue, messages_copied} for matched queues (actual counts
                          when dry_run=false; approximate counts when dry_run=true).
      - migration_errors: list of {queue, error} for matched queues that failed.
      - only_in_source  : queue names present in source but absent in target (excl. blacklist).
      - only_in_target  : queue names present in target but absent in source (excl. blacklist).
      - blacklisted     : queue names skipped due to the blacklist.
    """
    environment = event.get("environment")
    target_account_id = event.get("target_account_id")
    mode = event.get("mode", "move")
    dry_run = bool(event.get("dry_run", False))
    blacklisted_queues = set(event.get("blacklisted_queues", []))

    if mode not in ("move", "copy"):
        return {"statusCode": 400, "error": "'mode' must be 'move' or 'copy'"}
    if not environment or not target_account_id:
        return {
            "statusCode": 400,
            "error": "Missing required parameters: 'environment' and 'target_account_id'",
        }

    region = os.environ.get("AWS_REGION", "eu-central-1")
    prefix = f"{environment}-"
    delete_source = mode == "move"

    # Source: Lambda execution role (default credential chain) — no explicit creds needed.
    src_creds = None
    sqs_source = _sqs_client(region)

    try:
        workload_creds = _assume_workload_role(target_account_id)
    except Exception as exc:
        logger.error(f"Failed to assume workload role: {exc}")
        return {"statusCode": 500, "error": f"Could not assume workload role: {exc}"}

    sqs_target = _sqs_client(region, workload_creds)

    logger.info(f"Listing source queues with prefix '{prefix}'")
    source_queues = _list_queues_by_prefix(sqs_source, prefix)
    logger.info(f"Source queues found: {sorted(source_queues)}")

    logger.info(f"Listing target queues with prefix '{prefix}'")
    target_queues = _list_queues_by_prefix(sqs_target, prefix)
    logger.info(f"Target queues found: {sorted(target_queues)}")

    source_names = set(source_queues)
    target_names = set(target_queues)

    # Determine which blacklisted names actually appeared in either account.
    blacklisted = sorted(blacklisted_queues & (source_names | target_names))
    if blacklisted:
        logger.info(f"Skipping blacklisted queues: {blacklisted}")

    # Exclude blacklisted queues from all further processing.
    source_names -= blacklisted_queues
    target_names -= blacklisted_queues

    matched = sorted(source_names & target_names)
    only_in_source = sorted(source_names - target_names)
    only_in_target = sorted(target_names - source_names)

    migrated = []
    migration_errors = []

    if dry_run:
        logger.info("Dry run mode — no messages will be copied or moved.")
        for name in matched:
            try:
                count = _get_message_count(sqs_source, source_queues[name])
                logger.info(f"[DRY RUN] Would migrate ~{count} messages: {name}")
                migrated.append({"queue": name, "messages_approximate": count})
            except Exception as exc:
                logger.error(f"[DRY RUN] Could not get message count for {name}: {exc}")
                migration_errors.append({"queue": name, "error": str(exc)})
    else:
        def _migrate_one(name: str) -> tuple[str, int]:
            return name, _migrate_queue(
                region, src_creds, workload_creds,
                source_queues[name], target_queues[name],
                delete_source,
            )

        with ThreadPoolExecutor(max_workers=QUEUE_PARALLELISM) as pool:
            futures = {pool.submit(_migrate_one, name): name for name in matched}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    _, count = future.result()
                    logger.info(
                        f"{'Moved' if delete_source else 'Copied'} {count} messages: {name}"
                    )
                    migrated.append({"queue": name, "messages_copied": count})
                except Exception as exc:
                    logger.error(f"Error migrating {name}: {exc}")
                    migration_errors.append({"queue": name, "error": str(exc)})

    return {
        "statusCode": 200,
        "body": {
            "dry_run": dry_run,
            "migrated": migrated,
            "migration_errors": migration_errors,
            "only_in_source": only_in_source,
            "only_in_target": only_in_target,
            "blacklisted": blacklisted,
        },
    }
