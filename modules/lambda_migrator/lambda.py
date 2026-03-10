import os
import threading
import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Must match the role name created by the workload_iam module.
WORKLOAD_ROLE_NAME = "sqs-migration-workload-role"

# Concurrency tuning
WORKERS_PER_QUEUE = 50   # parallel receive→send pipelines draining the same queue
QUEUE_PARALLELISM = 10  # max queues migrated simultaneously
SEND_RETRIES = 2         # retries for failed send_message_batch entries


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


def _resolve_queues_by_name(sqs, names: list) -> dict:
    """
    Return {queue_name: queue_url} for an explicit list of queue names.
    Names that don't exist are skipped; any other error (e.g. access denied) is
    logged and re-raised so the caller can surface a clear failure.
    """
    import botocore.exceptions
    queues: dict = {}
    for name in names:
        try:
            resp = sqs.get_queue_url(QueueName=name)
            queues[name] = resp["QueueUrl"]
        except botocore.exceptions.ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"):
                logger.warning(f"Queue not found, skipping: {name}")
            else:
                logger.error(f"Unexpected error resolving queue '{name}': {code} — {exc}")
                raise
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
    src_creds: Optional[dict],
    dst_creds: dict,
    src_url: str,
    dst_url: str,
    is_fifo: bool,
    delete_source: bool,
    processed_ids: Optional[set] = None,
    processed_ids_lock: Optional[threading.Lock] = None,
) -> int:
    """
    One concurrent drain pipeline. Creates its own boto3 clients so threads
    never share mutable client state.
    Returns the count of messages successfully forwarded.

    In copy mode, processed_ids / processed_ids_lock are shared across all workers
    for the same queue. Each MessageId is forwarded exactly once: once a message is
    added to processed_ids by any worker, all subsequent workers that receive it
    release it back to visible and skip forwarding. A worker stops when an entire
    received batch consists only of already-seen messages (full-pass sentinel).
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

        # Copy mode: deduplicate across concurrent workers using the shared set.
        if processed_ids is not None:
            new_messages = []
            already_seen = []
            with processed_ids_lock:
                for msg in messages:
                    if msg["MessageId"] not in processed_ids:
                        processed_ids.add(msg["MessageId"])
                        new_messages.append(msg)
                    else:
                        already_seen.append(msg)

            # Release already-seen messages immediately so they don't occupy in-flight slots.
            if already_seen:
                release_entries = [
                    {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
                    for m in already_seen
                ]
                sqs_src.change_message_visibility_batch(QueueUrl=src_url, Entries=release_entries)

            if not new_messages:
                # Every message in this batch was already forwarded by another worker.
                # This is the sentinel that we've completed a full pass of the queue.
                break

            messages = new_messages

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

        # Send with retry for individual failed entries.
        remaining = send_entries
        successful_ids: set[str] = set()
        for attempt in range(SEND_RETRIES + 1):
            send_resp = sqs_dst.send_message_batch(QueueUrl=dst_url, Entries=remaining)
            successful_ids |= {r["Id"] for r in send_resp.get("Successful", [])}
            failed = send_resp.get("Failed", [])
            if not failed or attempt == SEND_RETRIES:
                break
            remaining = [
                e for e in remaining
                if e["Id"] in {f["Id"] for f in failed if not f.get("SenderFault", False)}
            ]
            if not remaining:
                break

        if delete_source:
            delete_entries = [
                {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
                for m in messages
                if m["MessageId"] in successful_ids
            ]
            if delete_entries:
                sqs_src.delete_message_batch(QueueUrl=src_url, Entries=delete_entries)
        else:
            # Copy mode: release forwarded messages back to visible immediately.
            # Keeps in-flight count near zero (SQS limit: 120k standard / 20k FIFO).
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
    src_creds: Optional[dict],
    dst_creds: dict,
    src_url: str,
    dst_url: str,
    delete_source: bool,
) -> int:
    """Fan out WORKERS_PER_QUEUE concurrent pipelines against the same queue pair."""
    is_fifo = src_url.endswith(".fifo")
    total = 0
    worker_errors = []

    # In copy mode, share a set across workers so each MessageId is forwarded exactly once.
    processed_ids = set() if not delete_source else None
    processed_ids_lock = threading.Lock() if not delete_source else None

    with ThreadPoolExecutor(max_workers=WORKERS_PER_QUEUE) as pool:
        futures = [
            pool.submit(
                _drain_worker,
                region, src_creds, dst_creds,
                src_url, dst_url, is_fifo, delete_source,
                processed_ids, processed_ids_lock,
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
      - target_queues_filter (list[str]): optional explicit list of exact queue names to
                    migrate. When provided, only these queues are looked up (in both accounts
                    via GetQueueUrl) — the prefix scan is skipped entirely. Queue names that
                    do not exist in either account are omitted from the response.
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
    target_queues_filter = event.get("target_queues_filter")  # list[str] | None
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

    if target_queues_filter:
        logger.info(f"Resolving {len(target_queues_filter)} explicit queue name(s) in source")
        source_queues = _resolve_queues_by_name(sqs_source, target_queues_filter)
        logger.info(f"Resolving {len(target_queues_filter)} explicit queue name(s) in target")
        target_queues = _resolve_queues_by_name(sqs_target, target_queues_filter)
    else:
        logger.info(f"Listing source queues with prefix '{prefix}'")
        source_queues = _list_queues_by_prefix(sqs_source, prefix)
        logger.info(f"Listing target queues with prefix '{prefix}'")
        target_queues = _list_queues_by_prefix(sqs_target, prefix)

    logger.info(f"Source queues found: {sorted(source_queues)}")
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
