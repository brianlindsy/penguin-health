"""
Rules Engine RAG - Validates documents against configurable LLM rules.

Uses Claude Sonnet 4.5 via AWS Bedrock for structured JSON rule evaluation.
Loads organization configuration and rules from DynamoDB.

Runs as a Fargate task (see `fargate/rules_engine/main.py`) — one task
per validation run. Core functionality is split into:
  - bedrock_client.py: Claude model invocation with JSON extraction
  - document_validator.py: Per-rule validation with multi-threading
  - results_handler.py: DynamoDB storage and CSV reporting
  - field_extractor.py: Text field extraction
"""

import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3

from audit import SystemPrincipal, emit as audit_emit
from multi_org_config import load_org_rules, build_env_config
from document_validator import validate_document
from results_handler import (
    store_results,
    aggregate_run_summary,
    store_run_summary,
    generate_csv_from_dynamodb,
    save_csv_to_s3,
    get_processed_s3_keys,
)
import queue_handler

_AUDIT_PRINCIPAL = SystemPrincipal(
    os.environ.get('RULES_ENGINE_TASK_NAME', 'rules-engine-rag')
)
from parquet_writer import save_parquet_to_s3

try:
    # Bundled as a flat `notifications` package via the rules-engine image.
    from notifications import (
        send_email,
        get_subscribers,
        EVENT_VALIDATION_RUN_COMPLETE,
    )
    from notifications.templates import render_validation_run_complete
    _NOTIFICATIONS_AVAILABLE = True
except ImportError:  # pragma: no cover — fail-safe if the image is older than the code
    _NOTIFICATIONS_AVAILABLE = False

# Earliest date the new layout supports. Validation runs targeting earlier
# dates make no sense — there is no data/{date}/ folder for them.
CUTOVER_DATE = '2026-05-01'

DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# Files are processed concurrently — each file already runs its rules in
# a 10-way inner pool, so total in-flight Bedrock calls ~= FILE_WORKERS *
# rule_workers. Kept well under the 10,000 RPM rate limiter budget.
_FILE_WORKERS_DEFAULT = 20


class _NullContext:
    def __enter__(self): return self
    def __exit__(self, *_): return False


_NULL_CTX = _NullContext()

s3_client = boto3.client('s3')


def _notify_validation_run_complete(org_id, validation_run_id, summary,
                                    queue_counters=None):
    """Best-effort email to opt-in subscribers. Failures here must not
    crash the run, so we swallow every exception with a single log line.

    `queue_counters` is the counter dict that `process_file` mutates
    (`new_documents`, `new_versions`, `duplicate_skips`). Passing it
    through lights up the reviewer-facing "queue changes" section on
    the email — the primary reason to send this notification post-cutover.
    """
    if not _NOTIFICATIONS_AVAILABLE:
        return
    try:
        recipients = get_subscribers(org_id, EVENT_VALIDATION_RUN_COMPLETE)
        if not recipients:
            return
        subject, body = render_validation_run_complete(
            org_id=org_id,
            validation_run_id=validation_run_id,
            summary=summary,
            queue_counters=queue_counters,
        )
        send_email(
            to=recipients,
            subject=subject,
            body_text=body,
            event_type=EVENT_VALIDATION_RUN_COMPLETE,
            org_id=org_id,
            template_name="validation_run_complete",
        )
    except Exception as e:  # noqa: BLE001 — email must never fail the run
        print(f"WARN: validation-complete email failed for run {validation_run_id}: {e}")


def filter_rules_by_categories(rules, categories):
    """
    Restrict a list of rule items to those whose category is in `categories`.

    `categories` is a list of canonical category names from the API caller.
    None or an empty list means "no filter" (run all enabled rules).
    """
    if not categories:
        return rules
    allowed = set(categories)
    return [r for r in rules if r.get('category') in allowed]


def today_utc():
    """Today's date in UTC, as a date object. Wrapped for testability."""
    return datetime.now(timezone.utc).date()


def compute_dates_from_window(window, today):
    """
    Resolve a relative `date_window` payload into concrete YYYY-MM-DD strings.

    Currently supports `{"days_back_from_today": [int, ...]}`. The values are
    used as-is (no business-day skipping) so the cron rule controls "what is
    a business day", not this code.

    Examples:
        {"days_back_from_today": [1]}      -> [yesterday]
        {"days_back_from_today": [3, 2, 1]} -> [3-days-ago, 2-days-ago, yesterday]
    """
    days = window.get('days_back_from_today') or []
    out = []
    for n in days:
        d = today - timedelta(days=int(n))
        out.append(d.isoformat())
    return out


def list_data_folder_keys(bucket, date_str):
    """Yield every S3 key under data/{date_str}/. Paginates through ListObjectsV2."""
    prefix = f'data/{date_str}/'
    continuation_token = None
    while True:
        kwargs = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') or key.endswith('.json'):
                yield key
        if not resp.get('IsTruncated'):
            break
        continuation_token = resp.get('NextContinuationToken')


def date_for_key(key):
    """Pull the YYYY-MM-DD segment out of a data/{date}/... S3 key."""
    parts = key.split('/')
    if len(parts) >= 2 and parts[0] == 'data':
        return parts[1]
    return None


def resolve_dates(event):
    """
    Decide which YYYY-MM-DD ingest dates this run should validate.

    Priority order:
      1. `dates: [...]` from the event (API caller path).
      2. `date_window: {...}` from the event (EventBridge schedule path).
      3. Fallback: today (UTC).
    """
    if event.get('dates'):
        return list(event['dates'])
    if event.get('date_window'):
        return compute_dates_from_window(event['date_window'], today_utc())
    return [today_utc().isoformat()]


def run_validation(event):
    """
    Validate processed JSON/CSV documents against configurable rules.

    Expects event with:
    - organization_id: required.
    - validation_run_id: optional. If absent, a new ID is generated.
    - dates: optional list of YYYY-MM-DD strings to validate.
    - date_window: optional relative date instruction from EventBridge,
      e.g. {"days_back_from_today": [1]} (Tue-Fri) or [3, 2, 1] (Monday).
    - categories: optional list of rule categories to filter by.
    """
    org_id = event.get('organization_id')
    if not org_id:
        raise ValueError("organization_id is required in event")

    print(f"Loading configuration for organization: {org_id}")
    env_config = build_env_config(org_id)
    config = load_org_rules(org_id)

    # Optional category filter passed in by the API caller.
    # If absent, run every enabled rule (legacy behavior).
    categories = event.get('categories') or []
    if categories:
        original_count = len(config['rules'])
        config['rules'] = filter_rules_by_categories(config['rules'], categories)
        print(f"Filtered rules to categories {categories}: "
              f"{len(config['rules'])} of {original_count} match")
        if not config['rules']:
            print("No rules match requested categories; nothing to validate")

    dates = resolve_dates(event)
    print(f"Run targets dates: {dates}")

    validation_run_id = event.get('validation_run_id') or datetime.utcnow().strftime('%Y%m%d-%H%M%S')

    print(f"Starting validation run: {validation_run_id}")
    audit_emit(
        action='execute',
        resource={'type': 'ValidationRun', 'id': validation_run_id,
                  'org': org_id},
        actor=_AUDIT_PRINCIPAL.as_actor(),
        org_id=org_id,
        purpose_of_use='DOC_PROCESSING',
        call_type='validation_run_start',
        external_control_number=validation_run_id,
    )

    bucket = env_config['BUCKET_NAME']

    # Files already processed by an earlier run with the same ID (an
    # operator-triggered rerun). Empty on the normal path.
    already_processed = get_processed_s3_keys(validation_run_id, env_config)
    if already_processed:
        print(f"Skipping {len(already_processed)} files already processed under this run id")

    files_found = False
    eligible_count = 0
    # aggregate_run_summary reads all rows written by the run, so summary
    # totals fall out naturally; these counters drive the reviewer email.
    queue_counters: dict[str, int] = {}
    counters_lock = threading.Lock()

    keys_to_process = []
    for date_str in dates:
        print(f"Listing data/{date_str}/ in bucket {bucket}")
        for key in list_data_folder_keys(bucket, date_str):
            if key in already_processed:
                continue
            keys_to_process.append(key)

    eligible_count = len(keys_to_process)
    files_found = eligible_count > 0

    file_workers = int(os.environ.get('FILE_WORKERS', _FILE_WORKERS_DEFAULT))
    max_workers = max(1, min(file_workers, eligible_count)) if eligible_count else 1
    print(f"Processing {eligible_count} files with {max_workers} workers")

    if eligible_count:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    process_file, bucket, key, config, org_id, env_config,
                    validation_run_id,
                    queue_counters=queue_counters,
                    counters_lock=counters_lock,
                )
                for key in keys_to_process
            ]
            for future in as_completed(futures):
                # Surface unexpected exceptions — process_file catches its
                # own errors and writes a sentinel row, so this only fires
                # on programming bugs.
                future.result()

    print(f"Processed {eligible_count} eligible files across {len(dates)} date(s); "
          f"queue counters: {queue_counters}")

    if not files_found and not already_processed:
        return {
            'status': 'no_files',
            'validation_run_id': validation_run_id,
            'organization_id': org_id,
            'dates': dates,
        }

    print(f"Aggregating run summary for: {validation_run_id}")
    summary = aggregate_run_summary(validation_run_id, env_config)
    run_categories = sorted({r.get('category') for r in config['rules']
                             if r.get('category')})
    store_run_summary(validation_run_id, org_id, summary, env_config,
                      categories=run_categories,
                      dates=dates,
                      queue_counters=queue_counters)

    _notify_validation_run_complete(org_id, validation_run_id, summary,
                                    queue_counters=queue_counters)

    print(f"Generating CSV report for run: {validation_run_id}")
    csv_report, run_items = generate_csv_from_dynamodb(validation_run_id, env_config)
    save_csv_to_s3(csv_report, validation_run_id, env_config)

    # Snapshot the run to Parquet for Athena analytics. The same items
    # that built the CSV pivot are reused, so this is a single S3 write
    # with no extra DynamoDB read. Failures here must not fail the run.
    try:
        save_parquet_to_s3(run_items, validation_run_id, env_config)
    except Exception as e:
        print(f"WARN: failed to save Parquet snapshot for {validation_run_id}: {e}")

    return {
        'status': 'ok',
        'validation_run_id': validation_run_id,
        'organization_id': org_id,
        'dates': dates,
        'eligible_count': eligible_count,
        'summary': summary,
        'queue_counters': queue_counters,
    }


def process_file(bucket, key, config, org_id, env_config, validation_run_id,
                 queue_counters=None, counters_lock=None):
    """
    Validate a single JSON or CSV file from data/{date}/.

    Files stay where they are — no move-to-processing, no archive — so the
    same file can be re-validated by a future run. To avoid an infinite
    retry loop on a file that *can't* be parsed, we always write at least
    a sentinel ERROR row to DynamoDB before returning.

    When the document queue is enabled and this file's raw record hashes
    to the latest queue-pointer's `content_hash`, `validate_document`
    returns a `skipped_duplicate` sentinel. We then:
      * bump the pointer's seen_count / last_seen_at
      * write a skinny "processed" marker row so re-runs skip this key
      * emit a `queue_duplicate_skip` audit event
    …and return WITHOUT calling `store_results` — no new per-doc row, no
    Bedrock/rule-eval cost paid.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        if key.endswith('.csv'):
            data = {'text': content}
        else:
            data = json.loads(content)

        results = validate_document(data, key, config, org_id, validation_run_id)

        if results.get('skipped_duplicate'):
            doc_id = results.get('document_id', 'UNKNOWN')
            pointer = queue_handler.lookup_pointer(org_id, doc_id) if doc_id != 'UNKNOWN' else None
            if pointer:
                queue_handler.record_duplicate_skip(pointer, validation_run_id)
                queue_handler.write_sentinel_row(
                    org_id=org_id,
                    document_id=doc_id,
                    validation_run_id=validation_run_id,
                    s3_key=key,
                    duplicate_of_version_sk=results.get('duplicate_of_version_sk'),
                    results_table_name=env_config['DYNAMODB_TABLE'],
                )
                if queue_counters is not None:
                    with (counters_lock or _NULL_CTX):
                        queue_counters['duplicate_skips'] = queue_counters.get('duplicate_skips', 0) + 1
                print(f"Skipped duplicate {key} (document_id={doc_id}) — content unchanged")
                return
            # Pointer disappeared between the lookup in validate_document
            # and now (auto-close race or manual delete). Fall through and
            # re-process as a fresh document so we don't leave the file
            # invisible.
            print(f"Duplicate flag set for {key} but pointer vanished — re-processing")
            results = validate_document(data, key, config, org_id, validation_run_id)

        results['s3_key'] = key
        store_results(results, env_config)

        doc_id = results.get('document_id', 'UNKNOWN')
        if doc_id == 'UNKNOWN' or doc_id is None:
            print(f"WARNING: No document_id extracted for {key}")

        if queue_handler.is_enabled() and doc_id and doc_id != 'UNKNOWN':
            pointer = queue_handler.lookup_pointer(org_id, doc_id)
            branch = queue_handler.upsert_new_or_version(results, pointer)
            queue_handler.emit_queue_write_audit(
                call_type=branch,
                org_id=org_id,
                document_id=doc_id,
                validation_run_id=validation_run_id,
            )
            if queue_counters is not None:
                counter_key = 'new_versions' if branch == 'queue_new_version' else 'new_documents'
                with (counters_lock or _NULL_CTX):
                    queue_counters[counter_key] = queue_counters.get(counter_key, 0) + 1

        print(f"Validated {key} (document_id={doc_id}): {results['summary']}")

    except Exception as e:
        # Write a sentinel row so a rerun with the same validation_run_id
        # doesn't retry this file forever. Keyed by s3_key so it counts
        # toward "already processed" without colliding with a real result.
        print(f"Error processing {key}: {str(e)}")
        store_results({
            'validation_run_id': validation_run_id,
            'organization_id': org_id,
            'document_id': f"ERROR#{key}",
            'filename': key,
            'validation_timestamp': datetime.utcnow().isoformat(),
            'config_version': config.get('version', 'unknown'),
            'summary': {'total_rules': 0, 'passed': 0, 'failed': 0, 'skipped': 0},
            'rules': [],
            'field_values': {},
            's3_key': key,
            'error': str(e),
        }, env_config)
