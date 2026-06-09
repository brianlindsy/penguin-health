# `audit` — HIPAA-Compliant Audit Log

Application-level audit log for every PHI access and PHI-bearing write
across Penguin Health. The package is bundled into every Lambda that
touches PHI; the CDK construct `infra/components/audit_layer.py`
provisions the durable substrate (S3 Object Lock + Kinesis Firehose +
DynamoDB hot mirror).

## Why this exists

HIPAA Security Rule audit controls are **Required**, not Addressable:

- **45 CFR § 164.312(b) Audit Controls** — mechanisms that record and
  examine activity in systems containing ePHI.
- **45 CFR § 164.308(a)(1)(ii)(D) Information System Activity Review** —
  procedures to regularly review audit logs.
- **45 CFR § 164.316(b)(2)** — 6-year retention floor.

OCR settlements (USR Holdings $337K, Oklahoma State CHS $875K) cite
modifiable logs and missing review procedures. Object Lock Compliance
mode gives us true WORM storage — not even root can delete or shorten
retention.

This layer covers application-level PHI events. It does **not** cover
AWS control-plane events (those land in CloudTrail) or operational logs
(CloudWatch). For an OCR auditor's "show me every PHI access by user X
between Y and Z" question, this is the system of record.

## Architecture

```
HTTP handler                Event-driven Lambda (no JWT)
    │                            │
    │ @audited(...)              │ from audit import emit, SystemPrincipal
    ▼                            ▼
                  audit.emit(action, resource, actor, …)
                              │
                ┌─────────────┴─────────────┐
                │ sync                       │ async (2 retries, never raises)
                ▼                            ▼
       penguin-health-audit (DDB)     penguin-health-audit Firehose
       pk=ORG#{org_id}                  DirectPut, 60s/64MB buffer
       sk=AUDIT#{ts}#{event_id}         Parquet via Glue
       gsi1 = patient_hash              KMS CMK
       TTL=90d, KMS CMK, PITR=ON        │
                                        ▼
                                 s3://penguin-health-audit
                                 Object Lock COMPLIANCE 7y
                                 KMS CMK, versioned, deny-delete
                                        │
                                        ▼
                                 Glue table → Athena (per-org WG)
```

The DDB write is the **synchronous durability guarantee**. Firehose is
the **WORM archive**. Up to 60s of events can live only in Lambda
memory between Firehose buffer flushes; the DDB hot mirror covers that
window, and a backfill Lambda can re-derive missing Firehose records
from DDB if needed.

## Event schema

Single flat JSON dict per emission (FHIR `AuditEvent`-flavored, Athena-
queryable). Same row goes to DDB and Firehose; DDB adds `expires_at`.

| field | type | notes |
|---|---|---|
| `event_id` | uuid | one per emit |
| `event_time` | ISO-8601 UTC | Firehose partitions on year/month/day |
| `schema_version` | string | bump on breaking changes |
| `action` | enum | `read \| write \| execute \| login \| export` |
| `outcome` | enum | `success \| minor-failure \| serious-failure \| major-failure` (mapped from 2xx/4xx/5xx/exception) |
| `purpose_of_use` | enum | `TREATMENT \| PAYMENT \| OPERATIONS \| ELIGIBILITY \| DOC_PROCESSING \| ANALYTICS \| DEMOGRAPHIC_SEARCH \| ADMIN_CONFIG` |
| `org_id` | string | partition |
| `agent_type` | enum | `human \| system` |
| `agent_id`, `agent_email`, `agent_groups` | strings | JWT-derived for humans; `SystemPrincipal.name` for systems |
| `client_ip`, `user_agent` | strings | `event.requestContext.http.*` |
| `source_lambda`, `request_id` | strings | join key to CloudWatch logs |
| `resource_type`, `resource_id` | strings | natural id only — **never PHI** |
| `patient_hash` | sha256 | derived; raw name never stored |
| `patient_first_initial`, `patient_last_initial`, `patient_dob` | strings | initials only |
| `member_id_last4`, `payer_id`, `payer_name` | strings | last 4 only |
| `call_type` | string | `eligibility \| discovery \| fhir_fetch \| bedrock_invoke \| s3_read \| s3_write \| ddb_write` |
| `external_control_number` | string | Stedi control # / FHIR Bundle.id / Textract jobId |
| `duration_ms` | int | wall-clock |
| `result_summary` | map | slimmed (status/active/plan_name/dates/auth_required) |
| `http_status` | int | HTTP only |
| `error_class` | string | type name only — **never the message** (PHI leak risk) |

### Forbidden in events

- Full SSN, full member IDs, full names, full DOBs that aren't audit-relevant
- Bedrock prompt bodies, FHIR resource bodies, Textract JSON, chart text
- Exception messages (the type name in `error_class` is enough)

The redaction is enforced in `schema.build_event`: callers pass raw
fields like `member_id` and `patient.first_name`; the builder hashes
and truncates before persistence.

## How to add audit to a new code path

### Decision tree

- Does the response return patient-level data or write PHI to a store? → **audit it**.
- Does it call Stedi / Bedrock / FHIR / Textract with PHI in the payload? → **audit it**.
- Does it read/write org config, rule definitions, permissions, or aggregate counts only? → **skip**.

When in doubt, audit. The cost per event is microscopic; the cost of
the missing event during an OCR audit is not.

### HTTP handler — use the decorator

```python
from audit import audited

@audited(action='read', resource_type='Coverage',
         purpose_of_use='ELIGIBILITY',
         call_type='eligibility',
         patient_from_body=True)
def verify(event, path_params, body, authorize_fn, **_):
    # business logic...
    return {'statusCode': 200, 'body': json.dumps(result)}
```

The decorator:
- Pulls actor from JWT claims (`event.requestContext.authorizer.jwt.claims`)
- Captures `client_ip` from `event.requestContext.http.sourceIp`
- Pulls `orgId` from path params, plus `resource_from_path` / `resource_from_body` if you set them
- Pulls `first_name`/`last_name`/`dob` from body when `patient_from_body=True`
- Maps the response `statusCode` to `outcome`
- On exception: emits `outcome='major-failure'` with `error_class=type(e).__name__`, then re-raises

### Event-driven Lambda — explicit emit

```python
from audit import emit, SystemPrincipal

principal = SystemPrincipal("fhir-eligibility-poller")  # module-level singleton

def handler(event, context):
    for encounter in encounters_to_process(event):
        started = time.monotonic()
        try:
            result = stedi_client.verify(encounter)
            emit(
                action='read',
                resource={'type': 'Coverage', 'id': encounter.id,
                          'org': org_id},
                actor=principal.as_actor(),
                org_id=org_id,
                purpose_of_use='ELIGIBILITY',
                call_type='eligibility',
                patient={'first_name': encounter.first_name,
                         'last_name': encounter.last_name,
                         'dob': encounter.dob},
                external_control_number=result.control_number,
                duration_ms=int((time.monotonic() - started) * 1000),
                result=result.summary,
            )
        except Exception as e:
            emit(action='read', ..., outcome='major-failure',
                 error_class=type(e).__name__)
            raise
```

Construct `SystemPrincipal` once at module load — it's immutable and
allocation-free per-call.

## Storage model

| store | role | retention | mutability | query path |
|---|---|---|---|---|
| `penguin-health-audit` (DDB) | hot mirror, sync write | 90d via `expires_at` TTL | mutable (PutItem only) — by design | pk/sk-range; `gsi1` for "who looked up this patient" |
| `s3://penguin-health-audit` | WORM archive, async write | 7y via Object Lock Compliance | immutable (not even root can delete) | Athena via `penguin_health_audit.audit_events` |

DDB is the queryable cache and the synchronous durability guarantee.
S3 is the integrity story for HIPAA. Both encrypt at rest with the same
KMS CMK (`alias/penguin-health-audit`, rotation on).

## Querying audit data

### From a Lambda (DDB hot mirror, last 90 days)

```python
import boto3
table = boto3.resource('dynamodb').Table('penguin-health-audit')

# Recent activity for an org
table.query(
    KeyConditionExpression='pk = :p AND sk BETWEEN :a AND :b',
    ExpressionAttributeValues={
        ':p': 'ORG#test-org',
        ':a': f'AUDIT#{since_iso}',
        ':b': f'AUDIT#{until_iso}~',
    },
)

# Who looked up this patient (preserves the Stedi dedup query)
table.query(
    IndexName='gsi1',
    KeyConditionExpression='gsi1pk = :p',
    ExpressionAttributeValues={
        ':p': f'PATIENT#test-org#{patient_hash}',
    },
    ScanIndexForward=False,
    Limit=20,
)
```

### From Athena (S3 WORM archive, 7-year window)

```sql
-- Every PHI access by a specific user in a date range
SELECT event_time, action, resource_type, resource_id, call_type, outcome
FROM penguin_health_audit.audit_events
WHERE year = 2026 AND month = 6 AND day BETWEEN 1 AND 8
  AND org_id = 'test-org'
  AND agent_email = 'someone@example.com'
ORDER BY event_time DESC;

-- Every Bedrock invocation for an org (cost + PHI inventory)
SELECT event_time, source_lambda, resource_id, duration_ms, outcome
FROM penguin_health_audit.audit_events
WHERE year = 2026 AND month = 6
  AND org_id = 'test-org'
  AND call_type = 'bedrock_invoke'
ORDER BY event_time DESC;

-- Failed eligibility checks last 7 days
SELECT event_time, agent_email, error_class, patient_first_initial,
       patient_last_initial, payer_name
FROM penguin_health_audit.audit_events
WHERE year = 2026 AND month = 6
  AND call_type = 'eligibility'
  AND outcome IN ('serious-failure', 'major-failure')
ORDER BY event_time DESC;
```

## Failure modes & guarantees

- `emit` **never raises** into the request path. Both DDB and Firehose
  failures are swallowed; CloudWatch metrics + ERROR-level logs are the
  notification surface.
- DDB failure → `PenguinHealth/Audit AuditEmitFailure` metric.
- Firehose failure (after 2 retries) → `PenguinHealth/Audit FirehosePutFailure` metric.
- The decorator re-raises any exception from the wrapped handler. Audit
  emission failures during the unhappy path are reported via the same
  metrics — they never block the original exception from propagating.

### Worst case

A cold Lambda crash AND a simultaneous DDB failure can lose in-memory
events from the 60-second Firehose buffer window. This is acceptable
because the DDB hot mirror covers all but the most extreme case;
backfill from DDB → S3 is possible.

### Alarms (configure separately)

- Alarm on `AuditEmitFailure > 0` for 5 minutes → DDB throttling, KMS
  quota, or IAM permission drift.
- Alarm on `FirehosePutFailure > 5` over 5 minutes → Firehose-side issue;
  triage with the Firehose-side CloudWatch logs at
  `/aws/kinesisfirehose/penguin-health-audit`.

## Tamper-evidence

- **S3 Object Lock Compliance mode**: not even root can delete or shorten
  retention before the 7-year retention period elapses.
- **Bucket policy** denies `s3:DeleteObject*`, `s3:PutObjectRetention`,
  `s3:PutObjectLegalHold`, `s3:BypassGovernanceRetention`, and
  `s3:PutBucketObjectLockConfiguration` to every principal except the
  break-glass role `penguin-health-audit-admin-break-glass`.
- **KMS CMK** with annual rotation; every Encrypt/Decrypt is in CloudTrail.
- **DDB hot mirror is NOT WORM** — it's a queryable cache with a 90d
  TTL. The WORM guarantee lives in S3. Don't try to enforce DDB
  immutability via IAM; we'd defeat the TTL.

## Retention

- DDB: 90 days via `expires_at` (epoch seconds, set on PutItem).
- S3: 7 years via Object Lock Compliance default retention. Exceeds the
  § 164.316(b)(2) 6-year floor; aligned with state medical-record
  retention norms.
- Legal hold (extending beyond 7y for litigation) is set via
  `s3:PutObjectLegalHold` — only the break-glass role has the IAM grant.

## Information System Activity Review (§ 164.308(a)(1)(ii)(D))

The audit *capture* lives in this layer. The activity *review* is a
separate compliance procedure:

- **Weekly**: review `AuditEmitFailure` and `FirehosePutFailure`
  CloudWatch metrics; investigate any non-zero count.
- **Monthly**: PHI access by user via the saved Athena query above.
  Reviewer attests in the compliance log.
- **Quarterly**: anomaly review — top 10 users by PHI access volume,
  out-of-hours access, failed-access spikes.

Saved Athena queries live in the per-org workgroup
`penguin-health-analytics-{org_id}`. Attestations are tracked in the
compliance record outside this codebase.

## Operating the layer

### Adding a new emitting Lambda

1. Add the Lambda to `lambda_fns_needing_audit` in
   `infra/stacks/penguin_health_stack.py`. The `AuditLayer._grant_emit`
   helper attaches the IAM policies and sets the
   `AUDIT_TABLE_NAME` / `AUDIT_FIREHOSE_NAME` env vars.
2. Bundle the `audit` package into the Lambda asset by adding
   `(audit_pkg_dir, "audit")` to the bundler's `source_dirs` in the
   relevant component (`infra/components/admin_ui.py` or
   `infra/components/audit_engine.py`).

### Bumping the schema

1. Add the field in `audit/schema.py` (both `build_event` and the
   inline comment).
2. Add the column to `_AUDIT_EVENT_COLUMNS` in
   `infra/components/audit_layer.py` — same name, Glue/Hive type.
3. Bump `SCHEMA_VERSION` so Athena queries can filter by it during the
   migration window.
4. Re-deploy. Firehose reads the new Glue schema on the next batch.

### Investigating a degraded-audit alert

Check `AuditEmitFailure`:
- Most common cause: DDB throttling (rare on pay-per-request — usually a
  hot-partition issue if you see it).
- KMS quota: the CMK has region-wide request limits. Look at
  CloudTrail `KMS GenerateDataKey` calls.
- IAM permission drift: confirm the Lambda still has the policy
  attached by `AuditLayer._grant_emit`.

### Break-glass deletion

In genuine emergencies (legal hold expiration, demonstrated
compromise), the break-glass role
`penguin-health-audit-admin-break-glass` can act on the bucket. Assume
role requires MFA. Every assumption is in CloudTrail. Document the
reason in the security incident log before the action.

## Migration history

This package subsumes the original `stedi/audit.py` `AUDIT#`-row
pattern. Per the plan in
`/Users/brianlindsey/.claude/plans/do-an-audit-of-fuzzy-lollipop.md`:

- **Phase 1**: `stedi/audit.write_audit` dual-writes via `audit.emit`.
- **Phase 2**: `stedi/audit.recent_check_summary` and
  `recent_checks_for_patient` cut over to query
  `penguin-health-audit`. 30-day backfill from
  `penguin-health-stedi`.
- **Phase 3**: Legacy `put_item` in `stedi/audit.py:write_audit`
  removed. Existing `AUDIT#` rows in `penguin-health-stedi` age out via
  their 7-year `expires_at` TTL through ~2032.

## References

- 45 CFR § 164.312(b) — Audit Controls
- 45 CFR § 164.308(a)(1)(ii)(D) — Information System Activity Review
- 45 CFR § 164.316(b)(2) — Documentation retention
- NIST SP 800-66 Rev. 2 (Feb 2024) — HIPAA Security Rule implementation guide
- FHIR R5 `AuditEvent` resource
- AWS S3 Object Lock — https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
- AWS HIPAA-eligible services — https://aws.amazon.com/compliance/hipaa-eligible-services-reference/
