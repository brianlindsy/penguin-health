# CentralReach API integration — design

Status: draft, awaiting approval

This document specifies the v2 CentralReach note-ingestion path. It
replaces the browser-driven RPA integration in `lambda/multi-org/rpa/`
end-to-end. Nothing currently in the RPA module is production: we have
no migration concerns and no audit history to preserve.

## Why we are rewriting

The RPA path used Playwright to drive CR's web UI: log in with the bot
cookies, navigate to Billing Manager, click into each entry's modal,
walk three iframes per note, extract via CSS selectors. The captures we
did against CR's UI revealed that the same data is reachable via three
internal HTTP endpoints that the UI itself calls. Talking to the API
directly removes the entire browser-automation surface — Playwright,
the playbook engine, the playbook JSON, the seed script — and replaces
it with HTTP calls.

The three CR endpoints used (none documented; reverse-engineered from
Network panel captures):

| Endpoint | Purpose |
|---|---|
| `POST /crxapi/internal/billing/query` | List billing entries in a date range |
| `GET /crxapi/billing/billing-entries/{id}/preview` | Per-entry metadata + signature + templateId |
| `POST /api/?resources.getresourceurl` | Presigned S3 URL for the rendered PDF of an entry |

The endpoints are undocumented and may change without notice. This is
the load-bearing risk of the integration; see Risks at the end.

We are not using the structured-HTML note endpoints
(`/crxapi/notes/{id}` + section URLs). Roughly half of CR's note
templates render an HTML editor view; the other half are PDF-only.
Building two ingest paths (HTML for the html-having templates, PDF for
the rest) creates two parsers, two failure modes, and two code paths
to maintain, with the registry still having to dispatch per templateId.
Going pure-PDF gives us one ingest path for every template at the cost
of routing narrative text through Bedrock at rule-evaluation time.
Bedrock + Claude reads PDFs well, and the simplicity of one ingest
path makes onboarding new templates a no-op at ingest time.

## Scope decisions, locked

These came out of design conversation; they shape everything below.

1. **Runtime**: Fargate. ~350 entries/day per org, ~17 min wall-clock
   per run at 1500ms pacing, well inside Fargate's open-ended runtime.
   Lambda's 15-minute limit at the volume we expect forces Step
   Functions chunking that costs more than it saves.
2. **Module name**: `lambda/multi-org/centralreach/`. Underscored to
   be importable as a Python package.
3. **External rename**: Audit event types, S3 prefixes, Secrets
   Manager paths, IAM role names — all rename from `rpa.*` to
   `centralreach.*`. Internal classes follow.
4. **Pure-PDF ingest**: Every entry's note is stored as a PDF in S3.
   No HTML parsing, no per-template HTML extractor, no section
   endpoints. `templateId` is recorded for ops visibility but does
   not drive dispatch logic at ingest.
5. **Bedrock at rule-eval, not ingest**: For rules that need narrative
   text (rules 2, 3, 11), each evaluation sends the PDF to Bedrock
   alongside the operator-authored `rule_text` from DynamoDB. Every
   evaluation goes through Bedrock.
6. **Patient hash uses `(first_name, last_name, ClientId)`**: The
   list endpoint gives us all three deterministically. DOB is not
   used because it requires PDF extraction; ClientId is more stable
   than DOB anyway (CR's internal id doesn't change; DOB renders in
   different formats across templates).
7. **Single Bedrock model**: Sonnet 4.5
   (`global.anthropic.claude-sonnet-4-5-20250929-v1:0`), the model the
   existing rules engine already uses
   (`lambda/multi-org/rules-engine/bedrock_client.py:32`). One model
   keeps judgment-quality debugging tractable.
8. **CSRF token**: CR's endpoints require a CSRF token in the
   `x-csrf-token` header. The authenticator fetches it once per
   session at startup; the HTTP client sets it on every request.

## Module layout

```
lambda/multi-org/centralreach/
  __init__.py
  exceptions.py            # CentralReachError hierarchy
  auth.py                  # cookies + CSRF; replaces authenticators/centralreach.py
  client.py                # HTTP client: session, rate limiter, retry, default headers
  list_query.py            # POST /crxapi/internal/billing/query
  preview.py               # GET /crxapi/billing/billing-entries/{id}/preview
  resources.py             # POST /api/?resources.getresourceurl + PDF fetch
  pipeline.py              # per-entry orchestration; persists or skips
  record_builder.py        # raw fields -> CentralReachNoteRecord
  record.py                # CentralReachNoteRecord dataclass (renamed RpaNoteRecord)
  pdf_storage.py           # write PDF bytes to per-org S3 prefix
  normalize.py             # date/time parsers (subset of rpa.normalize)
  parameters.py            # yesterday_eastern + env-var overrides (moved from rpa)
  rate_limiter.py          # min-delay between HTTP requests (moved from rpa)

fargate/centralreach_ingest/
  main.py                  # task entry point: env -> pipeline.run -> persist
  Dockerfile
  requirements.txt

lambda/multi-org/rules-engine/
  document_validator.py    # modified: new `_load_chart_input` helper;
                           #           PDF -> document content block branch
                           #           in `_extract_rule_fields` and `_validate_rule`
```

### What survives unchanged from the RPA module

These move to `centralreach/` with imports rewritten but otherwise
unmodified:

- `parameters.py` — yesterday-Eastern resolver + env-var overrides
- `rate_limiter.py` — async min-delay gate
- `normalize.py` (subset) — date parsing, whitespace cleanup. Most
  of the existing `normalize.py` was DOM-derived value parsing
  (`billing-grid-row-` prefix, `clientId:` regex, MM/DD/YYYY visit
  date) — the API returns ISO timestamps and numeric ids, so most
  of the parsers retire. The JSON-attribute parser for
  `providerSignatureCreationDate` stays.
- `result_writer.py` — adapted, not rewritten. Same S3 layout, same
  audit emission. The build_record signature stays.

### What gets deleted

All of:

- `playbook_engine.py`, `playbook_engine_playwright.py`
- `playbooks/centralreach/notes-v1.json` and the `playbooks/`
  directory
- `scripts/multi-org/seed_rpa_playbook.py` and its tests
- `fargate/rpa_runner/main.py` and the Fargate task definition
- All `tests/unit/rpa/test_playbook_*` files

The auth flow logic survives (cookies + JWT + CSRF), but the file
moves and gains the CSRF step.

## Per-entry pipeline

```
For each entry in query_billing(start_date, end_date) results:

  preview = get_preview(entry.Id)              # 1 HTTP request

  # Skip entries with no rendered PDF. Unsigned/draft notes return
  # preview.files == [] — the provider hasn't signed yet so CR has not
  # generated a PDF. Skip with a structured "no_pdf_available" event;
  # the entry will be picked up on a later run after signing.
  if not preview.files:
    record_skip(entry, reason="no_pdf_available")
    continue

  # resourceId is the file resource id from preview, NOT the billing
  # entry id. preview.files[0] is the rendered note PDF for this entry.
  file_id = preview.files[0].id
  pdf_url = get_resource_url(file_id)           # 1 HTTP request
  pdf_bytes = fetch_url(pdf_url)                # 1 HTTP request (S3, presigned)
  pdf_s3_key = write_pdf_to_s3(org_id, entry, pdf_bytes)

  record = build_record(
      entry=entry,                # from list query
      preview=preview,            # structured signature + billing fields
      pdf_s3_key=pdf_s3_key,      # S3 key for the PDF
  )
  persist_note(record)            # JSON record to S3 + audit event
```

Per-entry HTTP count: 3 for entries with a PDF (preview, resourceurl,
S3 GET); 1 for skipped/unsigned entries (preview only). At 1500ms
pacing between requests:

- 350 entries × 3 requests × 1500ms = ~26 minutes for the HTTP work
  (worst case — all entries signed and ingested)
- Skipped entries cost only the preview call, so a run mixed with
  drafts is faster, not slower
- Plus list query and auth: ~30 seconds total

So a typical run is ~26 minutes or less. Well within Fargate budgets.

S3 GET against the presigned URL is to `s3.amazonaws.com`, not CR's
domain. We rate-limit it the same way as the CR calls — partly out of
caution (the presigned URL ultimately came from CR; an unusual request
rate could trigger their monitoring) and partly because it doesn't
materially extend the run.

### List query — required body shape

The `POST /crxapi/internal/billing/query` endpoint validates the
request body server-side and rejects (HTTP 200 with `responseStatus`
errors, see [Error handling](#error-handling)) any call missing a
populated `dateRange` field. The UI's request body has ~200 fields;
most are empty-string UI state the validator accepts as empty. The
fields the validator actually requires:

| Field | Type | Notes |
|---|---|---|
| `startDate` | ISO date string | e.g. `"2026-06-29"` |
| `endDate` | ISO date string | same |
| `startDateDisplay` | string | non-empty; UI uses the ISO date |
| `endDateDisplay` | string | non-empty; UI uses the ISO date |
| `dateRange` | string | non-empty human-readable summary. UI builds `"Jun 29"` for single-day, `"Jun 22 - Jun 28"` for multi-day. Validator only enforces non-empty; format does not matter. |
| `page` | integer | starts at 1 |
| `pageSize` | integer | 1-500 per the UI dropdown options |
| `_utcOffsetMinutes` | integer | UTC offset of the org's timezone, in minutes. Resolve from `RPA_CONFIG.guardrails.timezone` via `zoneinfo` at session start. |

Builder responsibility: the client's `query_billing(start_date,
end_date)` populates all eight fields. `dateRange` and the `*Display`
strings come from a small formatter that takes `start_date` and
`end_date` and emits the UI's natural representation.

If CR's validator complains about other fields in production, the
error response names them (`responseStatus.errors[].fieldName`) — add
them to the required-fields table here when we encounter them. We do
not pre-populate the full 200-field UI request body.

### Resource URL request — required body shape

The `POST /api/?resources.getresourceurl` endpoint takes a small
body:

| Field | Type | Notes |
|---|---|---|
| `resourceId` | integer | The file resource id from `preview.files[0].id`. Not the billing entry id. |
| `_utcOffsetMinutes` | integer | UTC offset of the org's timezone, in minutes. Must match the org's actual timezone. Empirically, a value that doesn't match what CR has on file for the session is one source of the endpoint returning `success: false` with no URL. |

The `_utcOffsetMinutes` value here MUST match the `tzoffset` cookie
on the request and the org's actual configured timezone. The HTTP
client computes both from a single source — `RPA_CONFIG.guardrails.timezone`
resolved via `zoneinfo` at session start — to guarantee they stay
in sync.

## CentralReachNoteRecord

Field-level changes from `RpaNoteRecord`:

| Field | Change |
|---|---|
| `text` | Carries the Bedrock-extracted narrative prose from the PDF. Extracted once at ingest time by `centralreach.narrative_extractor.extract_narrative` between PDF fetch and record build. Rules 1/2/3 read it directly. |
| `body_html` | Always `None`. |
| `extracted_fields.pdf_s3_key` | New. Always set. Format `pdfs/{ingest_date}/{captured_at_compact}__{source_record_id}.pdf` on `penguin-health-{org_id}`. See [PDF storage](#pdf-storage). |
| `extracted_fields.preview_file_id` | New. Always set. The CR file resource id (`preview.files[i].id`) the pipeline picked via `first_accessible_file`. The document validation UI uses this to deep-link to the file screen in CentralReach. |
| `extracted_fields.text_source` | New. Always `"pdf_bedrock_extracted"` for centralreach records. |
| `extracted_fields.narrative_hash` | New. SHA-256 of the lowercased, whitespace-collapsed narrative. Consumed by rule 1 (`op_narrative_hash_unique`); identical hash function as `rpa.record.narrative_hash` so per-org dedup keys stay consistent. |
| `extracted_fields.template_id` | New. The CR templateId from preview, recorded for ops visibility. |
| `extracted_fields.note_provider_location` | New. Bedrock-extracted from the rendered PDF at ingest time. The location string the provider wrote on the note itself — may differ from `billing_list_location` (from the CR API). Omitted when Bedrock couldn't find one on the note. |
| `extracted_fields.note_provider_billed_time` | New. Bedrock-extracted, verbatim including units (e.g. `"75 minutes"`, `"1.25 hours"`). Not converted to minutes. Compared against `billing_list_time_worked_in_mins` by cross-check rules. |
| `extracted_fields.note_provider_billed` | New. Bedrock-extracted — the provider's name as it appears in the billed-provider or header section of the note (e.g. after a "Provider:" label). Distinct from `note_provider_signature_name` (the name at the bottom signature line): the two can differ when a note is billed under one provider but signed by another. Rule 7's three-way match compares `provider_display` (from the CR API), `note_provider_billed`, and `note_provider_signature_name`. |
| `extracted_fields.note_provider_signature_name` | New. Bedrock-extracted — the name at the provider signature line on the PDF. May differ from the preview's `providerSignatureName`. |
| `extracted_fields.note_supervisor_signature` | New. Bedrock-extracted boolean: `true` if a supervisor signature exists on the note, `false` if the note has a supervisor line but it's blank, omitted (via `None`) if the note has no supervisor line at all. Distinct from `supervisor_signature` (bool from the CR preview's provider-signature-present field). |
| `extracted_fields.note_supervisor_name` | New. Bedrock-extracted supervisor name text on the note. Compared against `supervisor_name` (from the CR preview) by cross-check rules. |
| `extracted_fields.billing_list_*` | New. Every non-identity column CR returned on the `/billing/query` list endpoint, mapped 1:1 from PascalCase to snake_case. `billing_list_` is the canonical form for list-endpoint values — rules reference e.g. `billing_list_time_worked_in_mins` and `billing_list_date_time_to` directly, no short-name aliases. Includes ~60 columns spanning session timing (`billing_list_date_time_from`, `billing_list_date_time_to`, `billing_list_creation_date`), session metadata (`billing_list_location`, `billing_list_time_worked_in_mins`, `billing_list_units_of_service`), financials (`billing_list_rate_client`, `billing_list_client_charges_total`), scheduling (`billing_list_authorization_id`, `billing_list_service_location_id`), and payor (`billing_list_payor_name`). Patient/provider identity columns are intentionally omitted; the record dataclass validator rejects records that smuggle them here. See `record_builder._BILLING_LIST_MAP` for the exhaustive list. |
| Source attribution | `source = "centralreach.api"` instead of `"rpa.centralreach"`. |

Two prefixes signal source unambiguously on `extracted_fields`:
* `billing_list_*` — from CR's `/billing/query` list endpoint,
  PascalCase → snake_case. Canonical form for list-endpoint values.
* `note_*` — Bedrock-extracted from the rendered PDF at ingest time.

Rules reference the prefixed names directly (`billing_list_time_worked_in_mins`,
`billing_list_date_time_to`, etc.); there are no short-name aliases
for list-endpoint columns. One value, one key. The `note_*` pattern
is the deliberate exception where two prefixed variants of the same
semantic value coexist — the API-derived `billing_list_location` and
the PDF-derived `note_provider_location` are meant to be
cross-checked by rules 6/7.

Preview-endpoint fields (`signed_at`, `provider_signature`,
`supervisor_signature`, `supervisor_name`) don't use a prefix.
Nothing else sources those values, so a `preview_` prefix would add
noise without resolving ambiguity.
| Patient hash inputs | `(first_name, last_name, ClientId)` instead of `(first_name, last_name, DOB)`. |

The on-disk JSON shape stays consistent for fields the rules engine
already reads (`source_record_id`, `encounter`, `patient`,
`extracted_fields.signed_at`, etc.). New fields are additive.

The dataclass validator accepts either `text` populated OR
`pdf_s3_key` set ("text or pdf_s3_key required"). centralreach
records satisfy both. The existing PHI-identity forbidden-keys check
on `extracted_fields` stays as-is.

## Bedrock rule evaluation for narrative-derived rules

This is a real change to the rules engine, not just the ingest path.
Calling it out so the scope is honest.

### The pattern

centralreach records carry both the Bedrock-extracted narrative on
`text` AND a reference to the original PDF on
`extracted_fields.pdf_s3_key`. Rules dispatch by which input they
need:

* **Default rules (1, 2, 3)** read `record.text` directly — same
  text path the rules engine already uses for non-centralreach
  records. Rule 1 (`op_narrative_hash_unique`) reads
  `extracted_fields.narrative_hash`, which was computed from the
  same `text` value at ingest time. No PDF fetch, no document-block
  Bedrock call, no per-eval extraction cost.
* **Rule 11** (data present in note as charts/percentages/graphs)
  opts into the PDF path via `requires_pdf: true` on its
  `rule_config` row. The validator fetches the PDF from S3 and ships
  it as a `{"type": "document"}` block. The narrative prose doesn't
  contain the visualizations rule 11 needs, so paying the
  document-extraction cost on every eval is unavoidable for that
  specific rule.

The deterministic rules (4, 5, 6, 7, 10, 12) still inspect the
record's structured fields (`signed_at`, `billed_start`,
`billed_location`, etc.) and don't touch text or PDF. ~half the rule
matrix is unchanged.

Rule 5 ("billed minutes matches session length") ships in DynamoDB
as a numeric equality with a `compare_expr` that computes the
comparison value inline from other fields:

```json
{
  "description": "billed_minutes == duration_minutes(billed_start, billed_end)",
  "field": "billed_minutes",
  "operator": "eq",
  "compare_expr": {
    "op": "duration_minutes",
    "from": "billed_start",
    "to": "billed_end"
  }
}
```

The `compare_expr` machinery lives in
`lambda/multi-org/rules-engine/deterministic_evaluator.py::COMPARE_EXPR_OPS`.
Source field names (`from`, `to`) are part of the rule config, so an
org with vendor-specific naming (`session_start`, `session_end`, etc.)
overrides them in DynamoDB without a code change. Adding a new
derivation type (e.g. `age_in_years`) is a single entry in
`COMPARE_EXPR_OPS`, not a refactor of the rule schema.

Failure modes for rule 5 all surface as SKIP with a clear message
rather than silently PASS/FAIL:
* `billed_start` or `billed_end` absent → "Comparison field '...' not found"
* Either value is unparseable as ISO-8601 → "Could not parse datetime"
* Typo in `op` → "Unknown compare_expr op: '...'"

This matches the philosophy for the rest of the deterministic
evaluator: missing/malformed data is a data-quality signal for the
operator, not a rule failure.

### Ingest-time extractions

Two Bedrock extractions run per record between PDF fetch and record
build. Both use the `centralreach_*_extract` `call_type` for
per-org cost attribution:

1. **`narrative_extractor.extract_narrative`** — the free-text
   clinical narrative. Populates `record.text` and
   `extracted_fields.narrative_hash`.
2. **`note_fields_extractor.extract_note_fields`** — five structured
   fields that appear on the rendered note but may differ from the
   values CR's list/preview endpoints returned:
   `note_provider_location`, `note_provider_billed_time`,
   `note_provider_billed`, `note_provider_signature_name`,
   `note_supervisor_signature`,
   `note_supervisor_name`. Every field is optional — a `null` from
   Bedrock means "not present on the note," and the builder omits
   the corresponding key so a rule can distinguish absence from a
   false match against empty string.

The `note_` prefix on the structured-extraction fields is
deliberate: rules 6 (location matches) and 7 (supervisor
documentation) cross-check the note-derived value against the
CR-API-derived value. Naming them separately keeps that comparison
explicit rather than relying on the rule engine to know which
`location` came from which source.

Both extractors follow the same pattern:

`centralreach.narrative_extractor.extract_narrative(pdf_bytes, ...)`
runs once per record between PDF fetch and record build. It sends
the PDF to Bedrock with a fixed system prompt that demands verbatim
narrative prose (no headers, no signature blocks, no identifiers).
The response is validated (non-empty `narrative` string, ≤8192
chars) and returned. Failure paths surface as
`NarrativeExtractionError`; the pipeline catches it and records a
skip with reason `narrative_extract_failed` so ops can monitor
extraction health independently.

Cost attribution: extraction calls use
`call_type="centralreach_narrative_extract"` so per-org dashboards
can split extraction spend from rule-eval spend (which uses
`chart_rule_validate` for text-path rules and
`centralreach_rule_validate:{rule_id}` for `requires_pdf` rules).

Why ingest-time:
* Rule 1's narrative_hash needs to be part of the record's identity
  from the start — running extraction at eval time would mean every
  rule-engine run pays the extraction cost.
* Text-path rules cost ~10x less against the ~1K-token narrative
  than against a ~10K-token PDF document block.
* Records are immutable once written, so the narrative + hash never
  needs recomputation.

### How the PDF flows into the existing Bedrock call (rule 11)

The existing `_validate_rule` (and `_extract_rule_fields`) build a
Bedrock message body and call `invoke_claude_model`. For rules with
`requires_pdf: true`, the only difference is the content block
construction:

```python
def _validate_rule(model_id, rule_text, notes, data, extracted_fields=None,
                   *, org_id=None, validation_run_id=None):
    chart_input_kind, chart_input = _load_chart_input(data)

    content = [
        {"type": "text", "text": f"Rule:\n{rule_text}\n\nNotes:\n{notes_text}"},
    ]
    if chart_input_kind == "text":
        content.append({"type": "text", "text": f"Chart text:\n\n{chart_input}"})
    else:  # "pdf"
        content.append({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": base64.b64encode(chart_input).decode("ascii"),
            },
        })
        # also emit the standard read-of-clinical-note audit event for
        # this PDF access; the existing 'execute BedrockPrompt' audit
        # in document_validator.py continues to fire for the Bedrock
        # call itself
        audit_emit_clinical_note_read(
            data, call_type="bedrock_rule_eval",
            extra={"rule_id": rule_id},
        )

    if extracted_fields:
        content.append({"type": "text",
                        "text": f"Extracted fields:\n\n{json.dumps(extracted_fields)}"})
    content.append({"type": "text", "text": f"JSON schema:\n\n{json.dumps(json_schema)}"})

    body = {"system": SYSTEM_PROMPT,
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "temperature": 0.01,
            "messages": [{"role": "user", "content": content}]}

    return invoke_claude_model(
        inference_profile_id=model_id, body=body, return_json_only=True,
        org_id=org_id, call_type=f"centralreach_rule_eval:{rule_id}",
        parent_request_id=validation_run_id,
    )
```

The pattern is the same for `_extract_rule_fields`. We reuse
`invoke_claude_model` from
`lambda/multi-org/rules-engine/bedrock_client.py` — it already
handles rate limiting via `bedrock_rate_limiter`, per-call cost
emission via `claude_cost.record_cost`, JSON extraction + retry, and
org-attribution.

Each daily run sends every entry's PDF through Bedrock.

### Prompts come from the existing per-org rules in DynamoDB

Rules live in DynamoDB under `pk=ORG#{org_id}, sk=RULES_CONFIG` with
fields `rule_id`, `name`, `type` (`"llm"` or `"deterministic"`),
`rule_text`, `notes`, and optional `fields_to_extract`. The existing
`evaluate_llm_rule` in
`lambda/multi-org/rules-engine/document_validator.py` assembles a
Bedrock prompt at runtime from these fields plus a fixed system
prompt ("You are a Healthcare Compliance Auditor..."), forcing
structured JSON output via an inline schema. The operator-owned rule
text in DynamoDB is the source of truth.

For centralreach records, the validator dispatches per rule. Rules
without `requires_pdf` get `chart_text = data['text']` — the
narrative extracted at ingest time. Rules with `requires_pdf: true`
fetch the PDF bytes from S3 via `pdf_s3_key` and add a
`{"type": "document", "source": {"type": "base64",
"media_type": "application/pdf", "data": <b64>}}` content block to
the Bedrock message alongside the existing rule/notes/schema text
blocks. Same system prompt, same `rule_text`, same `notes`, same
forced JSON schema, same `invoke_claude_model` call — just one
additional content block on the message.

Concrete edits to `document_validator.py`:

1. `_load_chart_input(data, rule_config)` returns either `("text",
   chart_text)` or `("pdf", pdf_bytes)` based on whether the rule
   opts into the PDF path via `rule_config.requires_pdf` AND the
   record has a `pdf_s3_key`. Without `requires_pdf`, the text path
   wins (including for centralreach records, which now carry
   `text`). The legacy fallback (empty text → fields JSON) remains
   for records without text.
2. `_extract_rule_fields` and `_validate_rule` check the tag and build
   either a text content block (existing behavior) or a text + document
   content block (PDF path). The system prompt, schema, and
   `rule_text`/`notes` content blocks are unchanged.
3. The audit emission in those functions stays as-is. The PDF S3 GET
   gets its own audit event (`read ClinicalNote`,
   `call_type=bedrock_rule_eval`) before the Bedrock invocation; same
   pattern as today's `audit_emit('execute', ...)` for the Bedrock
   call. Text-path evals do NOT emit this PDF-read audit because no
   PDF read occurred.

Operators continue to edit rule definitions the same way they do
today (DynamoDB writes via the existing admin tooling). Rule 11's
DynamoDB row carries `requires_pdf: true`; other rules omit the
field (defaults to false).

### Cost reality at 100% PDF

Numbers worth surfacing:

- 350 entries/day × 3 LLM-evaluated rules = 1,050 Bedrock calls/day/org
- Each call: ~10K input tokens (PDF + prompt) + ~200 output tokens
- Sonnet 4.5 input pricing: ~$3/M tokens; output: ~$15/M
- Daily cost per org: ~$30-50/day
- Monthly: $900-1500/org

Backfill runs over a date range cost proportionally more (same
per-day spend × number of days). Rule_text iteration that re-runs a
date range against new wording costs the full range each time.

The existing `claude_cost.record_cost` already emits per-call cost
attribution to CloudWatch; we reuse it for visibility.

### Audit emission

Every Bedrock invocation is a PHI access event:

- `actor`: rules engine system principal
- `resource`: record's source_record_id, org_id
- `action`: `read`
- `call_type`: `bedrock_rule_eval`
- `result.rule_id`: which rule
- `result.model_id`: the exact Bedrock model id used

Volume: 350 entries × 3 rules = 1,050 audit events/day/org for
LLM-rule evaluations. Plus the existing ingest events (350/day/org).
Audit table sized accordingly.

### Determinism trade-off, made explicit

Rules evaluated against PDFs produce LLM judgments that can vary
across model version upgrades. Mitigations:

- Audit event records `result.model_id`; auditors can trace which
  model version produced a given judgment
- Temperature is set to 0 on every Bedrock call to minimize
  within-version variance
- Bedrock retries with backoff if the response is unparseable;
  unparseable-after-retry is recorded as `judgment: "indeterminate"`
  (not pass, not fail)

This is documented explicitly so compliance auditors aren't surprised:
"rules 2, 3, and 11 are LLM-derived against rendered PDF content
under Bedrock model X; the audit trail records the model id used per
evaluation."

## Authentication

Adapted from `lambda/multi-org/rpa/authenticators/centralreach.py`.
Three steps now, was two:

1. POST `client_id + client_secret` → CR SSO `/connect/token` → JWT
2. POST `{token: jwt}` → `/api/?framework.authtoken` → cookies (`crsd`,
   `crud`)
3. **New**: GET `/api/?framework.csrf` with cookies → CSRF token,
   delivered via `Set-Cookie` response headers (NOT in the body)

### CSRF endpoint — `Set-Cookie`, not body

Despite the documented intent that this endpoint returns a CSRF token,
the actual response shape (verified against a real CR session) is:

- **Body**: a status object — `{"success":true,"result":"ok","cacheExpires":"...",...}`.
  No token. We parse this only to assert `success == true` and fail
  loud if CR ever changes the contract.
- **`Set-Cookie` headers**: where the token actually lives. Three
  cookies set in our test response — `csrf-token=<value>` (the token
  itself), a refreshed `crsd=<new value>`, and `uiver=<version>`.

The auth flow must read `Set-Cookie` headers from this response, parse
out the `csrf-token` value, AND merge any other rotated cookies into
the session's cookie jar before continuing. **`crsd` rotation is real**:
the value returned by step 2 is invalidated by step 3, and using the
step-2 value on subsequent requests returns 401/403.

### Double-submit cookie pattern

Every CR internal-API request after auth must carry the CSRF token in
TWO places:

1. As an `x-csrf-token` request header
2. As a `csrf-token` cookie in the request's cookie jar (alongside
   `crsd` + `crud`)

CR's server-side compares the two; mismatched values get rejected
regardless of how good the cookies are. The HTTP client must maintain
a cookie jar (not a static cookie list) and write the CSRF token into
both header and jar on every request.

### HTTP client owns a live cookie jar

The client maintains a `http.cookiejar.CookieJar` (Python stdlib) and
points the urllib opener at it. Every response feeds its `Set-Cookie`
headers back into the jar before the next request reads from it. The
"cookies returned by auth" concept is a *starting state*, not a
durable snapshot — every CR endpoint can set or rotate cookies, and
the client must honor that.

Concretely:

- The auth flow seeds the jar with cookies from steps 2 + 3
- Every subsequent request reads cookies from the jar at send time
- Every response writes its `Set-Cookie` headers back into the jar
  before the next request

This matters because the CSRF endpoint demonstrably rotates `crsd`,
and we should assume other endpoints can do the same. Pinning cookie
values into shell variables or a static dict — what `cr_session_env.py`
and the curl-based smoke tests do — is fine for one-shot testing but
not for sustained sessions. The runtime client cannot afford to drop
rotated cookies between requests.

A failure mode worth flagging: shell-based testing of this integration
will show intermittent `success: false` responses that don't reproduce
under a real cookie-jar-equipped client. That is the cookie-rotation
gap surfacing.

### Additional cookies observed in production

Beyond `crsd` + `crud` + `csrf-token`, requests to CR's internal API
include:

- `tzoffset=<minutes>` — UTC offset for the org's timezone, in
  minutes. The browser sets this from the user's local clock. We set
  it from the org's configured timezone in `RPA_CONFIG.guardrails.timezone`
  (resolve via `zoneinfo` at session start). Without this cookie some
  endpoints reject the request.

### Auth result shape

```python
{
    "cookies": [
        {"name": "crsd", ...},        # the rotated value from step 3
        {"name": "crud", ...},
        {"name": "csrf-token", ...},  # equal to extra_http_headers["x-csrf-token"]
        {"name": "tzoffset", ...},    # from org timezone
    ],
    "extra_http_headers": {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "x-csrf-token": "<token>",
        "x-crapi-clientsource": "members-spa",
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://members.centralreach.com/",
    },
    "access_token": "<jwt>",  # diagnostic, never logged
}
```

### ServiceStack content negotiation

CR's `crxapi` endpoints are ServiceStack services. ServiceStack
content-negotiates the response format from the `Accept` header — when
the request looks browser-like (`Accept: */*` plus typical browser
context), ServiceStack defaults to **HTML and returns its metadata
snapshot page for the request DTO**, not the JSON the caller wants.
The snapshot page is HTML titled
`Snapshot of <RequestType> generated by ServiceStack`. The status
code is still 200; only the body and `Content-Type` change.

The captured XHR requests we reverse-engineered worked because the
browser set `Sec-Fetch-Mode: cors` and `Sec-Fetch-Dest: empty` —
fetch-metadata headers ServiceStack uses to distinguish XHR from
top-level navigation. The runtime client does NOT send those headers
(they're browser-restricted per the Fetch spec). Instead, the client
sends the same `Accept` value the browser sends —
`application/json, text/javascript, */*; q=0.01` — plus
`x-requested-with: XMLHttpRequest` on every request. Together these
match the browser closely enough that ServiceStack content-negotiates
to JSON.

We send the verbatim browser `Accept` value rather than a simpler
`application/json` because empirical testing showed bare
`application/json` is rejected on some endpoints (returns HTML
metadata-snapshot page instead). The browser's value is what we
observe working; we mirror it.

A wrong `Accept` value is also why the client must inspect response
`Content-Type`, not just the status code — a successful 200 with
`text/html` is an integration failure, not a successful API call.
See [Error handling](#error-handling) for the failure type.

### Retry semantics

If a request 403s with a CSRF-related response, the client re-fetches
the token via `/api/?framework.csrf` once. The retry **must** merge
the new `Set-Cookie` rotations into the jar before replaying the
original request — including any `crsd` rotation. A second 403
propagates as `CentralReachAuthError`.

The CSRF retry rate is worth instrumenting as a CloudWatch metric:
sustained nonzero indicates either CR rotating tokens faster than the
documented session lifetime or session state drifting in some other
way. See Risks #5.

## PDF storage

### Bucket

The same per-org bucket the existing record-write path uses:
`penguin-health-{org_id}`. See
[`lambda/api/nl_agent_tools.py:1225`](../lambda/api/nl_agent_tools.py)
and
[`lambda/multi-org/rpa/result_writer.py:42`](../lambda/multi-org/rpa/result_writer.py).

PDFs are PHI but they're the same class of PHI the JSON record
already carries (signature timestamps, billing metadata, patient
hashes). The bucket-level KMS configuration that protects the JSON
records protects PDFs too — no per-object override required. A
separate bucket would only buy distinct lifecycle, KMS, or access
policy, and none of those apply.

### Key

```
pdfs/{ingest_date}/{captured_at_compact}__{source_record_id}.pdf
```

Mirroring the JSON record key shape:

| | JSON record | PDF |
|---|---|---|
| Bucket | `penguin-health-{org_id}` | `penguin-health-{org_id}` |
| Key | `data/{YYYY-MM-DD}/{ts}__{source_record_id}.json` | `pdfs/{YYYY-MM-DD}/{ts}__{source_record_id}.pdf` |

Symmetric naming means the JSON record and the PDF for the same
billing entry sit at the same date partition with matching basenames.
The CR-supplied filename from the `resources.getresourceurl` response
(e.g. `2026/6/28/<id>.pdf`, with the doc-name embedding patient
identity) is NOT preserved — the filename in S3 derives only from
`source_record_id`, which is CR's internal billing entry id.

`ingest_date` is the run's wall-clock date in `America/New_York`, not
UTC. A cron firing just after 00:00 UTC still belongs to the prior US
clinical day, matching the `parameters.resolve_date_range` default
("yesterday-Eastern"). Non-Eastern orgs will need a per-org tz here
when they land — do not generalize preemptively.

### Object metadata

Set on every PUT:

- `x-amz-meta-source-record-id`: the record's `source_record_id`
- `x-amz-meta-template-id`: the CR `templateId` from the preview
  response

Neither is PHI. Both are useful for ops queries against S3 inventory
without parsing JSON records.

Do NOT include patient identity fields in metadata. S3 object
metadata is logged in CloudTrail data events when enabled and is not
covered by the bucket-level KMS encryption applied to object bodies.

### IAM

Two role grants alongside the existing record-write IAM:

- Centralreach Fargate ingest role: `s3:PutObject` on
  `arn:aws:s3:::penguin-health-{org_id}/pdfs/*`
- Rules engine role: `s3:GetObject` and `s3:HeadObject` on
  `arn:aws:s3:::penguin-health-{org_id}/pdfs/*`

Per-org scoping; no wildcards on actions; no `s3:*`.

### Lifecycle

No expiration on the `pdfs/` prefix. The PDFs are the source of
truth for narrative-derived rule judgments — if a PDF gets deleted,
those judgments cannot be re-derived on any future run.

If the per-org bucket has a non-prefix-scoped lifecycle policy from
prior work, that gets migrated to a prefix-scoped policy before
centralreach PDFs land. The centralreach ingest role does NOT have
`s3:PutLifecycleConfiguration` — only the bucket owner role does.

## Error handling

### Per-run

- Auth failure → run aborts, audit emits `run_failed` with
  `reason="auth"`
- List query failure → run aborts (nothing to process); audit
  `run_failed` with `reason="list_query"`
- Anything else propagates to per-entry handling

### Per-entry

The pipeline catches all `CentralReachError` subclasses and any
`HTTPError` from the client; converts to structured failure events:

```python
try:
    process_entry(entry)
except (CentralReachError, HTTPError) as e:
    failure_count += 1
    structured_failures.append({
        "billing_entry_id": entry.Id,
        "error_type": type(e).__name__,
        "message": _redact(str(e)),
    })
    # Continue — partial run is fine
```

**Skips are not failures.** When `preview.files == []` (unsigned or
draft entry), the pipeline records a skip with a reason — distinct
from a failure. Skipped entries do NOT count toward the failure-rate
alarm; they reflect normal CR workflow state (the provider hasn't
signed yet) and will be ingested on a later run once the PDF exists.

End of run emits `run_completed` audit with:

```json
{
  "processed_count": 320,
  "failure_count": 3,
  "skipped_count": 27,
  "failures_by_type": {"HTTPError": 2, "PdfFetchError": 1},
  "skipped_by_reason": {"no_pdf_available": 27}
}
```

Dashboard alerts on `failure_count > 5% of processed_count`.
Skip rate is dashboarded separately — sustained high skip rate
(e.g., >50% of entries unsigned consistently) is an operational
signal worth surfacing to the org, but not an integration error.

### CR returns 200 + ServiceStack validation error

CR is a .NET ServiceStack service. When a request fails server-side
validation, CR returns HTTP **200** with a body shaped like:

```json
{
  "result": "OK",
  "failed": false,
  "cachedTime": 0,
  "responseStatus": {
    "errorCode": "NotEmpty",
    "message": "'DateRange' must not be empty.",
    "errors": [
      {
        "errorCode": "NotEmpty",
        "fieldName": "DateRange",
        "message": "'DateRange' must not be empty.",
        "meta": {"PropertyName": "DateRange"}
      }
    ]
  }
}
```

Two surprises:

1. `result` is `"OK"` and `failed` is `false` even though the request
   failed. The top-level fields are misleading — they reflect the HTTP
   transport's success, not the API call's success.
2. The validation failure lives under `responseStatus`. A populated
   `responseStatus.errors` array is the authoritative "this call
   failed" signal.

The API client must inspect every response for `responseStatus.errors`
before treating a 200 as success. A failed validation is a per-call
error of type `CentralReachValidationError` carrying the offending
`fieldName` from the first errors entry. The pipeline treats it the
same as any other per-entry failure (skip + log + continue).

`responseStatus.errors[].fieldName` is bytewise non-PHI (it's a schema
field name like `"DateRange"`, not a value), so it's safe to include
in audit and dashboard rollups. `responseStatus.errors[].message`
quotes the field name back; not PHI but redundant.

### CR returns 200 + HTML (ServiceStack snapshot page)

When ServiceStack content-negotiates against an `Accept` header it
can't disambiguate from a browser navigation, it returns an HTML
metadata snapshot page for the request type with status 200. Body
starts with `<h1>Snapshot of <i>...Request</i> generated by
ServiceStack</h1>`.

The client must check `Content-Type` on every response and raise
`CentralReachContentTypeError` when the response's content type does
not match what the endpoint is documented to return. Treating this
as a JSON parse error obscures the cause; the actual fix is to
review the request's `Accept` header.

This failure should not happen at runtime once the client always
sends `Accept: application/json`. If it does occur, it indicates CR
changed its content negotiation rules — investigate the captured
request shape against the live UI's network panel before patching.

### Per-Bedrock-call (rules engine)

- Bedrock throttle → SDK retry with backoff (existing
  `bedrock_rate_limiter` handles)
- Bedrock returns unparseable response after one retry → judgment
  recorded as `indeterminate` (not pass, not fail) with rationale
  text from the raw response stored for forensic review

## PHI handling

Concerns specific to this design.

### Bedrock invocation logging must be OFF

Bedrock can write request/response bodies to CloudWatch or S3. If
enabled, every PDF the rules engine sends to Bedrock lands in
CloudWatch as raw PHI. **Must be confirmed off in the AWS account
before any production run.**

Verification step in pre-prod checklist:
`aws bedrock get-model-invocation-logging-configuration` returns no
logging destination configured.

### BAA coverage

Confirmed (per design conversation): AWS BAA covers Bedrock for our
healthcare use case.

### Audit emission volume

| Event | Per-record per-day count |
|---|---|
| `centralreach_note_ingest` | 1 per record |
| `bedrock_rule_eval` (per LLM-evaluated rule) | up to 3 per record |

At 350 records × 4 events worst case = 1,400 audit events/day/org.
Existing audit infra handles this volume.

### Signature handling

Signatures (base64 PNG image data) are present in the preview
response under `providerSignature`. We **do not** persist these
bytes anywhere. The record stores:

- `extracted_fields.signed_at` (ISO timestamp from
  `providerSignatureCreationDate`)
- `extracted_fields.provider_signature` (boolean — true if
  `providerSignature` is non-empty)
- `extracted_fields.supervisor_name` (from `providerSignatureName`)

No raw signature bytes leave runner memory.

### PDFs in S3 are PHI

The PDF prefix on the per-org bucket carries the same PHI category
as the JSON record prefix: signature timestamps, billing metadata,
patient hashes. Bucket-level KMS encryption applies to both. See
[PDF storage](#pdf-storage) for the full bucket, key, IAM, and
lifecycle spec.

### Determinism of rule evaluation

Rules 2, 3, 11 are LLM-derived. Auditor expectations should be set
accordingly during onboarding.

## Rename surface

Touches IAM, audit, S3 prefixes, Secrets Manager paths, DynamoDB
schema. None has production traffic so this is a clean rename, not a
migration.

| Surface | Old | New |
|---|---|---|
| Python module | `lambda/multi-org/rpa/` | `lambda/multi-org/centralreach/` |
| Secrets Manager | `penguin-health/rpa/{org}/credentials` | `penguin-health/centralreach/{org}/credentials` |
| DynamoDB sk | `RPA_CONFIG` | `CENTRALREACH_CONFIG` |
| DynamoDB sk | `RPA_PLAYBOOK#*` | **deleted** |
| Record `source` field | `rpa.centralreach` | `centralreach.api` |
| Audit `call_type` | `rpa_note_extraction` | `centralreach_note_ingest` (ingest) |
| Audit `call_type` | n/a | `bedrock_rule_eval` (rule eval) |
| EventBridge source | `penguin-health.rpa` | `penguin-health.centralreach` |
| EventBridge detail-type | `RpaIngestComplete` | `CentralReachIngestComplete` |
| Fargate task | `fargate/rpa_runner/` | `fargate/centralreach_ingest/` |
| Lambda config seeder | `scripts/multi-org/add_rpa_config.py` | `scripts/multi-org/add_centralreach_config.py` |
| Exceptions | `RpaError`, `RpaAuthError`, etc. | `CentralReachError`, `CentralReachAuthError`, etc. |
| Dataclass | `RpaNoteRecord` | `CentralReachNoteRecord` |
| S3 prefix (records) | `data/{ingest_date}/` | unchanged — rules engine reads this |
| S3 prefix (PDFs) | n/a | `penguin-health-{org_id}/pdfs/{ingest_date}/` — new |
| Test paths | `tests/unit/rpa/` | `tests/unit/centralreach/` |

The on-disk JSON record shape stays compatible with the rules engine
— field names within the record do not change.

## Implementation sequence

Six PRs. Each lands independently, with tests, and leaves the
codebase runnable between PRs. The Fargate task isn't wired up
until PR F.

| PR | Scope |
|---|---|
| A | New `centralreach/` module skeleton: `auth.py` with CSRF, `client.py` (session + rate limiter + retry + default headers), `exceptions.py`. Tests for each. |
| B | API call wrappers: `list_query.py`, `preview.py`, `resources.py`. Typed response dataclasses. Captured-fixture tests against the redacted samples in this design conversation. |
| C | `pdf_storage.py`, `record_builder.py`, `CentralReachNoteRecord` dataclass (renamed `RpaNoteRecord`, text/body_html optional, new fields). Tests for record construction with PDF strategy. |
| D | `pipeline.py` orchestration end-to-end with all endpoints mocked + per-entry failure handling + structured failure event emission. |
| E | Rules engine integration: modify `document_validator.py` only. New `_load_chart_input(data)` helper picks text vs PDF. `_extract_rule_fields` and `_validate_rule` add a `{"type": "document", ...}` content block for PDF records; existing `rule_text` / `notes` / schema content blocks unchanged. |
| F | Fargate task definition + IAM + EventBridge schedule + Dockerfile + container build. Delete `lambda/multi-org/rpa/`, `fargate/rpa_runner/`, `scripts/multi-org/seed_rpa_playbook.py`, `playbooks/centralreach/`. README and onboarding doc updates. |

PR E is the only one that touches the rules engine. Flag explicitly
in PR description that the rules engine now has a Bedrock dependency
for rules 2, 3, 11.

## Risks

In rough order of how much they matter.

**1. CR endpoints carry implicit contract complexity we don't fully
understand.** Undocumented APIs have no formal contract. Reverse-
engineering against the live UI surfaced several behaviors that aren't
intuitive from the captured request shapes alone, each of which broke
naive shell-based testing before we found the right detail:

- ServiceStack content negotiation defaults to HTML metadata pages
  when `Accept` doesn't include `text/javascript` or fetch-metadata
  headers — easy to mistake for an auth failure
- Cookies (including `crsd`) rotate on responses, not just at auth
- The CSRF token arrives as a `Set-Cookie` header, not in the body
- Different endpoints use different success signals (`result:"OK"`
  + `failed:false` vs `result:"ok"` + `success:true`)
- `resourceId` is the file resource id from `preview.files[0].id`,
  not the billing entry id (subtle in captures where the values
  happened to coincide)
- `_utcOffsetMinutes` must match the org's tz on file with CR,
  not be a constant
- Endpoints can return HTTP 200 with `success: false` and no
  diagnostic detail, for reasons not yet fully understood

Mitigations: structured failure events surface breakage immediately
(failure_count alarms); integration tests run against captured
fixtures so we can detect drift in CI; the HTTP client is structured
around a real cookie jar and per-endpoint success-check helpers so
new contract surprises are localized to one place. We accept the
remaining risk that further surprises will surface during the first
weeks of production runs.

**2. Bedrock judgment quality may not match what auditors expect.**
LLM-derived rule judgments are non-deterministic across model
upgrades and have a small within-version variance even at
temperature 0. Mitigations: model id recorded per evaluation in the
audit trail; held-out judgment-quality regression suite gates
rule_text changes; every run re-judges against the current model so
no stale judgments persist.

**3. Cost growth.** $30-50/day/org steady-state at 350 entries; grows
linearly with volume. If an org doubles to 700 entries, cost doubles.
Per-org capacity planning is an ops responsibility during onboarding.

**4. PDF lifecycle ties us to bytes indefinitely.** If a PDF gets
lifecycle-deleted, the LLM judgments cannot be re-derived. Mitigation:
retention policy matches record retention (indefinite for compliance);
lifecycle policy is documented as "do not enable expiration on this
prefix."

**5. CSRF flow may fail silently.** Mitigation: client retries auth
once on 403; second failure raises. Instrument a CSRF-retry-rate
metric for early drift detection.

**6. CR may stop exposing the endpoints to bot users.** If CR adds
rate-limiting, IP-blocking, or CAPTCHA-style protection on the
internal API path, the bot is broken. Mitigation: 1500ms pacing
mimics UI cadence; we don't burst; we don't pattern as
session-rotating bot traffic. Single long-running Fargate session
per org is closer to a human user than rapid-fire short sessions.

## Open questions

These don't block the design doc but block implementation.

- **Service-account auth produces wrong session shape for per-user
  resources.** The existing `client_credentials` OAuth flow (the one
  in `lambda/multi-org/rpa/authenticators/centralreach.py`) produces
  a session whose `crud` cookie wraps an SSO JWT
  (`{"ssoUser":{"iss":"...","nbf":...}}`). The browser session of the
  same user has `crud={"type":<int>,"id":<int>,"isx":<bool>}` — an
  internal user-identity object. Endpoints that perform per-user
  permission checks (verified empirically: `resources.getresourceurl`)
  return `success: false` with no URL for service-account sessions,
  even though the same user accesses the same resource fine through
  the UI. The OAuth flow is insufficient for accessing per-user
  resources; the bot needs a real CR user session.

  Open subquestions:
  1. Does CR expose a ServiceStack auth endpoint
     (`/auth/credentials` or similar) that accepts username+password
     and issues `ss-id`+`ss-pid`+user-shaped `crud`? Test by POSTing
     bot credentials and watching the response.
  2. Or is there an SSO-JWT-to-user-session exchange endpoint we
     haven't discovered? Ask CR.
  3. If neither: bot users must be provisioned as real CR users with
     usernames + passwords, stored in Secrets Manager alongside (or
     replacing) `client_id`/`client_secret`. Operational change to
     org onboarding.

  Blocks: PR B (API call wrappers — `preview`, `resources.getresourceurl`,
  `notes` all need working auth), all downstream PRs. Does NOT block
  PR A (HTTP client with cookie jar, generic retry semantics, default
  headers are auth-agnostic).

  Discovered: 2026-06-30 via comparing Copy-as-cURL from a working
  browser session against equivalent shell-built curl using
  `cr_session_env.py` cookies. The browser request succeeded; the
  shell request failed with identical headers, body, and `crsd`/
  `crud`/`csrf-token` cookies — but with our SSO-shaped `crud`
  instead of the user-session `crud`.

- **Existing rule_text quality for PDF input.** Today's `rule_text`
  values in DynamoDB were written assuming text input. They likely
  work as-is for PDF input (the Bedrock model reads the PDF and
  evaluates the same question), but a held-out regression check
  against a sample of real PDFs validates this before production.

## What this doc does not cover

- Held-out judgment-quality regression set construction. Pre-PR-E
  work.
