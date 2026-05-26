# Stedi Insurance Eligibility Integration

Real-time insurance verification for behavioral-health admissions, powered by
Stedi's `/insurance-discovery` and `/medicalnetwork/eligibility/v3` APIs. This
package owns the full lifecycle of an eligibility check — from the manual
"Verify Patient" form to the scheduled morning census sweep that pre-runs
verification on every admission overnight.

For the workflow context (why this exists, what UR/Admissions does today,
which payers matter), see the plan file at `~/.claude/plans/walk-me-through-a-hashed-crayon.md`.

## Architecture at a glance

```
                           ┌────────── Manual: Verify Patient form ────────────────┐
                           │                                                       │
                           ▼                                                       │
                  POST /eligibility/verify ─────────► eligibility_api.verify ──┐   │
                                                                               │   │
  EventBridge cron (per org)                                                   │   │
  "cron(0 11 * * ? *)"                                                         │   │
         │                                                                     ▼   │
         ▼                                                              ┌───────────────┐
  penguin-health-census-runner                                          │  orchestrator │
    1. load_stedi_config(org_id)                                        │   .verify()   │
    2. ensure_demo_history_seeds       ─────────────────────────────►   │               │
    3. for patient in roster:                                           │   3-branch    │
       a. reserve_capacity (daily cap)                                  │   decision    │
       b. orchestrator.verify(...)                                      │   tree        │
       c. classify result                                               └───────┬───────┘
       d. put_item CENSUS_ITEM#                                                 │
    4. patch CENSUS_RUN# with totals                                            ▼
                                                                       ┌─────────────────┐
                                                                       │   Stedi client  │
                                                                       │ (or demo proxy) │
                                                                       └────────┬────────┘
                                                                                │
                                                  ┌─────────────────────────────┴────────────────────┐
                                                  ▼                                                  ▼
                                  https://healthcare.us.stedi.com                             demo_fixtures
                                  /2024-04-01/insurance-discovery/check/v1                    (canned 10 patients)
                                  /2024-04-01/change/medicalnetwork/eligibility/v3
```

## Module layout

| File | What it does |
|---|---|
| [stedi_client.py](stedi_client.py) | HTTPS client for Stedi's two endpoints. Retry/backoff on 429/5xx. SSN + member-ID redaction in logs. Forwards `X-Forwarded-For` (CMS-mandated). |
| [demo_client.py](demo_client.py) | Drop-in replacement for `StediClient` that returns canned fixtures based on patient name+DOB or (member_id, payer_id). Used when `STEDI_CONFIG.demo_mode == true`. |
| [demo_fixtures.py](demo_fixtures.py) | 10-patient roster, `SCENARIOS` (name → canned discovery + eligibility), `ELIGIBILITY_DIRECT_SCENARIOS` (member_id → canned eligibility), helper builders for inactive/Humana/auth-required/no-BH responses, `DEMO_HISTORY_SEEDS` (Linda's prior-Cigna audit row). |
| [client_factory.py](client_factory.py) | `build_client(org_config, client_ip)` — returns either `DemoStediClient(None)` or `StediClient(api_key, client_ip)` based on demo_mode. Used by both the API handler and the census runner. |
| [config.py](config.py) | `load_stedi_config(org_id)` — reads `STEDI_CONFIG` from the `penguin-health-org-config` DynamoDB table. lru_cache'd. |
| [secrets.py](secrets.py) | `get_stedi_api_key()` — Secrets Manager fetcher, cached for Lambda lifetime. |
| [payer_registry.py](payer_registry.py) | Static map of Stedi `tradingPartnerServiceId` ↔ friendly payer name. Used by transformers and the UI dropdown. |
| [eligibility_transformer.py](eligibility_transformer.py) | Stedi 271 → simplified `EligibilityResult` (active, status, payer, plan, copays, deductibles, auth_required, **service_types breakdown**, notes). |
| [discovery_transformer.py](discovery_transformer.py) | Stedi `/discovery` → `{ coverages_found, high_confidence[], review_needed[], errors[] }`. Each hit carries full `subscriber_demographics` + optional `dependent_demographics` for the UI diff. |
| [orchestrator.py](orchestrator.py) | `verify(input, ...)` — the 3-branch decision tree. Pure function; takes injected client + audit module so it's unit-testable. |
| [audit.py](audit.py) | Atomic daily-cap counter (DDB conditional update), immutable audit-log writer, patient_hash dedup query. |
| [census_runner.py](census_runner.py) | EventBridge handler that loops over the roster, calls `orchestrator.verify()` per patient, writes `CENSUS_RUN#` + `CENSUS_ITEM#` rows to the `penguin-health-stedi` table. |
| [copy_block.py](copy_block.py) | Plain-text Credible-paste block built from a `VerifyResult`. |
| [exceptions.py](exceptions.py) | `StediAuthError`, `StediRateLimited`, `StediUpstreamError`, `StediDailyCapExceeded`, `StediOrgNotConfigured`, `StediBadRequest`. |

The HTTP layer lives in [lambda/api/eligibility_api.py](../../api/eligibility_api.py) (the four eligibility routes) and [lambda/api/census_api.py](../../api/census_api.py) (the four census routes).

## Stedi APIs we use

| Endpoint | Why we call it |
|---|---|
| `POST /2024-04-01/insurance-discovery/check/v1` | We don't know the payer or member ID — Stedi searches its trading-partner network from demographics (name, DOB, SSN, address) and returns 0..N candidate coverages with confidence levels. Sync, can take up to 120s. |
| `POST /2024-04-01/change/medicalnetwork/eligibility/v3` | We know (or just discovered) the payer + member ID — Stedi calls the payer's real-time 270 service and returns a structured 271. Includes plan dates, benefits per service-type, copays, deductibles, auth requirements. Sync, typically <5s. |

**Auth:** `Authorization: Key <api_key>` (raw key, no `Bearer` prefix). One key per Stedi account; stored in Secrets Manager at `penguin-health/stedi/api-key` with payload `{"api_key": "..."}`.

**Base URL:** `https://healthcare.us.stedi.com` (configurable via `_DEFAULT_BASE_URL` in `stedi_client.py`).

**CMS requirement after 2025-11-08:** every request must include `X-Forwarded-For: <originating user IP>`. We pull it from `event['requestContext']['http']['sourceIp']` and forward verbatim.

## Decision tree: how `orchestrator.verify()` works

```python
verify(input_={
    "first_name": "Jane", "last_name": "Sample", "dob": "20010925",
    "middle_name": None, "suffix": None, "gender": "F",
    "ssn": None, "ssn_last4": "4421",
    "member_id": None, "payer_id": None,
    "address1": "812 Palm Ave", "city": "Tallahassee", "state": "FL", "postal_code": "32301",
}, org_id="demo", org_config=..., stedi_client=..., ...)
```

| Branch | Trigger | Calls | Transactions |
|---|---|---|---|
| **A. Direct** | both `member_id` AND `payer_id` provided | `/eligibility` only | 1 |
| **B. Discovery → eligibility** | `payer_id` only, no `member_id` | `/discovery`, then `/eligibility` for the matching HIGH hit | ≤2 |
| **C. Discovery first** | neither `member_id` nor `payer_id` | `/discovery`, then `/eligibility` for each HIGH hit (parallel, capped at 3) | ≤4 |

For C, REVIEW_NEEDED hits are surfaced to UR for manual portal verification — no follow-up `/eligibility` call (REVIEW means Stedi isn't confident, so eligibility would likely fail anyway).

Before any Stedi call, the orchestrator:
1. **Dedups** — checks `STEDI_AUDIT#` rows for the same `patient_hash` within the last 30 minutes; if found, attaches a `recent_check` field to the response (advisory only, doesn't block).
2. **Reserves capacity** — atomic DDB conditional increment on `USAGE#{yyyy-mm-dd}.count` with `ConditionExpression='count < cap'`. If the increment would exceed the org's `daily_cap`, raises `StediDailyCapExceeded` **before any Stedi traffic**.

After the merge, `_derive_discrepancies()` runs two checks against the audit history:
- **Primary changed** — current payer differs from any payer for this patient in the last 30 days.
- **Recently inactive** — current eligibility shows `inactive` with `expiration_date >= today - 30d`.

Both surface as text strings in the `discrepancies[]` field.

## VerifyResult shape

```python
{
    "path": "direct" | "discovery_then_eligibility" | "discovery_first",
    "primary_coverage": EligibilityResult | None,   # see below
    "secondary_coverages": [EligibilityResult, ...],
    "discovery_review_needed": [DiscoveryItem, ...],
    "discrepancies": [str, ...],
    "recent_check": { checked_by, checked_at, payer_name, result_status } | None,
    "audit_ids": [request_id, ...],                 # links to STEDI_AUDIT# rows
    "copy_block": "<plaintext Credible block>",
}
```

`EligibilityResult` (per `eligibility_transformer.transform()`):

```python
{
    "active": bool,
    "status": "active" | "inactive" | "no_coverage" | "unknown",
    "service_type_status": "covered" | "not_covered" | "unknown",  # rolled-up summary for inpatient BH (45/MH/AI)
    "service_types": [
        # One entry per X12 service-type code the payer mentioned, in
        # first-appearance order. Drives the per-code UI table.
        { "code": "45", "label": "Hospital - Inpatient",
          "status": "covered" | "not_covered" | "unknown",
          "auth_required": True | False | None,
          "copays": [{ "amount": "50.00", "in_or_out_of_network": "Y" }],
          "deductibles": [{ "total": ..., "in_or_out_of_network": ..., "time_period": ... }],
          "notes": [str, ...] },
        ...
    ],
    "payer": { "id": "60054", "name": "Aetna Comm & MCR", "payer_name_unknown": False },
    "subscriber": { "first_name", "last_name", "member_id", "group_number", "dob" },
    "plan": { "name", "effective_date", "expiration_date" },
    "copays": [{ "service_type", "amount" }],
    "deductibles": [...],
    "oop_max": [...],
    "auth_required": True | False | None,  # convenience summary across inpatient-BH benefits
    "notes": [str, ...],
    "raw": <full Stedi 271 pass-through>,
}
```

`DiscoveryItem`:

```python
{
    "confidence_level": "HIGH" | "REVIEW_NEEDED",
    "confidence_reason": "Last name mismatch (request: SAMPLE; payer: SMITH SAMPLE)…",
    "payer": { "id", "name", "payer_name_unknown" },
    "trading_partner_service_id": "60054",
    "member_id": "...",
    "group_number": "...",
    "subscriber_first_name": "JOHN", "subscriber_last_name": "SAMPLE",
    "subscriber_demographics": { "first_name", "middle_name", "last_name", "suffix",
                                 "dob", "gender", "address1", "address2",
                                 "city", "state", "postal_code" },  # only non-null keys
    "dependent_demographics": { ..., "relation_to_subscriber": "Child" } | None,
}
```

## DynamoDB schema (`penguin-health-stedi` table)

One physical table, four logical row types distinguished by sort-key prefix:

### `AUDIT#{iso_ts}#{request_id}` — one per Stedi call, immutable

```
pk:     ORG#{org_id}
sk:     AUDIT#{iso_ts}#{request_id}
gsi1pk: PATIENT#{org_id}#{patient_hash}          # for "recent checks for this patient" dedup
gsi1sk: {iso_ts}

request_id, user_email, requested_at, call_type ("eligibility" | "discovery"),
patient_hash (sha256 of lower(first)+lower(last)+dob),
patient_first_initial, patient_last_initial, patient_dob,
member_id_last4 (only stored if a member ID was used; never the full ID),
payer_name, payer_id, client_ip,
result_status, result_summary (slim — payer, plan, dates, auth_required),
stedi_control_number, duration_ms,
expires_at                                       # epoch s; requested_at + 7y
```

Inserts use `ConditionExpression='attribute_not_exists(pk)'` so this row type is truly immutable. **SSN is never persisted.** Full member IDs are never persisted (last-4 only).

### `USAGE#{yyyy-mm-dd}` — daily cap counter

```
pk:         ORG#{org_id}
sk:         USAGE#{yyyy-mm-dd}
count:      <atomic ADD>
expires_at: <90 days from creation>
```

Updated via conditional `ADD #c :one` with `ConditionExpression='count < cap'`. The conditional check + increment is atomic — no race between concurrent verify requests. If the increment would exceed cap, the orchestrator raises `StediDailyCapExceeded` before any Stedi call.

### `CENSUS_RUN#{run_date}#{run_id}` — one per scheduled census run

```
pk:     ORG#{org_id}
sk:     CENSUS_RUN#{yyyy-mm-dd}#{run_id}
gsi1pk: CENSUS_RUN#{org_id}                     # for newest-first list query
gsi1sk: {yyyy-mm-dd}#{run_id}

run_id, org_id, run_date, started_at, completed_at,
status ("running" | "complete" | "failed"),
source ("demo_roster" | "sftp" | "fhir"),
total, verified, discrepancy, no_coverage, review_needed,
pediatric_no_info, service_type_denied, error,
expires_at                                       # 90 days
```

### `CENSUS_ITEM#{run_date}#{run_id}#{patient_hash}` — one per patient per run

```
pk: ORG#{org_id}
sk: CENSUS_ITEM#{yyyy-mm-dd}#{run_id}#{patient_hash}

run_id, run_date, patient_hash,
patient_first_initial, patient_last_initial,
patient_first_name, patient_last_name, patient_dob,   # full names — demo-only; real PHI would be initials + DOB

submitted_demographics: {                              # what intake captured (never overwritten)
    first_name, middle_name, last_name, suffix,
    dob, gender, ssn_last4,
    address1, address2, city, state, postal_code,
},
corrected_demographics: { ... } | None,                # fields UR changed on rerun, null until first rerun
payer_demographics: {                                  # what discovery's matched hit said
    subscriber: { ... },
    dependent: { ... } | None,
    confidence_level, confidence_reason,
} | None,
rerun_history: [{                                      # append-only audit of rerun events
    rerun_id, rerun_by, rerun_at,
    corrected_fields: [...], previous_status, new_status,
    audit_ids: [...]
}],

result_status: "verified" | "discrepancy" | "no_coverage" | "review_needed"
             | "pediatric_no_info" | "service_type_denied" | "error",
result_summary: {
    payer_name, payer_id, plan_name, member_id_last4,
    effective_date, expiration_date,
    auth_required, service_type_status,
    service_types: [...],                              # full per-code breakdown
    active, discrepancies, secondary_count, review_needed_count,
},
audit_ids: [...],                                      # links to STEDI_AUDIT# rows for this verify
resolution: {                                          # UR's workflow state on top of the underlying status
    state: "unresolved" | "in_progress" | "resolved",
    action: <one of per-status options>, note, resolved_by, resolved_at,
    rerun_audit_id,
},
expires_at                                             # 90 days
```

### `STEDI_CONFIG` (lives on `penguin-health-org-config`, not `penguin-health-stedi`)

```
pk: ORG#{org_id}, sk: STEDI_CONFIG

enabled (bool), demo_mode (bool),
provider: { npi: "1999999984", organization_name: "Provider Name" },
daily_cap (int), preferred_payer_ids: ["60054", "09101", ...],
census_enabled (bool),
census_roster_source: "demo_roster" | "sftp" | "fhir",
census_schedule_cron: "0 11 * * ? *",
created_at, updated_at,
```

Provision via [scripts/multi-org/add_stedi_config.py](../../../scripts/multi-org/add_stedi_config.py).

## HTTP API

All routes live in the existing admin API Lambda (`penguin-health-admin-api`). RBAC categories use the existing `Eligibility` category in [permissions.py](../../api/permissions.py).

### Eligibility (manual verify)

| Method | Path | Purpose | RBAC |
|---|---|---|---|
| POST | `/api/organizations/{orgId}/eligibility/verify` | Manual single-patient verify | `Eligibility:run` |
| GET | `/api/organizations/{orgId}/eligibility/history` | Recent verify audits for a patient (dedup) | `Eligibility:view` |
| GET | `/api/organizations/{orgId}/eligibility/config` | Read STEDI_CONFIG + payer list | super-admin / org_admin |
| PUT | `/api/organizations/{orgId}/eligibility/config` | Write STEDI_CONFIG (NPI, daily_cap, payers) | super-admin |

### Morning census

| Method | Path | Purpose | RBAC |
|---|---|---|---|
| GET | `/api/organizations/{orgId}/eligibility/census/latest` | Latest run + all 10 items | `Eligibility:view` |
| GET | `/api/organizations/{orgId}/eligibility/census/runs` | History of recent runs | `Eligibility:view` |
| PUT | `/api/organizations/{orgId}/eligibility/census/items/{runId}/{patientHash}/resolve` | Mark a row resolved (with action + note) | `Eligibility:run` |
| POST | `/api/organizations/{orgId}/eligibility/census/items/{runId}/{patientHash}/rerun` | Rerun discovery+eligibility with corrected demographics | `Eligibility:run` |

The `/me/permissions` endpoint additionally surfaces `eligibility_unread_count` (cached 60s) — the count of attention-needed rows in the latest run that haven't been resolved yet. The frontend uses this for the nav badge.

## Demo mode

When `STEDI_CONFIG.demo_mode == true`:
- `client_factory.build_client()` returns `DemoStediClient(real_client=None)` — no Secrets Manager fetch, no real Stedi traffic.
- `DemoStediClient` looks up the inbound payload's patient name+DOB in [demo_fixtures.SCENARIOS](demo_fixtures.py) and returns the canned response.
- For `check_eligibility`, falls back to `(member_id, payer_id)` lookup in `ELIGIBILITY_DIRECT_SCENARIOS`.
- If neither lookup matches **and** `real_client is None`, returns an empty-but-valid response (no crashes when UR reruns with edits that no longer match a fixture).

### The 10-patient demo roster

| # | Name | DOB | Path | What it demonstrates |
|---|---|---|---|---|
| 1 | Jane Sample | 20010925 | discovery-first | `REVIEW_NEEDED` (last-name mismatch: payer has SMITH SAMPLE) |
| 2 | Robert Testpatient | 19780214 | discovery-first | 2 HIGH hits → primary Aetna + secondary FL Medicaid |
| 3 | Maria Mockerson | 19550630 | **direct** | Medicare A&B via MBI on file |
| 4 | Nora Faker | 19900101 | discovery-first | `coveragesFound: 0` — no coverage anywhere |
| 5 | Daniel Demoson | 19850712 | discovery-first | Aetna terminated 12 days ago → discrepancy "active within 30d" |
| 6 | Linda Sandbox | 19620818 | discovery-first | Now on Humana; prior Cigna seeded → "primary changed" discrepancy |
| 7 | Tyler Fixture | 20140315 | discovery-first | Pediatric (under 18) + no coverage → `pediatric_no_info` (call parent) |
| 8 | Patricia Stub | 19710505 | **direct** | Sunshine FL Medicaid with `authOrCertIndicator: Y` |
| 9 | James Example | 19831120 | discovery-first | Cigna active overall but inpatient BH non-covered → `service_type_denied` |
| 10 | Sarah Placeholder | 20030414 | discovery-first | Aged out at 26; Aetna inactive 2 days ago |

Every patient is intentionally a synthetic surname so screenshots never read as real.

### Linda's "primary changed" seed

The orchestrator's `_derive_discrepancies()` needs a prior audit row from a different payer to fire the "primary changed" signal. The census runner's `_ensure_demo_history_seeds()` writes a synthetic `AUDIT#` row for Linda dated 25 days ago (within the 30-day lookback window) with `payer_name: "Cigna"` on first run. It's idempotent — re-running the census doesn't duplicate the seed.

## Sample provisioning

```bash
# 1. Stedi API key in Secrets Manager (one-time, account-level)
aws secretsmanager create-secret \
  --name penguin-health/stedi/api-key \
  --secret-string '{"api_key":"<from-stedi-portal>"}'

# 2. Per-org STEDI_CONFIG
python3 scripts/multi-org/add_stedi_config.py \
  --org-id demo \
  --npi 1999999984 \
  --organization-name "Provider Name" \
  --daily-cap 200 \
  --preferred-payers 60054,09101,68068,61101,62308 \
  --demo-mode \
  --census-enabled \
  --census-roster-source demo_roster \
  --census-schedule-cron "0 11 * * ? *"

# 3. Verify the row landed
aws dynamodb get-item --table-name penguin-health-org-config \
  --key '{"pk":{"S":"ORG#demo"},"sk":{"S":"STEDI_CONFIG"}}' \
  --query 'Item'
```

## Running the census

The EventBridge rule (defined in [infra/components/admin_ui.py](../../../infra/components/admin_ui.py)) fires `cron(0 11 * * ? *)` daily — 11:00 UTC = 6am ET (winter) / 7am EDT. For demos, invoke manually:

```bash
aws lambda invoke \
  --function-name penguin-health-census-runner \
  --cli-binary-format raw-in-base64-out \
  --payload '{"organization_id":"demo"}' \
  /tmp/out.json && cat /tmp/out.json
# {"status":"complete","organization_id":"demo","run_id":"...","total":10,"verified":4, ...}
```

To wipe and re-run from scratch:

```bash
# Clear all census + audit rows for the org
aws dynamodb query --table-name penguin-health-stedi \
  --key-condition-expression "pk = :p" \
  --expression-attribute-values '{":p":{"S":"ORG#demo"}}' \
  --projection-expression "pk,sk" --output json \
  | jq -c '.Items[]' \
  | while read item; do
      aws dynamodb delete-item --table-name penguin-health-stedi --key "$item"
    done

# Re-run
aws lambda invoke --function-name penguin-health-census-runner \
  --cli-binary-format raw-in-base64-out \
  --payload '{"organization_id":"demo"}' \
  /tmp/out.json
```

## Frontend

| Page | Purpose |
|---|---|
| [/organizations/:orgId/eligibility](../../../admin-ui/src/pages/EligibilityPage.jsx) | Manual verify form. Free-form demographic input → calls `/eligibility/verify` → renders result card with copy block. Used for ad-hoc lookups outside the morning census. |
| [/organizations/:orgId/eligibility/census](../../../admin-ui/src/pages/CensusPage.jsx) | Morning worklist. Status pills, sortable table, click-to-expand detail with: service-type table, demographics diff (sent vs payer), inline edit + Rerun button, resolution actions, rerun history. |

The nav surface on `OrganizationDetail.jsx` shows "Morning Census" with a red unread-count badge driven by the `eligibility_unread_count` from `/me/permissions`. Badge counts only rows with non-verified `result_status` that aren't `resolved`.

## Security model

- **Authorization header.** Raw API key with `Key ` prefix (e.g. `Authorization: Key <api_key>`). Never log this header.
- **SSN handling.** Inbound only — never written to DynamoDB, never logged. The redactor in `stedi_client._scrub()` masks `ssn`/`socialSecurityNumber` fields before printing. We only ever store SSN **last 4** as `ssn_last4`.
- **Member IDs.** Full ID passed to Stedi but never persisted. Only `member_id_last4` (last 4 chars) goes to DynamoDB.
- **Patient hash.** `sha256(lower(first)+lower(last)+dob)` — deterministic, lets us query "recent checks for this patient" without storing PII on the audit row's GSI partition key.
- **Audit-row immutability.** `attribute_not_exists(pk)` ConditionExpression on every insert. No code path updates `AUDIT#` rows after creation.
- **CloudWatch hygiene.** `stedi_client._log_success()` only logs `path`, `status`, `duration_ms`, and Stedi's `controlNumber` (their trace ID, not PHI).
- **CMS X-Forwarded-For pass-through.** Required after 2025-11-08 for any payer request. We pull from API Gateway's `event.requestContext.http.sourceIp` and persist it on the audit row (for provenance proofs if CMS ever asks).
- **Daily cap.** Belt-and-suspenders against runaway loops or accidental fan-out. Set per org via `STEDI_CONFIG.daily_cap`.
- **Demo mode is per-org.** A misconfigured production org won't accidentally hit fixtures — `demo_mode` defaults to false and is explicitly set by `add_stedi_config.py --demo-mode`.

## Insurance verification process (end-to-end)

This is what actually happens, from intake to verified-in-Credible. The morning census handles steps 1–4; UR + Admissions handle 5–6 with the help of the worklist.

### 1. Intake captures demographics

A patient arrives — often in crisis, often without ID. Intake collects whatever they can: name, DOB, gender, address, sometimes SSN, sometimes a prior insurance card. This lands in the source system (today: Credible chart for returning patients; later: SFTP/FHIR feed).

For the demo, this is represented by `CENSUS_ROSTER` in `demo_fixtures.py` — each entry mirrors the realistic data quality intake collects (e.g., Nora Faker has no SSN and no address; Tyler Fixture is a minor with no insurance info).

### 2. Morning census auto-run (6am ET)

EventBridge fires the census runner. For each patient on the roster:

1. Reserve a slot on the daily-cap counter.
2. Build the Stedi input from the roster entry.
3. Call `orchestrator.verify()`:
   - If `member_id + payer_id` are on file → direct eligibility (1 transaction).
   - Otherwise → discovery first, then eligibility for HIGH-confidence hits (≤4 transactions).
4. Classify the result into one of 7 statuses (`verified`, `discrepancy`, `no_coverage`, `review_needed`, `pediatric_no_info`, `service_type_denied`, `error`).
5. Write a `CENSUS_ITEM#` row with: submitted demographics, payer-side demographics from the matched hit, slim result summary, audit-row links, empty resolution.

A `CENSUS_RUN#` summary row holds the rollup counts.

### 3. UR arrives at 8am, opens the worklist

`/organizations/demo/eligibility/census` shows the latest run with colored status pills. The nav badge shows "X need attention". Default sort puts attention-needed rows on top.

For each non-✅ row, UR can:
- **Click to expand** — see the full result detail, service-type breakdown, discrepancy notes, demographic diff between what intake sent and what the payer has on file.
- **Edit & rerun discovery** — fix typos (transposed DOB digits, wrong ZIP, missing middle initial) and re-run. New result lands in place; rerun history appends.
- **Mark resolved** — pick a per-status action ("Verified via payer portal", "Patient confirmed uninsured", "Parent contacted — got info", etc.) plus a free-text note. Row dims; badge ticks down.

### 4. Discrepancies surface their causes

The orchestrator surfaces two specific discrepancy types based on audit history:
- **Primary payer changed** — current verify returned payer A; a recent audit (last 30 days) shows the same patient on payer B. Catches the "Inovalon doesn't show other primary insurance" failure mode.
- **Recently terminated** — current eligibility says inactive, with `expiration_date >= today - 30d`. Catches the "Inovalon doesn't show inactivations within the last 30 days" failure mode.

Both render in the worklist as red discrepancy banners with the explanation.

### 5. Handoff to Admissions

When UR resolves a row with action "Verified correct via payer portal" or "Patient provided new info — rerun", the resolved row carries the canonical answer (corrected member ID, confirmed payer, valid coverage dates). Admissions (Sheryl/Lynette/Dawn/Cici) sees the resolved worklist and types the verified data into Credible.

V3 will replace the typing step with a structured handoff queue and (eventually) direct Credible write-back. V2 keeps it manual.

### 6. Audit trail

Every Stedi call writes one immutable `STEDI_AUDIT#` row (7-year TTL). Every census row links to its audit IDs via `audit_ids[]`. Every resolution event lives on the census-item row's `resolution` sub-object. Every rerun event appends to `rerun_history[]` with who/when/what changed. CMS provenance (which user, which IP, which Stedi control number) is queryable directly from DynamoDB.

## Testing

```bash
# All Stedi unit tests (50+ tests; no AWS dependencies beyond moto + pytest-mock)
python3 -m pytest lambda/tests/unit/stedi/ -q

# Plus the API-handler tests for census endpoints
python3 -m pytest lambda/tests/unit/api/test_census_api.py -q

# Full suite
python3 -m pytest lambda/tests/ -q
```

The test layout:

| Test file | Covers |
|---|---|
| [test_orchestrator.py](../../tests/unit/stedi/test_orchestrator.py) | All 3 verify branches, daily cap, discrepancy derivation, input validation |
| [test_demo_mode.py](../../tests/unit/stedi/test_demo_mode.py) | DemoStediClient fixture lookup + fallthrough + None-real-client safety net |
| [test_transformers.py](../../tests/unit/stedi/test_transformers.py) | 271 → EligibilityResult, discovery → DiscoveryItem, service_types breakdown |
| [test_audit.py](../../tests/unit/stedi/test_audit.py) | Atomic cap reservation, immutable inserts, patient_hash dedup query |
| [test_client.py](../../tests/unit/stedi/test_client.py) | SSN/member-ID redaction, X-Forwarded-For pass-through |
| [test_census_runner.py](../../tests/unit/stedi/test_census_runner.py) | Full 10-patient census, classification, partial-error tolerance, seed idempotency |
| [test_census_api.py](../../tests/unit/api/test_census_api.py) | get_latest_run, list_runs, resolve_item, rerun_census_item, unread_count_for_org |

## Operational runbook

### "I deployed and the Lambda still serves old code"

Asset-hashing bug from V1 — the bundler reads from `lambda/multi-org/stedi/` but CDK computes the asset hash from `lambda/api/`. Fixed in [admin_ui.py](../../../infra/components/admin_ui.py) via the `_hash_sources()` helper that walks both directories. If you see this again, delete `infra/cdk.out/` and redeploy. As a last resort:

```bash
cd infra/cdk.out/asset.<hash>
zip -qr /tmp/admin-api.zip .
aws lambda update-function-code --function-name penguin-health-admin-api \
  --zip-file fileb:///tmp/admin-api.zip
```

### "I got Stedi 403 / 400"

- **403** on first call usually means wrong auth scheme. Confirm the header is `Authorization: Key <key>` (not `Bearer`).
- **400** with `Missing required field: provider organizationName or lastName is required` means the STEDI_CONFIG row has `provider.npi` but no `provider.organization_name`. Re-run `add_stedi_config.py` with `--organization-name "..."`.
- **400** with `tradingPartnerServiceId` errors means the payer ID we sent isn't in Stedi's network. Cross-reference [payer_registry.py](payer_registry.py) — IDs there are real Stedi codes; placeholder/symbolic codes will be rejected.

### "Census ran but the nav badge is wrong"

`/me/permissions` caches `eligibility_unread_count` for 60 seconds (see `permissions._unread_cache`). Wait a minute or hard-refresh; if it persists, query the row directly:

```bash
aws dynamodb query --table-name penguin-health-stedi \
  --key-condition-expression "pk = :p AND begins_with(sk, :s)" \
  --expression-attribute-values '{":p":{"S":"ORG#demo"},":s":{"S":"CENSUS_ITEM#"}}' \
  --query 'Items[?result_status!=`verified` && resolution.state!=`resolved`] | length(@)'
```

### "Daily cap exceeded but I need to test"

```bash
# Reset the counter for today
aws dynamodb delete-item --table-name penguin-health-stedi \
  --key '{"pk":{"S":"ORG#demo"},"sk":{"S":"USAGE#'"$(date +%Y-%m-%d)"'"}}'
```

Or raise the cap via `add_stedi_config.py --daily-cap <higher>`.

### "Rerun crashed with `NoneType has no attribute check_insurance_discovery`"

V2-era bug, fixed: when an edit moved a patient out of fixture coverage and demo_mode had `real_client=None`, the fallthrough crashed. Now returns an empty discovery response. If you see this on a deployed Lambda, the fix is in [demo_client.py](demo_client.py) — redeploy.

## Out of scope (V3+)

- **Real FHIR encounter feed** for the census roster. The swap point is `census_runner._load_roster()` — `census_roster_source: "fhir"` currently raises `NotImplementedError`.
- **SFTP CSV upload** as an alternative roster source. Same swap point.
- **Direct write-back to Credible** of verified coverage. V2 still produces a copy block; UR/Admissions pastes manually.
- **Structured handoff queue** for Admissions. V2 uses the "Escalate" resolution action as a placeholder; V3 turns it into a real worklist with its own status flow.
- **Auth call log** — track who called the payer for precert, when, what auth number came back, when it renews.
- **Re-verify ticklers** — automatically re-run patients whose coverage is about to lapse.
- **Real-time FHIR webhook** for new admissions (so we don't wait for the 6am batch).
