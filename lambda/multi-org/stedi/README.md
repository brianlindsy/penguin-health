# Stedi Insurance Eligibility Integration

Real-time insurance verification for behavioral-health admissions, powered by
Stedi's `/insurance-discovery` and `/medicalnetwork/eligibility/v3` APIs. This
package owns the full lifecycle of an eligibility check — from the manual
"Verify Patient" form to the FHIR-encounter-triggered poller that runs
verification automatically as new admissions arrive in the EMR.

## Architecture at a glance

```
   Manual: Verify Patient form                       EventBridge rate(15 min)
                │                                              │
                ▼                                              ▼
        POST /eligibility/verify              penguin-health-fhir-eligibility-poller
                │                              1. read FHIR_POLL_CURSOR
                │                              2. fhir_query.search('Encounter',
                │                                   _lastUpdated=gt{cursor}, …)
                │                              3. for each encounter:
                │                                   - fhir_query.get_resource('Patient', id)
                │                                   - fhir_patient_mapper.to_verify_input
                │                                   - orchestrator.verify(...)
                │                                   - put_item ENCOUNTER_ITEM#
                │                              4. advance cursor
                │                                              │
                ▼                                              ▼
                              ┌──────────────────────────┐
                              │   orchestrator.verify    │
                              │     3-branch tree        │
                              └────────────┬─────────────┘
                                           ▼
                                ┌────────────────────┐
                                │  Stedi client OR   │
                                │   demo proxy       │
                                └─────────┬──────────┘
                              ┌───────────┴────────────┐
                              ▼                        ▼
              https://healthcare.us.stedi.com   demo_fixtures
              /insurance-discovery/check/v1     (canned 10 patients
              /medicalnetwork/eligibility/v3     + ENCOUNTER_STREAM)
```

## Module layout

| File | What it does |
|---|---|
| [stedi_client.py](stedi_client.py) | HTTPS client for Stedi's two endpoints. Retry/backoff on 429/5xx. SSN + member-ID redaction in logs. Forwards `X-Forwarded-For` (CMS-mandated). |
| [demo_client.py](demo_client.py) | Drop-in replacement for `StediClient` that returns canned fixtures based on patient name+DOB or (member_id, payer_id). Used when `STEDI_CONFIG.demo_mode == true`. |
| [demo_fixtures.py](demo_fixtures.py) | 10-patient roster, `SCENARIOS` (name → canned discovery + eligibility), `ELIGIBILITY_DIRECT_SCENARIOS` (member_id → canned eligibility), helper builders for inactive/Humana/auth-required/no-BH responses, `DEMO_HISTORY_SEEDS` (Linda's prior-Cigna audit row). |
| [client_factory.py](client_factory.py) | `build_client(org_config, client_ip)` — returns either `DemoStediClient(None)` or `StediClient(api_key, client_ip)` based on demo_mode. Used by both the manual API handler and the FHIR poller. |
| [config.py](config.py) | `load_stedi_config(org_id)` — reads `STEDI_CONFIG` from the `penguin-health-org-config` DynamoDB table. lru_cache'd. |
| [secrets.py](secrets.py) | `get_stedi_api_key()` — Secrets Manager fetcher, cached for Lambda lifetime. |
| [payer_registry.py](payer_registry.py) | Static map of Stedi `tradingPartnerServiceId` ↔ friendly payer name. Used by transformers and the UI dropdown. |
| [eligibility_transformer.py](eligibility_transformer.py) | Stedi 271 → simplified `EligibilityResult` (active, status, payer, plan, copays, deductibles, auth_required, **service_types breakdown**, notes). |
| [discovery_transformer.py](discovery_transformer.py) | Stedi `/discovery` → `{ coverages_found, high_confidence[], review_needed[], errors[] }`. Each hit carries full `subscriber_demographics` + optional `dependent_demographics` for the UI diff. |
| [orchestrator.py](orchestrator.py) | `verify(input, ...)` — the 3-branch decision tree. Pure function; takes injected client + audit module so it's unit-testable. |
| [audit.py](audit.py) | Atomic daily-cap counter (DDB conditional update), immutable audit-log writer, patient_hash dedup query. |
| [fhir_eligibility_poller.py](fhir_eligibility_poller.py) | EventBridge handler (every ~15 min per opted-in org) that polls the FHIR API for new Encounters since the last cursor, fetches each referenced Patient via `fhir_patient_mapper`, runs `orchestrator.verify()`, and writes `ENCOUNTER_ITEM#{encounter_id}` rows. |
| [fhir_patient_mapper.py](fhir_patient_mapper.py) | Pure mapper: FHIR R4 Patient → orchestrator.verify input dict. |
| [copy_block.py](copy_block.py) | Plain-text Credible-paste block built from a `VerifyResult`. |
| [exceptions.py](exceptions.py) | `StediAuthError`, `StediRateLimited`, `StediUpstreamError`, `StediDailyCapExceeded`, `StediOrgNotConfigured`, `StediBadRequest`. |

The HTTP layer lives in [lambda/api/eligibility_api.py](../../api/eligibility_api.py) (the ad-hoc verify routes) and [lambda/api/eligibility_worklist_api.py](../../api/eligibility_worklist_api.py) (the worklist list/resolve/rerun routes).

## Stedi APIs we use

| Endpoint | Why we call it |
|---|---|
| `POST /2024-04-01/insurance-discovery/check/v1` | We don't know the payer or member ID — Stedi searches its trading-partner network from demographics (name, DOB, SSN, address) and returns 0..N candidate coverages with confidence levels. Sync, can take up to 120s. |
| `POST /2024-04-01/change/medicalnetwork/eligibility/v3` | We know (or just discovered) the payer + member ID — Stedi calls the payer's real-time 270 service and returns a structured 271. Includes plan dates, benefits per service-type, copays, deductibles, auth requirements. Sync, typically <5s. |
| `POST /2024-04-01/coordination-of-benefits/check/v1` | Optional: when discovery + eligibility returned ≥2 active coverages, Stedi ranks them by primacy per payer-of-record rules (Medicare Secondary Payer, birthday rule, custodial-parent rules, Medicaid-as-payer-of-last-resort, etc.). Gated by `STEDI_CONFIG.cob_enabled`; never called on single-coverage cases. Result lands in `result_summary.cob_check` and re-ranks `primary_coverage` when it disagrees with our "first active wins" default. |

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
    "plan": { "name", "effective_date", "expiration_date", "premium_paid_through" },
    "copays": [{ "service_type", "amount" }],
    "deductibles": [...],
    "oop_max": [...],
    "auth_required": True | False | None,  # convenience summary across inpatient-BH benefits
    "notes": [str, ...],
    "grace_period_signals": {              # payer-agnostic raw extraction;
        "paid_through": "YYYYMMDD" | None, #   orchestrator gates on payer ID
        "has_code_5": bool,                #   (Ambetter 68069 / Cenpatico 68068)
        "additional_info_texts": [str],    #   before emitting a discrepancy
    },
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

### `ENCOUNTER_ITEM#{encounter_id}` — one per verified Encounter

```
pk:     ORG#{org_id}
sk:     ENCOUNTER_ITEM#{encounter_id}
gsi1pk: ENCOUNTER_ITEM#{org_id}                  # newest-first listing
gsi1sk: {meta_lastUpdated_iso}                   # chronological by FHIR timestamp

encounter_id, encounter_class, encounter_status, encounter_lastUpdated,
patient_hash, patient_first_initial, patient_last_initial,

submitted_demographics: {                              # what the FHIR Patient resource provided
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
    active, discrepancies, grace_period_risk, secondary_count, review_needed_count,
},
audit_ids: [...],                                      # links to STEDI_AUDIT# rows for this verify
resolution: {                                          # UR's workflow state on top of the underlying status
    state: "unresolved" | "in_progress" | "resolved",
    note, resolved_by, resolved_at, rerun_audit_id,
},
expires_at                                             # 90 days
```

Row writes use `ConditionExpression='attribute_not_exists(sk)'` so the
poller is idempotent on retries.

### `FHIR_POLL_CURSOR` — one per org

```
pk:                 ORG#{org_id}
sk:                 FHIR_POLL_CURSOR
last_updated_iso:   {iso}      # max meta.lastUpdated seen, advances monotonically
updated_at, last_poll_status, last_processed
```

### `STEDI_CONFIG` (lives on `penguin-health-org-config`, not `penguin-health-stedi`)

```
pk: ORG#{org_id}, sk: STEDI_CONFIG

enabled (bool), demo_mode (bool),
provider: { npi: "1999999984", organization_name: "Provider Name" },
daily_cap (int), preferred_payer_ids: ["60054", "09101", ...],
census_enabled (bool),                            # opts the org into FHIR-poll-triggered eligibility
cob_enabled (bool),                               # ≥2-active-coverages → call /coordination-of-benefits
encounter_filter: {                               # which FHIR Encounters the poller should match
    class_codes: ["IMP", ...] | omitted,          # FHIR Encounter.class codes
    type_codes:  ["..." , ...] | omitted,         # FHIR Encounter.type codes
    statuses:    ["arrived", "in-progress", ...] | omitted,
},
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

### Eligibility worklist

| Method | Path | Purpose | RBAC |
|---|---|---|---|
| GET | `/api/organizations/{orgId}/eligibility/encounters` | Recent encounter rows (newest first) + status counts | `Eligibility:view` |
| PUT | `/api/organizations/{orgId}/eligibility/encounters/{encounterId}/resolve` | Mark a row resolved (with action + note) | `Eligibility:run` |
| POST | `/api/organizations/{orgId}/eligibility/encounters/{encounterId}/rerun` | Rerun discovery+eligibility with corrected demographics | `Eligibility:run` |

The `/me/permissions` endpoint additionally surfaces `eligibility_unread_count` (cached 60s) — the count of recent attention-needed rows not yet resolved. The frontend uses this for the nav badge.

## Demo mode

When `STEDI_CONFIG.demo_mode == true`:
- `client_factory.build_client()` returns `DemoStediClient(real_client=None)` — no Secrets Manager fetch, no real Stedi traffic.
- `DemoStediClient` looks up the inbound payload's patient name+DOB in [demo_fixtures.SCENARIOS](demo_fixtures.py) and returns the canned response.
- For `check_eligibility`, falls back to `(member_id, payer_id)` lookup in `ELIGIBILITY_DIRECT_SCENARIOS`.
- If neither lookup matches **and** `real_client is None`, returns an empty-but-valid response (no crashes when UR reruns with edits that no longer match a fixture).

### The 11-patient demo roster

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
| 11 | Karen Examplez | 19840922 | discovery-first | Ambetter Marketplace, premium delinquent → grace-period risk discrepancy |

Every patient is intentionally a synthetic surname so screenshots never read as real.

### Linda's "primary changed" seed

The orchestrator's `_derive_discrepancies()` needs a prior audit row from a different payer to fire the "primary changed" signal. The poller's `_ensure_demo_history_seeds()` (demo mode only) writes a synthetic `AUDIT#` row for Linda dated 25 days ago (within the 30-day lookback window) with `payer_name: "Cigna"` on first run. It's idempotent — re-running the poller doesn't duplicate the seed.

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
  --encounter-filter-class-codes IMP \
  --encounter-filter-statuses planned,in-progress

# 3. Verify the row landed
aws dynamodb get-item --table-name penguin-health-org-config \
  --key '{"pk":{"S":"ORG#demo"},"sk":{"S":"STEDI_CONFIG"}}' \
  --query 'Item'
```

## Running the FHIR poller

The EventBridge rule (defined in [infra/components/admin_ui.py](../../../infra/components/admin_ui.py)) fires `rate(15 minutes)` per opted-in org. For demos, invoke manually:

```bash
aws lambda invoke \
  --function-name penguin-health-fhir-eligibility-poller \
  --cli-binary-format raw-in-base64-out \
  --payload '{"organization_id":"demo"}' \
  /tmp/out.json && cat /tmp/out.json
# {"status":"complete","organization_id":"demo","processed":10,"cursor":"...","verified":4, ...}
```

To wipe and re-run from scratch:

```bash
# Clear demo encounter + cursor + seeded audit rows
python3 scripts/multi-org/cleanup_demo_stedi.py --org-id demo

# Re-run
aws lambda invoke --function-name penguin-health-fhir-eligibility-poller \
  --cli-binary-format raw-in-base64-out \
  --payload '{"organization_id":"demo"}' \
  /tmp/out.json
```

## Frontend

| Page | Purpose |
|---|---|
| [/organizations/:orgId/eligibility](../../../admin-ui/src/pages/EligibilityPage.jsx) | Manual verify form. Free-form demographic input → calls `/eligibility/verify` → renders result card with copy block. Used for ad-hoc lookups outside the worklist. |
| [/organizations/:orgId/eligibility/worklist](../../../admin-ui/src/pages/EligibilityWorklistPage.jsx) | Rolling worklist of recent encounters verified by the FHIR poller. Status pills, sortable table, click-to-expand detail with: service-type table, demographics diff (sent vs payer), inline edit + Rerun button, optional resolution note, rerun history. |

The nav surface on `OrganizationDetail.jsx` shows "Eligibility Worklist" with a red unread-count badge driven by the `eligibility_unread_count` from `/me/permissions`. Badge counts only rows with non-verified `result_status` that aren't `resolved`.

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

This is what actually happens, from intake to verified-in-Credible. The FHIR poller handles steps 1–4; UR + Admissions handle 5–6 with the help of the worklist.

### 1. Intake captures demographics

A patient arrives — often in crisis, often without ID. Intake collects whatever they can: name, DOB, gender, address, sometimes SSN, sometimes a prior insurance card. This lands in the source system (today: Credible chart). When the EMR creates the corresponding FHIR `Encounter` resource, our poller picks it up on the next tick.

For the demo, the same data flow runs against synthetic Encounters in `demo_fixtures.ENCOUNTER_STREAM`, each referencing a `CENSUS_ROSTER` patient.

### 2. FHIR eligibility poller (every ~15 min)

EventBridge fires the poller per opted-in org. Each tick:

1. Read the `FHIR_POLL_CURSOR` watermark for the org.
2. Query `Encounter?_lastUpdated=gt{cursor}&_sort=_lastUpdated&class=…&status=…` with the per-org `encounter_filter`.
3. For each Encounter: extract `subject.reference`, `GET /Patient/{id}`, run `fhir_patient_mapper.to_verify_input(patient, encounter)`.
4. Reserve a slot on the daily-cap counter.
5. Call `orchestrator.verify()`:
   - If `member_id + payer_id` are on file → direct eligibility (1 transaction).
   - Otherwise → discovery first, then eligibility for HIGH-confidence hits (≤4 transactions).
6. Classify into one of 7 statuses (`verified`, `discrepancy`, `no_coverage`, `review_needed`, `pediatric_no_info`, `service_type_denied`, `error`).
7. Write an `ENCOUNTER_ITEM#{encounter_id}` row (idempotent on retries).
8. Advance the cursor to the max `meta.lastUpdated` actually processed.

On `StediDailyCapExceeded`, the poller halts immediately and leaves the cursor at the last successful encounter so the next tick (or the next day after the counter resets) picks up where it left off.

### 3. UR opens the worklist

`/organizations/demo/eligibility/worklist` shows recent encounters with colored status pills. The nav badge shows "X need attention". Default sort puts attention-needed rows on top.

For each non-✅ row, UR can:
- **Click to expand** — see the full result detail, service-type breakdown, discrepancy notes, demographic diff between what intake sent and what the payer has on file.
- **Edit & rerun discovery** — fix typos (transposed DOB digits, wrong ZIP, missing middle initial) and re-run. New result lands in place; rerun history appends.
- **Mark resolved** — click the button, optionally with a free-text note. Row dims; badge ticks down.

### 4. Discrepancies surface their causes

The orchestrator surfaces three specific discrepancy types:
- **Primary payer changed** — current verify returned payer A; a recent audit (last 30 days) shows the same patient on payer B. Catches the "Inovalon doesn't show other primary insurance" failure mode.
- **Recently terminated** — current eligibility says inactive, with `expiration_date >= today - 30d`. Catches the "Inovalon doesn't show inactivations within the last 30 days" failure mode.
- **Grace-period risk** — *only* for payers `68069` (Ambetter/Centene) and `68068` (Cenpatico Sunshine State), the two payers in the client's mix that reliably surface non-payment signals. Fires when any of three signals show up on the 271: `planDateInformation.premiumPaidToDateEnd` strictly before today, a `benefitsInformation` entry with code `5` ("Active – Pending Investigation"), or `additionalInformation[].description` matching grace-period language. The raw description text is intentionally **never** echoed into the discrepancy string (PHI guard) — only a generic "payer free-text indicates grace-period status" marker. Catches the "Availity shows active but member is in grace period and will be retro-terminated to day 31" failure mode.

All three render in the worklist as amber discrepancy banners with the explanation. Grace-period rows additionally get an orange inline "Grace risk" badge next to the status pill (driven by `result_summary.grace_period_risk: bool`) so UR can spot them at a glance.

### 5. Handoff to Admissions

When UR resolves a row with action "Verified correct via payer portal" or "Patient provided new info — rerun", the resolved row carries the canonical answer (corrected member ID, confirmed payer, valid coverage dates). Admissions (Sheryl/Lynette/Dawn/Cici) sees the resolved worklist and types the verified data into Credible.

V3 will replace the typing step with a structured handoff queue and (eventually) direct Credible write-back. V2 keeps it manual.

### 6. Audit trail

Every Stedi call writes one immutable `STEDI_AUDIT#` row (7-year TTL). Every encounter row links to its audit IDs via `audit_ids[]`. Every resolution event lives on the encounter-item row's `resolution` sub-object. Every rerun event appends to `rerun_history[]` with who/when/what changed. CMS provenance (which user, which IP, which Stedi control number) is queryable directly from DynamoDB.

## Testing

```bash
# All Stedi unit tests (no AWS dependencies beyond moto + pytest-mock)
python3 -m pytest lambda/tests/unit/stedi/ -q

# API-handler tests for the worklist endpoints
python3 -m pytest lambda/tests/unit/api/test_eligibility_worklist_api.py -q

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
| [test_fhir_patient_mapper.py](../../tests/unit/stedi/test_fhir_patient_mapper.py) | FHIR Patient → verify input dict (all field variants, edge cases) |
| [test_fhir_eligibility_poller.py](../../tests/unit/stedi/test_fhir_eligibility_poller.py) | Cursor advance, idempotency, demo-mode stream, daily-cap halt, encounter_filter pass-through |
| [test_eligibility_worklist_api.py](../../tests/unit/api/test_eligibility_worklist_api.py) | list_encounters, resolve_encounter, rerun_encounter, unread_count_for_org |

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

### "Poller ran but the nav badge is wrong"

`/me/permissions` caches `eligibility_unread_count` for 60 seconds (see `permissions._unread_cache`). Wait a minute or hard-refresh; if it persists, query the row directly:

```bash
aws dynamodb query --table-name penguin-health-stedi \
  --key-condition-expression "pk = :p AND begins_with(sk, :s)" \
  --expression-attribute-values '{":p":{"S":"ORG#demo"},":s":{"S":"ENCOUNTER_ITEM#"}}' \
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

## Eligibility ingestion

Two callers exercise `orchestrator.verify()`:

1. **Automated: FHIR encounter polling.** `fhir_eligibility_poller.handler`
   runs every ~15 minutes per opted-in org (EventBridge rate-rule). Each
   tick reads `FHIR_POLL_CURSOR`, queries
   `fhir_query.search('Encounter', _lastUpdated=gt{cursor}, _sort=_lastUpdated)`
   with the per-org `encounter_filter` (class/type/status), fetches each
   referenced `Patient`, maps via `fhir_patient_mapper.to_verify_input`,
   and writes an `ENCOUNTER_ITEM#{encounter_id}` row (idempotent on
   `encounter_id`). The cursor advances to the max processed
   `meta.lastUpdated`. On `StediDailyCapExceeded`, the poller halts and
   leaves the cursor at the last successful encounter so the next tick
   retries from there.

2. **Manual rerun: `eligibility_worklist_api.rerun_encounter`.** UR
   triggers a rerun from the worklist with corrected demographics; the
   API merges corrections over the row's submitted demographics, re-runs
   `orchestrator.verify`, and updates the same `ENCOUNTER_ITEM#` row in
   place (appending to `rerun_history[]`).

`demo_mode=true` swaps the FHIR API for `demo_fixtures.ENCOUNTER_STREAM`
so the demo org exercises the full poller pipeline without a real
Credible/FHIR endpoint.

## Out of scope (V3+)

- **Direct write-back to Credible** of verified coverage. V2 still produces a copy block; UR/Admissions pastes manually.
- **Structured handoff queue** for Admissions. V2 hands off via the resolved-row + optional note; V3 turns it into a real worklist with its own status flow.
- **Auth call log** — track who called the payer for precert, when, what auth number came back, when it renews.
- **Re-verify ticklers** — automatically re-run patients whose coverage is about to lapse.
- **Coverage extraction from `Encounter.account` / FHIR `Coverage`** — the patient mapper's `payer_id` slot is a placeholder today; populating it would let the poller go straight to the direct-eligibility path and skip discovery for known coverage.
