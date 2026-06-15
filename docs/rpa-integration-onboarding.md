# RPA integration onboarding

End-to-end checklist for bringing a new org onto the RPA browser-automation
pipeline. The RPA bot logs into a clinical portal as a dedicated bot user
and extracts clinical-note details that feed the existing rules engine —
all read-only on the portal, all writes go to per-org S3 + audit log.

The first supported vendor is CentralReach. Adding a new vendor is a
separate engineering task (see the [Adding a new vendor](#adding-a-new-vendor)
section at the end).

## What you need before you start

| Item | Source | Where it ends up |
|---|---|---|
| Bot `client_id` + `client_secret` | Provisioned by CR's Implementations team for your org's tenant | Secrets Manager at `penguin-health/rpa/{org_id}/credentials` |
| Org's preferred allowed-hours window | Compliance / clinical ops at the org | `RPA_CONFIG.guardrails.allowed_hours` |
| Org's blackout dates (holidays, system-maintenance windows) | Compliance / clinical ops at the org | `RPA_CONFIG.guardrails.blackout_dates` |
| `Compliance Audit` permission on this org | Cognito group / per-user perms | Required to call `POST /rpa/run` manually |

## 1. Provision the bot user at CentralReach

1. Ask CR's Implementations team to provision a **service / RPA client_id +
   client_secret** for your org's tenant. The bot is its own user account
   — do NOT reuse a clinician's credentials.
2. Confirm with CR that the credentials are scoped to **`cr-api`** scope
   and have **read access to clinical notes** (Internal API surface, not
   Public API). Per CR's auth doc, RPA requires the legacy session cookies
   (`crsd` + `crud`), which the `cr-api` scope grants.
3. Decide whether CR is provisioning **prod** or **sandbox**. The default
   authenticator URLs target prod
   (`login.centralreach.com` + `members.centralreach.com`). If CR has
   issued a sandbox tenant, pass `--cr-sandbox` to `add_rpa_config.py`
   so the per-org `vendor_settings.centralreach.base_overrides` redirects.

## 2. Store the credentials in Secrets Manager

Never commit credentials to the repo or paste them into a chat. Use:

```bash
aws secretsmanager create-secret \
    --name penguin-health/rpa/{org_id}/credentials \
    --secret-string "$(jq -nc --arg ci "$CR_CLIENT_ID" --arg cs "$CR_CLIENT_SECRET" \
        '{client_id:$ci,client_secret:$cs}')"
```

To rotate later (CR issues a new secret, old one stays valid for the
overlap window):

```bash
aws secretsmanager put-secret-value \
    --secret-id penguin-health/rpa/{org_id}/credentials \
    --secret-string "$(jq -nc --arg ci "$CR_CLIENT_ID" --arg cs "$CR_CLIENT_SECRET" \
        '{client_id:$ci,client_secret:$cs}')"
```

The runner reads the secret at the start of every Fargate task. No restart
needed — the next scheduled run picks up the new value.

## 3. Seed the playbook (one-time per vendor/version)

Playbooks are shared across orgs by default. If a CR-targeted playbook
already exists at `RPA_PLAYBOOK#cr-notes-v1`, skip this step.

```bash
python scripts/multi-org/seed_rpa_playbook.py \
    --playbook-id cr-notes-v1 \
    --vendor centralreach \
    --version 1 \
    --json playbooks/centralreach/notes-v1.json
```

The script validates the playbook's op vocabulary and selectors before
the DDB write. If it rejects your playbook, see the [Playbook authoring
rules](#playbook-authoring-rules) section.

## 4. Seed the per-org RPA_CONFIG

```bash
python scripts/multi-org/add_rpa_config.py \
    --org-id {org_id} \
    --vendor centralreach \
    --display-name "{Org Name} CR clinical-notes bot" \
    --base-url https://members.centralreach.com \
    --bot-username "rpa-bot+{org_id}" \
    --playbook-id cr-notes-v1 \
    --timezone {IANA_TZ} \
    --allowed-hours-start 06:00 \
    --allowed-hours-end 20:00 \
    --rate-limit-ms 1500 \
    --blackout-dates {YYYY-MM-DD,YYYY-MM-DD,...}
```

Tune these per org:

- `--timezone`: the **clinical day** timezone, not your laptop's tz.
- `--allowed-hours-start/--end`: when may the bot touch the portal.
  Default `06:00–20:00` aligns with normal clinical hours.
- `--rate-limit-ms`: minimum gap between Playwright actions. Default 1500
  ms. Decrease only after multiple successful runs without rate-limit
  errors.
- `--blackout-dates`: ISO `YYYY-MM-DD` dates the bot must skip entirely.

The script prints the resulting DDB item and a reminder of the three
remaining steps.

## 5. Verify end-to-end with a manual run

Until the org is added to the EventBridge schedule (step 6), the bot will
not run automatically. Manual runs prove the pipeline works:

```bash
curl -X POST "$ADMIN_API/api/organizations/{org_id}/rpa/run" \
    -H "Authorization: Bearer $JWT"
```

The response is `202` with a `run_id` and `execution_arn`. Track:

- **Step Functions console** → `penguin-health-rpa-run` → the matching
  execution. Status should progress
  `Running` → `Succeeded`. A `Failed` status with a `States.TaskFailed`
  cause means the Fargate task exited non-zero — check CloudWatch.
- **CloudWatch logs** at `/aws/ecs/penguin-health-rpa-runner`. The runner
  prints structured `rpa-runner: ...` lines. **No PHI is logged** — only
  identifiers, error class names, and counts. If you see PHI in logs,
  that's a bug; file it before proceeding.
- **S3** at `s3://penguin-health-{org_id}/data/{YYYY-MM-DD}/`. Each note
  lands as `{YYYYMMDDTHHMMSSZ}__{note_id}.json` matching `RpaNoteRecord`
  ([record.py](../lambda/multi-org/rpa/record.py)).
- **Audit DDB** `penguin-health-audit`. There should be exactly one
  `RpaPlaybookRun execute success` row and N `ClinicalNote read` rows
  matching the S3 object count. Patient identity surfaces only as a
  hash + initials.
- **EventBridge** event `penguin-health.rpa / RpaIngestComplete` carries
  `{organization_id, ingest_date, playbook_run_id, note_count, vendor}`.
- **Downstream**: run the rules engine for the same date and confirm
  the notes appear in `penguin-health-validation-results`.

If any of those don't fire, see [Troubleshooting](#troubleshooting) below.

## 6. Enable the EventBridge schedule

Once a manual run is clean, opt the org into automatic scheduled runs by
editing `_PER_ORG_SCHEDULES` in
[`infra/components/rpa.py`](../infra/components/rpa.py):

```python
_PER_ORG_SCHEDULES: list[dict] = [
    {"org_id": "{org_id}", "cron": "cron(0 22 * * ? *)"},
]
```

Cron syntax is EventBridge's, not crontab's — note the **six fields**
(min, hour, day-of-month, month, day-of-week, year) and the `?`
placeholder. Always set the schedule to the **UTC** clock; the runner
applies the org's timezone for the allowed-hours window inside the task.

Then redeploy:

```bash
cd infra && cdk deploy
```

The CDK output `RpaStateMachineArn` is the same ARN the admin Lambda
already has wired in; you don't need to copy anything out.

## 7. Wire up rule firing (optional)

The 13 documented compliance rules ([rule coverage matrix](#rule-coverage-matrix))
fire automatically once the playbook extracts the required fields. If
the org needs additional rules beyond the matrix, add them via the
existing rules-engine onboarding flow — RPA-sourced notes flow through
the engine the same way SFTP-sourced charts do.

---

## Playbook authoring rules

Read [`lambda/multi-org/rpa/playbook_engine.py`](../lambda/multi-org/rpa/playbook_engine.py)
for the full op vocabulary. The seed script
([scripts/multi-org/seed_rpa_playbook.py](../scripts/multi-org/seed_rpa_playbook.py))
enforces these rules at write time:

- **Op allowlist**: `navigate`, `click`, `wait_for_selector`, `extract`,
  `loop_over_list`, `if_exists`, `emit_note`, `log`, `stop`. No `fill`,
  no JS eval, no `evaluate`, no arbitrary HTTP.
- **Selectors must be CSS only**. No `text=`, `xpath=`, `role=`, `css=`
  prefixes; no XPath. The Playwright adapter rejects these at runtime
  too, but catching them in the seed script saves a Fargate cold start.
- **Read-only**: there is no way to POST to or modify the target
  system. If a use case appears that needs writes, do not add `fill`
  — design a separate engineering pass with explicit user authorization
  and a different audit pathway.

When the playbook engine yields a note via `emit_note`, the resulting
dict must contain at minimum the keys [`build_record`](../lambda/multi-org/rpa/result_writer.py#L60)
expects:

- `source_record_id`, `first_name`, `last_name`, `dob`,
  `source_patient_id`, `visit_date`, `provider_display`, `note_type`,
  `text`, `body_html`

Anything else goes into `extracted_fields`. Forbidden keys in
`extracted_fields` (raw PHI identity like `first_name`, `dob`, `ssn`)
are rejected at record-construction time so a buggy playbook can't
sidestep the patient hash.

The bundled example playbook
[`playbooks/centralreach/notes-v1.json`](../playbooks/centralreach/notes-v1.json)
extracts all 13 fields the rule matrix needs.

---

## Rule coverage matrix

The compliance-audit rules the RPA pipeline supports out of the box, and
the playbook fields that drive them. See
[the rules engine docs](#) for how rules are evaluated.

| # | Rule | Engine type | Required extracted fields |
|---|---|---|---|
| 1 | Narratives individualized (cross-doc) | **NOT in v1** — needs a vector index | n/a |
| 2 | ≥ 2 sentences per hour | LLM | `text`, `session_duration_minutes` |
| 3 | Third person | LLM | `text` |
| 4 | Signed within 5 min of billed end | deterministic | `signed_at`, `billed_end` |
| 5 | Billed time = session time | deterministic | `billed_duration_minutes`, `session_duration_minutes` |
| 6 | Location match | deterministic | `billed_location`, `note_location` |
| 7 | Provider 3-way match | deterministic | `provider_display`, `provider_billed`, `provider_signature` |
| 8 | (= rule 3) | LLM | `text` |
| 9 | No "nap" | deterministic regex | `text` |
| 10 | ≤ 4 hour billed | deterministic | `billed_duration_minutes` |
| 11 | Data present in note | LLM | `text` |
| 12 | Converted within 8 days | deterministic | `visit_date`, `note_created_at` |
| 13 | Supervisor name = signature | deterministic | `supervisor_name`, `supervisor_signature` |

Rule 1 is a separate workstream (vector index over historical notes); the
RPA pipeline produces the inputs but the engine can't evaluate it yet.

---

## Troubleshooting

### `RpaAuthError: CR SSO returned HTTP 401`

The `client_id`/`client_secret` Secrets Manager value is wrong or the
CR-issued credentials were revoked. Verify the secret with
`aws secretsmanager get-secret-value` (you'll need the
`secretsmanager:GetSecretValue` permission directly — the Fargate task
role is scoped to `penguin-health/rpa/*/credentials-*` only, so you
can't grant yourself the runner's perms). If the secret looks right,
ask CR's team to confirm the credentials are still active.

### `RpaAuthError: CR legacy-auth response missing required cookie(s): ['crsd']`

The JWT exchanged successfully but the legacy auth endpoint did not
return both required session cookies. Most common cause: the CR
account's `scope` is missing the `cr-api` permission that grants
internal-API access. Confirm with CR. Less commonly: CR rolled an
endpoint URL — the `base_overrides` would let you point at a new URL
without redeploying.

### `RpaOutsideWindow: outside_allowed_hours`

A scheduled run fired outside the configured window. This is expected
behavior when EventBridge fires at a different local time than the
org's allowed-hours window — the runner exits 0 and the next scheduled
run picks up the work. If runs are persistently skipped, your cron
expression and `--allowed-hours-*` don't line up. Remember:

- EventBridge cron is in **UTC**.
- `--allowed-hours-*` is in the **org's local timezone**.

### `RpaPlaybookError: wait_for_selector(...) failed`

The portal changed its DOM, or the bot loaded a different page than the
playbook expected (e.g., a session-timeout interstitial). Pull the
CloudWatch log for the failing run, look at the last successful
`navigate` op, and reproduce in a clinician browser to see what's on
the page. Update the playbook selectors, bump `version`, re-seed via
`seed_rpa_playbook.py`. Bump the version because the audit event
records `playbook_version` and you want a clean before/after.

### Notes land in S3 but don't show up in `penguin-health-validation-results`

The rules engine runs on its own schedule, not on the
`RpaIngestComplete` event. Trigger a validation run for that date
explicitly: `POST /api/organizations/{org_id}/validation-runs`. If
notes still don't appear, check the rules engine logs for the date —
the engine logs the count of input files it scanned under
`data/{date}/`.

---

## Adding a new vendor

If you need to support a portal other than CentralReach:

1. Read the vendor's auth doc carefully. The shape of "exchange
   credentials → session" varies meaningfully (some vendors use auth
   code grant, some use bearer-only). Do not try to fit a new vendor
   into the CR-shaped authenticator.
2. Add a new module under
   [`lambda/multi-org/rpa/authenticators/`](../lambda/multi-org/rpa/authenticators/)
   exporting:
   - `AUTH_VENDOR = "{vendor_key}"`
   - `def authenticate(*, org_id, vendor_cfg, credentials, ...) -> dict`
     returning `{"cookies": [...], "extra_http_headers": {...}, "access_token": "..."}`.
   Use the CR authenticator as a template, but expect to change the
   request shapes.
3. Register the module in
   [`lambda/multi-org/rpa/authenticators/__init__.py`](../lambda/multi-org/rpa/authenticators/__init__.py)'s
   `REGISTRY`.
4. Add the vendor key to `_VALID_VENDORS` in `add_rpa_config.py`.
5. Add unit tests for the new authenticator pinning every documented
   request shape (URL, body shape, headers) so future drift fails the
   suite.
6. Add a per-vendor playbook under `playbooks/{vendor}/`. Selectors
   will be different; the op vocabulary is the same.
7. Update this doc's "What you need" table for the new vendor's
   credential-provisioning quirks.

The runner code, Fargate infra, audit emission, and downstream rules
engine integration all remain unchanged — the per-vendor isolation is
the design's whole point.

---

## References

- Plan: [`/.claude/plans/i-need-to-add-frolicking-blossom.md`](../.claude/plans/i-need-to-add-frolicking-blossom.md)
- Module README: [`lambda/multi-org/rpa/README.md`](../lambda/multi-org/rpa/README.md)
- CentralReach auth doc: provided by CR's Implementations team
  ("Authenticating a Request to CentralReach's APIs", rev. 2024-07-31)
- Existing rules-engine onboarding:
  [`docs/stedi-encounter-filter-onboarding.md`](stedi-encounter-filter-onboarding.md)
  (different integration, same shape of operational checklist)
