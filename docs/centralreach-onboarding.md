# Onboarding a new org to CentralReach ingest

This is the operational checklist. The design rationale lives in
[`centralreach-api-integration.md`](centralreach-api-integration.md);
this doc is the step-by-step.

## Prerequisites

Before adding an org, confirm:

1. **AWS BAA covers Bedrock** for the target AWS account. Bedrock is
   used by the rules engine for narrative-derived rule evaluation on
   centralreach records (PR E).
2. **Bedrock model invocation logging is disabled** in the account.
   `aws bedrock get-model-invocation-logging-configuration` should
   return no logging destination configured. Per the design doc's PHI
   handling section, logging must stay off.
3. **The org has CR credentials** to provide to us. Currently:
   `{client_id, client_secret}` for the OAuth flow (see the design
   doc's Open Questions on the auth-flow gap — the secret payload
   shape may change to `{username, password}` once the gap resolves).

## Step 1 — Provision the credentials secret

Create the Secrets Manager secret holding the bot's credentials. The
runner reads this on every task invocation; the IAM grant is wildcard-
scoped to `penguin-health/centralreach/*/credentials`, so each org's
secret is isolated.

```bash
aws secretsmanager create-secret \
    --name penguin-health/centralreach/{org_id}/credentials \
    --secret-string '{"client_id":"...","client_secret":"..."}'
```

If a credentials secret already exists from a previous attempt, use
`put-secret-value` to update it rather than recreating.

## Step 2 — Seed the org's config row

Run `scripts/multi-org/add_centralreach_config.py`. The config drives
the runner's timezone, allowed-hours window, blackout dates, and the
per-request rate limit.

```bash
python3 scripts/multi-org/add_centralreach_config.py \
    --org-id {org_id} \
    --display-name "{Display name shown in runs UI}" \
    --bot-username "centralreach-bot+{org_id}" \
    --timezone {IANA_TZ} \
    --allowed-hours-start 06:00 \
    --allowed-hours-end 20:00 \
    --rate-limit-ms 1500 \
    --blackout-dates {YYYY-MM-DD,YYYY-MM-DD,...}
```

Flag notes:

- `--timezone`: the **clinical day** timezone, not your laptop's tz.
  This value drives both the `tzoffset` cookie and the
  `_utcOffsetMinutes` field on every CR request. CR rejects mismatched
  values, so this must match what CR has on file for the org.
- `--allowed-hours-start/--end`: when may the runner make requests.
  Default `06:00–20:00` aligns with normal clinical hours and stays
  well clear of CR's nightly maintenance windows.
- `--rate-limit-ms`: minimum gap between HTTP requests. Default 1500
  ms matches the UI cadence. Decrease only after multiple successful
  runs without rate-limit errors.
- `--blackout-dates`: dates the runner must skip entirely (typically
  holidays plus the org's scheduled CR-side maintenance days).

Run `--dry-run` to print the DDB item without writing. The CLI
output shows the eight guardrail fields it computed; if a value is
missing or wrong, fix the flag and rerun.

## Step 3 — Enable the EventBridge schedule

Add an entry to `_PER_ORG_SCHEDULES` in
[`infra/components/centralreach.py`](../infra/components/centralreach.py):

```python
_PER_ORG_SCHEDULES = [
    # ... existing orgs ...
    {"org_id": "{org_id}", "cron": "cron(0 22 * * ? *)"},
]
```

Cron syntax is EventBridge's, not crontab's — note the **six fields**
(min, hour, day-of-month, month, day-of-week, year) and the `?`
placeholder. Always set the schedule to the **UTC** clock; the runner
applies the org's timezone for the allowed-hours window inside the
task.

Then redeploy:

```bash
cd infra && npx cdk deploy PenguinHealth
```

The CDK component adds:

- a new EventBridge rule per org
- the rule's target is the same Step Functions state machine all orgs
  share — the rule injects `organization_id` into the SFN input

## Step 4 — First run + monitoring

Trigger a manual run through the admin API (avoids waiting for the
EventBridge cron):

```
POST /api/organizations/{org_id}/centralreach/run
```

Then watch:

- **CloudWatch Logs**: `/aws/ecs/penguin-health-centralreach-runner`.
  The runner prints `centralreach-ingest:` lines with the org_id and
  failure mode at each stage.
- **Step Functions console**: the state machine
  `penguin-health-centralreach-ingest` shows per-run execution
  history; failed task overrides will surface here.
- **Audit table**: the run emits `centralreach_ingest_run_started`
  and `centralreach_ingest_run_completed` events at minimum. Per-note
  reads emit `centralreach_note_ingest` (one per processed entry).

## Failure modes worth knowing about

### `CentralReachOrgNotConfigured`

The CENTRALREACH_CONFIG row is missing or `enabled: false`. Step 2
fixes it. The Fargate task exits 2, the SFN execution shows FAILED
with cause "centralreach-ingest: not configured".

### `CentralReachOutsideWindow: outside_allowed_hours`

A scheduled run fired outside the configured window. This is expected
behavior when EventBridge fires at a different local time than the
org's allowed-hours window — the runner exits 0 (clean skip) and the
next scheduled run picks up the work. If runs are persistently
skipped, your cron expression and `--allowed-hours-*` don't line up.
Remember:

- EventBridge cron is in **UTC**.
- `--allowed-hours-*` is in the **org's local timezone**.

### `CentralReachAuthError: PlaceholderAuthenticator is not a real auth flow`

The Fargate task can't run yet — the auth flow is incomplete. See the
design doc's Open Questions section on the auth gap. The placeholder
authenticator exists so the rest of the code can ship; replacing it
with the real implementation is its own piece of work.

### `success: false` from `resources.getresourceurl`

The runner ran, but the bot session can't access individual file
resources. This is the same root cause as the auth gap — a service-
account session has a different `crud` shape than a user-session
`crud`, and per-resource endpoints check identity in a way that
rejects the SSO-JWT-wrapped form. Resolve via the same auth-gap fix.

## When CR breaks an endpoint

Reverse-engineered APIs have no contract. If CR changes a request body
shape, response shape, or auth behavior, the runner surfaces this as:

- `CentralReachContentTypeError` — the response came back HTML instead
  of JSON. Usually a header-shape mismatch; check that `Accept`
  matches what the captured request sent.
- `CentralReachValidationError` carrying a `fieldName` — CR's
  validator named a field we're missing. Add it to the body builder.
- `CentralReachAPIError` with a status code — a 4xx or 5xx. Check
  CloudWatch for the full URL and any response body the runner logged.

The design doc's Risks section names these as the load-bearing
contract risks.
