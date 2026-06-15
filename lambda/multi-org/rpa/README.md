# `rpa` — Robotic Process Automation for clinical-portal scraping

Browser-driven extraction of clinical notes from vendor portals when no
SFTP feed or FHIR API is available. The bot logs into the target portal
as a dedicated bot user, navigates a configured set of screens, and
writes one JSON record per extracted note into the per-org S3 bucket —
the same prefix the existing rules engine already consumes for SFTP-
sourced charts.

## When to use this vs. FHIR or SFTP

| Source | Use when |
|---|---|
| FHIR API ([`lambda/multi-org/fhir/`](../fhir/)) | The vendor exposes a FHIR R4 endpoint and your org has `client_credentials` registered. Preferred when available. |
| SFTP CSV ([`lambda/multi-org/csv-splitter/`](../csv-splitter/)) | The vendor delivers nightly CSV exports. Works for batch backfill, less good for compliance audits that need same-day note review. |
| **RPA (this module)** | The vendor does **not** offer an API or feed for clinical notes, but does expose the data via a clinician-facing portal. Last resort, but the only option for some EMRs. |

RPA is read-only on the portal. There is no `fill` op in the playbook
vocabulary and the engine has no JS-eval primitive. The bot cannot
modify clinical data.

## Architecture

```
EventBridge cron (per org) ────► Step Functions  ────► Fargate task
                                  state machine        (Playwright +
                                                        this module)
                                                            │
                                                            ▼
                                            authenticator.py (per-vendor)
                                                            │
                                                            ▼
                                                playbook_engine.py
                                                  drives BrowserContext
                                                            │
                                                            ▼
                                              result_writer.persist_note
                                                            │
                                ┌───────────────────────────┼───────────────┐
                                ▼                           ▼               ▼
                S3: data/{date}/                 audit.emit            EventBridge
                {ts}__{id}.json                  ClinicalNote read     RpaIngestComplete
                                │
                                ▼
                rules engine (no changes — same `data['text']` contract)
```

The CDK construct that provisions the runtime is
[`infra/components/rpa.py`](../../../infra/components/rpa.py).
The Fargate runner entry point is
[`fargate/rpa_runner/main.py`](../../../fargate/rpa_runner/main.py).

## Public surface

```python
from rpa import (
    load_rpa_config, load_playbook,      # config.py
    RpaError, RpaOrgNotConfigured,
    RpaPlaybookNotFound, RpaOutsideWindow,
    RpaAuthError, RpaUnsupportedVendor,
    RpaPlaybookError,
)
from rpa.record import RpaNoteRecord, RpaPatient, RpaEncounter
from rpa.usage_guard import check_or_raise
from rpa.rate_limiter import RateLimiter
from rpa.authenticator import authenticate
from rpa.playbook_engine import execute
from rpa.playbook_engine_playwright import PlaywrightPage
from rpa.result_writer import persist_note
```

The HTTP API surface lives in
[`lambda/api/rpa_api.py`](../../api/rpa_api.py); the four routes are
wired into the admin Lambda dispatch table.

## Module layout

| File | Purpose |
|---|---|
| `config.py` | Load `RPA_CONFIG` and `RPA_PLAYBOOK` items from `penguin-health-org-config` |
| `secrets.py` | Read per-org `{client_id, client_secret}` from Secrets Manager |
| `authenticator.py` | Vendor dispatch — picks a per-vendor module from `REGISTRY` |
| `authenticators/centralreach.py` | CR-specific SSO → JWT → session-cookie flow |
| `playbook_engine.py` | Async generator that drives a `Page` protocol from a declarative playbook |
| `playbook_engine_playwright.py` | Playwright adapter for the `Page` protocol + CSS-only selector guard |
| `usage_guard.py` | Timezone-aware allowed-hours + blackout-date check |
| `rate_limiter.py` | Async minimum-delay gate between Playwright actions |
| `record.py` | `RpaNoteRecord` dataclass — the on-disk JSON shape with PHI-safety validators |
| `result_writer.py` | Build record + write S3 + emit audit event |
| `exceptions.py` | Module exception hierarchy rooted at `RpaError` |

## PHI handling

The runner sees raw PHI in memory (names, DOB, MRN, note text) for a few
seconds at a time. It is the entire team's responsibility to keep that
window short and the surface tight. The module enforces this:

1. **The S3 payload is the only place a note body lives.** The runner
   stores it under the per-org bucket's KMS-encrypted prefix. Logs,
   audit events, and DDB indexes never receive it.
2. **Patient identifiers in audit events are hash + initials + last-4 only.**
   `result_writer.persist_note` hands raw fields to the trusted
   `audit.emit` boundary; the emitter computes
   `audit.schema.patient_hash(first|last|dob)` and drops the raw values
   per [emitter.py:144-152](../audit/emitter.py).
3. **`extracted_fields` rejects identity keys at construction time.**
   `RpaNoteRecord` refuses keys like `first_name`, `dob`, `ssn`,
   `patient_name` in `extracted_fields` so a playbook can't smuggle raw
   PHI around the hash. Clinical fields the rules engine needs
   (`signed_at`, `billed_duration_minutes`, supervisor signatures, etc.)
   are explicitly allowed — only identity is forbidden.
4. **No screenshots in v1.** Image-format PHI is harder to redact and
   needs its own Object Lock bucket; the cost-benefit didn't justify it
   for v1.
5. **Cookies + JWT die with the Fargate task.** Nothing persists between
   runs. Every scheduled pass re-authenticates from the Secrets-Manager
   client credentials.

## Authentication

Per-vendor modules under `authenticators/` implement the vendor's
specific "credentials → session" flow. The CR implementation follows
CR's documented Examples A + B:

1. POST `client_id` + `client_secret` to `login.centralreach.com/connect/token`
   (form-encoded, `grant_type=client_credentials`, `scope=cr-api`).
2. POST `{"token": "<jwt>"}` (JSON body, no `Authorization` header) to
   `members.centralreach.com/api/?framework.authtoken`.
3. Harvest the **two required cookies** `crsd` + `crud` from the
   response. Missing either → `RpaAuthError`.

Both URLs are hardcoded to documented prod endpoints; per-org
`vendor_settings.centralreach.base_overrides` redirects to a sandbox
tenant when CR has provisioned one. There is no auth-code grant, no
state HMAC, no callback handler, no refresh token, no token persistence
between runs. See
[`docs/rpa-integration-onboarding.md`](../../../docs/rpa-integration-onboarding.md#adding-a-new-vendor)
for the per-vendor authoring contract when adding a second vendor.

## Playbook engine

Async generator. Talks to a `Page` protocol (defined in
`playbook_engine.py`) — `navigate`, `click`, `wait_for_selector`,
`query_text`, `query_attr`, `query_all`, `exists`. The Playwright
adapter (`playbook_engine_playwright.py`) implements that protocol
against a real browser context.

The op vocabulary is a strict allowlist, pinned by
`test_playbook_engine.py::test_fill_op_is_not_in_the_allowlist`:

- `navigate(url)`
- `click(selector)`
- `wait_for_selector(selector, timeout_ms?)`
- `extract(fields: {name: {selector, attr?}})`
- `loop_over_list(selector, max_items?, body: [ops])`
- `if_exists(selector, then: [ops], else?: [ops])`
- `emit_note` — flush the current extraction dict to the engine output
- `log(message)`
- `stop`

`emit_note` is explicit (not implicit at end-of-extract) so playbooks
can stitch together fields from multiple screens before yielding.

Selectors are **CSS only**. The Playwright adapter rejects `text=`,
`xpath=`, `role=`, `css=`, and `/`/`//` prefixes at runtime; the seed
script ([`scripts/multi-org/seed_rpa_playbook.py`](../../../scripts/multi-org/seed_rpa_playbook.py))
catches the same prefixes at config-write time.

## Per-vendor module contract

A vendor module under `authenticators/` must export:

```python
AUTH_VENDOR = "vendor_key_matching_RPA_CONFIG.vendor"

def authenticate(*, org_id, vendor_cfg, credentials,
                 http_post=..., fetch_cookies=...) -> dict:
    """Return:
        {
          "cookies": [{"name", "value", "domain", "path", "secure"}, ...],
          "extra_http_headers": {...},   # may be empty
          "access_token": "...",         # diagnostic; never logged
        }
    """
```

`http_post` and `fetch_cookies` must be injectable so tests can pin the
request shapes without touching the network. See
`authenticators/centralreach.py` as the reference.

Add the module to `authenticators/__init__.py`'s `REGISTRY` to make it
selectable via `RPA_CONFIG.vendor`. The dispatcher in `authenticator.py`
raises `RpaUnsupportedVendor` if no module is registered for the
configured vendor.

## Tests

```bash
cd lambda && python3 -m pytest tests/unit/rpa/ tests/unit/scripts/test_seed_rpa_playbook.py
```

Browser-driven integration test (requires Playwright + Chromium
installed locally):

```bash
pip install playwright aiohttp && playwright install chromium
cd lambda && python3 -m pytest tests/unit/rpa/test_playbook_engine_integration.py -m playwright
```

The integration test spins up an aiohttp fixture server, drives the
playbook engine through Playwright against it, and asserts the
extracted notes round-trip correctly. The marker is registered in
[`pytest.ini`](../../tests/pytest.ini) and skipped by default.

## Authoring a playbook from a real portal session

The selectors in [`playbooks/centralreach/notes-v1.json`](../../../playbooks/centralreach/notes-v1.json)
are **placeholders**. They will not match a real CentralReach DOM.
Before the playbook can extract anything, an engineer with access to a
real (sandbox) clinician session has to walk the screens once and
replace each placeholder with a selector that actually matches the
vendor's live UI.

The workflow below is the recommended path. Other approaches (Playwright
Codegen, a future `MODE=record` runner) exist but have higher overhead
for the v1 case.

### Practical session script

```
1. Open Chrome → DevTools → Elements + Network panels.
2. Navigate to https://members.centralreach.com/clinicians/worklist
3. Confirm a clinician login works manually.
4. In DevTools console, run:
       document.querySelectorAll('[data-testid]').forEach(e =>
         console.log(e.dataset.testid, e.tagName))
   → look for testids like "worklist-row", "patient-mrn", "open-chart"
5. Right-click a patient row → "Inspect" → look for a stable
   wrapper class.
6. For each field in the playbook (source_patient_id, first_name,
   last_name, dob, note_id, visit_date, signed_at, billed_*,
   supervisor_*), find the element and copy a stable CSS selector
   for it.
7. Repeat for the chart-open page and the note-detail page.
8. Edit playbooks/centralreach/notes-v1.json, replacing each
   placeholder.
9. Bump version to 2 in seed_rpa_playbook.py args (so the audit
   trail distinguishes the placeholder-era run from the real one).
10. Dry-run the seed script.
11. Seed into a sandbox org's RPA_PLAYBOOK#cr-notes-v2 with
    --org-id demo so it's an override, not the shared playbook
    (until you trust it).
12. Trigger a manual run, watch CloudWatch logs + S3 output.
13. Iterate until selectors are reliable across 3-5 runs.
14. Promote the playbook to shared (drop --org-id) once stable.
```

### Things to watch for during capture

- **Single-page-app navigation.** CR is likely a SPA; clicking a
  worklist row may patch the DOM rather than navigate. Watch the URL
  bar — if it doesn't change, drop the `navigate` op and just use
  `click` + `wait_for_selector`.
- **Lazy-loaded content.** A note's body may load after a click + a
  network call. The `wait_for_selector` op must target the *body*
  element, not the header, or extraction will read empty text. Use
  the Network panel to confirm what loads when.
- **Pagination on the worklist.** If a worklist has > one page of
  patients, the playbook's `loop_over_list` will only see the first
  page. Either set `max_items` low enough that pagination doesn't
  matter, or model "click next page → loop again" with `if_exists`
  on a Next button. (The engine has no native paginate op in v1.)
- **Modal vs. inline notes.** Some EMRs open notes as modals. The
  selector path differs from inline. Capture against the actual UX,
  not a guess.
- **Date formats.** `signed_at` may render as
  "Jun 15, 2026 10:30 AM" — that's a string the rules engine will
  try to parse. Prefer extracting the underlying `datetime` attribute
  on a `<time>` element if CR uses semantic HTML.

### Selector quality cheat sheet

Prefer, in this order:

1. `[data-testid="..."]`, `[data-cy="..."]`, `[data-qa="..."]` —
   vendors add these for their own automated testing; most stable.
2. Stable class names that look like a design-system primitive
   (`.mrn-display`, `.note-body`) rather than CSS-modules hashes
   (`.css-1k8j2lz`).
3. Semantic attributes like `[role="..."]`, `[aria-label="..."]`.

Avoid:

- `nth-child` / `nth-of-type` — fragile against reordering
- Deep descendant chains (`.a .b .c .d .e`)
- Auto-generated CSS-modules class names
- Anything Chrome DevTools' "Copy selector" produced verbatim without
  simplification

## Operations

Day-to-day operations (provisioning credentials, seeding configs,
authoring playbooks, enabling schedules, troubleshooting failures) are
in [`docs/rpa-integration-onboarding.md`](../../../docs/rpa-integration-onboarding.md).
