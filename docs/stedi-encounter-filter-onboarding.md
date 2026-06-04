# Stedi encounter-filter onboarding

How to determine the right `--encounter-filter-class-codes`,
`--encounter-filter-type-codes`, and `--encounter-filter-statuses` values
for a new organization before flipping `--census-enabled` on.

The poller passes these straight through to the FHIR Encounter search
([fhir_eligibility_poller.py:170](../lambda/multi-org/stedi/fhir_eligibility_poller.py#L170)).
Wrong filter values mean one of two failure modes:

- **Too narrow** — admissions silently never get verified.
- **Too broad** — every closed/cancelled stay re-burns a Stedi transaction
  and the org hits `daily_cap` mid-day, after which the poller halts and
  newer admissions get dropped until tomorrow.

Both are invisible from the UI. Do this process for every new org.

## What the filters are

The flags map to standard FHIR R4 valuesets.

### `class_codes` → `Encounter.class`

HL7 v3 ActEncounterCode. Common values for behavioral-health admits:

| Code | Meaning |
|---|---|
| `IMP` | Inpatient encounter |
| `EMER` | Emergency |
| `AMB` | Ambulatory / outpatient |
| `OBSENC` | Observation |
| `SS` | Short stay |
| `ACUTE` | Acute inpatient |
| `NONAC` | Non-acute inpatient |
| `PRENC` | Pre-admission |
| `HH` | Home health |
| `VR` | Virtual |

Spec: https://hl7.org/fhir/R4/v3/ActEncounterCode/vs.html

### `statuses` → `Encounter.status`

R4 valueset: `planned`, `arrived`, `triaged`, `in-progress`, `onleave`,
`finished`, `cancelled`, `entered-in-error`, `unknown`.

Spec: https://hl7.org/fhir/R4/valueset-encounter-status.html

For "verify everyone currently on census" the right answer is almost
always a subset of `{planned, arrived, in-progress}` — exclude `finished`,
`cancelled`, `entered-in-error` so we don't re-verify closed stays.

### `type_codes` → `Encounter.type`

Site-specific. Use this **only** when `class` is unreliable for the org
(see step 3). Codes come from whichever code system the EMR uses
(SNOMED, local code system, etc.) — there is no single canonical list.

## Process

### 1. Confirm the org has FHIR credentials

The sampling step below uses the same client the poller uses, so the org
needs a `FHIR_CONFIG` row in `penguin-health-org-config` already (see
[scripts/multi-org/add_fhir_config.py](../scripts/multi-org/add_fhir_config.py)).

```bash
aws dynamodb get-item --table-name penguin-health-org-config \
  --key '{"pk":{"S":"ORG#<new-org-id>"},"sk":{"S":"FHIR_CONFIG"}}' \
  --query 'Item'
```

If this is empty, provision FHIR first — encounter filtering can't be
sized without seeing real traffic.

### 2. Pull a sample of recent Encounters

Run a one-tick sample against the org's live FHIR endpoint. The cleanest
way is from inside a Lambda shell so credentials and KMS aliases resolve
exactly the way they do in production.

Invoke an ad-hoc Python session in the poller's runtime, or run locally
with AWS credentials that can reach KMS and Secrets Manager for that org:

```python
from collections import Counter
from fhir import fhir_query

ORG = '<new-org-id>'
CURSOR = '2026-05-01T00:00:00Z'      # ~30 days back is usually enough

encounters = list(fhir_query.search(
    ORG, 'Encounter',
    {'_lastUpdated': f'gt{CURSOR}', '_sort': '_lastUpdated', '_count': 200},
    max_results=500,
))

classes  = Counter(e.get('class', {}).get('code') for e in encounters)
statuses = Counter(e.get('status') for e in encounters)
types    = Counter(
    c.get('code')
    for e in encounters
    for t in (e.get('type') or [])
    for c in (t.get('coding') or [])
)

print(f'sampled {len(encounters)} encounters since {CURSOR}')
print('class  :', classes.most_common())
print('status :', statuses.most_common())
print('type   :', types.most_common(10))
```

**PHI guard.** The raw `encounters` list contains patient references,
demographics, and (depending on the EMR) free-text reason codes. Never
paste it into chat, tickets, screenshots, or commit it. Only the
aggregated `Counter` output is safe to share — it carries no patient
identifiers.

### 3. Read the histogram

You're looking for three things:

**(a) Is `class` populated?**

- If most encounters have a non-null `class.code`, filter on it.
- If `class.code` is mostly `null` or `unknown`, **don't** filter on
  class — you'll drop everything. Fall back to `type_codes`.

**(b) Which class codes correspond to admissions you care about?**

For behavioral-health inpatient, typically just `IMP`. Some EMRs route
admissions through `EMER` → `IMP` quickly enough that you want both. If
the org runs partial-hospitalization or IOP and wants verification on
those, you'll also want `AMB` or `OBSENC` — confirm with the org first;
this changes the daily-cap math.

**(c) Which statuses mean "currently on census"?**

This varies by EMR workflow. The rule of thumb:

- Include `in-progress` always.
- Include `planned` if the EMR creates the Encounter at scheduling
  (most do) — verifying before arrival is the whole point.
- Include `arrived` only if the EMR has a meaningful arrived→in-progress
  gap; otherwise it's redundant.
- **Exclude** `finished`, `cancelled`, `entered-in-error`, `onleave`,
  `unknown`, `triaged`.

### 4. Sanity-check against the daily cap

Estimate matches per day = (sampled encounters that pass the filter) ÷
(days in the sample window). Each match costs **1–4 Stedi transactions**
depending on which orchestrator branch fires (see the
[stedi README](../lambda/multi-org/stedi/README.md#decision-tree-how-orchestratorverify-works)):

- Direct (member_id + payer_id on file): 1
- Discovery → eligibility: ≤2
- Discovery-first: ≤4

For sizing, assume the worst case (×4) unless the org has confirmed
coverage data on the FHIR Patient resource. If projected daily
transactions exceed `daily_cap`, either raise the cap or narrow the
filter (drop `planned`, drop a class code) before turning it on.

### 5. Write the config

```bash
python3 scripts/multi-org/add_stedi_config.py \
  --org-id <new-org-id> \
  --npi <10-digit-NPI> \
  --organization-name "<Legal Org Name>" \
  --daily-cap <from step 4> \
  --preferred-payers <comma-separated, from payer_registry.py> \
  --census-enabled \
  --encounter-filter-class-codes IMP \
  --encounter-filter-statuses planned,in-progress \
  --dry-run
```

Inspect the printed JSON. Drop `--dry-run` to commit.

If you fell back to `type_codes` in step 3, swap
`--encounter-filter-class-codes` for `--encounter-filter-type-codes <codes>`.

### 6. Verify with one manual poller invocation

Before EventBridge takes over (it fires every ~15 minutes), run one tick
by hand and read the counts:

```bash
aws lambda invoke \
  --function-name penguin-health-fhir-eligibility-poller \
  --cli-binary-format raw-in-base64-out \
  --payload '{"organization_id":"<new-org-id>"}' \
  /tmp/out.json && cat /tmp/out.json
```

Expect `processed` and `verified` to be non-zero and roughly match your
step-4 estimate scaled to a 15-minute window. If `processed` is 0, the
filter is too narrow (or the cursor is in the future — check the
`FHIR_POLL_CURSOR` row). If it's much larger than expected, the filter
is too broad — narrow it before the next tick fires.

### 7. Watch the first day

- Check `USAGE#{today}` on `penguin-health-stedi` mid-afternoon. If
  `count` is already ≥80% of `daily_cap`, narrow the filter or raise
  the cap before tomorrow.
- Check the worklist (`/organizations/<new-org-id>/eligibility/worklist`).
  A reasonable distribution is mostly `verified` with a tail of
  `discrepancy` / `review_needed`. A flood of `no_coverage` usually
  means demographics from the EMR are too sparse — the FHIR Patient
  resource needs more than just name+DOB for discovery to land HIGH hits.

## Common patterns by EMR

These are starting points — still run the sampling step; vendors
customize per deployment.

| EMR | Typical class | Typical status | Notes |
|---|---|---|---|
| Credible Behavioral Health | `IMP` | `planned,in-progress` | Reliable `class`; creates Encounter at scheduling. |
| Epic | `IMP` (sometimes blank) | `arrived,in-progress` | If `class` blank, filter on `type` with Epic's local codes. |
| Cerner / Oracle Health | `IMP`, `EMER` | `planned,in-progress` | Often routes admits through `EMER` briefly — include both. |
| athenaOne | varies | varies | `class` frequently empty; expect to use `type_codes`. |

## Troubleshooting

**`processed: 0` after a manual invoke.** Either (a) the cursor is past
the most recent encounter — delete the `FHIR_POLL_CURSOR` row to reset,
or (b) the filter excludes everything — re-sample and check the
histograms. Don't widen the filter blindly; you'll blow the cap.

**Encounters match but no `ENCOUNTER_ITEM#` rows appear.** Check
CloudWatch for `StediDailyCapExceeded` — the poller halts on cap exceed
and leaves the cursor pinned so the next tick retries. Reset with:

```bash
aws dynamodb delete-item --table-name penguin-health-stedi \
  --key '{"pk":{"S":"ORG#<new-org-id>"},"sk":{"S":"USAGE#'"$(date +%Y-%m-%d)"'"}}'
```

**Worklist shows `error` rows with `missing_subject_reference`.** The
FHIR server is returning Encounters without `subject.reference`. Not a
filter problem — flag it to the EMR integration; the poller is doing
the right thing by recording the error and skipping verification.
