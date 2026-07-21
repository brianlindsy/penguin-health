# Rules Engine + LLM Cost Attribution

This package owns the per-rule LLM chart validation pipeline and the
**per-organization Claude cost attribution** that wraps every Bedrock
invocation in this repo.

The validation flow itself (field extraction → rule evaluation → results
storage) is in [rules_engine_rag.py](rules_engine_rag.py),
[document_validator.py](document_validator.py),
[field_extractor.py](field_extractor.py), and
[results_handler.py](results_handler.py). This README focuses on the
Claude/Bedrock layer that lives here and is shared with the admin API
and deep-analytics worker Lambdas.

## Module layout (Claude/Bedrock layer)

| File | What it does |
|---|---|
| [bedrock_client.py](bedrock_client.py) | Canonical `invoke_claude_model(...)` wrapper. Owns `_BEDROCK_BOTO_CONFIG` (300s read timeout for tool-use turns, retries disabled), rate limiting, JSON extraction (fenced + brace-matched), retry-on-no-JSON, and the per-call hook into `claude_cost.record_cost`. Imported by `document_validator.py` here and by [admin_api.py](../../api/admin_api.py) at the asset root. |
| [claude_cost.py](claude_cost.py) | CloudWatch metric emitter. One `PutMetricData` call per Bedrock invocation, namespace `PenguinHealth/LLMCost`, dimensioned by org. Fire-and-forget — failures never propagate. |
| [rate_limiter.py](rate_limiter.py) | Sliding-window RPM throttle used by `bedrock_client`. 10,000 RPM default. |
| [document_validator.py](document_validator.py) | Per-rule LLM evaluation. Threads `org_id` + `validation_run_id` into every `invoke_claude_model` call so per-org per-run spend is queryable. |

## LLM cost attribution

### What gets emitted

Every successful `bedrock-runtime:InvokeModel` call routed through
`invoke_claude_model` (with `org_id` set) emits one `PutMetricData` call
to **namespace `PenguinHealth/LLMCost`** with these metrics:

| Metric | Unit | Meaning |
|---|---|---|
| `CostMicros` | Count | USD × 1,000,000 (integer). Divide by 1e6 for USD. |
| `InputTokens` | Count | `response.usage.input_tokens` |
| `OutputTokens` | Count | `response.usage.output_tokens` |
| `CacheReadTokens` | Count | `response.usage.cache_read_input_tokens` |
| `CacheCreationTokens` | Count | `response.usage.cache_creation_input_tokens` |
| `CallCount` | Count | Always 1 — sum it for invocation counts |
| `DurationMs` | Milliseconds | Wall-clock around the Bedrock call (only when caller passes it) |

**Retries each emit their own metric.** Bedrock bills per `InvokeModel`
call regardless of whether downstream JSON parsing succeeds, so the wrapper
records on every successful HTTP response — including the retries it
performs after `extract_json_from_claude_response` returns None.

### Dimensions

| Dimension | Always present? | Source |
|---|---|---|
| `org_id` | yes | Caller passes (from JWT `custom:organization_id`, path param, or job item) |
| `model_id` | yes | Bedrock inference-profile ID, e.g. `global.anthropic.claude-sonnet-4-5-...` |
| `call_type` | yes | Caller-provided feature label (see below) |
| `parent_request_id` | only on a second emission | Set by agent-loop / rules-engine callers to roll up multi-call jobs |

When `parent_request_id` is provided, the cost recorder emits the metric
**twice** — once with base dims (the queryable per-feature breakdown) and
once with `parent_request_id` added (queryable per agent job or
validation run). Skipping the parent dim when unset keeps cardinality
manageable for non-agent calls.

### Pricing table

USD per 1M tokens, hard-coded in `claude_cost.MODEL_PRICING`. Current
values:

| Model | Input | Output | Cache read | Cache write |
|---|---|---|---|---|
| `global.anthropic.claude-sonnet-4-5-20250929-v1:0` | $3.00 | $15.00 | $0.30 | $3.75 |

Historical metric data is **not** retroactively repriced — datapoints
reflect the rate active at the time of the call. Document any rate
change in the commit message so finance can correlate trend shifts. An
unknown `model_id` logs a warning and emits `CostMicros=0` (token counts
still emit, so the gap is visible in CloudWatch).

### PHI/PII guarantees

`record_cost` stores **token counts and computed USD only** — no prompt
text, no response text, no patient identifiers. The PHI guard in
[test_claude_cost.py](../../tests/unit/rules_engine/test_claude_cost.py)
asserts that response prose with marker strings never appears anywhere
in the `PutMetricData` payload.

`call_type` is a fixed enum (see below) — never user-supplied. `org_id`
is a stable tenant slug, not a name. `user_email` is accepted by
`record_cost` but **not currently emitted as a dimension** to keep
cardinality bounded; it's available in the function signature for
future log-side correlation if needed.

## Call types

Every call site declares a feature label so spend rolls up by feature.
Adding a new label is described under "How to add a new `call_type`".

| `call_type` | Emitted by | What it covers |
|---|---|---|
| `rule_fields_enhance` | [admin_api.enhance_fields](../../api/admin_api.py) | Admin UI "Enhance Fields" button on a rule — Claude proposes field schemas to extract. |
| `rule_note_enhance` | [admin_api.enhance_note](../../api/admin_api.py) | Admin UI "Enhance Note" — converts UR feedback into a reusable clarification note. |
| `deep_extract_row` | [admin_api._deep_extract_for_row](../../api/admin_api.py) | One Claude call per row inside a Deep Analytics extraction. `parent_request_id = job_id`. |
| `nl_agent_step` | [admin_api._agent_worker_run](../../api/admin_api.py) | One step of an NL Explorer agent loop. `parent_request_id = job_id` so total agent spend is queryable. |
| `chart_field_extract` | [document_validator._extract_rule_fields](document_validator.py) | Step 1 of LLM rule evaluation — extract fields from chart text. `parent_request_id = validation_run_id`. |
| `chart_rule_validate` | [document_validator._validate_rule](document_validator.py) | Step 2 of LLM rule evaluation — pass/fail decision. `parent_request_id = validation_run_id`. |

## Query recipes (CloudWatch Metrics Insights)

All examples run in the CloudWatch console (Metrics → Query) or via
`aws cloudwatch get-metric-data` / boto3's `client('cloudwatch').get_metric_data`.

### Total spend for one org over a time range

```sql
SELECT SUM(CostMicros)
FROM "PenguinHealth/LLMCost"
WHERE org_id = 'catholic-charities-multi-org'
```

Set the time range in the console (e.g., last 30 days). Divide the
result by 1e6 → USD. This is the headline number for monthly invoicing.

### Spend per org, ranked

```sql
SELECT SUM(CostMicros)
FROM "PenguinHealth/LLMCost"
GROUP BY org_id
ORDER BY SUM() DESC
```

Top-N orgs by spend.

### Spend for one feature within one org

```sql
SELECT SUM(CostMicros)
FROM "PenguinHealth/LLMCost"
WHERE org_id = 'catholic-charities-multi-org'
  AND call_type = 'chart_rule_validate'
```

### Feature breakdown for one org

```sql
SELECT SUM(CostMicros)
FROM "PenguinHealth/LLMCost"
WHERE org_id = 'catholic-charities-multi-org'
GROUP BY call_type
ORDER BY SUM() DESC
```

Useful for product decisions ("is the NL agent eating their budget?").

### Cost of a single NL agent job

```sql
SELECT SUM(CostMicros)
FROM "PenguinHealth/LLMCost"
WHERE parent_request_id = '<job_id>'
```

Same trick works for `validation_run_id` — every per-rule LLM call in a
validation run shares that parent.

### Daily trend chart for one org

In the CloudWatch console:
1. Pick metric `CostMicros`, dimension filter `org_id = <org>`.
2. `Statistic = Sum`, `Period = 1 day`.
3. Apply math expression `m1/1000000` to display USD instead of micros.
4. Add a second metric with `call_type` grouping for a stacked-area
   per-feature view.

### Token volume (for capacity / TPM planning)

Replace `CostMicros` with `InputTokens` / `OutputTokens` /
`CacheReadTokens` / `CacheCreationTokens` in any query above. Forecasts
Bedrock TPM quota pressure per org.

### Reconciling against the AWS bill

CloudWatch metric sums are the *internal* attribution. The AWS Bedrock
invoice is the *external* truth. They should agree within rounding (we
sum integer micros; AWS bills full precision and may round differently).
If they diverge by more than ~1%, suspect:

1. **Calls escaping the wrapper.** Run `grep -rn 'bedrock-runtime' lambda/`
   and confirm every `invoke_model` call goes through
   `bedrock_client.invoke_claude_model`.
2. **Unknown `model_id`.** Check CloudWatch logs for
   `claude_cost: unknown model_id` warnings. Update `MODEL_PRICING` in
   [claude_cost.py](claude_cost.py).
3. **Stale `MODEL_PRICING`.** Cross-reference current Bedrock pricing.

## How to add a new `call_type`

`call_type` is a string label — there's no enum to update — but new
values should be added with care to avoid CloudWatch metric cardinality
sprawl.

1. **Pick a stable, snake_case name** that describes the *feature*, not
   the call site. Good: `chart_rule_validate`. Bad:
   `validate_at_line_228`.
2. **At the call site**, pass it to `invoke_claude_model`:
   ```python
   invoke_claude_model(
       inference_profile_id=MODEL_ID,
       body=body_payload,
       return_json_only=True,
       org_id=org_id,
       user_email=claims.get('email'),       # optional
       call_type='your_new_call_type',
       parent_request_id=job_id_or_run_id,   # optional, for rollups
   )
   ```
3. **Confirm `org_id` is in scope.** Sources, in order of preference:
   - Path parameter (`path_params.get('orgId')`) for admin API routes
   - JWT claim (`claims.get('organization_id')`) for authenticated calls
   - Job item field (`item.get('org_id')` or
     `event.get('org_id')`) for worker / EventBridge handlers
   - Function argument that's already threaded through the call chain
     (the pattern used in `document_validator.py`)
4. **Add the new label to the "Call types" table** in this README so
   future readers know what it covers.
5. **Update the test suite** if the new call site has its own handler
   tests; the cost emission itself is already covered by
   [test_claude_cost.py](../../tests/unit/rules_engine/test_claude_cost.py).

**Cardinality budget.** CloudWatch bills per unique dimension
combination. With dimensions `(org_id, model_id, call_type)` and ~10
orgs × 1 active model × N `call_type` values, each new `call_type`
costs ~10 datapoints/minute when active. Don't generate `call_type`
values dynamically (e.g., one per rule ID) — that explodes cardinality.
Keep `call_type` to one value per feature.

## Infrastructure

The IAM permission to write these metrics is namespace-scoped — each
role can only emit to `PenguinHealth/LLMCost`, not any other namespace:

- **Rules engine** — granted on the Fargate task role in
  [infra/components/rules_engine.py](../../../infra/components/rules_engine.py).
- **Admin API + deep worker** — granted in
  [infra/components/admin_ui.py](../../../infra/components/admin_ui.py)
  in the shared loop after the Bedrock grants.

Bundling: `bedrock_client.py`, `claude_cost.py`, and `rate_limiter.py`
are pulled into the admin-API and deep-worker Lambda assets via
`DirectoryBundler` (see the `shared_llm_modules` list in
[admin_ui.py](../../../infra/components/admin_ui.py)). The rules-engine
Fargate image copies the whole rules-engine directory into `/app/` via
[fargate/rules_engine/Dockerfile](../../../fargate/rules_engine/Dockerfile).
In both cases the modules land at the Python path root, so
`from bedrock_client import invoke_claude_model` and
`import claude_cost` work identically everywhere.

## Testing

```bash
# Cost-attribution unit tests
python3 -m pytest lambda/tests/unit/rules_engine/test_claude_cost.py -q

# Wrapper characterization tests (regression net for the consolidation)
python3 -m pytest lambda/tests/unit/rules_engine/test_bedrock_client.py -q

# Full rules-engine suite
python3 -m pytest lambda/tests/unit/rules_engine/ -q
```

Coverage:

| Test file | Covers |
|---|---|
| [test_claude_cost.py](../../tests/unit/rules_engine/test_claude_cost.py) | Price calculation across all four token types, unknown model fallback, missing-usage tolerance, PHI marker-string guard, CloudWatch failure swallowing, `parent_request_id` plumbing, end-to-end `invoke_claude_model → record_cost` integration |
| [test_bedrock_client.py](../../tests/unit/rules_engine/test_bedrock_client.py) | Body forwarded byte-for-byte to Bedrock, JSON extraction (fenced + raw), retry-on-no-JSON, raise/return-None modes, injected-client respected, rate limiter consulted exactly once per call |

## Operational runbook

### "Cost data isn't showing up in CloudWatch"

1. Confirm the call site passes `org_id`. Cost emission is skipped when
   `org_id is None` (back-compat — the wrapper accepts pre-migration
   callers without crashing). Grep for `invoke_claude_model(` and check
   every call.
2. Confirm the Lambda has the `cloudwatch:PutMetricData` grant for
   `PenguinHealth/LLMCost`. CDK should have wired this up; if a new
   Lambda was added, mirror the grant block in `audit_engine.py` /
   `admin_ui.py`.
3. CloudWatch ingest can lag ~1–2 minutes. Don't conclude "broken"
   without waiting.
4. Check the Lambda's CloudWatch logs for `claude_cost: record_cost failed`
   warnings — `record_cost` swallows exceptions but always logs them.

### "I see `CostMicros = 0` for real traffic"

Either:

- The model_id is missing from `MODEL_PRICING` (look for
  `claude_cost: unknown model_id` warnings in logs). Update the dict.
- The `usage` block was missing from the Bedrock response (look for
  `claude_cost: missing usage block` warnings). Bedrock should always
  return it for Anthropic models — if you see this, something upstream
  has changed.

### "I want to back out the cost layer entirely"

Cost emission is purely additive — no schema, no DB writes, no Bedrock
behavior change. Revert the commits that introduced
`claude_cost.py` and the call-site kwargs; CloudWatch datapoints stop
arriving. Already-emitted metrics live ~15 months in CloudWatch and are
harmless.
