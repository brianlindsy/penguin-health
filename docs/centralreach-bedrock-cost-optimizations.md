# CentralReach Bedrock cost optimizations — options analysis

Status: reference. Not a commitment to build any of these.

This document inventories ways to reduce Bedrock spend on the
centralreach PDF rule-evaluation path described in
`docs/centralreach-api-integration.md`. The main design ships
unoptimized — every PDF is sent to Bedrock at real-time pricing on
the existing Sonnet 4.5 model, three calls per record (one per
LLM-evaluated rule). The expected steady-state is ~$30-50/day/org at
350 entries/day.

These options exist for triage after real-world cost data lands. Each
section states the impact, the complexity, and the risk. Decide based
on actual spend, not predicted spend.

## How to read this

For each option:

- **Impact**: rough percentage of cost reduction
- **Complexity**: low, medium, high — relative to the baseline
  implementation
- **Risk**: what could degrade if we do this
- **Stackability**: which other options it composes with

Options are not ordered by priority — they're independent choices.
Several can be combined.

## Option 1 — Compress the PDF before sending

CR's PDFs are typically rendered at print resolution with embedded
fonts. A pre-Bedrock pass through `pikepdf` or
`ghostscript -dCompatibilityLevel=1.4 -dPDFSETTINGS=/ebook` shrinks
them 3-10x with no quality loss for text-based content. We send
fewer input tokens to Bedrock.

- **Impact**: 30-60% input token reduction per call
- **Complexity**: low. One library, one CPU pass per PDF before each
  Bedrock call.
- **Risk**: compression artifacts in scanned/image content could
  degrade judgment quality. CR's notes are text + tables, not scanned
  images, so likely safe — but worth a quality regression check
  against a held-out PDF set before enabling.
- **Stackability**: composes with everything below.

## Option 2 — Bedrock prompt caching

Anthropic's prompt cache holds repeated prefix content for 5 minutes.
The repeated content in our calls is:

- The system prompt ("You are a Healthcare Compliance Auditor...")
- The JSON schema
- The `rule_text` + `notes` (same per rule across all PDFs)

What changes per call: the PDF itself.

If we structure the message so the static parts come first and tag
them `cache_control: ephemeral`, the cache hit covers ~80% of the
non-PDF input tokens after the first call. Per-call non-PDF input
drops from ~500 tokens to ~50.

- **Impact**: ~5-10% total cost reduction. PDF dominates token count,
  so absolute savings are small.
- **Complexity**: low. One `cache_control` flag per content block in
  the Bedrock message body.
- **Risk**: none. Cache miss falls back to normal pricing.
- **Stackability**: composes with everything. Pure win if we wanted it
  but the absolute dollar impact is small.

## Option 3 — Batch all three rules into one Bedrock call per PDF

Today's plan: 3 Bedrock calls per PDF (one per LLM-evaluated rule).
Alternative: one PDF, one prompt that asks all three rule questions
at once, structured output returns three judgments. Input cost drops
3x because the PDF is sent once not three times.

- **Impact**: ~60% cost reduction. The biggest single lever.
- **Complexity**: medium. The prompt aggregates three rule questions,
  the output schema becomes three-field, the rules engine has to
  deconstruct the batched response and attribute pass/fail per rule.
  Audit emission changes: either one event listing all three
  rule_ids, or three events for one Bedrock call (the first means
  audit consumers must read multiple rule judgments per event;
  the second decouples the audit event count from the Bedrock call
  count).
- **Risk**: judgment quality may drift if asking three questions at
  once causes the model to half-answer each. Mitigated by a held-out
  regression check comparing batched vs. one-at-a-time judgments
  before enabling.
- **Stackability**: composes with 1, 2, 4, 5, 8. Doesn't compose with
  7 (judgment cache) cleanly — the cache key would change from
  per-rule to per-rule-set.

## Option 4 — AWS Bedrock Batch Inference for the daily run

Bedrock batch inference is 50% off real-time pricing in exchange for
asynchronous processing (results within 24 hours instead of seconds).
The daily ingest doesn't need real-time judgments — records get
reviewed by ops on a 24-hour cycle anyway.

- **Impact**: ~50% cost reduction on whatever portion of calls we
  route through batch.
- **Complexity**: medium. Batch jobs are submitted as JSONL files to
  S3; results come back as JSONL. Rules engine has to handle the
  async pickup (separate Lambda triggered by batch-complete event).
  Doesn't fit the existing synchronous `invoke_claude_model` shape —
  we'd add a sibling.
- **Risk**: latency. Manual operator-triggered runs (rule_text
  iteration, ad-hoc audits) would still go through synchronous
  Bedrock. Two paths to maintain.
- **Stackability**: composes with 1, 2, 3, 5, 8. Doesn't compose with
  7 (cache mostly redundant at batch pricing).

Stacked with Option 3: batched + multi-rule = ~75% off real-time
single-call pricing.

## Option 5 — Drop to Haiku for cheaper rules

Haiku 4.5 is ~$1/M input tokens vs Sonnet 4.5's $3/M — 3x cheaper.
Whether Haiku judgment quality is acceptable depends on each rule.

- **Rule 2 (≥2 sentences per hour)**: counting and arithmetic.
  Probably Haiku-quality work.
- **Rule 3 (third person)**: pronoun and grammar inspection.
  Probably Haiku-quality work.
- **Rule 11 (data present in note)**: judgment-heavy. Probably
  needs Sonnet.

- **Impact**: ~30-50% cost reduction if two of three rules move to
  Haiku.
- **Complexity**: low at the Bedrock call site (different model id),
  medium at the regression test level — now we need per-rule
  per-model quality baselines.
- **Risk**: judgment quality regression. Per-rule held-out test set
  required before enabling.
- **Stackability**: doesn't compose with Option 3 (batched into one
  call → can only use one model at a time). Composes with 1, 2, 4, 8.

If we go with Option 3, this becomes irrelevant — the single batched
call uses one model.

## Option 6 — PDF page truncation

CR's PDFs have headers, footers, signature blocks. If only the
narrative pages are rule-relevant, we send only those. `pypdf` or
`pikepdf` can extract specific pages.

- **Impact**: variable. CR's 2-5 page PDFs probably don't have much
  skippable content. Diminishing returns for the complexity.
- **Complexity**: medium. Per-template page-range logic — which is
  the template registry we deliberately excluded from the main
  design.
- **Risk**: skipping the wrong page silently drops information. Hard
  to detect in production.
- **Stackability**: composes with everything, but reintroduces the
  per-template registry concern the main design deleted.

Likely not worth pursuing — the cost gain is small and the
complexity reintroduces structural decisions we already made.

## Option 7 — Judgment cache for backfills and rule iteration

The main design deliberately omits a judgment cache because the
daily run hits near-zero cache rate. But backfill runs (operator
re-processes a date range) and rule_text iteration runs (operator
edits a rule, re-runs to see new judgments) DO benefit from caching.

A targeted cache keyed by `(pdf_etag, rule_id, rule_text_hash,
model_id)` would catch these high-cost scenarios without affecting
daily steady-state.

- **Impact**: zero on daily steady-state. Material on backfill and
  iteration runs (the operations that scale cost worst).
- **Complexity**: medium. DynamoDB cache table, ETag lookups,
  rule_text_hash computation, audit semantics around hit/miss.
- **Risk**: stale judgments if invalidation logic is wrong. The
  `rule_text_hash` covers operator edits; `pdf_etag` covers CR
  re-uploads; `model_id` covers Bedrock upgrades. The remaining gap
  is system-prompt changes — currently hardcoded so changes would
  require manual invalidation.
- **Stackability**: composes with 1, 2, 5, 8. Doesn't compose cleanly
  with 3 (cache key would be per-rule-set, not per-rule) or 4 (batch
  inference is async, cache lookup happens before submission anyway).

This is the option you explicitly rejected for v1. Listed here for
the case where backfill/iteration costs become the dominant problem.

## Option 8 — Cap output tokens

Existing `_validate_rule` uses `max_tokens: 1024`. Real judgments
need ~50 output tokens (judgment + short rationale). Capping at 256
saves ~4x on output tokens. Output is ~$15/M vs input's $3/M, so
worth caring about per token.

- **Impact**: ~5% total cost reduction. Output is a minority of
  total cost but a higher rate.
- **Complexity**: tiny. One number change in the body builder.
- **Risk**: the rationale gets clipped on long responses. Audit
  traceability degrades. The rule_id + pass/fail status is the
  load-bearing part; the rationale is forensic context.
- **Stackability**: composes with everything.

## Stacked impact estimates

If the v1 baseline is ~$30-50/day/org, here's what stacks look like:

| Stack | Estimated cost | Reduction |
|---|---|---|
| Baseline (v1) | $30-50/day/org | 0% |
| + Option 2 (cache) + Option 8 (max_tokens) | $25-43/day/org | ~13% |
| + Option 1 (compress) | $15-30/day/org | ~40% |
| + Option 3 (batch rules) | $6-12/day/org | ~75% |
| + Option 4 (batch inference) | $3-6/day/org | ~85% |

The estimates are rough; real measurements supersede.

## Decision triggers

Use these as rough thresholds for when to revisit:

- **Daily steady-state > $50/org**: triage starts. Cheapest moves
  first (Options 1, 2, 8). They're nearly free.
- **Daily steady-state > $100/org**: Option 3 (batch rules) worth
  the prompt complexity.
- **Backfill or rule iteration runs > $200/run**: Option 7 (cache)
  pays off.
- **All-org monthly Bedrock spend > $10k**: Option 4 (batch
  inference) worth the async architecture.

Numbers are not calibrated — adjust against real spend.

## What's not in this list

Options that affect ingest cost rather than rule-eval cost (Fargate
sizing, S3 storage class for PDFs, etc.) are out of scope here. The
Bedrock spend dominates by orders of magnitude.

Options that change which rules are LLM-evaluated (e.g., "rewrite
rule 3 to be deterministic on extracted fields") are a rules-engine
decision, not a Bedrock optimization. Out of scope for this doc.
