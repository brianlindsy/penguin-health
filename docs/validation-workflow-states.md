# Validation workflow states

Reference for the rule- and document-level states surfaced in the admin UI's
Validation Run Detail page and the DDB fields that back them.

## Rule states

Each rule row on a document has a `status` field with one of four values,
plus optional reviewer-applied flags layered on top.

### Evaluation status (set by the rules engine)

| status  | Meaning                                                                 | Set by                          |
| ------- | ----------------------------------------------------------------------- | ------------------------------- |
| `PASS`  | Rule ran, all conditions met.                                           | rules engine                    |
| `FAIL`  | Rule ran, one or more conditions not met.                               | rules engine                    |
| `SKIP`  | Rule could not evaluate (missing required field, unparseable input).    | rules engine                    |
| `ERROR` | Unexpected failure during evaluation (LLM error, JSON parse, uncaught). | rules engine                    |

### Reviewer flags (set via admin API)

| Flag                | Endpoint                            | Sets                                            | Preconditions                                     |
| ------------------- | ----------------------------------- | ----------------------------------------------- | ------------------------------------------------- |
| `finding_confirmed` | `PUT .../confirm-finding`           | `finding_confirmed{,_at,_by}`                   | `status='FAIL'`; document not confirmed           |
| `fixed`             | `PUT .../mark-resolved`             | `fixed{,_at,_by}`, clears `finding_confirmed_*` | `status='FAIL'`; document not confirmed           |
| `feedback_given`    | `PUT .../mark-incorrect`            | `feedback_given{,_at,_by}`, overrides `status`  | `status='FAIL'` or `'SKIP'`; document not confirmed |

`mark-incorrect` accepts an `outcome` body field (`'PASS'` or `'FAIL'`,
default `'PASS'`):
- **`outcome='PASS'`** — used for a FAIL that turns out to be a false positive,
  or a SKIP that should have passed. Rule leaves the FAIL rollup entirely.
- **`outcome='FAIL'`** — used when a SKIP should have flagged an issue. Rule
  enters the normal FAIL workflow (needs Confirm Finding → Mark Resolved).

### Rule state diagram

```
                     ┌──────────────────────────────────┐
                     │ Rules-engine evaluation output   │
                     └──────────────────────────────────┘
                                     │
             ┌───────────┬───────────┼─────────────────────┐
             ▼           ▼           ▼                     ▼
          PASS        SKIP        FAIL                   ERROR
           │           │           │                       │
           │           │           │           (ops-side fix / rerun run)
           │           │           │
           │           │  ┌──────────────────┐
           │           │  │ finding_confirmed│──┐
           │           │  └──────────────────┘  │
           │           │           │            ▼
           │           │           │      ┌──────────┐
           │           │           │      │  fixed   │
           │           │           │      └──────────┘
           │           │           │
           │           │           │ mark-incorrect (outcome=PASS)
           │           │           │────────────► feedback_given, status=PASS
           │           │
           │           │ mark-incorrect (outcome=PASS)
           │           │────────────► feedback_given, status=PASS
           │           │
           │           │ mark-incorrect (outcome=FAIL)
           │           │────────────► feedback_given, status=FAIL
           │                          (re-enters the FAIL flow)
           │
           │  (no reviewer actions apply to a natively-PASS rule)
```

## Document states

The four summary cards on the Validation Run Detail page bucket documents
by a rollup over their rules plus one document-level flag.

### Document-level flag

| Field                 | Endpoint                    | Preconditions                                              |
| --------------------- | --------------------------- | ---------------------------------------------------------- |
| `document_confirmed`  | `PUT .../confirm-document`  | Every rule has `status` in `{PASS, SKIP}`; not yet confirmed |

`document_confirmed` is **terminal**. There is no un-confirm endpoint, and
while the flag is set the three rule-level mutation endpoints
(`confirm-finding`, `mark-resolved`, `mark-incorrect`) return `409` on this
document. The admin UI hides the corresponding buttons.

### Rollup buckets

Buckets are mutually exclusive. Given the rules on a document:

- Let `hasFail = any rule.status === 'FAIL'`
- Let `allFixed = hasFail && every FAIL rule has fixed=true`
- Let `allConfirmedOrFixed = hasFail && every FAIL rule has finding_confirmed=true OR fixed=true`
- Let `noOpenFailures = !hasFail || allFixed`

| Card             | Condition                                                       |
| ---------------- | --------------------------------------------------------------- |
| Needs Action     | `hasFail && !allConfirmedOrFixed && !allFixed`                  |
| Awaiting Staff   | `hasFail && allConfirmedOrFixed && !allFixed`                   |
| Passed           | `noOpenFailures && !document_confirmed`                         |
| Confirmed        | `noOpenFailures && document_confirmed`                          |

A fully-resolved document (had FAILs, all fixed) sits in **Passed** by
default; the reviewer still has to click *Confirm Document* to promote it
to Confirmed.

### Document state diagram

```
                    ┌─────────────────────────────┐
                    │ document has some rule FAIL │
                    └─────────────────────────────┘
                                 │
             ┌───────────────────┼────────────────────────┐
             │                   │                        │
             ▼                   ▼                        ▼
      Needs Action ──confirm──► Awaiting Staff ──resolve──► Passed
                                                            │
                                                            │ Confirm Document
                                                            ▼
                                                         Confirmed  (terminal)
                    ┌─────────────────────────────┐
                    │ document has no FAIL rules  │
                    │  (all PASS / SKIP from run) │
                    └─────────────────────────────┘
                                 │
                                 ▼
                              Passed ──Confirm Document──► Confirmed
```

A rule marked incorrect with `outcome='FAIL'` on a Passed document moves it
back to Needs Action.

## Storage

- Rule flags live inside the `rules` array on the DDB item at
  `pk=DOC#<doc_id>, sk=VALIDATION#<timestamp>` in
  `penguin-health-validation-results`.
- `document_confirmed{,_at,_by}` live at the top level of the same item.
- Mutable reviewer fields are intentionally excluded from the Parquet
  analytics snapshot (`lambda/multi-org/rules-engine/parquet_writer.py`) —
  DDB is the source of truth for current review state; Parquet captures
  the evaluation result at run time.

## Audit

Every reviewer action emits an audit event via the `@audited` decorator
in `lambda/api/admin_api.py`. Rule-level actions use resource type
`ValidationFinding`; document-level confirmation uses `ValidationDocument`.
The immutable history of who did what lives in the audit store — the DDB
row only carries the *current* summary.
