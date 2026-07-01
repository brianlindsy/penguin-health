# Project instructions for Claude Code

These rules apply to all work in this repository. Follow them by default; deviate only when the user explicitly asks.

## Tests and documentation

- Every major feature ships with tests and updated docs in the same change. "Major" = new endpoint, new module, new infra resource, new user-visible behavior, or non-trivial logic.
- Bug fixes get a regression test that fails before the fix and passes after.
- Update relevant docs (READMEs, `docs/`, inline module docstrings) when behavior, interfaces, or operational steps change. Do not leave docs describing the old behavior.
- Trivial changes (typos, formatting, comment edits) do not require tests.
- **Logic touching secrets or PHI/PII gets extra tests and extra scrutiny.** Cover the unhappy paths explicitly: missing/invalid auth, redaction, audit-log emission, encryption boundaries, and accidental leakage into logs or error messages. Call out the sensitive path in the PR description so a reviewer knows where to look.
- **Design and reference docs describe only what's in scope.** Don't include rejected alternatives, deferred work, "v2 ideas," "future options," "earlier drafts said," or defensive statements about things we are explicitly not doing ("we do NOT add X"). If a decision was made to drop something, the doc reflects the decision — it does not document the rejected option to refute it. Same rule for inline code comments. Pollution from out-of-scope content hides the real decisions a reader needs.

## Infrastructure

- New infrastructure must be secure, readable, and maintainable.
- Prefer managed/standard primitives over bespoke ones. Reuse existing modules in `infra/` before adding new ones.
- Least-privilege IAM by default — no wildcard `*` actions or resources unless justified in a comment.
- No secrets in code or committed config. Use the project's existing secrets mechanism (SSM/Secrets Manager/env at deploy time).
- Encryption at rest and in transit is required for any store or transport that could touch PHI/PII (see below).
- Name and structure resources so a new engineer can map code → infra without tribal knowledge.

## Code style

- Write the shortest code that stays clear and maintainable. Brevity is a tiebreaker, not the goal — never sacrifice readability for line count.
- No premature abstraction. Three similar lines beats a helper used once.
- No defensive error handling for cases that cannot happen. Validate only at system boundaries (user input, external APIs).
- No feature flags, backwards-compat shims, or dead code paths unless there is a concrete reason.
- Prefer editing existing files over creating new ones.

## Unused and dead code

- Delete clearly-unused code on sight: unreachable branches, unused imports, unused locals, commented-out blocks, orphaned files, and helpers with no remaining callers.
- Flag ambiguous cases before removing — exported APIs, public module entry points, anything that might be called dynamically (reflection, string-based dispatch, dynamic imports, IaC references, cron/lambda handlers wired up out-of-band). When unsure, ask rather than delete.
- Do not leave tombstones — no `// removed` comments, no renamed-to-`_unused` variables, no kept-but-unexported "just in case" code. Git history is the backup.
- **Always rerun the test suite after any deletion**, even when the removal looks obviously safe. Dynamic references and integration paths often aren't caught by static analysis.

## PHI and PII

This project handles healthcare data. Treat **any field that could contain PHI or PII as sensitive** — names, DOB, addresses, phone, email, MRN, member ID, claims, diagnoses, medications, eligibility, demographics, free-text notes, and identifiers derived from them.

Required for all code paths touching this data:

- **Encrypted at rest and in transit.** No plaintext in S3, logs, queues, caches, or local files.
- **Audit trail.** Access to PHI/PII must be logged (who, what, when, why) via the project's existing audit mechanism. If no audit hook exists on a new path, add one or flag it.
- **Never log raw values.** Log identifiers (record id, hash) or redacted forms — never the underlying PHI/PII. This includes error messages, debug output, and exception traces.
- **Least-privilege access.** Restrict to the specific roles/services that need it; do not broaden an existing role to fit new code.
- **Call it out in review.** When a change introduces or modifies a PHI/PII path, surface it explicitly in the PR description and in any summary you produce — do not let it pass silently.

When in doubt about whether a field is PHI/PII, treat it as if it is and ask.
