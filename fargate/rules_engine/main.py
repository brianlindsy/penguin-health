"""Fargate task entry point for one rules-engine validation run.

Reads the following env vars (set by the Step Functions state machine
that invokes this task):

  ORG_ID          required. The organization to validate.
  RUN_ID          optional. The validation_run_id to write under. Absent
                  means the runner mints one at start.
  MODE            optional. "scheduled" | "manual" — for audit metadata.
  CATEGORIES      optional JSON-encoded list of rule categories to filter to.
  DATES           optional JSON-encoded list of "YYYY-MM-DD" strings.
  DATE_WINDOW     optional JSON-encoded relative-date instruction,
                  e.g. {"days_back_from_today": [2, 1, 0]}.

Delegates to `rules_engine_rag.run_validation`, which owns the actual
per-file validation, DDB/S3/SES output, and audit emission. The wrapper
here exists to:

  * marshal env vars into the event dict the core function expects
  * turn exceptions into a non-zero exit code so the Step Functions
    execution surfaces the failure
  * echo a run summary to stderr so the CloudWatch log is scannable

PHI handling: no PHI touches this file. `run_validation` handles all
document IO through the existing S3/DDB code paths, which are already
KMS-encrypted and audit-emitted.
"""

from __future__ import annotations

import json
import os
import sys
import traceback

from rules_engine_rag import run_validation


_ABSENT_MARKERS = {"", "null", "__NULL__", '"__NULL__"'}


def _plain_env(name: str) -> str | None:
    """Return the env var, treating absent/null/placeholder as absent.

    Two paths feed these env vars:
      * `States.JsonToString(null)` on a JSON-null input yields "null".
      * EventBridge target inputs strip literal `null` values, so we
        send the placeholder "__NULL__" — after `States.JsonToString`
        that becomes '"__NULL__"' for keys we JSON-encode.
    Both round-trip forms + "" collapse to `None` here.
    """
    raw = os.environ.get(name)
    if raw is None or raw in _ABSENT_MARKERS:
        return None
    return raw


def _parse_json_env(name: str):
    raw = _plain_env(name)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{name} is not valid JSON: {e}") from e
    # The parsed value can still be the placeholder string when the
    # env var was double-quoted "\"__NULL__\"" — json.loads turns it
    # back into "__NULL__". Treat as absent.
    if parsed == "__NULL__":
        return None
    return parsed if parsed else None


def _build_event() -> dict:
    org_id = os.environ.get("ORG_ID")
    if not org_id:
        raise ValueError("ORG_ID env var is required")

    event: dict = {"organization_id": org_id}

    # RUN_ID arrives JSON-encoded from the state machine: `"abc"` when
    # set, `"null"` when unset. json.loads normalises both.
    run_id = _parse_json_env("RUN_ID")
    if run_id:
        event["validation_run_id"] = run_id

    categories = _parse_json_env("CATEGORIES")
    if categories:
        event["categories"] = categories

    dates = _parse_json_env("DATES")
    if dates:
        event["dates"] = dates

    date_window = _parse_json_env("DATE_WINDOW")
    if date_window:
        event["date_window"] = date_window

    return event


def main() -> int:
    try:
        event = _build_event()
    except ValueError as e:
        print(f"rules-engine: bad input: {e}", file=sys.stderr)
        return 2

    mode = _plain_env("MODE") or "scheduled"
    print(f"rules-engine: mode={mode} event={event}", file=sys.stderr)

    try:
        result = run_validation(event)
    except Exception as e:  # noqa: BLE001 — surface any failure to SFN
        print(f"rules-engine: run failed: {type(e).__name__}: {e}",
              file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 1

    print(f"rules-engine: result={json.dumps(result, default=str)}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
