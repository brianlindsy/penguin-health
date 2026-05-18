"""
Tool implementations for the NL Explorer agent loop.

Each tool is a pair:
  - SCHEMA: a Bedrock tool-use schema (name, description, input_schema)
  - a callable invoked by nl_agent.run_agent_loop with the tool's input dict

The tools share the same org_id + a NarrativeExtractor callable so the
agent worker can wire concurrency / retries / Bedrock auth at construction
time and the tools themselves stay pure.

The S3-spill helper is used by `run_sql` and `extract_from_rows` to keep
large row payloads out of the DynamoDB job item (400KB cap). Trace entries
carry an `output_s3_key` pointer when the payload spilled.
"""

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

import analytics_helpers


# Bytes threshold above which we spill a tool output to S3 instead of
# echoing it through DynamoDB. Tuned so a 100-row, 5-col result stays
# inline but a 200-row scope with narratives spills.
SPILL_THRESHOLD_BYTES = 10_000

# Max rows the agent may pass to extract_from_rows in a single call. Above
# this, the extraction tool refuses and the agent must narrow its scope.
# Matches MAX_DEEP_SCOPE_LIMIT so the surface stays consistent with the
# legacy deep mode.
MAX_EXTRACT_ROWS = analytics_helpers.MAX_DEEP_SCOPE_LIMIT


# ---- Bedrock tool schemas ---------------------------------------------

INSPECT_SCHEMA_TOOL = {
    "name": "inspect_schema",
    "description": (
        "Return the chart + validation table schemas for this org, "
        "including per-column notes about value formats and quirks. "
        "Call this once at the start if you need to remember exact "
        "column names or date formats."
    ),
    "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
}

RUN_SQL_TOOL = {
    "name": "run_sql",
    "description": (
        "Execute an Athena SELECT against the org's analytics tables. "
        "SQL is validated (SELECT-only, table allowlist) and a LIMIT is "
        "injected if absent. Returns {columns, rows, row_count}. Use this "
        "for structured queries, scoping for narrative extraction, and "
        "any aggregation that can be expressed as SQL."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "Athena/Presto SELECT statement."},
        },
        "required": ["sql"],
        "additionalProperties": False,
    },
}

EXTRACT_FROM_ROWS_TOOL = {
    "name": "extract_from_rows",
    "description": (
        "For each row, call Claude to extract a short string answer to the "
        "given question from the row's narrative fields. Returns a list of "
        "{row_index, extracted_value} where extracted_value is a short "
        "string or 'unknown'. Use this when an answer is buried in prose "
        "and cannot be filtered with SQL LIKE. Cap: 200 rows per call."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "rows": {
                "type": "array",
                "description": (
                    "Row objects as {column_name: value}. Include the "
                    "narrative columns you want the extractor to read."
                ),
                "items": {"type": "object"},
            },
            "question": {
                "type": "string",
                "description": "What to extract from each row's narrative.",
            },
        },
        "required": ["rows", "question"],
        "additionalProperties": False,
    },
}

AGGREGATE_TOOL = {
    "name": "aggregate",
    "description": (
        "Group rows by one or more keys and count or sum. Use this after "
        "extract_from_rows to turn per-row extracted values into a "
        "{value, count} table. Operates in-memory on the rows you pass; "
        "do NOT re-query Athena for this."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "rows": {
                "type": "array",
                "items": {"type": "object"},
                "description": "Row objects to aggregate.",
            },
            "group_by": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Field names to group by.",
            },
            "agg": {
                "type": "string",
                "enum": ["count", "sum"],
                "description": (
                    "Aggregation to apply. 'count' counts rows per group; "
                    "'sum' requires sum_field."
                ),
            },
            "sum_field": {
                "type": "string",
                "description": "Field to sum when agg='sum'.",
            },
            "order_by": {
                "type": "string",
                "enum": ["count_desc", "count_asc", "key_asc"],
                "description": "How to sort the output rows.",
            },
        },
        "required": ["rows", "group_by", "agg"],
        "additionalProperties": False,
    },
}

FINALIZE_TOOL = {
    "name": "finalize",
    "description": (
        "Emit the final answer to the user. Pass the columns and rows you "
        "want rendered. The loop ends after this call. Choose viz_type "
        "based on the shape: 'bar' for categorical counts, 'line' for "
        "time series, 'pie' for parts-of-whole with <=8 slices, 'table' "
        "otherwise."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "type": {"type": "string"},
                    },
                    "required": ["name"],
                },
            },
            "rows": {
                "type": "array",
                "items": {"type": "array"},
                "description": "Rows as arrays of values, parallel to columns.",
            },
            "viz_type": {
                "type": "string",
                "enum": ["bar", "line", "pie", "table"],
            },
            "explanation": {
                "type": "string",
                "description": "One-sentence summary of how the answer was derived.",
            },
        },
        "required": ["columns", "rows"],
        "additionalProperties": False,
    },
}


ALL_TOOL_SCHEMAS = [
    INSPECT_SCHEMA_TOOL,
    RUN_SQL_TOOL,
    EXTRACT_FROM_ROWS_TOOL,
    AGGREGATE_TOOL,
    FINALIZE_TOOL,
]


# ---- Tool implementations ---------------------------------------------

def _serialize_columns(schema_cols: list) -> list:
    """Strip notes that are too long for the wire — Claude only needs
    name/type/narrative + the note text, and we already truncate notes
    sensibly in the source registry."""
    out = []
    for c in schema_cols:
        entry = {"name": c["name"], "type": c["type"]}
        if c.get("narrative"):
            entry["narrative"] = True
        if c.get("notes"):
            entry["notes"] = c["notes"]
        out.append(entry)
    return out


def make_inspect_schema_handler(org_id: str) -> Callable:
    def handler(_input: dict) -> dict:
        schema = analytics_helpers.ORG_SCHEMAS[org_id]
        suffix = analytics_helpers.table_suffix(org_id)
        return {
            "tables": {
                f"charts_{suffix}": {
                    "columns": _serialize_columns(schema["chart_columns"]),
                    "partition_key": schema.get("chart_partition_key"),
                    "partition_notes": schema.get("chart_partition_notes"),
                },
                f"validation_results_{suffix}": {
                    "columns": _serialize_columns(schema["validation_columns"]),
                    "partition_key": schema.get("validation_partition_key"),
                    "partition_notes": schema.get("validation_partition_notes"),
                },
            },
        }
    return handler


def make_run_sql_handler(
    org_id: str,
    spill: Optional[Callable[[bytes, str], str]] = None,
) -> Callable:
    """Build the run_sql tool handler for an org.

    spill: optional callable(bytes, suffix_hint) -> s3_key. When the result
    exceeds SPILL_THRESHOLD_BYTES we still send a truncated preview back to
    Claude (so it can keep planning) but stash the full payload via spill.
    The returned dict carries an `s3_key` pointer the worker can persist.
    """
    def handler(tu_input: dict) -> dict:
        sql_raw = tu_input.get("sql") or ""
        try:
            sql = analytics_helpers.validate_athena_sql(sql_raw, org_id)
        except analytics_helpers.SqlValidationError as e:
            return {
                "error": e.message,
                "code": e.code,
                "sql": sql_raw,
            }
        try:
            result = analytics_helpers.run_athena_query(sql, org_id)
        except analytics_helpers.AthenaQueryError as e:
            return {
                "error": e.message,
                "code": "ATHENA_ERROR",
                "sql": sql,
            }

        payload = {
            "sql": sql,
            "columns": result["columns"],
            "rows": result["rows"],
            "row_count": result["row_count"],
        }
        size = _approx_json_size(payload)
        if size > SPILL_THRESHOLD_BYTES and spill is not None:
            s3_key = spill(json.dumps(payload, default=str).encode("utf-8"), "run_sql.json")
            preview = {
                **payload,
                "rows": payload["rows"][:20],
                "truncated": True,
                "full_row_count": payload["row_count"],
                "s3_key": s3_key,
                "note": (
                    f"Preview only (first 20 of {payload['row_count']} rows). "
                    f"Full result spilled to S3."
                ),
            }
            return preview
        return payload
    return handler


def make_extract_from_rows_handler(
    extractor: Callable[[str, dict], str],
    max_workers: int = 10,
    spill: Optional[Callable[[bytes, str], str]] = None,
) -> Callable:
    """Build the extract_from_rows tool handler.

    extractor(question, row_payload) -> extracted_value str. Defaults to
    admin_api._deep_extract_for_row when wired by the worker. Wrapped here
    in a thread pool so a 100-row call doesn't serialize Bedrock latency.
    """
    def handler(tu_input: dict) -> dict:
        rows = tu_input.get("rows") or []
        question = tu_input.get("question") or ""
        if not isinstance(rows, list):
            return {"error": "rows must be a list", "code": "BAD_INPUT"}
        if len(rows) > MAX_EXTRACT_ROWS:
            return {
                "error": (
                    f"extract_from_rows received {len(rows)} rows; cap is "
                    f"{MAX_EXTRACT_ROWS}. Narrow the scope and call again."
                ),
                "code": "TOO_MANY_ROWS",
            }
        if not isinstance(question, str) or not question.strip():
            return {"error": "question is required", "code": "BAD_INPUT"}

        extracted: list = [None] * len(rows)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(extractor, question, row if isinstance(row, dict) else {}): i
                for i, row in enumerate(rows)
            }
            for fut in futures:
                idx = futures[fut]
                try:
                    extracted[idx] = fut.result()
                except Exception as e:
                    extracted[idx] = f"error: {type(e).__name__}"

        result_rows = [
            {"row_index": i, "extracted_value": v}
            for i, v in enumerate(extracted)
        ]
        payload = {"results": result_rows, "row_count": len(result_rows)}

        size = _approx_json_size(payload)
        if size > SPILL_THRESHOLD_BYTES and spill is not None:
            s3_key = spill(json.dumps(payload, default=str).encode("utf-8"), "extract.json")
            preview = {
                "results": result_rows[:20],
                "row_count": len(result_rows),
                "truncated": True,
                "s3_key": s3_key,
                "note": (
                    f"Preview only (first 20 of {len(result_rows)} results). "
                    f"Full result spilled to S3."
                ),
            }
            return preview
        return payload
    return handler


def aggregate_handler(tu_input: dict) -> dict:
    """In-memory group_by + count/sum. Pure-Python, no AWS deps."""
    rows = tu_input.get("rows") or []
    group_by = tu_input.get("group_by") or []
    agg = tu_input.get("agg") or "count"
    sum_field = tu_input.get("sum_field")
    order_by = tu_input.get("order_by") or "count_desc"

    if not isinstance(rows, list):
        return {"error": "rows must be a list", "code": "BAD_INPUT"}
    if not isinstance(group_by, list) or not group_by:
        return {"error": "group_by must be a non-empty list", "code": "BAD_INPUT"}
    if agg == "sum" and not sum_field:
        return {"error": "agg='sum' requires sum_field", "code": "BAD_INPUT"}

    buckets: dict = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = tuple(str(row.get(k, "")) for k in group_by)
        bucket = buckets.setdefault(key, {"count": 0, "sum": 0.0})
        bucket["count"] += 1
        if agg == "sum":
            try:
                bucket["sum"] += float(row.get(sum_field, 0) or 0)
            except (TypeError, ValueError):
                pass

    value_col = "count" if agg == "count" else f"sum_{sum_field}"
    columns = [{"name": k, "type": "string"} for k in group_by] + [
        {"name": value_col, "type": "number"}
    ]
    out_rows = []
    for key, bucket in buckets.items():
        value = bucket["count"] if agg == "count" else bucket["sum"]
        out_rows.append(list(key) + [value])

    if order_by == "count_desc":
        out_rows.sort(key=lambda r: r[-1], reverse=True)
    elif order_by == "count_asc":
        out_rows.sort(key=lambda r: r[-1])
    elif order_by == "key_asc":
        out_rows.sort(key=lambda r: r[: len(group_by)])

    return {"columns": columns, "rows": out_rows, "row_count": len(out_rows)}


# ---- S3 spill helper --------------------------------------------------

def org_data_bucket(org_id: str) -> str:
    """Bucket name for an org's data + analytics results.

    Mirrors infra/components/analytics.py:`f"{PROJECT_NAME}-{org_id}"`.
    Agent intermediate payloads include scoped chart rows (derivative PHI),
    so they must stay inside the same compliance boundary as Athena's
    `athena-results/` output — i.e. the org's own bucket, never a shared
    one.
    """
    return f"penguin-health-{org_id}"


def make_s3_spill(s3_client, bucket: str, job_id: str) -> Callable[[bytes, str], str]:
    """Return a spill(bytes, suffix_hint) -> s3_key function for one job.

    Writes to `agent-io/{job_id}/{ts}-{rand}-{hint}` in the given bucket.
    The `agent-io/` prefix is parallel to `athena-results/`; operators
    should apply a lifecycle rule on `agent-io/` matching the DynamoDB
    job TTL (24h) so spilled payloads expire alongside the job item that
    references them.
    """
    def spill(data: bytes, suffix_hint: str) -> str:
        key = f"agent-io/{job_id}/{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}-{suffix_hint}"
        s3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
        return key
    return spill


# ---- Misc -------------------------------------------------------------

def _approx_json_size(obj) -> int:
    """Estimate JSON-encoded size in bytes without holding two full copies."""
    try:
        return len(json.dumps(obj, default=str).encode("utf-8"))
    except Exception:
        return 0


def make_tool_handlers(
    *,
    org_id: str,
    extractor: Callable[[str, dict], str],
    spill: Optional[Callable[[bytes, str], str]] = None,
) -> dict:
    """Convenience wiring used by the worker; tests can build their own."""
    return {
        "inspect_schema": make_inspect_schema_handler(org_id),
        "run_sql": make_run_sql_handler(org_id, spill=spill),
        "extract_from_rows": make_extract_from_rows_handler(extractor, spill=spill),
        "aggregate": aggregate_handler,
    }


