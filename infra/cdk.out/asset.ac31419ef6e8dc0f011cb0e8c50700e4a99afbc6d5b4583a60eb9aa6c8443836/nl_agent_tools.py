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

# How many rows of a run_sql / extract result to include inline as a
# "preview" so the agent can sanity-check the shape without dumping the
# full payload into the prompt. Full rows live in the run cache, keyed
# by run_id, and downstream tools fetch them by id.
PREVIEW_ROWS = 5


class RunCache:
    """In-memory cache of intermediate tool results keyed by run_id.

    Exists for the lifetime of one agent worker invocation. The reason it
    exists at all: tool-use loops are unreliable at copying structured row
    data verbatim between tool calls (the model omits, summarizes, or
    hallucinates). Returning a short run_id and letting downstream tools
    reference it by id eliminates the copy entirely.

    Rows are stored as a list of dicts (column-name keyed). run_sql output
    is converted on insert; extract/aggregate output is already in that
    shape.
    """

    def __init__(self):
        self._runs: dict = {}
        self._seq = 0

    def put(self, kind: str, columns: list, rows: list, meta: Optional[dict] = None) -> str:
        self._seq += 1
        run_id = f"{kind}-{self._seq:03d}"
        self._runs[run_id] = {
            "columns": columns,
            "rows": rows,
            "meta": meta or {},
        }
        return run_id

    def get(self, run_id: str) -> Optional[dict]:
        return self._runs.get(run_id)


def _positional_rows_to_dicts(columns: list, rows: list) -> list:
    """Convert run_sql's positional row shape ([[v1, v2], ...]) into the
    column-keyed dict shape ([{col1: v1, col2: v2}, ...]) downstream tools
    expect. Idempotent: if rows already look like dicts, returns them
    unchanged.
    """
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return rows
    col_names = [c.get("name") if isinstance(c, dict) else str(c) for c in columns]
    return [
        {name: (row[i] if i < len(row) else None) for i, name in enumerate(col_names)}
        for row in rows
    ]


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
        "injected if absent. Returns "
        "{run_id, columns, preview_rows, row_count, sql}. The FULL rows "
        "are held server-side under run_id; downstream tools "
        "(extract_from_rows, aggregate) accept `from_run_id` so you "
        "never need to copy rows into their inputs."
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
        "Per-row Claude extraction of a short string from narrative "
        "fields. Use this when an answer is buried in prose and cannot "
        "be filtered with SQL LIKE. Cap: 200 rows per call.\n\n"
        "PREFERRED: pass `from_run_id` with the run_id you got from "
        "run_sql. The server pulls the rows from cache; you don't need "
        "to copy them into the input. Example: "
        '{"from_run_id": "sql-001", "question": "What agency referred?"}.\n'
        "Alternative (only for small literal data): pass `rows` as an "
        'array of column-keyed objects, e.g. [{"clientvisit_id": "abc", '
        '"answer": "text..."}].\n\n'
        "Returns {run_id, row_count, preview_results}. The output rows "
        "preserve EVERY source-row field (e.g. clientvisit_id, answer) "
        "AND add `extracted_value` — so you do NOT need to merge the "
        "extraction back to the source. Just pass the extract run_id "
        "straight to aggregate, select_columns, or finalize."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "from_run_id": {
                "type": "string",
                "description": (
                    "Server-side row reference returned by a previous "
                    "run_sql call. Strongly preferred over literal `rows`."
                ),
            },
            "rows": {
                "type": "array",
                "description": (
                    "Optional literal rows. Use ONLY when you don't "
                    "already have a run_id (e.g. tiny ad-hoc data). "
                    "Array of column-keyed objects."
                ),
                "items": {"type": "object"},
            },
            "question": {
                "type": "string",
                "description": "What to extract from each row's narrative.",
            },
        },
        "required": ["question"],
        "additionalProperties": False,
    },
}

AGGREGATE_TOOL = {
    "name": "aggregate",
    "description": (
        "Group rows by one or more keys and count or sum. Use this after "
        "extract_from_rows to turn per-row extracted values into a "
        "{value, count} table. Operates in-memory; do NOT re-query Athena.\n\n"
        "PREFERRED: pass `from_run_id` with the run_id from a previous "
        "run_sql or extract_from_rows call. For extract_from_rows output, "
        "group_by typically = [\"extracted_value\"].\n"
        "Alternative: pass `rows` as a literal array of column-keyed objects.\n\n"
        "Returns {run_id, columns, rows, row_count} in positional shape "
        "ready to feed into finalize."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "from_run_id": {
                "type": "string",
                "description": (
                    "Server-side row reference. Preferred over literal `rows`."
                ),
            },
            "rows": {
                "type": "array",
                "items": {"type": "object"},
                "description": "Optional literal row objects.",
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
        "required": ["group_by", "agg"],
        "additionalProperties": False,
    },
}

SELECT_COLUMNS_TOOL = {
    "name": "select_columns",
    "description": (
        "Shape the rows behind a run_id into the columns the user asked "
        "for. Pick a subset of existing columns, optionally with a "
        "computed column derived from a CASE-WHEN expression (e.g. "
        "'show answer only when extracted_value == UNKNOWN'). Returns "
        "{run_id, columns, rows, row_count} ready for finalize.\n\n"
        "Use this when you need a final shape that's a strict subset of "
        "an extract_from_rows or run_sql result, or when you need a "
        "conditional column. Do NOT try to encode conditional logic "
        "inside an extract_from_rows prompt — use select_columns.computed."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "from_run_id": {
                "type": "string",
                "description": (
                    "Server-side row reference returned by a previous "
                    "tool (run_sql, extract_from_rows, aggregate)."
                ),
            },
            "columns": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Output column names, in display order. Each name "
                    "must either match a column in the source run_id "
                    "OR be a key in `computed`."
                ),
            },
            "computed": {
                "type": "object",
                "description": (
                    "Optional map of output_column_name -> "
                    "{case_when: {field, op, value, then, else}}. "
                    "`field` is the source column to test; `op` is "
                    "'==' or '!='; `value` is the literal to compare; "
                    "`then` and `else` are EITHER literal strings OR "
                    "names of source columns (looked up if they match)."
                ),
            },
        },
        "required": ["from_run_id", "columns"],
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
    SELECT_COLUMNS_TOOL,
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
    cache: RunCache,
    spill: Optional[Callable[[bytes, str], str]] = None,
) -> Callable:
    """Build the run_sql tool handler for an org.

    Stashes the FULL row data in the worker-local RunCache under a fresh
    run_id and returns only a short preview to Claude. Downstream tools
    (extract_from_rows, aggregate) accept `from_run_id` so the agent
    never has to copy rows between tool calls — which it's bad at.

    spill: optional callable(bytes, suffix_hint) -> s3_key for very large
    results, so the in-memory cache doesn't balloon. The spill key is
    stored in the cache's `meta` so a UI deep-dive can fetch it.
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

        columns = result["columns"]
        rows_positional = result["rows"]
        rows_dicts = _positional_rows_to_dicts(columns, rows_positional)

        s3_key = None
        full_payload = {
            "sql": sql,
            "columns": columns,
            "rows": rows_positional,
            "row_count": result["row_count"],
        }
        if _approx_json_size(full_payload) > SPILL_THRESHOLD_BYTES and spill is not None:
            s3_key = spill(
                json.dumps(full_payload, default=str).encode("utf-8"),
                "run_sql.json",
            )

        run_id = cache.put(
            "sql",
            columns=columns,
            rows=rows_dicts,
            meta={"sql": sql, "s3_key": s3_key},
        )

        return {
            "run_id": run_id,
            "sql": sql,
            "columns": columns,
            "preview_rows": rows_positional[:PREVIEW_ROWS],
            "row_count": result["row_count"],
            "note": (
                f"Full {result['row_count']} rows cached under "
                f"run_id={run_id!r}; pass `from_run_id` to extract_from_rows "
                f"or aggregate."
            ),
        }
    return handler


def make_extract_from_rows_handler(
    extractor: Callable[[str, dict], str],
    cache: RunCache,
    max_workers: int = 10,
    spill: Optional[Callable[[bytes, str], str]] = None,
) -> Callable:
    """Build the extract_from_rows tool handler.

    Accepts EITHER `from_run_id` (preferred, pulls rows from cache) or a
    literal `rows` array (fallback for small ad-hoc data). Stores its
    own result under a new run_id so the agent can chain into aggregate
    without copying.

    extractor(question, row_payload) -> extracted_value str. Defaults to
    admin_api._deep_extract_for_row when wired by the worker. Wrapped
    here in a thread pool so a 100-row call doesn't serialize Bedrock
    latency.
    """
    def handler(tu_input: dict) -> dict:
        question = tu_input.get("question") or ""
        from_run_id = tu_input.get("from_run_id")

        if not isinstance(question, str) or not question.strip():
            return {"error": "question is required", "code": "BAD_INPUT"}

        if from_run_id:
            run = cache.get(from_run_id)
            if run is None:
                return {
                    "error": (
                        f"Unknown run_id {from_run_id!r}. Use the run_id "
                        f"returned by your most recent run_sql call."
                    ),
                    "code": "UNKNOWN_RUN_ID",
                }
            rows = run["rows"]
            source_columns = run["columns"]
        else:
            rows = tu_input.get("rows") or []
            if not isinstance(rows, list):
                return {"error": "rows must be a list", "code": "BAD_INPUT"}
            if len(rows) == 0:
                return {
                    "error": (
                        "No rows provided. Pass `from_run_id` with the "
                        "run_id from your run_sql call (preferred), or a "
                        "literal `rows` array of column-keyed objects."
                    ),
                    "code": "EMPTY_ROWS",
                }
            source_columns = None  # not known for ad-hoc literal rows

        if len(rows) > MAX_EXTRACT_ROWS:
            return {
                "error": (
                    f"extract_from_rows received {len(rows)} rows; cap is "
                    f"{MAX_EXTRACT_ROWS}. Narrow the scope and call again."
                ),
                "code": "TOO_MANY_ROWS",
            }

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

        # Preserve EVERY source-row field next to extracted_value. Without
        # this, "show clientvisit_id alongside its extracted agency" is
        # unreachable through the tool catalog and the agent wastes turns
        # trying to encode joins into the extraction prompt. With it, the
        # extract run_id already carries everything finalize/select_columns
        # need — no merge tool required.
        result_rows = []
        for i, (src, value) in enumerate(zip(rows, extracted)):
            entry = {"row_index": i}
            if isinstance(src, dict):
                entry.update(src)
            entry["extracted_value"] = value
            result_rows.append(entry)

        # Build the column list: row_index first, then the source columns
        # in their original order, then extracted_value. Prefer the cached
        # source_columns when available (came from run_sql via from_run_id);
        # otherwise walk the first row's keys.
        src_col_names: list = []
        if source_columns:
            for c in source_columns:
                name = c.get("name") if isinstance(c, dict) else str(c)
                if name and name not in src_col_names:
                    src_col_names.append(name)
        else:
            for row in rows:
                if isinstance(row, dict):
                    for k in row.keys():
                        if k not in src_col_names:
                            src_col_names.append(k)
                    break
        result_columns = [{"name": "row_index", "type": "number"}]
        result_columns += [{"name": k, "type": "string"} for k in src_col_names]
        result_columns.append({"name": "extracted_value", "type": "string"})

        # Spill the full extract for very large results so the cache doesn't
        # balloon — but we still keep the dict rows in cache so downstream
        # tools can use them by run_id.
        s3_key = None
        if _approx_json_size({"results": result_rows}) > SPILL_THRESHOLD_BYTES and spill is not None:
            s3_key = spill(
                json.dumps({"results": result_rows}, default=str).encode("utf-8"),
                "extract.json",
            )

        run_id = cache.put(
            "extract",
            columns=result_columns,
            rows=result_rows,
            meta={"question": question, "source_columns": source_columns, "s3_key": s3_key},
        )

        return {
            "run_id": run_id,
            "row_count": len(result_rows),
            "preview_results": result_rows[:PREVIEW_ROWS],
            "note": (
                f"Extracted {len(result_rows)} values; cached under "
                f"run_id={run_id!r}. Each row preserves ALL source "
                f"columns ({', '.join(src_col_names) or '(none)'}) plus "
                f"`extracted_value` — so you can: pass `from_run_id` to "
                f"aggregate (e.g. group_by=['extracted_value']), or pass "
                f"it to select_columns to pick a final shape, or pass it "
                f"straight to finalize. NEVER try to merge the extraction "
                f"back to a separate run_id — it's already merged."
            ),
        }
    return handler


def _aggregate_impl(rows: list, group_by: list, agg: str,
                    sum_field: Optional[str], order_by: str) -> dict:
    """Pure in-memory group_by + count/sum. Returns positional rows."""
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


def make_aggregate_handler(cache: RunCache) -> Callable:
    """Build the aggregate tool handler. Accepts either `from_run_id`
    (pulls rows from cache) or a literal `rows` array. Stashes its own
    result under a new run_id so the agent can pass it to finalize by id
    if desired (though for typical small aggregate results, inlining is fine).
    """
    def handler(tu_input: dict) -> dict:
        group_by = tu_input.get("group_by") or []
        agg = tu_input.get("agg") or "count"
        sum_field = tu_input.get("sum_field")
        order_by = tu_input.get("order_by") or "count_desc"
        from_run_id = tu_input.get("from_run_id")

        if not isinstance(group_by, list) or not group_by:
            return {"error": "group_by must be a non-empty list", "code": "BAD_INPUT"}
        if agg == "sum" and not sum_field:
            return {"error": "agg='sum' requires sum_field", "code": "BAD_INPUT"}

        if from_run_id:
            run = cache.get(from_run_id)
            if run is None:
                return {
                    "error": (
                        f"Unknown run_id {from_run_id!r}. Use the run_id "
                        f"returned by your most recent run_sql or "
                        f"extract_from_rows call."
                    ),
                    "code": "UNKNOWN_RUN_ID",
                }
            rows = run["rows"]
        else:
            rows = tu_input.get("rows") or []
            if not isinstance(rows, list):
                return {"error": "rows must be a list", "code": "BAD_INPUT"}
            if len(rows) == 0:
                return {
                    "error": (
                        "No rows. Pass `from_run_id` from a prior run_sql "
                        "or extract_from_rows call, or a literal `rows` array."
                    ),
                    "code": "EMPTY_ROWS",
                }

        result = _aggregate_impl(rows, group_by, agg, sum_field, order_by)
        run_id = cache.put(
            "agg",
            columns=result["columns"],
            rows=_positional_rows_to_dicts(result["columns"], result["rows"]),
            meta={"group_by": group_by, "agg": agg},
        )
        return {
            "run_id": run_id,
            "columns": result["columns"],
            "rows": result["rows"],
            "row_count": result["row_count"],
        }
    return handler


# Back-compat alias for tests that imported the old function directly.
def aggregate_handler(tu_input: dict) -> dict:
    """Legacy literal-rows aggregate. Prefer make_aggregate_handler() with
    a RunCache for the agent-loop path; this remains for unit tests and
    any caller that just wants pure aggregation."""
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
    return _aggregate_impl(rows, group_by, agg, sum_field, order_by)


# ---- select_columns handler -------------------------------------------

_SELECT_COLUMNS_VALID_OPS = ("==", "!=")


def make_select_columns_handler(cache: RunCache) -> Callable:
    """Build the select_columns tool handler.

    Picks a subset of columns from the rows behind a run_id, with optional
    per-column CASE-WHEN substitution. The result is cached under a new
    run_id and also returned inline in positional shape so finalize can
    consume it directly.

    Supported `computed[col]` shape:
        {"case_when": {
            "field": <source_col>,
            "op": "==" | "!=",
            "value": <literal>,
            "then": <literal_str OR source_col_name>,
            "else": <literal_str OR source_col_name>,
        }}
    `then` / `else` are looked up as source columns when their string
    matches a source column name; otherwise treated as literals. This
    lets the agent express "show answer when extracted_value == UNKNOWN,
    else empty string" without any procedural code.
    """
    def handler(tu_input: dict) -> dict:
        from_run_id = tu_input.get("from_run_id")
        out_cols = tu_input.get("columns") or []
        computed = tu_input.get("computed") or {}

        if not from_run_id:
            return {
                "error": "from_run_id is required",
                "code": "BAD_INPUT",
            }
        if not isinstance(out_cols, list) or not out_cols:
            return {
                "error": "columns must be a non-empty list of names",
                "code": "BAD_INPUT",
            }
        if not isinstance(computed, dict):
            return {
                "error": "computed must be an object",
                "code": "BAD_INPUT",
            }

        run = cache.get(from_run_id)
        if run is None:
            return {
                "error": (
                    f"Unknown run_id {from_run_id!r}. Use the run_id "
                    f"returned by a previous tool."
                ),
                "code": "UNKNOWN_RUN_ID",
            }
        src_rows = run["rows"]
        src_col_names = {
            (c.get("name") if isinstance(c, dict) else str(c))
            for c in (run.get("columns") or [])
        }
        # Source-row keys actually present (some rows may carry extras the
        # source columns list missed — be lenient).
        if src_rows and isinstance(src_rows[0], dict):
            src_col_names |= set(src_rows[0].keys())

        # Validate every output column resolves either to a source column
        # or to a computed entry — fail fast with a helpful message.
        unresolved = [
            c for c in out_cols
            if c not in src_col_names and c not in computed
        ]
        if unresolved:
            return {
                "error": (
                    f"Unknown output column(s): {unresolved}. Must be "
                    f"either a source column ({sorted(src_col_names)}) "
                    f"or a key in `computed`."
                ),
                "code": "UNKNOWN_COLUMN",
            }

        # Validate each computed spec up-front so we don't crash mid-row.
        for col, spec in computed.items():
            if not isinstance(spec, dict) or "case_when" not in spec:
                return {
                    "error": (
                        f"computed[{col!r}] must be {{case_when: {{...}}}}"
                    ),
                    "code": "BAD_COMPUTED",
                }
            cw = spec["case_when"]
            for k in ("field", "op", "value", "then", "else"):
                if k not in cw:
                    return {
                        "error": (
                            f"computed[{col!r}].case_when missing key {k!r}"
                        ),
                        "code": "BAD_COMPUTED",
                    }
            if cw["op"] not in _SELECT_COLUMNS_VALID_OPS:
                return {
                    "error": (
                        f"computed[{col!r}].case_when.op must be one of "
                        f"{_SELECT_COLUMNS_VALID_OPS}, got {cw['op']!r}"
                    ),
                    "code": "BAD_COMPUTED",
                }

        def _resolve(value_or_colname, row: dict):
            """If value_or_colname matches a source column name, return
            the row's value for that column; otherwise return it as a
            literal string."""
            if isinstance(value_or_colname, str) and value_or_colname in src_col_names:
                return row.get(value_or_colname, "")
            return value_or_colname

        def _eval_case(spec: dict, row: dict):
            cw = spec["case_when"]
            actual = row.get(cw["field"], None)
            target = cw["value"]
            matches = (str(actual) == str(target)) if cw["op"] == "==" else (str(actual) != str(target))
            return _resolve(cw["then"] if matches else cw["else"], row)

        # Project the rows.
        positional_rows = []
        dict_rows = []
        for row in src_rows:
            if not isinstance(row, dict):
                continue
            picked = {}
            for c in out_cols:
                if c in computed:
                    picked[c] = _eval_case(computed[c], row)
                else:
                    picked[c] = row.get(c, "")
            dict_rows.append(picked)
            positional_rows.append([picked[c] for c in out_cols])

        out_columns = [{"name": c, "type": "string"} for c in out_cols]
        run_id = cache.put(
            "select",
            columns=out_columns,
            rows=dict_rows,
            meta={"from_run_id": from_run_id, "computed_keys": list(computed.keys())},
        )
        return {
            "run_id": run_id,
            "columns": out_columns,
            "rows": positional_rows,
            "row_count": len(positional_rows),
        }
    return handler


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
    cache: Optional[RunCache] = None,
    spill: Optional[Callable[[bytes, str], str]] = None,
) -> dict:
    """Convenience wiring used by the worker; tests can build their own.

    The RunCache is the shared scratch space for `run_id` handoffs between
    tools. The worker creates one per agent invocation and passes it in
    so the cache lives only for the duration of that one run.
    """
    if cache is None:
        cache = RunCache()
    return {
        "inspect_schema": make_inspect_schema_handler(org_id),
        "run_sql": make_run_sql_handler(org_id, cache=cache, spill=spill),
        "extract_from_rows": make_extract_from_rows_handler(
            extractor, cache=cache, spill=spill,
        ),
        "aggregate": make_aggregate_handler(cache),
        "select_columns": make_select_columns_handler(cache),
    }


