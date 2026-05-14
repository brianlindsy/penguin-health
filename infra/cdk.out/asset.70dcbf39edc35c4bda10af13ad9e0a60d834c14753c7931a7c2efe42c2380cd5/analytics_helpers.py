"""
Analytics helpers: schema registry, SQL validation, Athena query execution.

Source of truth for table schemas:
  - infra/components/analytics.py::ORG_TABLES (charts_{org} columns)
  - lambda/multi-org/rules-engine/parquet_writer.py::PARQUET_COLUMNS
    (validation_results_{org} columns)

Keep ORG_SCHEMAS below in sync when columns change in either source.

The narrative flag on chart columns is a judgment call about which fields
hold free-form prose. Columns flagged narrative are still queryable via
SQL (LIKE), but the NL endpoint can also route them through the deep
extraction path when keyword matching isn't sufficient.

TODO: if p95 query time exceeds ~20s, refactor run_athena_query to async —
return execution_id immediately and add a polling endpoint.
"""

import re
import time
import uuid
from typing import Optional

import boto3

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Punctuation


GLUE_DATABASE = "penguin_health_analytics"
ATHENA_WORKGROUP_PREFIX = "penguin-health-analytics-"
MAX_SQL_LIMIT = 1000
MAX_DEEP_SCOPE_LIMIT = 200
ATHENA_POLL_INTERVAL_SEC = 1.0
ATHENA_TIMEOUT_SEC = 25.0
MAX_RESULT_ROWS = 1100  # small headroom above MAX_SQL_LIMIT for header row


athena_client = boto3.client('athena')


# ---- Schema registry ---------------------------------------------------

# Validation results schema is fixed across orgs. Mirror of PARQUET_COLUMNS
# in lambda/multi-org/rules-engine/parquet_writer.py.
#
# Each entry is (name, type, notes). The notes field carries domain hints
# the LLM cannot infer from the column name alone — discovered by sampling
# the actual data (see audit done 2026-05-11). Keep these in sync with the
# rules-engine writer when its output changes.
_VALIDATION_COLUMNS = [
    ("organization_id", "string", None),
    ("validation_run_id", "string", None),
    (
        "validation_timestamp",
        "string",
        "ISO-8601 without timezone, e.g. '2026-05-11T11:15:44.204536'. "
        "Use date_parse(col, '%Y-%m-%dT%H:%i:%s.%f') if parsing.",
    ),
    ("document_id", "string", None),
    ("filename", "string", None),
    ("s3_key", "string", None),
    ("rule_id", "string", None),
    ("rule_name", "string", None),
    (
        "category",
        "string",
        "Org-defined rule category (open set). Use SELECT DISTINCT to "
        "discover values rather than guessing.",
    ),
    (
        "rule_type",
        "string",
        "One of: 'llm', 'deterministic'. Lowercase.",
    ),
    (
        "status",
        "string",
        "One of: 'PASS', 'FAIL', 'SKIP', 'ERROR'. UPPERCASE. "
        "Do NOT use 'passed'/'failed'/'error'.",
    ),
    ("message", "string", None),
    (
        "field_date",
        "string",
        "Service date promoted from chart field_values. Format: "
        "'YYYY-MM-DD HH:MM:SS.SSS' with a SPACE separator (not 'T'). "
        "Use date_parse(col, '%Y-%m-%d %H:%i:%s.%f') if parsing.",
    ),
    ("field_employee_name", "string", None),
    ("field_program", "string", None),
    ("field_cpt_code", "string", None),
    (
        "field_rate",
        "string",
        "Dollar rate stored as a string (e.g. '0.0000' or '125.5000'). "
        "CAST(field_rate AS double) to aggregate; some values may be "
        "empty or non-numeric — use TRY_CAST or filter first.",
    ),
    (
        "field_values_json",
        "string",
        "JSON-encoded dict of any field_values not promoted to a "
        "field_* column. Use json_extract_scalar(field_values_json, "
        "'$.key') if you need a value from it.",
    ),
]


# Per-column notes for chart columns. Audited against live data 2026-05-11.
# Keyed by (org_id, column_name) → free-text hint. Anything not in this map
# is documented with name + type alone.
_CHART_COLUMN_NOTES = {
    ("circles-of-care", "service_date"): (
        "Format: 'YYYY-MM-DD HH:MM:SS.SSS' with a SPACE separator "
        "(not 'T'). Use date_parse(col, '%Y-%m-%d %H:%i:%s.%f') if "
        "parsing, or substr(service_date, 1, 10) for just the date."
    ),
    ("circles-of-care", "admission_date"): (
        "Same format as service_date: 'YYYY-MM-DD HH:MM:SS.SSS' with "
        "space separator."
    ),
    ("circles-of-care", "discharge_date"): (
        "Same format as service_date: 'YYYY-MM-DD HH:MM:SS.SSS' with "
        "space separator."
    ),
    ("circles-of-care", "age_at_service"): (
        "Integer-valued string. CAST(age_at_service AS integer) to "
        "aggregate."
    ),
    ("circles-of-care", "rate"): (
        "Numeric-valued string (may be empty). Use TRY_CAST(rate AS "
        "double) to aggregate."
    ),
    ("circles-of-care", "sex"): "Observed values: 'M', 'F'.",
}


# Narrative columns per org — these hold free-text prose. The LLM may use
# them in WHERE/LIKE filters, and they are the inputs the deep extraction
# path reads per row.
_NARRATIVE_COLUMNS = {
    "circles-of-care": {"question_text", "answer", "first_referral"},
    "catholic-charities-multi-org": {
        "narrative_simple_27",
        "narrative_rich_28",
        "next_steps_29",
        "next_steps_effective_29c",
        "next_steps_form_only_29b",
        "form_q_and_a_30_31",
        "plan_goals_only_47b",
        "plan_objectives_only_47c",
        "plan_interventions_only_47d",
    },
    "demo": {
        "narrative_simple_27",
        "narrative_rich_28",
        "next_steps_29",
        "next_steps_effective_29c",
        "next_steps_form_only_29b",
        "form_q_and_a_30_31",
        "plan_goals_only_47b",
        "plan_objectives_only_47c",
        "plan_interventions_only_47d",
        "tx_plus_intervention_documentation_47g",
        "plan_documentation_47e",
    },
}


# Chart columns per org — mirror of ORG_TABLES in infra/components/analytics.py.
# All chart columns are typed as string (OpenCSVSerde). Numeric/date columns
# need CAST or date_parse before arithmetic / comparison.
_CHART_COLUMNS = {
    "circles-of-care": [
        "fake_client_id", "clientvisit_id", "grade", "race_desc",
        "ethnicity_desc", "sex", "marital_status", "age_at_service",
        "visittype", "plan_id", "service_date", "episode_id",
        "program_desc", "admission_date", "discharge_date", "icd10_codes",
        "problem_list_order", "diagnose_on_visit_bedday", "fake_client_id2",
        "clientvisit_id2", "first_referral", "question_text", "answer",
        "type", "episode_id2", "cptcode", "first_name", "last_name", "rate",
        "initial_appt", "agegroup", "diagnose_on_visit2",
    ],
    # TODO(audit): catholic-charities and demo column notes have not been
    # verified against live data — only circles-of-care has been audited.
    "catholic-charities-multi-org": [
        "visit_link_50", "service_id_1", "consumer_name_2", "consumer_dob_3",
        "episode_id_4", "staff_name_5", "staff_id_6", "program_7",
        "program_name_7b", "service_date_8", "start_time_9", "end_time_10",
        "revised_start_11", "revised_end_12", "duration_13", "signed_time_14",
        "transferred_time_15", "cpt_code_16", "client_insurance_order_26s",
        "billing_order_26b", "billing_sequence_26c", "billing_group_id_26t",
        "billing_group_name_26u", "units_17", "rate_18", "modifier_1_19",
        "modifier_2_20", "visit_type_id_21", "visit_type_name_21b",
        "location_code_22", "location_label_22b", "recipient_code_23",
        "recipient_label_23b", "approved_24", "non_billable_25",
        "authorization_id_26", "narrative_simple_27", "narrative_rich_28",
        "next_steps_effective_29c", "next_steps_form_only_29b",
        "next_steps_29", "form_q_and_a_30_31", "vitals_bp_32",
        "vitals_pulse_33", "vitals_temp_34", "vitals_weight_35",
        "plan_start_date_38", "plan_end_date_39", "plan_status_40",
        "diagnosis_code_primary_41b", "external_id_44", "insurance_45",
        "has_next_insurance_flag_26o", "plan_type_46", "plan_goals_only_47b",
        "plan_objectives_only_47c", "plan_interventions_only_47d",
        "plan_signed_date_48", "plan_signer_49", "plan_qp_signer_49g",
        "plan_signed_date_qp_49h",
    ],
    "demo": [
        "visit_link_50", "service_id_1", "consumer_name_2", "consumer_dob_3",
        "episode_id_4", "staff_name_5", "staff_id_6", "program_7",
        "program_name_7b", "service_date_8", "start_time_9", "end_time_10",
        "revised_start_11", "revised_end_12", "duration_13", "signed_time_14",
        "transferred_time_15", "cpt_code_16", "client_insurance_order_26s",
        "billing_order_26b", "billing_sequence_26c", "billing_group_id_26t",
        "billing_group_name_26u", "units_17", "rate_18", "modifier_1_19",
        "modifier_2_20", "visit_type_id_21", "visit_type_name_21b",
        "location_code_22", "location_label_22b", "recipient_code_23",
        "recipient_label_23b", "approved_24", "non_billable_25",
        "authorization_id_26", "narrative_simple_27", "narrative_rich_28",
        "next_steps_effective_29c", "next_steps_form_only_29b",
        "next_steps_29", "form_q_and_a_30_31", "vitals_bp_32",
        "vitals_pulse_33", "vitals_temp_34", "vitals_weight_35",
        "plan_start_date_38", "plan_end_date_39", "plan_status_40",
        "diagnosis_code_primary_41b", "external_id_44", "insurance_45",
        "has_next_insurance_flag_26o", "plan_type_46", "plan_goals_only_47b",
        "plan_objectives_only_47c", "plan_interventions_only_47d",
        "plan_signed_date_48", "plan_signer_49", "plan_qp_signer_49g",
        "plan_signed_date_qp_49h", "tx_plus_intervention_documentation_47g",
        "plan_documentation_47e",
    ],
}


def _build_org_schemas():
    schemas = {}
    for org_id, cols in _CHART_COLUMNS.items():
        narrative = _NARRATIVE_COLUMNS.get(org_id, set())
        chart_columns = []
        for c in cols:
            entry = {
                "name": c,
                "type": "string",
                "narrative": c in narrative,
            }
            note = _CHART_COLUMN_NOTES.get((org_id, c))
            if note:
                entry["notes"] = note
            chart_columns.append(entry)
        validation_columns = []
        for c, t, note in _VALIDATION_COLUMNS:
            entry = {"name": c, "type": t, "narrative": False}
            if note:
                entry["notes"] = note
            validation_columns.append(entry)
        schemas[org_id] = {
            "chart_columns": chart_columns,
            "chart_partition_key": "ingest_date",
            "chart_partition_notes": (
                "Format yyyy-MM-dd. Reflects when the CSV was ingested, "
                "NOT the clinical service date — use service_date for "
                "clinical date filters."
            ),
            "validation_columns": validation_columns,
            "validation_partition_key": "validation_date",
            "validation_partition_notes": (
                "Format yyyy-MM-dd, UTC. Derived from validation_timestamp."
            ),
        }
    return schemas


ORG_SCHEMAS = _build_org_schemas()


def table_suffix(org_id: str) -> str:
    """
    Match infra/components/analytics.py::_table_suffix — Glue identifiers
    can't contain dashes.
    """
    return org_id.replace("-", "_")


def allowed_tables(org_id: str) -> set:
    suffix = table_suffix(org_id)
    return {f"charts_{suffix}", f"validation_results_{suffix}"}


def narrative_columns_for_org(org_id: str) -> set:
    return set(_NARRATIVE_COLUMNS.get(org_id, set()))


# ---- Exceptions --------------------------------------------------------

class SqlValidationError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class AthenaQueryError(Exception):
    def __init__(self, message: str, state: Optional[str] = None):
        self.message = message
        self.state = state
        super().__init__(message)


# ---- SQL validation ----------------------------------------------------

_FORBIDDEN_KEYWORDS = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "CALL", "MERGE", "GRANT", "REVOKE",
)
_FORBIDDEN_RE = re.compile(
    r"\b(" + "|".join(_FORBIDDEN_KEYWORDS) + r")\b",
    re.IGNORECASE,
)
_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_COMMENT_LINE_RE = re.compile(r"--[^\n]*")


def _strip_comments(sql: str) -> str:
    sql = _COMMENT_BLOCK_RE.sub(" ", sql)
    sql = _COMMENT_LINE_RE.sub(" ", sql)
    return sql


def _normalize_table_name(name: str) -> str:
    """Strip quotes and database qualifier from a table identifier."""
    name = name.strip().strip('"').strip("`")
    if "." in name:
        name = name.rsplit(".", 1)[-1]
        name = name.strip().strip('"').strip("`")
    return name.lower()


def _extract_table_names(parsed) -> list:
    """
    Walk the parsed statement and collect every identifier that follows
    FROM or JOIN. Handles comma-separated lists and aliased identifiers.
    """
    tables = []
    from_seen = False

    def emit_from_token(tok):
        if isinstance(tok, IdentifierList):
            for sub in tok.get_identifiers():
                tables.append(_normalize_table_name(sub.get_real_name() or str(sub)))
        elif isinstance(tok, Identifier):
            tables.append(_normalize_table_name(tok.get_real_name() or str(tok)))
        else:
            text = str(tok).strip()
            if text:
                tables.append(_normalize_table_name(text))

    def walk(tokens):
        nonlocal from_seen
        for tok in tokens:
            if tok.is_whitespace:
                continue
            if tok.ttype is Keyword and tok.normalized.upper() in ("FROM", "JOIN"):
                from_seen = True
                continue
            if tok.ttype is Keyword and tok.normalized.upper() in (
                "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN",
                "CROSS JOIN", "LEFT OUTER JOIN", "RIGHT OUTER JOIN",
                "FULL OUTER JOIN",
            ):
                from_seen = True
                continue
            if from_seen:
                emit_from_token(tok)
                from_seen = False
            # Recurse into groups (parentheses, subqueries, identifier lists).
            if hasattr(tok, "tokens"):
                walk(tok.tokens)

    walk(parsed.tokens)
    return [t for t in tables if t]


def _find_limit_value(parsed) -> Optional[int]:
    """Return the integer following the first top-level LIMIT keyword, or None."""
    tokens = [t for t in parsed.flatten() if not t.is_whitespace]
    for i, tok in enumerate(tokens):
        if tok.ttype is Keyword and tok.normalized.upper() == "LIMIT":
            # Next non-whitespace token should be the integer.
            for nxt in tokens[i + 1:]:
                text = str(nxt).strip()
                if text.isdigit():
                    return int(text)
                break
    return None


def _apply_limit(sql: str, max_limit: int) -> str:
    """
    Ensure the SQL has a top-level LIMIT clause <= max_limit.
    - If absent, append ` LIMIT max_limit`.
    - If present and over max_limit, rewrite it.
    - If present and within bounds, leave it.
    """
    parsed = sqlparse.parse(sql)[0]
    current = _find_limit_value(parsed)
    sql_stripped = sql.rstrip().rstrip(";").rstrip()
    if current is None:
        return f"{sql_stripped} LIMIT {max_limit}"
    if current > max_limit:
        return re.sub(
            r"\bLIMIT\s+\d+\b",
            f"LIMIT {max_limit}",
            sql_stripped,
            count=1,
            flags=re.IGNORECASE,
        )
    return sql_stripped


def validate_athena_sql(
    sql: str,
    org_id: str,
    max_limit: int = MAX_SQL_LIMIT,
) -> str:
    """
    Validate and sanitize a SQL string produced by Claude.

    Returns the sanitized SQL (with LIMIT injected/capped if needed).
    Raises SqlValidationError on rejection.
    """
    if not sql or not isinstance(sql, str) or not sql.strip():
        raise SqlValidationError("EMPTY", "SQL is empty.")

    stripped = _strip_comments(sql).strip()

    # Forbidden keyword scan happens on comment-stripped SQL.
    m = _FORBIDDEN_RE.search(stripped)
    if m:
        raise SqlValidationError(
            "FORBIDDEN_KEYWORD",
            f"SQL contains disallowed keyword: {m.group(1).upper()}.",
        )

    parsed_list = sqlparse.parse(stripped)
    parsed_list = [p for p in parsed_list if str(p).strip()]
    if len(parsed_list) == 0:
        raise SqlValidationError("PARSE_ERROR", "Could not parse SQL.")
    if len(parsed_list) > 1:
        raise SqlValidationError(
            "MULTIPLE_STATEMENTS",
            "Only a single SQL statement is allowed.",
        )
    parsed = parsed_list[0]

    stmt_type = parsed.get_type()
    if stmt_type != "SELECT":
        # sqlparse returns "UNKNOWN" for CTE-wrapped selects starting with WITH.
        # Check for that explicitly.
        first = None
        for tok in parsed.flatten():
            if tok.is_whitespace:
                continue
            first = tok
            break
        if not (first is not None and first.ttype is Keyword.CTE) and \
           not (first is not None and first.normalized.upper() == "WITH"):
            raise SqlValidationError(
                "NOT_SELECT",
                f"Only SELECT statements are allowed (got {stmt_type}).",
            )

    # Table allowlist.
    allowed = allowed_tables(org_id)
    tables = _extract_table_names(parsed)
    if not tables:
        raise SqlValidationError(
            "NO_TABLE",
            "Query must reference at least one allowed table.",
        )
    for t in tables:
        if t not in allowed:
            raise SqlValidationError(
                "DISALLOWED_TABLE",
                f"Table '{t}' is not in the analytics allowlist for this org. "
                f"Allowed: {sorted(allowed)}.",
            )

    # LIMIT enforcement (rewrite raw SQL string).
    sanitized = _apply_limit(stripped, max_limit)
    return sanitized


# ---- Athena execution --------------------------------------------------

def run_athena_query(sql: str, org_id: str) -> dict:
    """
    Execute SQL against the org's Athena workgroup with sync polling.

    Returns:
        {
            "columns": [{"name": str, "type": str}, ...],
            "rows":    [[str, str, ...], ...],
            "row_count": int,
            "query_execution_id": str,
        }

    Raises AthenaQueryError if the query fails, is cancelled, or times out.
    """
    workgroup = f"{ATHENA_WORKGROUP_PREFIX}{org_id}"

    start_resp = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ClientRequestToken=str(uuid.uuid4()),
    )
    qid = start_resp["QueryExecutionId"]

    deadline = time.monotonic() + ATHENA_TIMEOUT_SEC
    while True:
        exec_resp = athena_client.get_query_execution(QueryExecutionId=qid)
        status = exec_resp["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "no reason given")
            raise AthenaQueryError(
                f"Athena query {state.lower()}: {reason}",
                state=state,
            )
        if time.monotonic() >= deadline:
            try:
                athena_client.stop_query_execution(QueryExecutionId=qid)
            except Exception:
                pass
            raise AthenaQueryError(
                f"Athena query timed out after {ATHENA_TIMEOUT_SEC:.0f}s.",
                state="TIMED_OUT",
            )
        time.sleep(ATHENA_POLL_INTERVAL_SEC)

    columns, rows = _fetch_results(qid)
    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "query_execution_id": qid,
    }


def _fetch_results(query_execution_id: str):
    """
    Paginate get_query_results. First row of the first page is the header
    (column names) — drop it. Cap at MAX_RESULT_ROWS rows total.
    """
    columns = None
    rows = []
    next_token = None
    first_page = True

    while True:
        kwargs = {"QueryExecutionId": query_execution_id, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena_client.get_query_results(**kwargs)
        rs = resp.get("ResultSet", {})

        if columns is None:
            meta = rs.get("ResultSetMetadata", {}).get("ColumnInfo", [])
            columns = [
                {"name": c.get("Name"), "type": c.get("Type", "string")}
                for c in meta
            ]

        page_rows = rs.get("Rows", [])
        if first_page and page_rows:
            # First row is the column header — skip it.
            page_rows = page_rows[1:]
            first_page = False

        for r in page_rows:
            row = [d.get("VarCharValue") for d in r.get("Data", [])]
            rows.append(row)
            if len(rows) >= MAX_RESULT_ROWS:
                return columns or [], rows

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return columns or [], rows
