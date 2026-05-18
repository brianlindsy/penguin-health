"""
Unit tests for nl_agent_tools — the catalog of tool handlers Claude calls
from inside run_agent_loop.

These tests don't hit Athena or Bedrock; the run_sql handler is exercised
through monkeypatching analytics_helpers.run_athena_query, and the
extract_from_rows handler takes an injected extractor callable.

The handlers share a RunCache for tool-to-tool row handoffs. Tests that
chain run_sql → extract_from_rows → aggregate build a single cache and
verify the run_id flow.
"""

import pytest

import analytics_helpers
import nl_agent_tools


CIRCLES = "circles-of-care"


# ----------------------------------------------------------------------
# Aggregate handler (legacy literal-rows entry point)
# ----------------------------------------------------------------------

class TestAggregateHandlerLegacy:
    """The bare `aggregate_handler` function operates on literal rows;
    callers that just want pure aggregation (no run_id flow) keep using
    this entry point."""

    def test_count_groups_and_sorts_desc_by_default(self):
        out = nl_agent_tools.aggregate_handler({
            "rows": [
                {"agency": "A"}, {"agency": "B"}, {"agency": "A"}, {"agency": "A"},
            ],
            "group_by": ["agency"],
            "agg": "count",
        })
        assert [c["name"] for c in out["columns"]] == ["agency", "count"]
        assert out["rows"] == [["A", 3], ["B", 1]]
        assert out["row_count"] == 2

    def test_count_with_key_asc(self):
        out = nl_agent_tools.aggregate_handler({
            "rows": [{"k": "z"}, {"k": "a"}, {"k": "a"}],
            "group_by": ["k"],
            "agg": "count",
            "order_by": "key_asc",
        })
        assert out["rows"] == [["a", 2], ["z", 1]]

    def test_sum_requires_sum_field(self):
        out = nl_agent_tools.aggregate_handler({
            "rows": [{"k": "a", "v": 1}],
            "group_by": ["k"],
            "agg": "sum",
        })
        assert out.get("code") == "BAD_INPUT"

    def test_sum_works_with_numeric_strings(self):
        out = nl_agent_tools.aggregate_handler({
            "rows": [
                {"k": "a", "v": "1.5"},
                {"k": "a", "v": "2.5"},
                {"k": "b", "v": "1"},
            ],
            "group_by": ["k"],
            "agg": "sum",
            "sum_field": "v",
            "order_by": "key_asc",
        })
        names = [c["name"] for c in out["columns"]]
        assert names == ["k", "sum_v"]
        assert out["rows"][0] == ["a", 4.0]
        assert out["rows"][1] == ["b", 1.0]

    def test_rejects_empty_group_by(self):
        out = nl_agent_tools.aggregate_handler({
            "rows": [{"k": "a"}],
            "group_by": [],
            "agg": "count",
        })
        assert out.get("code") == "BAD_INPUT"


# ----------------------------------------------------------------------
# run_sql handler — caches full rows, returns preview + run_id
# ----------------------------------------------------------------------

class TestRunSqlHandler:
    def test_returns_validation_error_without_calling_athena(self, mocker):
        called = []
        mocker.patch.object(
            analytics_helpers, "run_athena_query",
            side_effect=lambda *a, **k: called.append(("athena", a, k)) or {},
        )

        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_run_sql_handler(CIRCLES, cache=cache)
        out = handler({"sql": "DELETE FROM charts_circles_of_care"})

        assert out.get("code") == "FORBIDDEN_KEYWORD"
        assert called == []

    def test_returns_athena_error_dict_not_raise(self, mocker):
        mocker.patch.object(
            analytics_helpers, "run_athena_query",
            side_effect=analytics_helpers.AthenaQueryError("table missing"),
        )
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_run_sql_handler(CIRCLES, cache=cache)
        out = handler({"sql": "SELECT clientvisit_id FROM charts_circles_of_care LIMIT 1"})

        assert out["code"] == "ATHENA_ERROR"
        assert "table missing" in out["error"]

    def test_returns_run_id_and_preview_on_success(self, mocker):
        mocker.patch.object(
            analytics_helpers, "run_athena_query",
            return_value={
                "columns": [
                    {"name": "clientvisit_id", "type": "string"},
                    {"name": "answer", "type": "string"},
                ],
                "rows": [["abc", "narrative one"], ["def", "narrative two"]],
                "row_count": 2,
                "query_execution_id": "q-1",
            },
        )
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_run_sql_handler(CIRCLES, cache=cache)
        out = handler({
            "sql": "SELECT clientvisit_id, answer FROM charts_circles_of_care LIMIT 10",
        })

        assert out["row_count"] == 2
        assert out["run_id"].startswith("sql-")
        # Preview returned in positional shape (no copy needed from the
        # model — it just needs to see the data is sane).
        assert out["preview_rows"] == [["abc", "narrative one"], ["def", "narrative two"]]
        # Full rows in cache, in dict shape.
        cached = cache.get(out["run_id"])
        assert cached is not None
        assert cached["rows"][0] == {"clientvisit_id": "abc", "answer": "narrative one"}

    def test_spills_when_payload_exceeds_threshold(self, mocker):
        big_rows = [["x" * 200] for _ in range(200)]
        mocker.patch.object(
            analytics_helpers, "run_athena_query",
            return_value={
                "columns": [{"name": "c", "type": "string"}],
                "rows": big_rows,
                "row_count": len(big_rows),
                "query_execution_id": "q-1",
            },
        )

        spills = []

        def fake_spill(data, hint):
            spills.append((len(data), hint))
            return f"agent-io/test/{hint}"

        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_run_sql_handler(CIRCLES, cache=cache, spill=fake_spill)
        out = handler({"sql": "SELECT c FROM charts_circles_of_care LIMIT 1000"})

        # Preview is small even though the full result is large.
        assert len(out["preview_rows"]) == nl_agent_tools.PREVIEW_ROWS
        assert out["row_count"] == 200
        # Spill happened; S3 key is on the cache entry's meta.
        cached = cache.get(out["run_id"])
        assert cached["meta"]["s3_key"].startswith("agent-io/test/")
        assert spills and spills[0][1] == "run_sql.json"


# ----------------------------------------------------------------------
# extract_from_rows handler — from_run_id path + literal rows fallback
# ----------------------------------------------------------------------

class TestExtractFromRowsHandler:
    def test_from_run_id_pulls_rows_from_cache(self):
        cache = nl_agent_tools.RunCache()
        # Seed the cache with what run_sql would have stored.
        run_id = cache.put(
            "sql",
            columns=[{"name": "answer"}],
            rows=[{"answer": "alpha"}, {"answer": "beta"}],
            meta={},
        )

        def fake_extract(question, row):
            return (row.get("answer") or "").upper() or "unknown"

        handler = nl_agent_tools.make_extract_from_rows_handler(
            fake_extract, cache=cache, max_workers=2,
        )
        out = handler({"from_run_id": run_id, "question": "what?"})

        assert out["row_count"] == 2
        # Result also lives in cache under a new run_id, ready for aggregate.
        assert out["run_id"].startswith("extract-")
        cached = cache.get(out["run_id"])
        values = sorted(r["extracted_value"] for r in cached["rows"])
        assert values == ["ALPHA", "BETA"]

    def test_source_row_fields_preserved_in_extract_output(self):
        # This is the key contract that lets the agent avoid trying to
        # merge two run_ids: every source-row field flows through to the
        # extract result under the same column name.
        cache = nl_agent_tools.RunCache()
        run_id = cache.put(
            "sql",
            columns=[
                {"name": "clientvisit_id"},
                {"name": "answer"},
            ],
            rows=[
                {"clientvisit_id": "v1", "answer": "Referred by Melbourne PD"},
                {"clientvisit_id": "v2", "answer": "Walked in voluntarily"},
            ],
            meta={},
        )

        def fake_extract(question, row):
            text = (row.get("answer") or "").lower()
            return "Police" if "pd" in text else "unknown"

        handler = nl_agent_tools.make_extract_from_rows_handler(
            fake_extract, cache=cache, max_workers=2,
        )
        out = handler({"from_run_id": run_id, "question": "agency?"})

        cached = cache.get(out["run_id"])
        # Source columns appear in the cache's column list, between
        # row_index and extracted_value.
        col_names = [c["name"] for c in cached["columns"]]
        assert col_names == ["row_index", "clientvisit_id", "answer", "extracted_value"]
        # Each row dict carries the source fields.
        rows_by_visit = {r["clientvisit_id"]: r for r in cached["rows"]}
        assert rows_by_visit["v1"]["answer"] == "Referred by Melbourne PD"
        assert rows_by_visit["v1"]["extracted_value"] == "Police"
        assert rows_by_visit["v2"]["extracted_value"] == "unknown"

    def test_literal_rows_still_work_as_fallback(self):
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_extract_from_rows_handler(
            lambda q, r: (r.get("answer") or "").upper() or "unknown",
            cache=cache,
            max_workers=2,
        )
        out = handler({
            "rows": [{"answer": "alpha"}, {"answer": "beta"}, {"answer": ""}],
            "question": "what?",
        })
        assert out["row_count"] == 3
        cached = cache.get(out["run_id"])
        values = sorted(r["extracted_value"] for r in cached["rows"])
        assert values == ["ALPHA", "BETA", "unknown"]

    def test_rejects_unknown_run_id_with_helpful_error(self):
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_extract_from_rows_handler(
            lambda q, r: "x", cache=cache,
        )
        out = handler({"from_run_id": "sql-does-not-exist", "question": "q"})
        assert out["code"] == "UNKNOWN_RUN_ID"
        # Error must point the agent at the right concept.
        assert "run_sql" in out["error"]

    def test_rejects_empty_rows_when_no_run_id(self):
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_extract_from_rows_handler(
            lambda q, r: "x", cache=cache,
        )
        out = handler({"rows": [], "question": "what?"})
        assert out["code"] == "EMPTY_ROWS"
        # Error must steer the agent toward from_run_id.
        assert "from_run_id" in out["error"]

    def test_rejects_over_cap(self):
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_extract_from_rows_handler(
            lambda q, r: "x", cache=cache,
        )
        out = handler({
            "rows": [{} for _ in range(nl_agent_tools.MAX_EXTRACT_ROWS + 1)],
            "question": "q",
        })
        assert out["code"] == "TOO_MANY_ROWS"

    def test_per_row_exception_recorded_as_error_string(self):
        cache = nl_agent_tools.RunCache()

        def bad_extract(q, row):
            if row.get("fail"):
                raise ValueError("nope")
            return "ok"

        handler = nl_agent_tools.make_extract_from_rows_handler(
            bad_extract, cache=cache, max_workers=2,
        )
        out = handler({
            "rows": [{"fail": False}, {"fail": True}],
            "question": "q",
        })
        cached = cache.get(out["run_id"])
        results = {r["row_index"]: r["extracted_value"] for r in cached["rows"]}
        assert results[0] == "ok"
        assert results[1].startswith("error: ValueError")


# ----------------------------------------------------------------------
# aggregate handler factory — from_run_id path
# ----------------------------------------------------------------------

class TestAggregateHandlerWithCache:
    def test_aggregates_from_extract_run_id(self):
        cache = nl_agent_tools.RunCache()
        run_id = cache.put(
            "extract",
            columns=[
                {"name": "row_index", "type": "number"},
                {"name": "extracted_value", "type": "string"},
            ],
            rows=[
                {"row_index": 0, "extracted_value": "Police"},
                {"row_index": 1, "extracted_value": "Hospital"},
                {"row_index": 2, "extracted_value": "Police"},
                {"row_index": 3, "extracted_value": "Police"},
            ],
            meta={},
        )
        handler = nl_agent_tools.make_aggregate_handler(cache)
        out = handler({
            "from_run_id": run_id,
            "group_by": ["extracted_value"],
            "agg": "count",
        })
        assert out["run_id"].startswith("agg-")
        assert [c["name"] for c in out["columns"]] == ["extracted_value", "count"]
        assert out["rows"] == [["Police", 3], ["Hospital", 1]]

    def test_unknown_run_id_returns_error(self):
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_aggregate_handler(cache)
        out = handler({
            "from_run_id": "extract-missing",
            "group_by": ["x"],
            "agg": "count",
        })
        assert out["code"] == "UNKNOWN_RUN_ID"


# ----------------------------------------------------------------------
# select_columns handler — final-shape control + conditional values
# ----------------------------------------------------------------------

class TestSelectColumnsHandler:
    def _seed(self, cache):
        return cache.put(
            "extract",
            columns=[
                {"name": "row_index", "type": "number"},
                {"name": "clientvisit_id", "type": "string"},
                {"name": "answer", "type": "string"},
                {"name": "extracted_value", "type": "string"},
            ],
            rows=[
                {"row_index": 0, "clientvisit_id": "v1",
                 "answer": "Brought in by Melbourne PD", "extracted_value": "Police"},
                {"row_index": 1, "clientvisit_id": "v2",
                 "answer": "Voluntary walk-in", "extracted_value": "UNKNOWN"},
                {"row_index": 2, "clientvisit_id": "v3",
                 "answer": "Sent by HRMC", "extracted_value": "Hospital"},
            ],
            meta={},
        )

    def test_literal_pick_subset_of_columns(self):
        cache = nl_agent_tools.RunCache()
        run_id = self._seed(cache)
        handler = nl_agent_tools.make_select_columns_handler(cache)

        out = handler({
            "from_run_id": run_id,
            "columns": ["clientvisit_id", "extracted_value"],
        })
        assert [c["name"] for c in out["columns"]] == ["clientvisit_id", "extracted_value"]
        assert out["rows"] == [
            ["v1", "Police"],
            ["v2", "UNKNOWN"],
            ["v3", "Hospital"],
        ]
        assert out["run_id"].startswith("select-")

    def test_case_when_substitutes_source_column_value(self):
        # The flagship case: "answer_if_unknown" = answer when
        # extracted_value == UNKNOWN, else empty string.
        cache = nl_agent_tools.RunCache()
        run_id = self._seed(cache)
        handler = nl_agent_tools.make_select_columns_handler(cache)

        out = handler({
            "from_run_id": run_id,
            "columns": ["clientvisit_id", "extracted_value", "answer_if_unknown"],
            "computed": {
                "answer_if_unknown": {
                    "case_when": {
                        "field": "extracted_value", "op": "==", "value": "UNKNOWN",
                        "then": "answer", "else": "",
                    }
                }
            },
        })
        assert out["rows"] == [
            ["v1", "Police", ""],
            ["v2", "UNKNOWN", "Voluntary walk-in"],
            ["v3", "Hospital", ""],
        ]

    def test_case_when_literal_then_else(self):
        cache = nl_agent_tools.RunCache()
        run_id = self._seed(cache)
        handler = nl_agent_tools.make_select_columns_handler(cache)

        out = handler({
            "from_run_id": run_id,
            "columns": ["clientvisit_id", "status"],
            "computed": {
                "status": {
                    "case_when": {
                        "field": "extracted_value", "op": "!=", "value": "UNKNOWN",
                        "then": "RESOLVED", "else": "NEEDS_REVIEW",
                    }
                }
            },
        })
        assert out["rows"] == [
            ["v1", "RESOLVED"],
            ["v2", "NEEDS_REVIEW"],
            ["v3", "RESOLVED"],
        ]

    def test_unknown_run_id_returns_error(self):
        cache = nl_agent_tools.RunCache()
        handler = nl_agent_tools.make_select_columns_handler(cache)
        out = handler({
            "from_run_id": "extract-missing",
            "columns": ["x"],
        })
        assert out["code"] == "UNKNOWN_RUN_ID"

    def test_unknown_column_returns_error_listing_available(self):
        cache = nl_agent_tools.RunCache()
        run_id = self._seed(cache)
        handler = nl_agent_tools.make_select_columns_handler(cache)
        out = handler({
            "from_run_id": run_id,
            "columns": ["not_a_real_column"],
        })
        assert out["code"] == "UNKNOWN_COLUMN"
        # Error must list available source columns so the agent can self-correct.
        assert "clientvisit_id" in out["error"]

    def test_bad_op_in_case_when_rejected(self):
        cache = nl_agent_tools.RunCache()
        run_id = self._seed(cache)
        handler = nl_agent_tools.make_select_columns_handler(cache)
        out = handler({
            "from_run_id": run_id,
            "columns": ["x"],
            "computed": {
                "x": {"case_when": {
                    "field": "extracted_value", "op": ">=", "value": "X",
                    "then": "a", "else": "b",
                }}
            },
        })
        assert out["code"] == "BAD_COMPUTED"


# ----------------------------------------------------------------------
# concat_runs handler — UNION ALL across run_ids
# ----------------------------------------------------------------------

class TestConcatRunsHandler:
    def _seed(self, cache, run_kind, rows_per_run):
        ids = []
        for rows in rows_per_run:
            ids.append(cache.put(
                run_kind,
                columns=[
                    {"name": "row_index"},
                    {"name": "clientvisit_id"},
                    {"name": "extracted_value"},
                ],
                rows=rows,
                meta={},
            ))
        return ids

    def test_concatenates_matching_runs(self):
        cache = nl_agent_tools.RunCache()
        ids = self._seed(cache, "extract", [
            [{"row_index": 0, "clientvisit_id": "v1", "extracted_value": "Police"},
             {"row_index": 1, "clientvisit_id": "v2", "extracted_value": "Hospital"}],
            [{"row_index": 0, "clientvisit_id": "v3", "extracted_value": "Self"},
             {"row_index": 1, "clientvisit_id": "v4", "extracted_value": "Police"}],
        ])
        handler = nl_agent_tools.make_concat_runs_handler(cache)
        out = handler({"from_run_ids": ids})

        assert out["row_count"] == 4
        cached = cache.get(out["run_id"])
        # row_index re-numbered globally across the combined run.
        assert [r["row_index"] for r in cached["rows"]] == [0, 1, 2, 3]
        # All four clientvisit_ids present in order.
        assert [r["clientvisit_id"] for r in cached["rows"]] == ["v1", "v2", "v3", "v4"]

    def test_rejects_single_run_id(self):
        cache = nl_agent_tools.RunCache()
        run_id = cache.put("extract", columns=[{"name": "x"}], rows=[{"x": "a"}], meta={})
        handler = nl_agent_tools.make_concat_runs_handler(cache)
        out = handler({"from_run_ids": [run_id]})
        assert out["code"] == "BAD_INPUT"

    def test_rejects_unknown_run_id(self):
        cache = nl_agent_tools.RunCache()
        good = cache.put("extract", columns=[{"name": "x"}], rows=[{"x": "a"}], meta={})
        handler = nl_agent_tools.make_concat_runs_handler(cache)
        out = handler({"from_run_ids": [good, "extract-missing"]})
        assert out["code"] == "UNKNOWN_RUN_ID"

    def test_rejects_mismatched_columns(self):
        cache = nl_agent_tools.RunCache()
        a = cache.put("extract", columns=[
            {"name": "clientvisit_id"}, {"name": "extracted_value"},
        ], rows=[{"clientvisit_id": "v1", "extracted_value": "x"}], meta={})
        # Different column set — should fail with COLUMN_MISMATCH.
        b = cache.put("sql", columns=[
            {"name": "clientvisit_id"}, {"name": "answer"},
        ], rows=[{"clientvisit_id": "v2", "answer": "..."}], meta={})
        handler = nl_agent_tools.make_concat_runs_handler(cache)
        out = handler({"from_run_ids": [a, b]})
        assert out["code"] == "COLUMN_MISMATCH"
        # Error must surface the actual column sets so the agent can
        # see what's different.
        assert "extracted_value" in out["error"]
        assert "answer" in out["error"]


# ----------------------------------------------------------------------
# Cap test — extract_from_rows cap raised to 500
# ----------------------------------------------------------------------

class TestMaxExtractRows:
    def test_cap_is_500(self):
        # Pinned at 500 deliberately: high enough to handle realistic
        # narrative-extraction questions in one call, low enough to fit
        # in the worker's 10-min Lambda ceiling at 10-way concurrency.
        assert nl_agent_tools.MAX_EXTRACT_ROWS == 500


# ----------------------------------------------------------------------
# End-to-end run_id chain (no Bedrock, no Athena)
# ----------------------------------------------------------------------

class TestRunIdChain:
    """Smoke test: run_sql → extract_from_rows → aggregate chained by
    run_id, simulating the agent's happy path. Proves the worker-level
    contract: the model only needs to pass strings (run_ids) between
    tools, never copy row payloads."""

    def test_full_chain(self, mocker):
        mocker.patch.object(
            analytics_helpers, "run_athena_query",
            return_value={
                "columns": [
                    {"name": "clientvisit_id", "type": "string"},
                    {"name": "answer", "type": "string"},
                ],
                "rows": [
                    ["v1", "Referred by Melbourne PD"],
                    ["v2", "Walked in voluntarily"],
                    ["v3", "Sent by Melbourne PD"],
                    ["v4", "Hospital transfer from HRMC"],
                    ["v5", "Self-referred"],
                ],
                "row_count": 5,
                "query_execution_id": "q-1",
            },
        )

        def fake_extract(question, row):
            text = (row.get("answer") or "").lower()
            if "pd" in text or "police" in text:
                return "Police"
            if "hospital" in text or "hrmc" in text:
                return "Hospital"
            if "self" in text or "voluntar" in text:
                return "Self"
            return "unknown"

        cache = nl_agent_tools.RunCache()
        handlers = nl_agent_tools.make_tool_handlers(
            org_id=CIRCLES,
            extractor=fake_extract,
            cache=cache,
        )

        sql_out = handlers["run_sql"]({
            "sql": "SELECT clientvisit_id, answer FROM charts_circles_of_care LIMIT 5",
        })
        assert sql_out["row_count"] == 5

        extract_out = handlers["extract_from_rows"]({
            "from_run_id": sql_out["run_id"],
            "question": "What referred?",
        })
        assert extract_out["row_count"] == 5

        agg_out = handlers["aggregate"]({
            "from_run_id": extract_out["run_id"],
            "group_by": ["extracted_value"],
            "agg": "count",
        })
        # Expect Police=2, Self=2, Hospital=1, sorted count_desc.
        cols = [c["name"] for c in agg_out["columns"]]
        assert cols == ["extracted_value", "count"]
        counts = {row[0]: row[1] for row in agg_out["rows"]}
        assert counts == {"Police": 2, "Self": 2, "Hospital": 1}

    def test_chain_with_select_columns_and_conditional(self, mocker):
        # Pattern B from the system prompt: per-row extraction + a final
        # shape that conditionally surfaces the raw answer when the
        # extraction came back UNKNOWN.
        mocker.patch.object(
            analytics_helpers, "run_athena_query",
            return_value={
                "columns": [
                    {"name": "clientvisit_id", "type": "string"},
                    {"name": "answer", "type": "string"},
                ],
                "rows": [
                    ["v1", "Brought in by Melbourne PD"],
                    ["v2", "Voluntary walk-in"],
                    ["v3", "Sent by HRMC"],
                ],
                "row_count": 3,
                "query_execution_id": "q-2",
            },
        )

        def fake_extract(question, row):
            text = (row.get("answer") or "").lower()
            if "pd" in text:
                return "Police"
            if "hrmc" in text or "hospital" in text:
                return "Hospital"
            return "UNKNOWN"

        cache = nl_agent_tools.RunCache()
        handlers = nl_agent_tools.make_tool_handlers(
            org_id=CIRCLES, extractor=fake_extract, cache=cache,
        )

        sql_out = handlers["run_sql"]({
            "sql": "SELECT clientvisit_id, answer FROM charts_circles_of_care LIMIT 3",
        })
        extract_out = handlers["extract_from_rows"]({
            "from_run_id": sql_out["run_id"],
            "question": "agency?",
        })
        select_out = handlers["select_columns"]({
            "from_run_id": extract_out["run_id"],
            "columns": ["clientvisit_id", "extracted_value", "answer_if_unknown"],
            "computed": {
                "answer_if_unknown": {
                    "case_when": {
                        "field": "extracted_value", "op": "==", "value": "UNKNOWN",
                        "then": "answer", "else": "",
                    }
                }
            },
        })

        assert [c["name"] for c in select_out["columns"]] == [
            "clientvisit_id", "extracted_value", "answer_if_unknown",
        ]
        assert select_out["rows"] == [
            ["v1", "Police", ""],
            ["v2", "UNKNOWN", "Voluntary walk-in"],
            ["v3", "Hospital", ""],
        ]


# ----------------------------------------------------------------------
# inspect_schema (unchanged contract)
# ----------------------------------------------------------------------

class TestInspectSchemaHandler:
    def test_returns_chart_and_validation_tables(self):
        handler = nl_agent_tools.make_inspect_schema_handler(CIRCLES)
        out = handler({})
        suffix = analytics_helpers.table_suffix(CIRCLES)
        assert f"charts_{suffix}" in out["tables"]
        assert f"validation_results_{suffix}" in out["tables"]
        chart = out["tables"][f"charts_{suffix}"]
        answer = [c for c in chart["columns"] if c["name"] == "answer"]
        assert answer and answer[0].get("narrative") is True
        assert chart["partition_key"] == "ingest_date"
