"""
Unit tests for analytics_helpers — SQL validation and schema registry.

The validator is the safety boundary for the NL analytics endpoint: every
SQL string from Claude flows through validate_athena_sql before reaching
Athena. Rejections must be deterministic and the SELECT-only / table
allowlist guarantees are exercised here.
"""

import pytest


# conftest.py already puts lambda/api on sys.path.
from analytics_helpers import (
    ORG_SCHEMAS,
    SqlValidationError,
    allowed_tables,
    narrative_columns_for_org,
    table_suffix,
    validate_athena_sql,
)


CIRCLES = "circles-of-care"
DEMO = "demo"


class TestSchemaRegistry:
    def test_known_orgs_present(self):
        assert CIRCLES in ORG_SCHEMAS
        assert "catholic-charities-multi-org" in ORG_SCHEMAS
        assert DEMO in ORG_SCHEMAS

    def test_circles_chart_columns_include_known_fields(self):
        names = {c["name"] for c in ORG_SCHEMAS[CIRCLES]["chart_columns"]}
        assert {"fake_client_id", "service_date", "cptcode"} <= names

    def test_validation_columns_uniform_across_orgs(self):
        circles = [c["name"] for c in ORG_SCHEMAS[CIRCLES]["validation_columns"]]
        demo = [c["name"] for c in ORG_SCHEMAS[DEMO]["validation_columns"]]
        assert circles == demo
        assert "rule_id" in circles
        assert "status" in circles

    def test_narrative_flag_on_circles(self):
        cols = {c["name"]: c["narrative"] for c in ORG_SCHEMAS[CIRCLES]["chart_columns"]}
        assert cols["question_text"] is True
        assert cols["answer"] is True
        assert cols["service_date"] is False

    def test_narrative_columns_helper(self):
        assert "question_text" in narrative_columns_for_org(CIRCLES)
        assert "narrative_rich_28" in narrative_columns_for_org(DEMO)
        assert narrative_columns_for_org("nonexistent-org") == set()

    def test_status_column_documents_uppercase_enum(self):
        """The rules engine writes PASS|FAIL|SKIP|ERROR (uppercase). If this
        comment ever says 'passed'/'failed' Claude will silently match nothing.
        Audited against live data on 2026-05-11."""
        status_col = next(
            c for c in ORG_SCHEMAS[CIRCLES]["validation_columns"]
            if c["name"] == "status"
        )
        assert "notes" in status_col
        notes = status_col["notes"]
        assert "PASS" in notes and "FAIL" in notes and "SKIP" in notes
        # Don't let anyone re-introduce the lowercase docs.
        assert "passed" not in notes.lower().split() or "Do NOT" in notes

    def test_rule_type_column_documents_lowercase_enum(self):
        rule_type_col = next(
            c for c in ORG_SCHEMAS[CIRCLES]["validation_columns"]
            if c["name"] == "rule_type"
        )
        assert "notes" in rule_type_col
        assert "llm" in rule_type_col["notes"]
        assert "deterministic" in rule_type_col["notes"]

    def test_field_date_documents_space_separator(self):
        """field_date uses 'YYYY-MM-DD HH:MM:SS.SSS' — space, not 'T'.
        A wrong format string returns nulls silently."""
        col = next(
            c for c in ORG_SCHEMAS[CIRCLES]["validation_columns"]
            if c["name"] == "field_date"
        )
        assert "space" in col["notes"].lower() or "%Y-%m-%d %H" in col["notes"]

    def test_chart_partition_note_distinguishes_ingest_vs_service_date(self):
        """ingest_date is when the CSV was loaded, not the clinical date —
        a common mistake when generating queries."""
        note = ORG_SCHEMAS[CIRCLES]["chart_partition_notes"]
        assert "service_date" in note

    def test_circles_service_date_has_format_note(self):
        col = next(
            c for c in ORG_SCHEMAS[CIRCLES]["chart_columns"]
            if c["name"] == "service_date"
        )
        assert "notes" in col
        # Either reference the format mask or explicitly call out the space.
        assert "%Y-%m-%d %H" in col["notes"] or "SPACE" in col["notes"].upper()

    def test_table_suffix_dashes_to_underscores(self):
        assert table_suffix("circles-of-care") == "circles_of_care"
        assert table_suffix("demo") == "demo"

    def test_allowed_tables_set(self):
        assert allowed_tables(CIRCLES) == {
            "charts_circles_of_care",
            "validation_results_circles_of_care",
        }


class TestValidateAthenaSqlHappyPath:
    def test_simple_select_against_charts(self):
        out = validate_athena_sql(
            "SELECT count(*) FROM charts_circles_of_care",
            CIRCLES,
        )
        assert "LIMIT 1000" in out
        assert "charts_circles_of_care" in out

    def test_select_against_validation_results(self):
        out = validate_athena_sql(
            "SELECT rule_id, count(*) FROM validation_results_circles_of_care "
            "WHERE status = 'failed' GROUP BY rule_id LIMIT 50",
            CIRCLES,
        )
        # User-supplied LIMIT 50 should be preserved.
        assert "LIMIT 50" in out

    def test_explicit_limit_under_cap_preserved(self):
        out = validate_athena_sql(
            "SELECT * FROM charts_demo LIMIT 100",
            DEMO,
        )
        assert "LIMIT 100" in out
        assert "LIMIT 1000" not in out

    def test_limit_over_cap_rewritten(self):
        out = validate_athena_sql(
            "SELECT * FROM charts_demo LIMIT 5000",
            DEMO,
        )
        assert "LIMIT 1000" in out
        assert "LIMIT 5000" not in out

    def test_missing_limit_injected(self):
        out = validate_athena_sql(
            "SELECT service_date FROM charts_circles_of_care WHERE ingest_date = '2025-03-01'",
            CIRCLES,
        )
        assert out.rstrip().endswith("LIMIT 1000")

    def test_join_between_allowed_tables(self):
        sql = (
            "SELECT v.rule_id, c.program_desc FROM "
            "validation_results_circles_of_care v JOIN charts_circles_of_care c "
            "ON v.document_id = c.clientvisit_id LIMIT 100"
        )
        out = validate_athena_sql(sql, CIRCLES)
        assert "validation_results_circles_of_care" in out
        assert "charts_circles_of_care" in out

    def test_quoted_table_identifier_accepted(self):
        out = validate_athena_sql(
            'SELECT 1 FROM "charts_circles_of_care" LIMIT 5',
            CIRCLES,
        )
        assert "LIMIT 5" in out

    def test_db_qualified_table_accepted(self):
        out = validate_athena_sql(
            "SELECT 1 FROM penguin_health_analytics.charts_demo LIMIT 5",
            DEMO,
        )
        assert "LIMIT 5" in out

    def test_block_comment_stripped_safely(self):
        out = validate_athena_sql(
            "SELECT 1 /* harmless comment */ FROM charts_demo LIMIT 5",
            DEMO,
        )
        assert "LIMIT 5" in out


class TestValidateAthenaSqlRejections:
    def test_insert_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("INSERT INTO charts_demo VALUES (1)", DEMO)
        assert exc.value.code == "FORBIDDEN_KEYWORD"

    def test_update_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("UPDATE charts_demo SET x = 1", DEMO)
        assert exc.value.code == "FORBIDDEN_KEYWORD"

    def test_delete_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("DELETE FROM charts_demo", DEMO)
        assert exc.value.code == "FORBIDDEN_KEYWORD"

    def test_drop_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("DROP TABLE charts_demo", DEMO)
        assert exc.value.code == "FORBIDDEN_KEYWORD"

    def test_multiple_statements_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql(
                "SELECT * FROM charts_demo; SELECT * FROM charts_demo",
                DEMO,
            )
        assert exc.value.code == "MULTIPLE_STATEMENTS"

    def test_select_then_destructive_rejected_via_keyword(self):
        # Even if multi-statement parsing didn't catch it, the keyword scan should.
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql(
                "SELECT 1 FROM charts_demo; DELETE FROM charts_demo",
                DEMO,
            )
        assert exc.value.code == "FORBIDDEN_KEYWORD"

    def test_disallowed_table(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("SELECT * FROM auth_users", DEMO)
        assert exc.value.code == "DISALLOWED_TABLE"

    def test_disallowed_table_via_join(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql(
                "SELECT * FROM charts_demo c JOIN secrets s ON c.id = s.id",
                DEMO,
            )
        assert exc.value.code == "DISALLOWED_TABLE"

    def test_other_orgs_table_rejected(self):
        # charts_demo from a circles-of-care request must be rejected —
        # this is the cross-tenant guard.
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("SELECT * FROM charts_demo", CIRCLES)
        assert exc.value.code == "DISALLOWED_TABLE"

    def test_empty_sql_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("", DEMO)
        assert exc.value.code == "EMPTY"

    def test_whitespace_only_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql("   \n  ", DEMO)
        assert exc.value.code == "EMPTY"

    def test_create_table_rejected(self):
        with pytest.raises(SqlValidationError) as exc:
            validate_athena_sql(
                "CREATE TABLE foo AS SELECT * FROM charts_demo",
                DEMO,
            )
        assert exc.value.code == "FORBIDDEN_KEYWORD"


class TestNLSystemPrompt:
    """Regression tests for the Claude system prompt.

    These guard against silent re-introduction of the wrong value
    enums or date formats. The prompt is what tells Claude how to
    write SQL — a wrong line here makes every query return 0 rows
    against real data.
    """

    def test_prompt_surfaces_status_uppercase_enum(self):
        from admin_api import _build_nl_system_prompt
        prompt = _build_nl_system_prompt(CIRCLES)
        assert "PASS" in prompt
        assert "FAIL" in prompt
        assert "SKIP" in prompt
        # The old lying line "status is one of: passed, failed, error"
        # must NOT reappear.
        assert "passed, failed, error" not in prompt

    def test_prompt_surfaces_rule_type_lowercase_enum(self):
        from admin_api import _build_nl_system_prompt
        prompt = _build_nl_system_prompt(CIRCLES)
        assert "llm" in prompt
        assert "deterministic" in prompt

    def test_prompt_surfaces_field_date_space_format(self):
        from admin_api import _build_nl_system_prompt
        prompt = _build_nl_system_prompt(CIRCLES)
        # The format mask with space separator must appear so Claude
        # doesn't generate date_parse(... '%Y-%m-%dT%H:%i:%s').
        assert "%Y-%m-%d %H" in prompt or "SPACE separator" in prompt

    def test_prompt_distinguishes_ingest_vs_service_date(self):
        from admin_api import _build_nl_system_prompt
        prompt = _build_nl_system_prompt(CIRCLES)
        assert "ingest" in prompt.lower()
        assert "service_date" in prompt

    def test_prompt_lists_only_allowed_tables(self):
        from admin_api import _build_nl_system_prompt
        prompt = _build_nl_system_prompt(CIRCLES)
        assert "charts_circles_of_care" in prompt
        assert "validation_results_circles_of_care" in prompt
        # No other org's tables.
        assert "charts_demo" not in prompt
        assert "charts_catholic" not in prompt


class TestValidateAthenaSqlDeepScope:
    def test_deep_scope_caps_at_200(self):
        from analytics_helpers import MAX_DEEP_SCOPE_LIMIT

        out = validate_athena_sql(
            "SELECT * FROM charts_demo LIMIT 500",
            DEMO,
            max_limit=MAX_DEEP_SCOPE_LIMIT,
        )
        assert "LIMIT 200" in out

    def test_deep_scope_preserves_smaller_limit(self):
        out = validate_athena_sql(
            "SELECT * FROM charts_demo LIMIT 50",
            DEMO,
            max_limit=200,
        )
        assert "LIMIT 50" in out
