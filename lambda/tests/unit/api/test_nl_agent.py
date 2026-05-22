"""
Unit tests for nl_agent.run_agent_loop.

The loop is intentionally infrastructure-free, so these tests inject a
fake invoke_fn that returns scripted Bedrock-shaped responses and verify:
  - One tool call + finalize works.
  - Multi-step chains thread tool_use_id → tool_result correctly.
  - Hitting max_steps without finalize raises AGENT_DID_NOT_CONVERGE.
  - A tool handler raising surfaces as a tool_result error rather than
    crashing the loop.
  - Calling an unknown tool surfaces as is_error tool_result.
  - Claude returning text without tool_use raises NO_FINAL_ANSWER.
"""

import pytest

import nl_agent_tools
from nl_agent import AgentError, run_agent_loop, FINALIZE_TOOL


def _tool_use_block(tu_id: str, name: str, input_):
    return {"type": "tool_use", "id": tu_id, "name": name, "input": input_}


def _make_invoke(scripted_responses):
    """Return an invoke_fn that yields scripted Bedrock responses in order.

    Each scripted response is a dict like
        {"stop_reason": "tool_use", "content": [...]}.

    Captures a deep copy of messages on each call so post-call mutations
    in the loop don't bleed back into assertions.
    """
    import copy
    calls = []

    def invoke(messages, tools, system):
        idx = len(calls)
        calls.append({
            "messages": copy.deepcopy(messages),
            "tools": tools,
            "system": system,
        })
        if idx >= len(scripted_responses):
            raise AssertionError(
                f"invoke_fn called {idx + 1}x but only {len(scripted_responses)} "
                f"responses were scripted."
            )
        return scripted_responses[idx]

    invoke.calls = calls
    return invoke


class TestRunAgentLoopHappyPaths:
    def test_single_tool_call_then_finalize(self):
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", "run_sql", {"sql": "SELECT 1"})],
            },
            {
                "stop_reason": "tool_use",
                "content": [
                    _tool_use_block("t2", FINALIZE_TOOL, {
                        "columns": [{"name": "n", "type": "number"}],
                        "rows": [[1]],
                        "viz_type": "table",
                        "explanation": "ok",
                    }),
                ],
            },
        ])
        handlers = {
            "run_sql": lambda inp: {"columns": [{"name": "n"}], "rows": [[1]], "row_count": 1},
        }

        result = run_agent_loop(
            system="sys",
            initial_user="how many?",
            tools=[],
            tool_handlers=handlers,
            invoke_fn=invoke,
        )

        assert result["final"]["rows"] == [[1]]
        assert result["steps"] == 2
        # Trace records both calls.
        assert [t["tool"] for t in result["trace"]] == ["run_sql", FINALIZE_TOOL]
        assert result["trace"][0]["status"] == "ok"
        assert result["trace"][1]["status"] == "finalize"

    def test_messages_thread_tool_use_id_to_tool_result(self):
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("call-A", "run_sql", {"sql": "SELECT 1"})],
            },
            {
                "stop_reason": "tool_use",
                "content": [
                    _tool_use_block("call-B", FINALIZE_TOOL, {
                        "columns": [], "rows": [],
                    }),
                ],
            },
        ])
        handlers = {"run_sql": lambda inp: {"row_count": 1}}

        run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers=handlers,
            invoke_fn=invoke,
        )

        # The second invoke should have seen a user turn containing a
        # tool_result block keyed to the first tool_use_id.
        second_messages = invoke.calls[1]["messages"]
        # [user, assistant, user]
        assert second_messages[-1]["role"] == "user"
        results = second_messages[-1]["content"]
        assert isinstance(results, list)
        assert results[0]["type"] == "tool_result"
        assert results[0]["tool_use_id"] == "call-A"

    def test_on_step_callback_fires_pre_and_post(self):
        # on_step fires twice per turn: once before Bedrock invoke
        # (pre-step, no post_step flag) and once after tool calls resolve
        # (post-step, post_step=True). The post-step fire carries the
        # trace-so-far so the worker can persist it.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        seen = []
        run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers={},
            invoke_fn=invoke,
            on_step=lambda info: seen.append(info),
        )
        # Two fires: pre-step (no post_step), then post-step.
        assert len(seen) == 2
        assert seen[0]["step"] == 1
        assert seen[0].get("post_step") is None or seen[0].get("post_step") is False
        assert seen[1]["step"] == 1
        assert seen[1]["post_step"] is True
        # Post-step fire carries the trace.
        assert len(seen[1]["trace"]) == 1
        assert seen[1]["trace"][0]["tool"] == FINALIZE_TOOL

    def test_on_step_trace_grows_across_steps(self):
        # On a multi-step run, the post-step trace should accumulate.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", "run_sql", {"sql": "SELECT 1"})],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t2", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        post_step_traces = []
        run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers={"run_sql": lambda inp: {"row_count": 0}},
            invoke_fn=invoke,
            on_step=lambda info: (
                post_step_traces.append(len(info["trace"]))
                if info.get("post_step") else None
            ),
        )
        # First post-step: trace has 1 entry (run_sql). Second post-step:
        # trace has 2 entries (run_sql + finalize).
        assert post_step_traces == [1, 2]


class TestRunAgentLoopFailures:
    def test_max_steps_without_finalize_raises(self):
        # Every response asks for the same tool, never finalize.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block(f"t{i}", "run_sql", {"sql": "SELECT 1"})],
            }
            for i in range(5)
        ])
        handlers = {"run_sql": lambda inp: {"row_count": 0}}

        with pytest.raises(AgentError) as excinfo:
            run_agent_loop(
                system="sys",
                initial_user="q",
                tools=[],
                tool_handlers=handlers,
                invoke_fn=invoke,
                max_steps=3,
            )
        assert excinfo.value.code == "AGENT_DID_NOT_CONVERGE"
        assert len(excinfo.value.trace) == 3

    def test_text_only_response_raises_no_final_answer(self):
        invoke = _make_invoke([
            {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "I don't know."}],
            },
        ])
        with pytest.raises(AgentError) as excinfo:
            run_agent_loop(
                system="sys",
                initial_user="q",
                tools=[],
                tool_handlers={},
                invoke_fn=invoke,
            )
        assert excinfo.value.code == "NO_FINAL_ANSWER"

    def test_handler_exception_becomes_tool_result_error(self):
        # First turn: run_sql; handler raises. Second turn: finalize.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", "run_sql", {"sql": "BAD"})],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t2", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])

        def bad_handler(inp):
            raise RuntimeError("boom")

        result = run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers={"run_sql": bad_handler},
            invoke_fn=invoke,
        )
        # Loop completes; trace records the error.
        trace = result["trace"]
        assert trace[0]["status"] == "error"
        assert "RuntimeError" in trace[0]["error"]
        # Second invoke saw an is_error tool_result.
        results = invoke.calls[1]["messages"][-1]["content"]
        assert results[0].get("is_error") is True

    def test_unknown_tool_name_surfaces_as_error_result(self):
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", "made_up_tool", {})],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t2", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        result = run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers={},  # No handlers registered.
            invoke_fn=invoke,
        )
        assert result["trace"][0]["status"] == "error"
        assert "unknown tool" in result["trace"][0]["error"]
        results = invoke.calls[1]["messages"][-1]["content"]
        assert results[0].get("is_error") is True


class TestRunAgentLoopAssistantText:
    def test_text_block_captured_into_trace_entry(self):
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [
                    {"type": "text", "text": "I'll start by counting visits."},
                    _tool_use_block("t1", "run_sql", {"sql": "SELECT 1"}),
                ],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t2", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        result = run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers={"run_sql": lambda inp: {"row_count": 1}},
            invoke_fn=invoke,
        )
        first = result["trace"][0]
        assert first["assistant_text"] == "I'll start by counting visits."

    def test_text_attached_to_all_tool_calls_in_same_turn(self):
        # When Claude emits text + multiple tool_use blocks in one turn,
        # every trace entry from that turn should carry the same text.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [
                    {"type": "text", "text": "Fanning out two probes."},
                    _tool_use_block("a", "tool_a", {}),
                    _tool_use_block("b", "tool_b", {}),
                ],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("c", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        result = run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers={
                "tool_a": lambda inp: {},
                "tool_b": lambda inp: {},
            },
            invoke_fn=invoke,
        )
        turn_one = [t for t in result["trace"] if t["step"] == 1]
        assert len(turn_one) == 2
        assert all(t["assistant_text"] == "Fanning out two probes." for t in turn_one)

    def test_no_text_block_means_no_assistant_text_key(self):
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        result = run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers={},
            invoke_fn=invoke,
        )
        # Finalize has no preceding text — the key should be absent, not
        # an empty string (keeps trace entries lean in DynamoDB).
        assert "assistant_text" not in result["trace"][0]


class TestRunAgentLoopFinalizeHydration:
    """finalize(from_run_id=...) hydrates columns + rows from the cache so
    the agent doesn't have to copy large row payloads into the tool input.
    Without this, Bedrock truncates and the user gets an empty answer."""

    def test_from_run_id_hydrates_columns_and_rows_from_cache(self):
        cache = nl_agent_tools.RunCache()
        run_id = cache.put(
            "select",
            columns=[
                {"name": "consumer_name_2", "type": "string"},
                {"name": "extracted_value", "type": "string"},
            ],
            rows=[
                {"consumer_name_2": "v1", "extracted_value": "Police"},
                {"consumer_name_2": "v2", "extracted_value": "Hospital"},
            ],
        )
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "from_run_id": run_id,
                    "viz_type": "table",
                    "explanation": "Listing consumers and their referral source",
                })],
            },
        ])
        result = run_agent_loop(
            system="sys", initial_user="q",
            tools=[], tool_handlers={},
            invoke_fn=invoke,
            run_cache=cache,
        )
        final = result["final"]
        # Columns come from cache; viz_type/explanation preserved.
        assert [c["name"] for c in final["columns"]] == [
            "consumer_name_2", "extracted_value",
        ]
        # Rows hydrated into POSITIONAL shape parallel to columns.
        assert final["rows"] == [["v1", "Police"], ["v2", "Hospital"]]
        assert final["viz_type"] == "table"
        assert "referral" in final["explanation"]

    def test_finalize_trace_summary_reflects_hydrated_row_count(self):
        # The model passed from_run_id with no literal rows; the trace
        # entry must show the hydrated count, not 0. Otherwise the trace
        # UI says "0 rows" when the user actually got 50.
        cache = nl_agent_tools.RunCache()
        run_id = cache.put(
            "select",
            columns=[{"name": "x", "type": "string"}],
            rows=[{"x": str(i)} for i in range(50)],
        )
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "from_run_id": run_id,
                })],
            },
        ])
        result = run_agent_loop(
            system="sys", initial_user="q",
            tools=[], tool_handlers={},
            invoke_fn=invoke,
            run_cache=cache,
        )
        finalize_entry = result["trace"][0]
        assert finalize_entry["status"] == "finalize"
        assert "50 rows" in finalize_entry["input_summary"]

    def test_literal_columns_and_rows_still_work(self):
        # Backward compat: the agent can still pass columns + rows
        # literally when the data is small / ad-hoc.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "columns": [{"name": "n", "type": "number"}],
                    "rows": [[42]],
                    "viz_type": "table",
                })],
            },
        ])
        result = run_agent_loop(
            system="sys", initial_user="q",
            tools=[], tool_handlers={},
            invoke_fn=invoke,
            run_cache=nl_agent_tools.RunCache(),
        )
        assert result["final"]["rows"] == [[42]]

    def test_literal_overrides_from_run_id_when_both_passed(self):
        # Escape hatch: when the agent provides BOTH literal columns+rows
        # AND a from_run_id, the literals win. This lets the agent rename
        # / re-shape the final output without us inventing extra knobs.
        cache = nl_agent_tools.RunCache()
        run_id = cache.put(
            "select",
            columns=[{"name": "original", "type": "string"}],
            rows=[{"original": "should-not-appear"}],
        )
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "from_run_id": run_id,
                    "columns": [{"name": "renamed", "type": "string"}],
                    "rows": [["overridden"]],
                })],
            },
        ])
        result = run_agent_loop(
            system="sys", initial_user="q",
            tools=[], tool_handlers={},
            invoke_fn=invoke,
            run_cache=cache,
        )
        assert [c["name"] for c in result["final"]["columns"]] == ["renamed"]
        assert result["final"]["rows"] == [["overridden"]]

    def test_unknown_run_id_surfaces_error_and_loop_continues(self):
        # The exact failure mode this whole feature was added to prevent:
        # an unknown run_id must NOT terminate the loop with empty rows.
        # It must come back as a tool_result error so the agent can fix
        # it and try again.
        cache = nl_agent_tools.RunCache()
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "from_run_id": "select-does-not-exist",
                })],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t2", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        result = run_agent_loop(
            system="sys", initial_user="q",
            tools=[], tool_handlers={},
            invoke_fn=invoke,
            run_cache=cache,
        )
        # First turn produced an error trace entry, NOT a finalize.
        assert result["trace"][0]["status"] == "error"
        assert "Unknown run_id" in result["trace"][0]["error"]
        # Loop kept going; the second finalize is what succeeded.
        assert result["trace"][1]["status"] == "finalize"
        # And the error came back as a tool_result error to the model.
        first_tool_result = invoke.calls[1]["messages"][-1]["content"][0]
        assert first_tool_result.get("is_error") is True

    def test_no_cache_with_from_run_id_surfaces_clear_error(self):
        # If a caller forgets to pass run_cache but the agent uses
        # from_run_id, surface a clear error rather than silently
        # finalizing with empty rows.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t1", FINALIZE_TOOL, {
                    "from_run_id": "select-001",
                })],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("t2", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        result = run_agent_loop(
            system="sys", initial_user="q",
            tools=[], tool_handlers={},
            invoke_fn=invoke,
            # run_cache intentionally omitted
        )
        assert result["trace"][0]["status"] == "error"
        assert "run_cache" in result["trace"][0]["error"]


class TestRunAgentLoopMultipleToolUsesPerTurn:
    def test_two_tool_uses_in_one_assistant_turn(self):
        # Claude can emit multiple tool_use blocks; loop must service all of
        # them before re-invoking.
        invoke = _make_invoke([
            {
                "stop_reason": "tool_use",
                "content": [
                    _tool_use_block("a", "tool_a", {}),
                    _tool_use_block("b", "tool_b", {}),
                ],
            },
            {
                "stop_reason": "tool_use",
                "content": [_tool_use_block("c", FINALIZE_TOOL, {
                    "columns": [], "rows": [],
                })],
            },
        ])
        handlers = {
            "tool_a": lambda inp: {"a": 1},
            "tool_b": lambda inp: {"b": 2},
        }
        result = run_agent_loop(
            system="sys",
            initial_user="q",
            tools=[],
            tool_handlers=handlers,
            invoke_fn=invoke,
        )
        # Both calls recorded in trace.
        tools_called = [t["tool"] for t in result["trace"]]
        assert "tool_a" in tools_called and "tool_b" in tools_called
        # Second invoke saw both tool_results.
        results = invoke.calls[1]["messages"][-1]["content"]
        ids = {r["tool_use_id"] for r in results}
        assert ids == {"a", "b"}
