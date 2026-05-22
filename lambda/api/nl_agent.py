"""
NL Explorer agent loop: Claude orchestrates multi-step analytical work
by calling registered tools (run_sql, inspect_schema, extract_from_rows,
aggregate, finalize) instead of producing a single canned SQL or extraction
plan up front.

The loop itself lives here and is intentionally infrastructure-free — no
boto3, no Athena, no DynamoDB. Tools are passed in as plain Python
callables so the loop is unit-testable with mocked tool handlers and a
mocked invoke fn.

Tool-use contract on Bedrock (Messages API, Sonnet 4.5):
  - Request body includes a `tools` list. Each tool is
    {"name", "description", "input_schema"}.
  - Claude responds with content blocks. When it wants to call a tool,
    a `tool_use` block appears alongside any `text` blocks, and
    stop_reason == "tool_use".
  - We append the assistant message verbatim, then append a user message
    whose content is a list of `tool_result` blocks (one per tool_use,
    matched by tool_use_id), and re-invoke.
  - Loop exits when stop_reason != "tool_use" OR the agent calls the
    sentinel `finalize` tool (which terminates the loop and returns its
    input as the final result).
"""

from typing import Callable, Optional


class AgentError(Exception):
    """Raised when the agent loop cannot continue."""

    def __init__(self, code: str, message: str, trace: Optional[list] = None):
        self.code = code
        self.message = message
        self.trace = trace or []
        super().__init__(f"{code}: {message}")


# Sentinel tool name. When the agent calls this, the loop captures its
# input as the final result and exits without re-invoking Claude.
FINALIZE_TOOL = "finalize"


def _coerce_text(value) -> str:
    """tool_result.content must be a string or a list of blocks. Coerce
    arbitrary tool outputs (dicts, lists, primitives) to a JSON string so
    Claude always gets readable structured data back."""
    import json
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def run_agent_loop(
    *,
    system: str,
    initial_user: str,
    tools: list,
    tool_handlers: dict,
    invoke_fn: Callable[[list, list, str], dict],
    max_steps: int = 15,
    on_step: Optional[Callable[[dict], None]] = None,
    run_cache=None,
) -> dict:
    """
    Drive Claude through tool calls until it calls `finalize` (success) or
    hits `max_steps` (raises AgentError).

    Args:
      system: System prompt.
      initial_user: First user message (the question).
      tools: Bedrock tool schemas (passed straight through in the request body).
      tool_handlers: dict mapping tool name -> callable(input_dict) -> any.
        The `finalize` handler, if present, is NOT called — the loop captures
        the tool_use input directly. Other handlers' return values are
        forwarded to Claude as tool_result content.
      invoke_fn: callable(messages, tools, system) -> response_body dict.
        Lets tests inject a fake Bedrock call.
      max_steps: hard cap on tool-use turns. Each iteration is one Bedrock
        invocation. Counts toward the cap even if the model returns text
        instead of tool calls.
      on_step: optional callback fired before each Bedrock invocation with
        a step descriptor: {"step": int, "label": str, "tool_calls": [...]}.
        Used by the worker to push progress to DynamoDB.
      run_cache: optional nl_agent_tools.RunCache shared with the tool
        handlers. When provided, `finalize(from_run_id=...)` is hydrated
        from the cache so the agent doesn't have to copy large row
        payloads into the tool input (which silently truncates).

    Returns:
      {
        "final": <input dict the agent passed to finalize>,
        "trace": [...],          # one entry per tool call
        "messages": [...],       # full message history for debugging
        "steps": int,            # number of Bedrock invocations made
      }

    Raises AgentError on:
      - Bedrock returning a non-tool-use, non-end_turn stop_reason without
        calling finalize (NO_FINAL_ANSWER)
      - Hitting max_steps without finalize (AGENT_DID_NOT_CONVERGE)
      - A tool handler raising (TOOL_HANDLER_FAILED). The exception is
        captured in the trace so callers can show what went wrong.
    """
    messages: list = [{"role": "user", "content": initial_user}]
    trace: list = []
    final_input = None
    steps = 0

    for step in range(1, max_steps + 1):
        steps = step
        if on_step is not None:
            # Fired before each Bedrock invocation. The `trace` list grows
            # as the loop runs; passing the live reference lets the worker
            # persist the trace-so-far to DynamoDB on every step, so a UI
            # poll mid-run can see the agent's history (and a crashed
            # worker still leaves a partial record).
            on_step({
                "step": step,
                "label": f"Step {step}: thinking",
                "tool_calls": [],
                "trace": trace,
            })

        response = invoke_fn(messages, tools, system)
        content = response.get("content") or []
        stop_reason = response.get("stop_reason")

        # Always append the assistant turn verbatim so the next request
        # carries the tool_use blocks Claude is asking us to honor.
        messages.append({"role": "assistant", "content": content})

        tool_uses = [b for b in content if isinstance(b, dict) and b.get("type") == "tool_use"]
        # Capture Claude's interleaved text commentary so the trace can
        # show "what the agent was thinking" alongside each tool call.
        # All text blocks from this turn get attached to every tool call
        # from this turn — they share a single assistant message.
        assistant_text = " ".join(
            b.get("text", "").strip()
            for b in content
            if isinstance(b, dict) and b.get("type") == "text" and b.get("text")
        ).strip()

        if not tool_uses:
            # Claude returned a final text response without calling
            # finalize. That's a misbehavior for our contract — finalize
            # is mandatory because we need a structured result. Bail.
            text_blocks = [b.get("text", "") for b in content
                           if isinstance(b, dict) and b.get("type") == "text"]
            raise AgentError(
                "NO_FINAL_ANSWER",
                "Agent ended without calling finalize. Last assistant text: "
                + " ".join(text_blocks).strip()[:500],
                trace=trace,
            )

        # Service every tool_use block from this turn before re-invoking.
        # Claude may emit multiple tool_use blocks in a single message.
        tool_results = []
        finalize_input = None
        for tu in tool_uses:
            name = tu.get("name")
            tu_id = tu.get("id")
            tu_input = tu.get("input") or {}

            trace_entry = {
                "step": step,
                "tool": name,
                "input_summary": _summarize_input(name, tu_input),
            }
            if assistant_text:
                trace_entry["assistant_text"] = assistant_text

            if name == FINALIZE_TOOL:
                hydrated, hydrate_err = _resolve_finalize_input(tu_input, run_cache)
                if hydrate_err is not None:
                    # The agent referenced a run_id we can't find. Don't
                    # capture as finalize — surface the error as a
                    # tool_result so the loop continues and the agent can
                    # retry with a real run_id. This is the failure mode
                    # the from_run_id support was added to prevent: a
                    # silently-empty final answer.
                    trace_entry["status"] = "error"
                    trace_entry["error"] = hydrate_err
                    trace.append(trace_entry)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tu_id,
                        "is_error": True,
                        "content": hydrate_err,
                    })
                    continue
                # Capture the structured final answer; stop after this turn.
                finalize_input = hydrated
                trace_entry["status"] = "finalize"
                # Re-summarize after hydration so the trace shows the real
                # row count rather than the literal-zero the model may
                # have sent.
                trace_entry["input_summary"] = _summarize_input(name, hydrated)
                trace.append(trace_entry)
                # Per Bedrock contract, every tool_use needs a matching
                # tool_result in the same user turn, even finalize. We send
                # a minimal ack so the message history stays valid.
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "content": "ok",
                })
                continue

            handler = tool_handlers.get(name)
            if handler is None:
                trace_entry["status"] = "error"
                trace_entry["error"] = f"unknown tool: {name}"
                trace.append(trace_entry)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "is_error": True,
                    "content": f"Unknown tool: {name}",
                })
                continue

            try:
                output = handler(tu_input)
                trace_entry["status"] = "ok"
                trace_entry["output_summary"] = _summarize_output(name, output)
                trace.append(trace_entry)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "content": _coerce_text(output),
                })
            except Exception as e:
                # Surface the error to Claude as a tool_result so it can
                # retry with a narrower query instead of crashing the job.
                err = f"{type(e).__name__}: {e}"
                trace_entry["status"] = "error"
                trace_entry["error"] = err
                trace.append(trace_entry)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "is_error": True,
                    "content": err,
                })

        messages.append({"role": "user", "content": tool_results})

        # Post-tool flush: every tool call from this step is now in the
        # trace. Fire on_step again so the worker can persist trace + a
        # fresh label like "Step 3 done; 2 tool calls" before we loop
        # back into the next Bedrock invocation.
        if on_step is not None:
            last_tool = trace[-1]["tool"] if trace else None
            on_step({
                "step": step,
                "label": (
                    f"Step {step} done"
                    + (f": {last_tool}" if last_tool else "")
                ),
                "tool_calls": [t["tool"] for t in trace if t["step"] == step],
                "trace": trace,
                "post_step": True,
            })

        if finalize_input is not None:
            final_input = finalize_input
            break

    if final_input is None:
        raise AgentError(
            "AGENT_DID_NOT_CONVERGE",
            f"Agent ran {steps} steps without calling finalize. "
            f"Try a tighter question.",
            trace=trace,
        )

    return {
        "final": final_input,
        "trace": trace,
        "messages": messages,
        "steps": steps,
    }


def _resolve_finalize_input(tu_input: dict, run_cache) -> tuple:
    """Hydrate a finalize tool_use input from the RunCache when the agent
    passed `from_run_id`.

    The agent is bad at copying large row payloads into a literal `rows`
    field — Bedrock may truncate the tool input, silently producing a
    finalize with `rows=[]`. Letting the agent reference a run_id avoids
    the copy entirely.

    Resolution order:
      1. If literal `columns` and `rows` are both present, use them as-is
         (escape hatch for tiny ad-hoc answers).
      2. Else if `from_run_id` is set, look it up in run_cache, project
         dict-keyed rows into positional rows parallel to the cached
         columns, and use those.
      3. Otherwise return the input unchanged (caller may produce
         columns=[], rows=[]).

    Returns (resolved_input_dict, error_str_or_None). When error_str is
    not None, the caller should surface it as a tool_result and NOT
    capture finalize_input.
    """
    if not isinstance(tu_input, dict):
        return tu_input, None

    literal_cols = tu_input.get("columns")
    literal_rows = tu_input.get("rows")
    from_run_id = tu_input.get("from_run_id")

    # Honor explicit literal columns+rows even if from_run_id is also set;
    # this lets the agent override shape (rename, reorder, subset) when
    # it really needs to.
    if literal_cols and literal_rows is not None:
        return tu_input, None

    if not from_run_id:
        # No run_id and no literal data — pass through. The downstream
        # consumer will see empty columns/rows; that's the agent's bug,
        # not the loop's.
        return tu_input, None

    if run_cache is None:
        return tu_input, (
            f"finalize was called with from_run_id={from_run_id!r} but "
            f"the agent loop was not given a run_cache. Pass literal "
            f"`columns` and `rows` instead."
        )

    run = run_cache.get(from_run_id)
    if run is None:
        return tu_input, (
            f"Unknown run_id {from_run_id!r}. Use the run_id returned by "
            f"a previous tool (run_sql, extract_from_rows, aggregate, "
            f"select_columns, filter_rows, concat_runs)."
        )

    cached_cols = run.get("columns") or []
    cached_rows = run.get("rows") or []
    col_names = [
        (c.get("name") if isinstance(c, dict) else str(c))
        for c in cached_cols
    ]
    positional_rows = [
        [r.get(name, "") if isinstance(r, dict) else None for name in col_names]
        for r in cached_rows
    ]

    resolved = dict(tu_input)
    resolved["columns"] = cached_cols
    resolved["rows"] = positional_rows
    return resolved, None


def _summarize_input(tool_name: str, tu_input: dict) -> str:
    """One-line human-readable summary of a tool call's input.

    Keeps the trace UI-friendly (we don't dump full row payloads into
    DynamoDB-stored trace entries). Bulk inputs live elsewhere; here we
    just want enough text for a developer or analyst to scan.
    """
    if tool_name == "run_sql":
        sql = (tu_input.get("sql") or "").strip().replace("\n", " ")
        return sql[:240] + ("…" if len(sql) > 240 else "")
    if tool_name == "extract_from_rows":
        # Prefer the cleaner from_run_id summary; fall back to literal row count.
        rid = tu_input.get("from_run_id")
        rows = tu_input.get("rows") or []
        source = f"from_run_id={rid}" if rid else f"{len(rows)} literal rows"
        q = (tu_input.get("question") or "").strip()
        return f"{source}; question: {q[:120]}"
    if tool_name == "aggregate":
        rid = tu_input.get("from_run_id")
        rows = tu_input.get("rows") or []
        source = f"from_run_id={rid}" if rid else f"{len(rows)} literal rows"
        gb = tu_input.get("group_by") or []
        agg = tu_input.get("agg") or "count"
        return f"{source}; group_by={gb}; agg={agg}"
    if tool_name == "select_columns":
        rid = tu_input.get("from_run_id")
        cols = tu_input.get("columns") or []
        computed = list((tu_input.get("computed") or {}).keys())
        extra = f"; computed={computed}" if computed else ""
        return f"from_run_id={rid}; columns={cols}{extra}"
    if tool_name == "concat_runs":
        ids = tu_input.get("from_run_ids") or []
        return f"from_run_ids={ids}"
    if tool_name == "inspect_schema":
        return ""
    if tool_name == FINALIZE_TOOL:
        cols = tu_input.get("columns") or []
        rows = tu_input.get("rows") or []
        return f"{len(rows)} rows, {len(cols)} columns"
    # Fallback: short JSON repr.
    import json
    s = json.dumps(tu_input, default=str)
    return s[:240] + ("…" if len(s) > 240 else "")


def _summarize_output(tool_name: str, output) -> str:
    """One-line summary of a tool's output for trace display."""
    if isinstance(output, dict):
        if "rows" in output and isinstance(output["rows"], list):
            n = len(output["rows"])
            cols = output.get("columns") or []
            col_names = [c.get("name") if isinstance(c, dict) else str(c) for c in cols]
            return f"{n} rows; columns: {', '.join(col_names[:6])}"
        if "row_count" in output:
            return f"row_count={output['row_count']}"
    if isinstance(output, list):
        return f"{len(output)} items"
    s = str(output)
    return s[:240] + ("…" if len(s) > 240 else "")
