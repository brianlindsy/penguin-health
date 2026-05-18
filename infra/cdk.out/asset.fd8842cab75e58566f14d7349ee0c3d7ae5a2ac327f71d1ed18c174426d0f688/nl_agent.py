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
    max_steps: int = 8,
    on_step: Optional[Callable[[dict], None]] = None,
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
            on_step({
                "step": step,
                "label": f"Step {step}: thinking",
                "tool_calls": [],
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
                # Capture the structured final answer; stop after this turn.
                finalize_input = tu_input
                trace_entry["status"] = "finalize"
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
        rows = tu_input.get("rows") or []
        q = (tu_input.get("question") or "").strip()
        return f"{len(rows)} rows; question: {q[:120]}"
    if tool_name == "aggregate":
        rows = tu_input.get("rows") or []
        gb = tu_input.get("group_by") or []
        agg = tu_input.get("agg") or "count"
        return f"{len(rows)} rows; group_by={gb}; agg={agg}"
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
