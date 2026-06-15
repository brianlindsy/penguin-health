"""Declarative-playbook execution against a browser page.

A playbook is a small list of ops the engine interprets to drive a
Playwright session. The engine talks to a `Page` protocol (see below)
rather than Playwright directly so we can unit-test the op dispatch
with a synchronous fake page — Playwright-driven tests live behind a
`playwright` pytest marker and run separately.

Op vocabulary (v1 — strict allowlist, no fill, no JS eval):
    navigate         {url}
    click            {selector}
    wait_for_selector{selector, timeout_ms?}
    extract          {fields: {name: {selector, attr}}}
                       attr: "text" | "innerHTML" | <html attr name>
                       Captured fields merge into the current extraction.
    loop_over_list   {selector, max_items?, body: [ops]}
                       For each match (capped by max_items), body executes
                       with a fresh extraction dict; emit_note flushes it.
    if_exists        {selector, then: [ops], else?: [ops]}
    emit_note        — flush the current extraction dict to the engine's
                       output stream. Required keys must be present (see
                       result_writer.build_record); the engine itself does
                       not validate keys — the caller's writer does.
    log              {message}
    stop             — terminate the playbook cleanly

`fill` is intentionally absent. RPA is strictly read-only on the portal:
no form submission, no edits to clinical data, no interaction with the
target system beyond clicks needed to navigate read views.

The engine awaits the rate limiter before every page-touching op so a
misbehaving playbook can't burst-request the target portal.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Protocol

from .exceptions import RpaPlaybookError
from .rate_limiter import RateLimiter


# ----- Page protocol -----------------------------------------------------


class Page(Protocol):
    """The minimum surface the engine needs from a browser page.

    A Playwright-backed adapter lives in `playbook_engine_playwright.py`
    (chunk 3 follow-up); tests pass a synchronous fake.
    """

    async def navigate(self, url: str) -> None: ...
    async def click(self, selector: str) -> None: ...
    async def wait_for_selector(self, selector: str, *, timeout_ms: int) -> None: ...
    async def query_text(self, selector: str) -> str: ...
    async def query_attr(self, selector: str, attr: str) -> str: ...
    async def query_all(self, selector: str) -> list[Any]: ...
    async def exists(self, selector: str) -> bool: ...


# ----- engine ------------------------------------------------------------


_ALLOWED_OPS = frozenset({
    "navigate", "click", "wait_for_selector", "extract",
    "loop_over_list", "if_exists", "emit_note", "log", "stop",
})

_DEFAULT_NAVIGATION_TIMEOUT_MS = 15_000
_DEFAULT_SELECTOR_TIMEOUT_MS = 8_000

# Sentinel raised internally by `stop` to unwind the recursive op runner
# without conflating with an actual error.
class _StopExecution(Exception):
    pass


async def execute(
    playbook: dict,
    page: Page,
    *,
    rate_limiter: RateLimiter,
    on_log: Callable[[str], None] = lambda _msg: None,
) -> AsyncIterator[dict]:
    """Run the playbook, yielding one extraction dict per `emit_note`.

    Yields incrementally so the runner can persist + audit each note
    immediately rather than buffering until the run ends.
    """
    steps = playbook.get("steps") or []
    timeouts = playbook.get("default_timeouts") or {}
    nav_timeout = int(timeouts.get("navigation_ms",
                                   _DEFAULT_NAVIGATION_TIMEOUT_MS))
    sel_timeout = int(timeouts.get("selector_ms",
                                   _DEFAULT_SELECTOR_TIMEOUT_MS))

    # The current extraction dict is shared across ops in the same
    # iteration scope. `loop_over_list` pushes a fresh dict per iteration;
    # `emit_note` yields the current dict and clears the slot.
    ctx = {
        "extraction": {},
        "page": page,
        "rate_limiter": rate_limiter,
        "on_log": on_log,
        "nav_timeout_ms": nav_timeout,
        "sel_timeout_ms": sel_timeout,
    }

    try:
        async for note in _run_steps(steps, ctx):
            yield note
    except _StopExecution:
        return


async def _run_steps(steps: list, ctx: dict):
    for step in steps:
        async for note in _run_one(step, ctx):
            yield note


async def _run_one(step: dict, ctx: dict):
    op = step.get("op")
    if op not in _ALLOWED_OPS:
        raise RpaPlaybookError(
            f"unsupported op {op!r}; allowed: {sorted(_ALLOWED_OPS)}"
        )

    if op == "navigate":
        url = step["url"]
        await ctx["rate_limiter"].wait()
        try:
            await ctx["page"].navigate(url)
        except Exception as e:
            raise RpaPlaybookError(f"navigate({url!r}) failed: {type(e).__name__}") from e
        return

    if op == "click":
        selector = step["selector"]
        await ctx["rate_limiter"].wait()
        try:
            await ctx["page"].click(selector)
        except Exception as e:
            raise RpaPlaybookError(
                f"click({selector!r}) failed: {type(e).__name__}"
            ) from e
        return

    if op == "wait_for_selector":
        selector = step["selector"]
        timeout_ms = int(step.get("timeout_ms", ctx["sel_timeout_ms"]))
        await ctx["rate_limiter"].wait()
        try:
            await ctx["page"].wait_for_selector(selector, timeout_ms=timeout_ms)
        except Exception as e:
            raise RpaPlaybookError(
                f"wait_for_selector({selector!r}) failed: {type(e).__name__}"
            ) from e
        return

    if op == "extract":
        await ctx["rate_limiter"].wait()
        for name, spec in (step.get("fields") or {}).items():
            value = await _extract_one(ctx["page"], spec)
            ctx["extraction"][name] = value
        return

    if op == "loop_over_list":
        selector = step["selector"]
        max_items = step.get("max_items")
        body = step.get("body") or []
        await ctx["rate_limiter"].wait()
        try:
            elements = await ctx["page"].query_all(selector)
        except Exception as e:
            raise RpaPlaybookError(
                f"loop_over_list({selector!r}) query_all failed: "
                f"{type(e).__name__}"
            ) from e
        if max_items is not None:
            elements = elements[: int(max_items)]
        # Each loop iteration starts with a fresh extraction dict so
        # leftover fields from a previous patient don't bleed into the
        # next. Callers using `loop_over_list` typically pair it with
        # `emit_note` inside the body.
        for _ in elements:
            saved = ctx["extraction"]
            ctx["extraction"] = {}
            try:
                async for note in _run_steps(body, ctx):
                    yield note
            finally:
                ctx["extraction"] = saved
        return

    if op == "if_exists":
        selector = step["selector"]
        await ctx["rate_limiter"].wait()
        try:
            present = await ctx["page"].exists(selector)
        except Exception as e:
            raise RpaPlaybookError(
                f"if_exists({selector!r}) failed: {type(e).__name__}"
            ) from e
        branch = step.get("then") if present else step.get("else")
        if branch:
            async for note in _run_steps(branch, ctx):
                yield note
        return

    if op == "emit_note":
        if not ctx["extraction"]:
            raise RpaPlaybookError(
                "emit_note called with no fields extracted in current scope"
            )
        yield ctx["extraction"]
        ctx["extraction"] = {}
        return

    if op == "log":
        ctx["on_log"](str(step.get("message", "")))
        return

    if op == "stop":
        raise _StopExecution()

    # Defensive — the allowlist check above should make this unreachable.
    raise RpaPlaybookError(f"unhandled op {op!r}")  # pragma: no cover


async def _extract_one(page: Page, spec: dict) -> str:
    selector = spec["selector"]
    attr = spec.get("attr", "text")
    if attr == "text":
        return await page.query_text(selector)
    return await page.query_attr(selector, attr)
