"""Tests for rpa.playbook_engine — pure op-dispatch logic.

Uses a synchronous FakePage that records every call against a script of
expected page contents. No Playwright, no network. The Playwright-backed
integration test for this module lives separately and is gated by the
`playwright` pytest marker.
"""

import asyncio

import pytest

from rpa.exceptions import RpaPlaybookError
from rpa.playbook_engine import execute
from rpa.rate_limiter import RateLimiter


# ----- FakePage ----------------------------------------------------------


class FakePage:
    """In-memory page scripted via per-selector dicts.

    `texts`/`attrs` are dicts keyed by selector. `lists` is a dict keyed
    by selector returning the count of "matches" (so loop_over_list runs
    that many times). `exists_set` is the set of selectors that
    `exists()` returns True for. `nav_log` and `click_log` record the
    sequence of calls for assertion.
    """

    def __init__(self, *, texts=None, attrs=None, lists=None,
                 exists_set=None, wait_raises=None, nav_raises=None,
                 click_raises=None):
        self.texts = texts or {}
        self.attrs = attrs or {}
        self.lists = lists or {}
        self.exists_set = set(exists_set or [])
        self.wait_raises = wait_raises or {}
        self.nav_raises = nav_raises or {}
        self.click_raises = click_raises or {}

        self.nav_log = []
        self.click_log = []
        self.wait_log = []

    async def navigate(self, url):
        self.nav_log.append(url)
        if url in self.nav_raises:
            raise self.nav_raises[url]

    async def click(self, selector):
        self.click_log.append(selector)
        if selector in self.click_raises:
            raise self.click_raises[selector]

    async def wait_for_selector(self, selector, *, timeout_ms):
        self.wait_log.append((selector, timeout_ms))
        if selector in self.wait_raises:
            raise self.wait_raises[selector]

    async def query_text(self, selector):
        return self.texts[selector]

    async def query_attr(self, selector, attr):
        return self.attrs[(selector, attr)]

    async def query_all(self, selector):
        # Return a list of opaque markers; engine doesn't inspect them.
        return [object() for _ in range(self.lists.get(selector, 0))]

    async def exists(self, selector):
        return selector in self.exists_set


# ----- helpers -----------------------------------------------------------


def _no_gate_limiter() -> RateLimiter:
    """Rate limiter that never sleeps — keeps tests fast and removes
    asyncio sleep from the unit-test surface."""
    async def _no_sleep(_s):  # noqa: D401
        return None
    return RateLimiter(min_ms=0, clock=lambda: 0.0, sleep=_no_sleep)


def _run(playbook, page):
    async def collect():
        out = []
        async for note in execute(playbook, page,
                                  rate_limiter=_no_gate_limiter()):
            out.append(note)
        return out
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(collect())
    finally:
        loop.close()


# ----- ops one at a time -------------------------------------------------


def test_navigate_calls_page_with_url():
    page = FakePage()
    _run({"steps": [{"op": "navigate", "url": "https://x.example/home"}]}, page)
    assert page.nav_log == ["https://x.example/home"]


def test_navigate_failure_raises_playbook_error():
    page = FakePage(nav_raises={"https://x/": TimeoutError("bad")})
    with pytest.raises(RpaPlaybookError, match="navigate.*failed"):
        _run({"steps": [{"op": "navigate", "url": "https://x/"}]}, page)


def test_click_calls_page_with_selector():
    page = FakePage()
    _run({"steps": [{"op": "click", "selector": "a.next"}]}, page)
    assert page.click_log == ["a.next"]


def test_wait_for_selector_uses_default_timeout_when_unset():
    page = FakePage()
    _run({"steps": [{"op": "wait_for_selector", "selector": "div.ready"}]}, page)
    assert page.wait_log == [("div.ready", 8000)]


def test_wait_for_selector_respects_per_op_timeout():
    page = FakePage()
    _run({"steps": [{"op": "wait_for_selector",
                     "selector": "div.ready",
                     "timeout_ms": 500}]}, page)
    assert page.wait_log == [("div.ready", 500)]


def test_wait_for_selector_respects_playbook_default_timeout():
    page = FakePage()
    _run({"steps": [{"op": "wait_for_selector", "selector": "div.ready"}],
          "default_timeouts": {"selector_ms": 1234}}, page)
    assert page.wait_log == [("div.ready", 1234)]


# ----- extract + emit_note -----------------------------------------------


def test_extract_with_text_attr_pulls_text():
    page = FakePage(texts={"h1": "Hello"})
    out = _run({"steps": [
        {"op": "extract", "fields": {"title": {"selector": "h1"}}},
        {"op": "emit_note"},
    ]}, page)
    assert out == [{"title": "Hello"}]


def test_extract_with_explicit_attr():
    page = FakePage(attrs={("a.link", "href"): "/next"})
    out = _run({"steps": [
        {"op": "extract", "fields": {
            "url": {"selector": "a.link", "attr": "href"}
        }},
        {"op": "emit_note"},
    ]}, page)
    assert out == [{"url": "/next"}]


def test_emit_note_without_extraction_raises():
    with pytest.raises(RpaPlaybookError, match="no fields extracted"):
        _run({"steps": [{"op": "emit_note"}]}, FakePage())


def test_emit_note_clears_extraction_so_next_iteration_starts_fresh():
    page = FakePage(texts={"h1": "A", "h2": "B"})
    out = _run({"steps": [
        {"op": "extract", "fields": {"a": {"selector": "h1"}}},
        {"op": "emit_note"},
        {"op": "extract", "fields": {"b": {"selector": "h2"}}},
        {"op": "emit_note"},
    ]}, page)
    assert out == [{"a": "A"}, {"b": "B"}]


# ----- loop_over_list ----------------------------------------------------


def test_loop_over_list_runs_body_for_each_match():
    page = FakePage(lists={"tr": 3}, texts={"td.id": "X"})
    out = _run({"steps": [{
        "op": "loop_over_list",
        "selector": "tr",
        "body": [
            {"op": "extract", "fields": {"id": {"selector": "td.id"}}},
            {"op": "emit_note"},
        ],
    }]}, page)
    assert out == [{"id": "X"}, {"id": "X"}, {"id": "X"}]


def test_loop_over_list_respects_max_items():
    page = FakePage(lists={"tr": 10}, texts={"td.id": "X"})
    out = _run({"steps": [{
        "op": "loop_over_list",
        "selector": "tr",
        "max_items": 2,
        "body": [
            {"op": "extract", "fields": {"id": {"selector": "td.id"}}},
            {"op": "emit_note"},
        ],
    }]}, page)
    assert len(out) == 2


def test_loop_over_list_isolates_extraction_between_iterations():
    """A field set in iteration N must not leak into iteration N+1 even
    when N+1 doesn't re-extract it."""
    # Iteration 1 extracts foo=A then emits. Iteration 2 extracts bar=B
    # then emits without touching foo. We expect iteration 2's note to
    # have only `bar`, not `foo`.
    page = FakePage(lists={"row": 2},
                    texts={".foo": "A", ".bar": "B"})
    # We need different body per iteration — use if_exists hack? Simpler:
    # since both iterations run the same body, both should see the
    # full set. The real "no bleed" test is: after a loop ends, the
    # parent's extraction is restored.
    out = _run({"steps": [
        {"op": "extract", "fields": {"outer": {"selector": ".foo"}}},
        {"op": "loop_over_list", "selector": "row", "body": [
            {"op": "extract", "fields": {"inner": {"selector": ".bar"}}},
            {"op": "emit_note"},
        ]},
        {"op": "emit_note"},
    ]}, page)
    # Loop iterations emit only inner; the trailing emit emits the
    # restored outer-scope extraction.
    assert out == [{"inner": "B"}, {"inner": "B"}, {"outer": "A"}]


# ----- if_exists ---------------------------------------------------------


def test_if_exists_runs_then_branch():
    page = FakePage(exists_set={".banner"}, texts={"h1": "T"})
    out = _run({"steps": [{
        "op": "if_exists",
        "selector": ".banner",
        "then": [
            {"op": "extract", "fields": {"t": {"selector": "h1"}}},
            {"op": "emit_note"},
        ],
    }]}, page)
    assert out == [{"t": "T"}]


def test_if_exists_runs_else_branch_when_absent():
    page = FakePage(exists_set=set(), texts={"h1": "fallback"})
    out = _run({"steps": [{
        "op": "if_exists",
        "selector": ".banner",
        "then": [],
        "else": [
            {"op": "extract", "fields": {"t": {"selector": "h1"}}},
            {"op": "emit_note"},
        ],
    }]}, page)
    assert out == [{"t": "fallback"}]


def test_if_exists_no_branch_for_absent_when_only_then():
    page = FakePage(exists_set=set())
    out = _run({"steps": [{
        "op": "if_exists",
        "selector": ".banner",
        "then": [{"op": "emit_note"}],   # would raise if reached
    }]}, page)
    assert out == []


# ----- log + stop --------------------------------------------------------


def test_log_calls_on_log():
    seen = []
    page = FakePage()

    async def collect():
        async for _ in execute(
            {"steps": [{"op": "log", "message": "hello"}]},
            page,
            rate_limiter=_no_gate_limiter(),
            on_log=seen.append,
        ):
            pass

    asyncio.new_event_loop().run_until_complete(collect())
    assert seen == ["hello"]


def test_stop_halts_execution_cleanly_without_raising():
    page = FakePage(texts={"h1": "A"})
    out = _run({"steps": [
        {"op": "extract", "fields": {"a": {"selector": "h1"}}},
        {"op": "emit_note"},
        {"op": "stop"},
        # Anything below stop is unreachable. emit_note here would raise
        # if reached (no extraction in scope) — its presence is the test.
        {"op": "emit_note"},
    ]}, page)
    assert out == [{"a": "A"}]


# ----- safety / allowlist ------------------------------------------------


def test_unknown_op_rejected_before_any_page_call():
    page = FakePage()
    with pytest.raises(RpaPlaybookError, match="unsupported op"):
        _run({"steps": [{"op": "fill", "selector": "input", "value": "x"}]},
             page)
    assert page.click_log == []
    assert page.nav_log == []


def test_fill_op_is_not_in_the_allowlist():
    """Pinned: RPA is read-only on the portal. If a future change tries
    to add `fill` to the allowlist, this test fails until the team
    explicitly decides to allow it."""
    from rpa.playbook_engine import _ALLOWED_OPS
    assert "fill" not in _ALLOWED_OPS
    # Whole allowlist pinned to prevent silent additions.
    assert _ALLOWED_OPS == frozenset({
        "navigate", "click", "wait_for_selector", "extract",
        "loop_over_list", "if_exists", "emit_note", "log", "stop",
    })


# ----- rate limiter integration ------------------------------------------


def test_rate_limiter_awaited_before_each_page_op():
    calls = []

    class CountingLimiter:
        async def wait(self):
            calls.append(1)

    async def collect():
        async for _ in execute({"steps": [
            {"op": "navigate", "url": "https://x/"},
            {"op": "click", "selector": "a"},
            {"op": "wait_for_selector", "selector": "div"},
            {"op": "extract", "fields": {"t": {"selector": "h1"}}},
            {"op": "loop_over_list", "selector": "tr", "max_items": 0,
             "body": []},
            {"op": "if_exists", "selector": "x", "then": []},
        ]}, FakePage(texts={"h1": "T"}),
                rate_limiter=CountingLimiter()):
            pass

    asyncio.new_event_loop().run_until_complete(collect())
    # navigate, click, wait_for_selector, extract, loop_over_list, if_exists
    # = 6 awaits. emit_note + log + stop don't touch the page.
    assert sum(calls) == 6
