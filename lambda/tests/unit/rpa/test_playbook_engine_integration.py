"""End-to-end test of the playbook engine against a real headless browser.

Spins up a tiny aiohttp server serving a fake clinician-portal page,
drives the engine through Playwright, and asserts the extracted notes
match the page's content.

Gated by the `playwright` marker; skipped by default. To run:
    pip install playwright aiohttp
    playwright install chromium
    pytest -m playwright lambda/tests/unit/rpa/test_playbook_engine_integration.py
"""

import asyncio
import contextlib
import socket

import pytest


pytestmark = pytest.mark.playwright

# Defer imports so collection doesn't fail in environments without these.
aiohttp_web = pytest.importorskip("aiohttp.web")
playwright_api = pytest.importorskip("playwright.async_api")


# ----- fake portal -------------------------------------------------------


_WORKLIST_HTML = """
<!doctype html>
<html><body>
  <h1>Worklist</h1>
  <table class="patient-list">
    <tbody>
      <tr><td class="mrn">M-001</td><td><a class="open" href="/chart/M-001">open</a></td></tr>
      <tr><td class="mrn">M-002</td><td><a class="open" href="/chart/M-002">open</a></td></tr>
    </tbody>
  </table>
</body></html>
"""

_CHARTS = {
    "M-001": """
<!doctype html><html><body>
  <h1>Chart M-001</h1>
  <section class="notes-list">
    <div class="note-row" data-note-id="N-001">
      <div class="note-date">2026-06-08</div>
      <div class="note-author">Dr. Alice Smith</div>
      <div class="note-body">Patient presents in stable condition.</div>
    </div>
  </section>
</body></html>
""",
    "M-002": """
<!doctype html><html><body>
  <h1>Chart M-002</h1>
  <section class="notes-list">
    <div class="note-row" data-note-id="N-002">
      <div class="note-date">2026-06-08</div>
      <div class="note-author">Dr. Riya Patel</div>
      <div class="note-body">Client engaged actively in session.</div>
    </div>
  </section>
</body></html>
""",
}


async def _worklist(_request):
    return aiohttp_web.Response(text=_WORKLIST_HTML, content_type="text/html")


async def _chart(request):
    mrn = request.match_info["mrn"]
    if mrn not in _CHARTS:
        return aiohttp_web.Response(status=404)
    return aiohttp_web.Response(text=_CHARTS[mrn], content_type="text/html")


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@contextlib.asynccontextmanager
async def _serve():
    app = aiohttp_web.Application()
    app.router.add_get("/", _worklist)
    app.router.add_get("/chart/{mrn}", _chart)

    runner = aiohttp_web.AppRunner(app)
    await runner.setup()
    port = _free_port()
    site = aiohttp_web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


# ----- test --------------------------------------------------------------


async def _run_test():
    from rpa.playbook_engine import execute
    from rpa.playbook_engine_playwright import PlaywrightPage
    from rpa.rate_limiter import RateLimiter

    async with _serve() as base:
        playbook = {
            "default_timeouts": {"navigation_ms": 5000, "selector_ms": 3000},
            "steps": [
                {"op": "navigate", "url": f"{base}/"},
                {"op": "wait_for_selector",
                 "selector": "table.patient-list tbody tr"},
                {"op": "loop_over_list",
                 "selector": "table.patient-list tbody tr",
                 "body": [
                     {"op": "extract", "fields": {
                         "source_patient_id": {"selector": "td.mrn"},
                     }},
                     {"op": "click", "selector": "a.open"},
                     {"op": "wait_for_selector",
                      "selector": "section.notes-list .note-row"},
                     {"op": "extract", "fields": {
                         "note_id": {"selector": ".note-row",
                                     "attr": "data-note-id"},
                         "visit_date": {"selector": ".note-date"},
                         "provider": {"selector": ".note-author"},
                         "body": {"selector": ".note-body"},
                     }},
                     {"op": "emit_note"},
                     {"op": "navigate", "url": f"{base}/"},
                     {"op": "wait_for_selector",
                      "selector": "table.patient-list tbody tr"},
                 ]},
            ],
        }

        async with playwright_api.async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context()
                page = await context.new_page()
                adapter = PlaywrightPage(page)
                limiter = RateLimiter(min_ms=0)

                notes = []
                async for note in execute(playbook, adapter,
                                          rate_limiter=limiter):
                    notes.append(note)
            finally:
                await browser.close()

    assert len(notes) == 2
    assert {n["source_patient_id"] for n in notes} == {"M-001", "M-002"}
    assert {n["note_id"] for n in notes} == {"N-001", "N-002"}
    # Real text from the fixture
    bodies = [n["body"] for n in notes]
    assert "Patient presents in stable condition." in bodies
    assert "Client engaged actively in session." in bodies


def test_playbook_engine_against_real_browser_and_server():
    asyncio.new_event_loop().run_until_complete(_run_test())
