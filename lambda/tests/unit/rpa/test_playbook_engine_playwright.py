"""Tests for the Playwright adapter's CSS-only selector guard.

The adapter wraps a Playwright Page; we don't need Playwright itself to
verify the selector allowlist. Full browser-driven integration tests for
the adapter + engine live behind the `playwright` marker.
"""

import pytest

from rpa.playbook_engine_playwright import _css


@pytest.mark.parametrize("bad", [
    "text=Sign in",
    "xpath=//div[@class='x']",
    "role=button",
    "css=div.x",
    "/html/body/div",
    "//div[@id='x']",
    "  text=after-whitespace",
])
def test_rejects_non_css_selector_prefixes(bad):
    with pytest.raises(ValueError, match="non-CSS prefix"):
        _css(bad)


@pytest.mark.parametrize("good", [
    "div.class",
    "#id",
    "a[href]",
    "div > .child",
    "table.patient-list tbody tr",
    "[data-testid='note-row']",
    "div:has(> a.next)",   # CSS :has() is fine
])
def test_accepts_plain_css(good):
    assert _css(good) == good
