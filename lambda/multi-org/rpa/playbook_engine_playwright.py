"""Playwright adapter for the playbook engine's `Page` protocol.

Kept in a separate module so the core engine (`playbook_engine.py`) and
its unit tests have no Playwright import dependency. The Fargate runner
imports this module to wire a real browser into the engine.
"""

from __future__ import annotations

from typing import Any


class PlaywrightPage:
    """Adapt a playwright.async_api.Page to the engine's `Page` protocol.

    Selector strategy is CSS-only (v1) — no XPath, no text= selectors.
    Lock that down here so a playbook author can't smuggle one in via
    selector strings the engine wouldn't otherwise inspect.
    """

    def __init__(self, page: Any) -> None:
        self._page = page

    async def navigate(self, url: str) -> None:
        await self._page.goto(url, wait_until="domcontentloaded")

    async def click(self, selector: str) -> None:
        await self._page.click(_css(selector))

    async def wait_for_selector(self, selector: str, *, timeout_ms: int) -> None:
        await self._page.wait_for_selector(_css(selector), timeout=timeout_ms)

    async def query_text(self, selector: str) -> str:
        el = await self._page.query_selector(_css(selector))
        if el is None:
            return ""
        return (await el.text_content()) or ""

    async def query_attr(self, selector: str, attr: str) -> str:
        el = await self._page.query_selector(_css(selector))
        if el is None:
            return ""
        if attr == "innerHTML":
            return await el.inner_html()
        if attr == "innerText":
            return await el.inner_text()
        return (await el.get_attribute(attr)) or ""

    async def query_all(self, selector: str) -> list[Any]:
        return await self._page.query_selector_all(_css(selector))

    async def exists(self, selector: str) -> bool:
        el = await self._page.query_selector(_css(selector))
        return el is not None


def _css(selector: str) -> str:
    """Reject non-CSS selector prefixes Playwright supports natively.

    Playwright accepts `text=`, `xpath=`, `role=`, etc. We don't want
    playbooks to use any of those — CSS-only keeps the surface small
    and predictable.
    """
    s = selector.lstrip()
    for prefix in ("text=", "xpath=", "role=", "css=", "/", "//"):
        if s.startswith(prefix):
            raise ValueError(
                f"selector {selector!r} uses non-CSS prefix; v1 playbooks "
                "must use plain CSS selectors only"
            )
    return selector
