"""Async rate limiter: enforces a minimum delay between successive actions.

The playbook engine awaits limiter.wait() before every Playwright action
(navigate / click / extract / wait_for_selector). The delay is configured
per-org via `RPA_CONFIG.guardrails.rate_limit_ms_between_requests` and
keeps the bot's request cadence below thresholds the portal would treat
as abusive.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable


class RateLimiter:
    def __init__(
        self,
        min_ms: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if min_ms < 0:
            raise ValueError(f"min_ms must be non-negative; got {min_ms}")
        self.min_ms = min_ms
        self._clock = clock
        self._sleep = sleep
        self._last: float = 0.0

    async def wait(self) -> None:
        """Block until at least `min_ms` has elapsed since the previous
        call. The first call returns immediately.
        """
        if self.min_ms == 0:
            return
        now = self._clock()
        elapsed_ms = (now - self._last) * 1000.0
        deficit_ms = self.min_ms - elapsed_ms
        if self._last > 0 and deficit_ms > 0:
            await self._sleep(deficit_ms / 1000.0)
        self._last = self._clock()
