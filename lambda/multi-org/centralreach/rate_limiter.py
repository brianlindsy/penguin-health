"""Sync rate limiter: enforces a minimum delay between HTTP requests.

The centralreach HTTP client calls `wait()` before every outbound CR
request. The delay matches CR's documented UI cadence (~1500ms between
requests in v1) to avoid pattern-matching as bot traffic.

Sync because the centralreach Fargate task drives requests
sequentially, one entry at a time. The `rpa/rate_limiter.py` async
variant exists for the Playwright path and will be deleted in PR F
when the rpa module is removed.
"""

from __future__ import annotations

import time
from typing import Callable


class RateLimiter:
    """Minimum-delay gate between successive `wait()` calls.

    The first call returns immediately. Subsequent calls block until
    `min_ms` has elapsed since the previous call.
    """

    def __init__(
        self,
        min_ms: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if min_ms < 0:
            raise ValueError(f"min_ms must be non-negative; got {min_ms}")
        self.min_ms = min_ms
        self._clock = clock
        self._sleep = sleep
        # `None` distinguishes "wait() has never been called" from "the
        # clock happens to be at 0.0" — a sentinel value rather than a
        # numeric default catches the test-fake-clock case correctly.
        self._last: float | None = None

    def wait(self) -> None:
        """Block until at least `min_ms` has elapsed since the previous
        call. First call returns immediately."""
        if self.min_ms == 0:
            return
        now = self._clock()
        if self._last is not None:
            elapsed_ms = (now - self._last) * 1000.0
            deficit_ms = self.min_ms - elapsed_ms
            if deficit_ms > 0:
                self._sleep(deficit_ms / 1000.0)
        self._last = self._clock()
