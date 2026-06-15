"""Tests for rpa.rate_limiter — async min-delay between actions.

The limiter takes injectable `clock` and `sleep` callables so we can
assert its math deterministically without touching the real clock or
the asyncio event loop's internal time source.
"""

import asyncio

import pytest

from rpa.rate_limiter import RateLimiter


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _FakeClock:
    def __init__(self, ticks):
        self._ticks = list(ticks)
        self._i = 0

    def __call__(self):
        v = self._ticks[self._i]
        self._i += 1
        return v


def test_first_call_returns_without_sleeping():
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    limiter = RateLimiter(
        min_ms=500,
        clock=_FakeClock([100.0, 100.0]),  # one for now, one for _last update
        sleep=fake_sleep,
    )
    _run(limiter.wait())
    assert sleeps == []


def test_back_to_back_calls_sleep_the_deficit():
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    # Two calls 100 ms apart; limit 500 ms → sleep ~400 ms on call 2.
    limiter = RateLimiter(
        min_ms=500,
        clock=_FakeClock([100.0, 100.0,  # first call: now, _last
                          100.1, 100.1]),  # second call: now, _last
        sleep=fake_sleep,
    )
    _run(limiter.wait())
    _run(limiter.wait())

    assert len(sleeps) == 1
    assert sleeps[0] == pytest.approx(0.4, abs=0.001)


def test_call_after_window_does_not_sleep():
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    # Two calls 2 seconds apart; limit 500 ms → no sleep.
    limiter = RateLimiter(
        min_ms=500,
        clock=_FakeClock([100.0, 100.0, 102.0, 102.0]),
        sleep=fake_sleep,
    )
    _run(limiter.wait())
    _run(limiter.wait())
    assert sleeps == []


def test_zero_ms_means_no_gate():
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    limiter = RateLimiter(min_ms=0, clock=_FakeClock([1.0] * 10), sleep=fake_sleep)
    _run(limiter.wait())
    _run(limiter.wait())
    _run(limiter.wait())
    assert sleeps == []


def test_negative_min_ms_rejected():
    with pytest.raises(ValueError, match="non-negative"):
        RateLimiter(min_ms=-1)


def test_defaults_use_real_clock_and_real_sleep():
    # Smoke check: with defaults and min_ms=0 it doesn't block; the real
    # clock + real asyncio.sleep wiring is exercised in chunk 3 via the
    # playbook engine tests.
    limiter = RateLimiter(min_ms=0)
    _run(limiter.wait())
