"""Tests for centralreach.rate_limiter — sync minimum-delay gate."""

import pytest

from centralreach.rate_limiter import RateLimiter


def _fake_clock():
    """Returns a closure pair: (clock_fn, advance_fn). Tests advance
    the clock manually rather than calling time.monotonic, so the
    delay math is exact."""
    now = [0.0]

    def clock():
        return now[0]

    def advance(seconds):
        now[0] += seconds

    return clock, advance


def _recording_sleep():
    """Replacement for time.sleep that records call durations without
    actually sleeping. Returns (sleep_fn, calls_list)."""
    calls = []

    def sleep(seconds):
        calls.append(seconds)

    return sleep, calls


def test_first_wait_returns_immediately():
    clock, _ = _fake_clock()
    sleep, sleep_calls = _recording_sleep()
    rl = RateLimiter(1500, clock=clock, sleep=sleep)
    rl.wait()
    assert sleep_calls == []


def test_second_wait_blocks_for_full_min_when_no_elapsed_time():
    clock, _ = _fake_clock()
    sleep, sleep_calls = _recording_sleep()
    rl = RateLimiter(1500, clock=clock, sleep=sleep)
    rl.wait()
    rl.wait()  # zero elapsed time -> full 1500ms wait
    assert sleep_calls == [1.5]


def test_partial_elapsed_yields_partial_sleep():
    clock, advance = _fake_clock()
    sleep, sleep_calls = _recording_sleep()
    rl = RateLimiter(1500, clock=clock, sleep=sleep)
    rl.wait()
    advance(0.6)  # 600ms elapsed
    rl.wait()      # should sleep ~900ms
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(0.9, abs=1e-9)


def test_elapsed_exceeding_min_does_not_sleep():
    clock, advance = _fake_clock()
    sleep, sleep_calls = _recording_sleep()
    rl = RateLimiter(1500, clock=clock, sleep=sleep)
    rl.wait()
    advance(5.0)  # plenty of time
    rl.wait()
    assert sleep_calls == []


def test_min_ms_zero_disables_the_gate():
    clock, _ = _fake_clock()
    sleep, sleep_calls = _recording_sleep()
    rl = RateLimiter(0, clock=clock, sleep=sleep)
    rl.wait()
    rl.wait()
    rl.wait()
    assert sleep_calls == []


def test_negative_min_ms_raises():
    with pytest.raises(ValueError, match="non-negative"):
        RateLimiter(-1)
