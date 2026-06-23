"""Backoff math + the auto-reconnect stream wrapper."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from cryptonorm.common.logging import get_logger
from cryptonorm.ingest.reconnect import backoff_delay, reconnecting_stream


def test_backoff_grows_and_caps():
    full = {"rng": lambda: 1.0}  # no jitter -> returns the ceiling
    assert backoff_delay(0, 0.5, 30, **full) == 0.5
    assert backoff_delay(1, 0.5, 30, **full) == 1.0
    assert backoff_delay(2, 0.5, 30, **full) == 2.0
    assert backoff_delay(20, 0.5, 30, **full) == 30  # capped


def test_backoff_jitter_within_bounds():
    for attempt in range(6):
        delay = backoff_delay(attempt, 0.5, 30, rng=lambda: 0.5)
        ceiling = min(30, 0.5 * 2**attempt)
        assert 0.0 <= delay <= ceiling


class _FakeAdapter:
    """stream() raises `fail_times` times, then yields three frames."""

    def __init__(self, fail_times: int):
        self.log = get_logger("test")
        self._fail_times = fail_times
        self.calls = 0

    async def stream(self) -> AsyncIterator[dict]:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise ConnectionError(f"boom {self.calls}")
        for i in range(3):
            yield {"i": i}


@pytest.mark.asyncio
async def test_reconnecting_stream_retries_then_streams():
    adapter = _FakeAdapter(fail_times=2)
    delays: list[float] = []

    async def fake_sleep(d: float) -> None:
        delays.append(d)

    out: list[dict] = []
    stream = reconnecting_stream(adapter, 0.5, 30, sleep=fake_sleep, rng=lambda: 1.0)
    async for frame in stream:
        out.append(frame)
        if len(out) == 3:
            break

    assert out == [{"i": 0}, {"i": 1}, {"i": 2}]
    assert adapter.calls == 3  # two failures + one good run
    assert delays == [0.5, 1.0]  # exponential backoff between the two failures
