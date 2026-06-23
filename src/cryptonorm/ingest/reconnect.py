"""Auto-reconnect wrapper for exchange adapters.

Wraps an adapter's ``stream()`` so a dropped connection — or a
``FeedGapError`` raised by the adapter on a detected desync — reconnects
with exponential backoff + full jitter. A clean run resets the backoff.
Reconnecting re-subscribes and re-snapshots, which is exactly the resync a
gap requires, so gap-handling and reconnect share one path.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from cryptonorm.common.errors import FeedGapError
from cryptonorm.ingest.base import ExchangeAdapter


def backoff_delay(
    attempt: int,
    base: float,
    cap: float,
    *,
    factor: float = 2.0,
    rng: Callable[[], float] = random.random,
) -> float:
    """Full-jitter backoff: a random delay in [0, min(cap, base*factor^attempt)].

    Full jitter (vs. fixed exponential) avoids a thundering herd of
    reconnections all firing at the same instant after an outage.
    """
    ceiling = min(cap, base * (factor**attempt))
    return rng() * ceiling


async def reconnecting_stream(
    adapter: ExchangeAdapter,
    base_delay: float,
    max_delay: float,
    *,
    stop_event: asyncio.Event | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    rng: Callable[[], float] = random.random,
) -> AsyncIterator[dict[str, Any]]:
    """Yield frames from ``adapter.stream()``, reconnecting forever on failure."""
    attempt = 0
    while stop_event is None or not stop_event.is_set():
        try:
            async for frame in adapter.stream():
                attempt = 0  # healthy traffic resets backoff
                yield frame
            adapter.log.warning("stream ended cleanly; reconnecting")
        except asyncio.CancelledError:
            raise
        except FeedGapError as exc:
            adapter.log.warning("feed desync; resync via reconnect", reason=str(exc))
        except Exception as exc:
            adapter.log.warning("connection error; reconnecting", error=repr(exc))

        if stop_event is not None and stop_event.is_set():
            break
        delay = backoff_delay(attempt, base_delay, max_delay, rng=rng)
        attempt += 1
        adapter.log.info("reconnecting", attempt=attempt, delay_s=round(delay, 3))
        await sleep(delay)
