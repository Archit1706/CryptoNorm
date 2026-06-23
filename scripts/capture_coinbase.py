"""One-off live capture of Coinbase Advanced Trade public WS frames.

Used to record real sample messages as test fixtures (brief: do not
fabricate exchange schemas). Not part of the running service.

Note: Coinbase's initial level2 snapshot frame is ~1 MB, which exceeds
the websockets default max_size (1 MiB). The real adapter must raise it.
"""

import asyncio
import json
import sys

import websockets

URL = "wss://advanced-trade-ws.coinbase.com"


async def capture(channel: str, n: int) -> list[str]:
    sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": channel}
    frames: list[str] = []
    async with websockets.connect(URL, open_timeout=15, max_size=8 * 1024 * 1024) as ws:
        await ws.send(json.dumps(sub))
        while len(frames) < n:
            raw = await asyncio.wait_for(ws.recv(), timeout=15)
            frames.append(raw)
    return frames


def trim_snapshot(obj: dict, keep: int = 5) -> dict:
    """Trim the giant snapshot updates list to a few levels, keep structure."""
    for ev in obj.get("events", []):
        if ev.get("type") == "snapshot" and "updates" in ev:
            bids = [u for u in ev["updates"] if u["side"] == "bid"][:keep]
            asks = [u for u in ev["updates"] if u["side"] == "offer"][:keep]
            ev["updates"] = bids + asks
    return obj


async def main() -> None:
    frames = await capture("level2", 3)
    print(f"===== channel=level2: {len(frames)} frames =====")
    for f in frames:
        obj = json.loads(f)
        ch = obj.get("channel")
        ev_type = obj.get("events", [{}])[0].get("type")
        if ev_type == "snapshot":
            obj = trim_snapshot(obj)
        print(f"### channel={ch} event_type={ev_type} raw_bytes={len(f)}")
        print(json.dumps(obj, indent=2)[:2500])
        print("-" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
