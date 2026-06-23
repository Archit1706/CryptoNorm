"""Live capture of Binance + Kraken public WS (and Binance REST snapshot).

Records real sample frames so normalizers are written against actual bytes.
Not part of the running service.
"""

import asyncio
import json
import sys

import aiohttp
import websockets

BINANCE_WS = "wss://stream.binance.com:9443/ws"
BINANCE_SNAPSHOT = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5"
KRAKEN_WS = "wss://ws.kraken.com/v2"


async def capture_binance() -> None:
    print("\n########## BINANCE ##########")
    try:
        async with websockets.connect(BINANCE_WS, open_timeout=15) as ws:
            sub = {
                "method": "SUBSCRIBE",
                "params": ["btcusdt@depth", "btcusdt@trade"],
                "id": 1,
            }
            await ws.send(json.dumps(sub))
            seen = {"depthUpdate": 0, "trade": 0, "ack": 0}
            for _ in range(40):
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
                obj = json.loads(raw)
                etype = obj.get("e", "ack")
                if etype in seen and seen[etype] < 1:
                    seen[etype] += 1
                    print(f"### binance event={etype}")
                    print(json.dumps(obj, indent=2)[:1500])
                    print("-" * 50)
                if all(v >= 1 for k, v in seen.items() if k != "ack"):
                    break
    except Exception as exc:  # noqa: BLE001
        print(f"BINANCE WS FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)

    try:
        async with aiohttp.ClientSession() as s, s.get(BINANCE_SNAPSHOT) as r:
            print(f"### binance REST snapshot (status {r.status})")
            print(json.dumps(await r.json(), indent=2)[:1200])
    except Exception as exc:  # noqa: BLE001
        print(f"BINANCE REST FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)


async def capture_kraken() -> None:
    print("\n########## KRAKEN v2 ##########")
    try:
        async with websockets.connect(KRAKEN_WS, open_timeout=15) as ws:
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 10},
            }))
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": ["BTC/USD"]},
            }))
            shown: set[str] = set()
            for _ in range(40):
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
                obj = json.loads(raw)
                channel = obj.get("channel", obj.get("method", "?"))
                msgtype = obj.get("type", "")
                key = f"{channel}:{msgtype}"
                if key not in shown:
                    shown.add(key)
                    print(f"### kraken channel={channel} type={msgtype}")
                    print(json.dumps(obj, indent=2)[:1500])
                    print("-" * 50)
    except Exception as exc:  # noqa: BLE001
        print(f"KRAKEN FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)


async def main() -> None:
    await capture_binance()
    await capture_kraken()


if __name__ == "__main__":
    asyncio.run(main())
