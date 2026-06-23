"""Capture live binance.us + Kraken v2 frames and SAVE them as test fixtures.

binance.com is geoblocked (HTTP 451) from many regions, so we use binance.us
(identical schema, native USD pairs). Run once to (re)generate fixtures.
"""

import asyncio
import json
from pathlib import Path

import aiohttp
import websockets

FIX = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
BINANCE_WS = "wss://stream.binance.us:9443/ws"
BINANCE_REST = "https://api.binance.us/api/v3/depth?symbol=BTCUSD&limit=10"
KRAKEN_WS = "wss://ws.kraken.com/v2"
KRAKEN_ASSETPAIRS = "https://api.kraken.com/0/public/AssetPairs?pair=XBTUSD,ETHUSD"


def save(name: str, obj: dict) -> None:
    (FIX / name).write_text(json.dumps(obj, indent=2))
    print(f"saved {name}")


async def binance() -> None:
    async with aiohttp.ClientSession() as s, s.get(BINANCE_REST) as r:
        save("binance_rest_snapshot.json", await r.json())
    async with websockets.connect(BINANCE_WS, open_timeout=15) as ws:
        await ws.send(json.dumps(
            {"method": "SUBSCRIBE", "params": ["btcusd@depth", "btcusd@trade"], "id": 1}
        ))
        need = {"depthUpdate", "trade"}
        while need:
            obj = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            e = obj.get("e")
            if e == "depthUpdate" and "depthUpdate" in need:
                save("binance_depth_update.json", obj)
                need.discard("depthUpdate")
            elif e == "trade" and "trade" in need:
                save("binance_trade.json", obj)
                need.discard("trade")


async def kraken() -> None:
    async with aiohttp.ClientSession() as s, s.get(KRAKEN_ASSETPAIRS) as r:
        save("kraken_assetpairs.json", await r.json())
    async with websockets.connect(KRAKEN_WS, open_timeout=15) as ws:
        await ws.send(json.dumps(
            {"method": "subscribe", "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 10}}
        ))
        await ws.send(json.dumps(
            {"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}
        ))
        need = {"book:snapshot", "book:update", "trade:update"}
        while need:
            obj = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
            key = f"{obj.get('channel')}:{obj.get('type')}"
            if key == "book:snapshot" and key in need:
                save("kraken_book_snapshot.json", obj)
                need.discard(key)
            elif key == "book:update" and key in need:
                save("kraken_book_update.json", obj)
                need.discard(key)
            elif key == "trade:update" and key in need:
                save("kraken_trade.json", obj)
                need.discard(key)


async def main() -> None:
    await binance()
    await kraken()


if __name__ == "__main__":
    asyncio.run(main())
