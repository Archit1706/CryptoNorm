"""FastAPI dashboard backend: serves the UI and pushes live state over WS."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from cryptonorm.api.state import build_state
from cryptonorm.common.config import get_settings
from cryptonorm.common.logging import configure_logging, get_logger
from cryptonorm.pipeline.redis_state import RedisState

_STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level, json=settings.log_json)
    app.state.settings = settings
    app.state.redis = RedisState(settings.redis_url)
    get_logger("api").info("dashboard up", port=settings.api_port)
    try:
        yield
    finally:
        await app.state.redis.close()


app = FastAPI(title="cryptonorm dashboard", lifespan=lifespan)


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse((_STATIC / "index.html").read_text(encoding="utf-8"))


@app.get("/api/state")
async def api_state() -> JSONResponse:
    return JSONResponse(await build_state(app.state.redis, app.state.settings))


@app.get("/healthz")
async def healthz() -> dict[str, bool]:
    return {"ok": await app.state.redis.ping()}


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    await websocket.accept()
    settings = websocket.app.state.settings
    redis = websocket.app.state.redis
    try:
        while True:
            await websocket.send_json(await build_state(redis, settings))
            await asyncio.sleep(settings.dashboard_interval_seconds)
    except WebSocketDisconnect:
        pass
