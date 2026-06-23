# cryptonorm

Real-time crypto market-data normalizer + P&L / risk dashboard.

Ingests live order-book and trade data from **Coinbase, Binance (.us), and
Kraken** over their public WebSocket APIs, normalizes every venue's differing
schema into one internal contract, streams it through **Kafka**, maintains
paper positions with real-time mark-to-market **P&L and risk**, and serves a
trader-facing **dashboard** with live spreads, exposure, and threshold alerts.

> A portfolio project demonstrating production-grade, event-driven async
> Python. **Not** a trading strategy. Public market data only — no API keys,
> no auth, no real orders.

All six phases are complete; `docker compose up` brings up the whole stack.

---

## What it does

- **Ingest** live L2 order book + trades from 3 exchanges (BTC-USD, ETH-USD).
- **Normalize** each venue's schema into one pydantic contract (the system's
  internal language).
- **Stream** normalized events through Kafka (producer → topics → consumers).
- **Maintain** L2 books, positions (from a paper-fill simulator), and compute
  real-time mark-to-market P&L, exposure, drawdown, and VaR.
- **Serve** a live dashboard (FastAPI + WebSocket) with P&L, per-asset
  exposure, cross-exchange spreads, feed health, and visible alerts.

### Reliability (the point of the project)

- **Auto-reconnect** with exponential backoff + full jitter.
- **Order-book gap detection + resync**, implemented correctly *per exchange*:
  - Coinbase — connection-level `sequence_num` continuity.
  - Binance — REST snapshot seed + per-symbol `U`/`u` continuity.
  - Kraken — CRC32 book checksum on every update (validated against live frames).
- **Staleness monitor** — flags a feed STALE if silent past a threshold (and
  catches total ingest death).
- **Reconciliation job** — cross-checks computed positions against the
  simulator's source-of-truth ledger; logs/alerts on mismatch.
- **Graceful shutdown** — SIGTERM/SIGINT drain consumers and close sockets
  (verified under `docker stop`).

---

## Architecture

```
                        public WebSocket feeds
       ┌───────────────┬───────────────────┬────────────────┐
   Coinbase        Binance(.us)          Kraken v2
   (seq_num)     (REST snap + U/u)     (CRC32 checksum)
       │               │                   │
       └──── ingest service (adapter per venue, behind reconnect wrapper) ────┘
                       │  normalize → NormalizedEvent (the contract)
                       ▼
                 ┌──────────────┐   Kafka (KRaft)
                 │  md.book.v1  │   md.trade.v1   paper.fills.v1
                 └──────────────┘        ▲              ▲
                       │                 │              │
                       ▼                 │         paper-fill simulator
        pipeline / risk consumer ────────┘         (sim → Kafka + ledger)
            • L2 books → BBO / spread
            • positions → P&L, exposure, drawdown, VaR
            • staleness watchdog + reconciliation
                       │ current-state cache
                       ▼
                    ┌───────┐        ┌─────────────────────────┐
                    │ Redis │ ◀───── │ FastAPI  ──WS──▶ browser │
                    └───────┘        │  dashboard (HTML/JS)     │
                                     └─────────────────────────┘
```

### Module layout (`src/cryptonorm/`)

| Package      | Responsibility |
|--------------|----------------|
| `common/`    | normalized schema (the contract), config, structlog logging, symbol map, errors, shutdown |
| `ingest/`    | adapter ABC + registry; Coinbase / Binance / Kraken adapters; reconnect wrapper |
| `normalize/` | raw venue frame → `NormalizedEvent`, one module per exchange |
| `pipeline/`  | Kafka producer/consumer, topic routing, JSON serde, Redis state cache |
| `risk/`      | L2 book, positions, P&L/VaR engine, spread, alerts, reconciliation, staleness |
| `sim/`       | paper-fill simulator |
| `api/`       | FastAPI app + WS, state assembler, static dashboard |
| `services/`  | entrypoints: `run_ingest`, `run_pipeline`, `run_sim`, `run_api` |

### The contract — normalized event schema

A discriminated union on `event_type` (see [`schemas.py`](src/cryptonorm/common/schemas.py)):
`book_snapshot`, `book_delta`, `trade`, `fill`. Prices/sizes are **`Decimal`**,
serialized to JSON as **strings**, so they round-trip over Kafka without float
drift. A common envelope carries `exchange`, canonical `symbol`,
`exchange_symbol`, timestamps, and `sequence`.

### Kafka topics & Redis keys

- Topics (keyed by canonical symbol): `md.book.v1`, `md.trade.v1`, `paper.fills.v1`.
- Redis: `cn:bbo:*`, `cn:book:*`, `cn:trade:*`, `cn:status:*` (feed health),
  `cn:ledger:*` (recon truth), `cn:risk:snapshot`, `cn:recon`, `cn:feeds`.

---

## Quick start (Docker — the primary path)

Requires Docker Desktop. Brings up Kafka, Redis, and all four app services.

```bash
docker compose up -d --build
docker compose ps                 # wait for kafka/redis "healthy"
open http://localhost:8000        # the dashboard
```

You should see live BBO from all three venues, cross-exchange spreads, paper
positions accruing, and P&L / drawdown / VaR updating ~1×/s.

```bash
docker compose logs -f pipeline   # follow the risk consumer
docker compose down               # stop everything (clears Kafka/Redis — no volumes)
```

> **First build note:** the four app services share one image. If a parallel
> build races on the image tag, run `docker compose build pipeline` once, then
> `docker compose up -d`.

**Host ports:** dashboard `:8000`, Kafka `:29092`, Redis `:6380` — Kafka and
Redis use non-default host ports so they never clash with a local install.

## Running on the host (development)

Useful for iterating without rebuilding images. Requires Python 3.11+.

```bash
py -3.12 -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"   # Windows
cp .env.example .env

docker compose up -d kafka redis                  # just the infrastructure
.venv/Scripts/python -m cryptonorm.services.run_ingest     # terminal 1
.venv/Scripts/python -m cryptonorm.services.run_pipeline   # terminal 2
.venv/Scripts/python -m cryptonorm.services.run_sim        # terminal 3
.venv/Scripts/python -m cryptonorm.services.run_api        # terminal 4 -> :8000
```

Configuration is via env vars (`CN_*`); see [`.env.example`](.env.example).

## Tests, lint, types

```bash
.venv/Scripts/python -m pytest -q          # 56 tests
.venv/Scripts/python -m ruff check src tests
.venv/Scripts/python -m mypy
```

Tests cover: per-exchange normalization (against **recorded live frames**),
the Kraken CRC32 checksum (validated against a captured snapshot), P&L math
(opens/adds/partial-close/flip/short-cover), sequence-gap detection,
backoff/reconnect, staleness, spread, alerts, and reconciliation. Exchange
schemas are **not fabricated** — samples are captured live (`scripts/`) and
recorded under `tests/fixtures/`.

---

## Design decisions & trade-offs

- **`Decimal` everywhere money-touching, serialized as JSON strings.** Float
  drift is unacceptable in P&L. The cost is negligible at these volumes.

- **Positions marked at each venue's own mid.** Most realistic; also makes the
  cross-exchange spread a natural side-output. (Alternative: one consolidated
  mid — simpler but hides per-venue basis.)

- **Full L2 book maintenance** (not just top-N) so sequence-gap detection +
  re-snapshot resync are real. Each adapter owns its feed integrity and raises
  `FeedGapError`; the reconnect wrapper resyncs by reconnecting. Gap detection
  is genuinely *per-exchange* (counter / `U`-`u` / checksum), which only became
  clear from live data — e.g. Coinbase's `sequence_num` is connection-level,
  not per-symbol.

- **binance.us, not binance.com.** binance.com returns HTTP 451 (geoblocked)
  in many regions; binance.us has native USD pairs (no USDT proxy). Identical
  WS/REST schema.

- **Kraken WS v2** (`BTC/USD`, not v1's `XBT/USD`). Its depth book is
  fixed-size and pushes levels out of the window *without* sending removals, so
  the shadow book must **trim to depth** or the checksum diverges — found via
  live replay, fixed, and regression-tested.

- **Large Kafka messages.** A full L2 snapshot is ~1.6 MB, over Kafka's 1 MB
  default; the ceiling is raised on producer, consumer, and broker.

- **Plain HTML/JS dashboard over Streamlit.** Streamlit reruns the whole script
  per update — clumsy for sub-second WebSocket push and a custom alert banner.
  One static file served by FastAPI gives true streaming and trivial
  containerization.

- **Reconciliation is detect-and-alert, not auto-heal.** The simulator writes
  the ledger directly (source of truth) while the engine derives positions from
  Kafka — two independent paths. **Known trade-off:** the engine rebuilds
  positions by consuming the fills topic, so a mid-life pipeline restart with a
  committed offset would under-count until a clean replay; the recon job exists
  precisely to surface such divergence. Production would persist position state
  or replay from a log-compacted topic. A clean run starts from clean topics
  (`docker compose down` clears them, since no volumes are mounted).

- **Non-default host ports (Kafka 29092, Redis 6380).** On Windows, `localhost`
  resolves to IPv6 `::1`, which a pre-existing local Redis can shadow — caught
  during Phase 2 when writes silently hit the wrong instance.

## Adding a 4th exchange

1. Add the canonical↔raw symbol mapping in [`common/types.py`](src/cryptonorm/common/types.py).
2. Write `normalize/<venue>.py` (raw frame → `NormalizedEvent`).
3. Write `ingest/<venue>.py` (transport + gap detection) and register it in
   [`ingest/registry.py`](src/cryptonorm/ingest/registry.py).

Nothing downstream changes — the normalized schema is the contract.

## Deployment

`docker compose up` is the deployable artifact: it runs on any single host with
Docker (a small VM / VPS, or a container host like Fly.io / Railway / Render).

**Not deployable to Vercel.** Vercel runs stateless serverless functions and
static sites — it cannot host always-on background workers (the ingest /
pipeline / sim loops), a Kafka broker, Redis, or a long-lived WebSocket server.
This system is inherently stateful and always-on; it needs a real host.
