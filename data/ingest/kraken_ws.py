"""Async consumers for Kraken WebSocket market data."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence


class MissingDependencyError(RuntimeError):
    """Raised when optional ingest dependencies are unavailable."""


class _StreamRestart(RuntimeError):
    """Internal signal used to trigger a WebSocket reconnection."""


try:  # pragma: no cover - optional dependency
    import aiohttp
except Exception as exc:  # pragma: no cover - executed when aiohttp missing
    aiohttp = None  # type: ignore[assignment]
    _AIOHTTP_IMPORT_ERROR = exc
else:
    _AIOHTTP_IMPORT_ERROR = None

_SQLALCHEMY_AVAILABLE = True
_SQLALCHEMY_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency
    from sqlalchemy import BigInteger, Column, DateTime, JSON, MetaData, Numeric, String, Table
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.engine import Engine, create_engine
except Exception as exc:  # pragma: no cover - executed when SQLAlchemy absent
    _SQLALCHEMY_AVAILABLE = False
    _SQLALCHEMY_IMPORT_ERROR = exc
else:
    if not hasattr(Table, "c"):
        _SQLALCHEMY_AVAILABLE = False
        _SQLALCHEMY_IMPORT_ERROR = RuntimeError("SQLAlchemy table metadata is unavailable")

if not _SQLALCHEMY_AVAILABLE:
    BigInteger = Column = DateTime = JSON = Numeric = String = Table = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]

    def create_engine(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for Kraken ingest") from _SQLALCHEMY_IMPORT_ERROR

    def pg_insert(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for Kraken ingest") from _SQLALCHEMY_IMPORT_ERROR

    metadata = None
    orderbook_events_table = None
else:
    metadata = MetaData()
    orderbook_events_table = Table(
        "orderbook_events",
        metadata,
        Column("symbol", String(32), primary_key=True),
        Column("ts", DateTime(timezone=True), primary_key=True),
        Column("side", String(4), primary_key=True),
        Column("price", Numeric(28, 10), primary_key=True),
        Column("size", Numeric(28, 10), nullable=False),
        Column("action", String(16), nullable=False),
        Column("sequence", BigInteger, nullable=True),
        Column("meta", JSON, nullable=True),
    )

from shared.postgres import normalize_sqlalchemy_dsn

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - optional dependency
    Producer = None  # type: ignore

try:
    from nats.aio.client import Client as NATS
except ImportError:  # pragma: no cover - optional dependency
    NATS = None  # type: ignore

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

KRAKEN_WS_URL = os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com")
_RECONNECT_BASE_SECONDS = 1.0
_RECONNECT_MAX_SECONDS = 30.0
_RECONNECT_BACKOFF_CAP = 5
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kraken.marketdata")
_SQLITE_FALLBACK_FLAG = "KRAKEN_WS_ALLOW_SQLITE_FOR_TESTS"
_INSECURE_DEFAULTS_FLAG = "KRAKEN_WS_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "KRAKEN_WS_STATE_DIR"
_STATE_DIR_FALLBACK = ".aether_state/kraken_ws"
_POLL_INTERVAL_SECONDS = float(os.getenv("KRAKEN_WS_POLL_INTERVAL_SECONDS", "5"))


def _insecure_defaults_enabled() -> bool:
    """Return whether insecure defaults are explicitly enabled for local runs."""

    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1"


def _state_root() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, _STATE_DIR_FALLBACK))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _state_file_for_symbol(symbol: str) -> Path:
    normalised = symbol.replace("/", "_").replace(" ", "_").lower()
    return _state_root() / f"{normalised}.json"


def _require_aiohttp() -> None:
    if aiohttp is None:  # pragma: no cover - executed when aiohttp missing
        if _insecure_defaults_enabled():
            LOGGER.warning(
                "aiohttp is not available; falling back to local snapshot polling for Kraken ingest",
            )
            return
        raise MissingDependencyError("aiohttp is required for Kraken ingest") from _AIOHTTP_IMPORT_ERROR


def _require_sqlalchemy() -> None:
    if not _SQLALCHEMY_AVAILABLE or metadata is None or orderbook_events_table is None:
        if _insecure_defaults_enabled():
            LOGGER.warning(
                "SQLAlchemy is not available; using JSON persistence fallback for Kraken ingest",
            )
            return
        raise MissingDependencyError("SQLAlchemy is required for Kraken ingest") from _SQLALCHEMY_IMPORT_ERROR


def _load_database_url() -> str | None:
    """Best-effort database URL resolver used during module import."""

    raw_url = os.getenv("DATABASE_URL", "").strip()
    if not raw_url:
        if _insecure_defaults_enabled():
            return None
        raise RuntimeError(
            "Kraken ingest requires DATABASE_URL to be set to a PostgreSQL/Timescale DSN."
        )

    allow_sqlite = os.getenv(_SQLITE_FALLBACK_FLAG) == "1"
    database_url = normalize_sqlalchemy_dsn(
        raw_url,
        allow_sqlite=allow_sqlite,
        label="Kraken ingest database URL",
    )

    if database_url.startswith("sqlite"):
        LOGGER.warning(
            "Using SQLite database '%s' for Kraken ingest; allowed only for tests.",
            database_url,
        )

    return database_url


DATABASE_URL = _load_database_url()
_WARNED_MISSING_DATABASE_URL = False


def _database_url_or_fallback() -> str | None:
    """Return the configured DSN or raise when insecure defaults are disabled."""

    global _WARNED_MISSING_DATABASE_URL

    if DATABASE_URL:
        return DATABASE_URL

    if _insecure_defaults_enabled():
        if not _WARNED_MISSING_DATABASE_URL:
            LOGGER.warning(
                "DATABASE_URL is not configured; Kraken ingest will persist JSON snapshots locally",
            )
            _WARNED_MISSING_DATABASE_URL = True
        return None

    raise RuntimeError(
        "Kraken ingest requires DATABASE_URL to be set to a PostgreSQL/Timescale DSN."
    )
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222").split(",")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "marketdata.kraken.orderbook")


@dataclass
class OrderBookEvent:
    symbol: str
    ts: dt.datetime
    side: str
    price: float
    size: float
    action: str
    sequence: int | None
    meta: Dict[str, Any]


def kafka_producer() -> Producer | None:
    if Producer is None:
        LOGGER.warning("Kafka producer not available; messages will not be published")
        return None
    return Producer({"bootstrap.servers": KAFKA_BROKERS})


async def subscribe(session: aiohttp.ClientSession, pairs: Sequence[str]) -> aiohttp.ClientWebSocketResponse:
    _require_aiohttp()
    LOGGER.info("Connecting to Kraken WebSocket", extra={"url": KRAKEN_WS_URL, "pairs": pairs})
    ws = await session.ws_connect(KRAKEN_WS_URL)
    subscribe_message = {
        "event": "subscribe",
        "pair": list(pairs),
        "subscription": {"name": "book", "depth": 25},
    }
    await ws.send_str(json.dumps(subscribe_message))
    return ws


def flatten_updates(symbol: str, payload: Dict[str, Any]) -> List[OrderBookEvent]:
    """Normalise Kraken order book payloads into orderbook_events rows."""
    bids = payload.get("b") or payload.get("bs") or []
    asks = payload.get("a") or payload.get("as") or []
    timestamp = payload.get("timestamp")
    if not timestamp and bids:
        timestamp = bids[0][2] if len(bids[0]) > 2 else None
    if not timestamp and asks:
        timestamp = asks[0][2] if len(asks[0]) > 2 else None
    if timestamp is None:
        return []
    event_time = datetime_from_kraken(timestamp)
    sequence_value = payload.get("sequence", payload.get("checksum"))
    sequence = int(sequence_value) if sequence_value not in (None, "") else None
    action = "snapshot" if any(key in payload for key in ("as", "bs")) else "update"
    updates: List[OrderBookEvent] = []
    for side, levels in (("bid", bids), ("ask", asks)):
        for level in levels:
            price = float(level[0])
            size = float(level[1]) if len(level) > 1 else 0.0
            updates.append(
                OrderBookEvent(
                    symbol=symbol,
                    ts=event_time,
                    side=side,
                    price=price,
                    size=size,
                    action=action,
                    sequence=sequence,
                    meta={"payload": payload, "side": side, "price": price, "size": size},
                )
            )
    return updates


def datetime_from_kraken(timestamp: Any) -> dt.datetime:
    from datetime import datetime, timezone

    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    if isinstance(timestamp, str):
        try:
            return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
        except ValueError:
            dt_obj = datetime.fromisoformat(timestamp)
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return dt_obj
    dt_obj = datetime.fromisoformat(str(timestamp))
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj


def _persist_updates_locally(updates: Sequence[OrderBookEvent]) -> None:
    if not updates:
        return
    root = _state_root()
    for update in updates:
        symbol_file = root / f"{update.symbol.replace('/', '_').replace(' ', '_').lower()}_events.jsonl"
        payload = {
            "symbol": update.symbol,
            "ts": update.ts.isoformat(),
            "side": update.side,
            "price": update.price,
            "size": update.size,
            "action": update.action,
            "sequence": update.sequence,
            "meta": update.meta,
        }
        with symbol_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")


def persist_updates(engine: Engine | None, updates: Sequence[OrderBookEvent]) -> None:
    if not updates:
        return
    if not _SQLALCHEMY_AVAILABLE or metadata is None or orderbook_events_table is None or engine is None:
        if _insecure_defaults_enabled():
            _persist_updates_locally(updates)
            return
        raise MissingDependencyError("SQLAlchemy is required for Kraken ingest") from _SQLALCHEMY_IMPORT_ERROR

    with engine.begin() as connection:
        for update in updates:
            record = {
                "symbol": update.symbol,
                "ts": update.ts,
                "side": update.side,
                "price": update.price,
                "size": update.size,
                "action": update.action,
                "sequence": update.sequence,
                "meta": update.meta,
            }
            stmt = pg_insert(orderbook_events_table).values(**record)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    orderbook_events_table.c.symbol,
                    orderbook_events_table.c.ts,
                    orderbook_events_table.c.side,
                    orderbook_events_table.c.price,
                ],
                set_={
                    "size": stmt.excluded.size,
                    "action": stmt.excluded.action,
                    "sequence": stmt.excluded.sequence,
                    "meta": stmt.excluded.meta,
                },
            )
            connection.execute(stmt)


def publish_updates(producer: Producer | None, updates: Sequence[OrderBookEvent]) -> None:
    if producer is None:
        return
    for update in updates:
        payload = json.dumps(
            {
                "symbol": update.symbol,
                "ts": update.ts.isoformat(),
                "side": update.side,
                "price": update.price,
                "size": update.size,
                "action": update.action,
                "sequence": update.sequence,
                "meta": update.meta,
            }
        ).encode("utf-8")
        producer.produce(KAFKA_TOPIC, payload)
    producer.flush()


async def publish_to_nats(updates: Sequence[OrderBookEvent]) -> None:
    if NATS is None or not updates:
        return
    client = NATS()
    await client.connect(servers=NATS_SERVERS)
    try:
        for update in updates:
            payload = json.dumps(
                {
                    "symbol": update.symbol,
                    "ts": update.ts.isoformat(),
                    "side": update.side,
                    "price": update.price,
                    "size": update.size,
                    "action": update.action,
                    "sequence": update.sequence,
                    "meta": update.meta,
                }
            ).encode("utf-8")
            await client.publish(NATS_SUBJECT, payload)
    finally:
        await client.drain()


def _seed_local_snapshot(symbol: str, rng: random.Random) -> Dict[str, Any]:
    base_price = rng.uniform(50, 50000)
    tick = max(base_price * 0.0005, 0.05)
    depth = 5
    bids = []
    asks = []
    for level in range(depth):
        bid_price = base_price - tick * (level + 1)
        ask_price = base_price + tick * (level + 1)
        bids.append([f"{bid_price:.2f}", f"{rng.uniform(0.1, 1.5):.4f}", ""])
        asks.append([f"{ask_price:.2f}", f"{rng.uniform(0.1, 1.5):.4f}", ""])
    return {"b": bids, "a": asks, "sequence": 0}


def _apply_local_jitter(payload: Dict[str, Any], rng: random.Random, timestamp: str) -> Dict[str, Any]:
    bids = payload.get("b") or []
    asks = payload.get("a") or []
    drift = rng.uniform(-0.5, 0.5)

    def _update_levels(levels: List[List[Any]], direction: int) -> List[List[str]]:
        updated: List[List[str]] = []
        for idx, raw_level in enumerate(levels):
            try:
                price = float(raw_level[0])
            except (TypeError, ValueError, IndexError):
                price = rng.uniform(50, 50000)
            step = drift + rng.uniform(0.01, 0.2) * (idx + 1)
            price = max(0.01, price + step * direction)
            try:
                size = float(raw_level[1])
            except (TypeError, ValueError, IndexError):
                size = rng.uniform(0.05, 1.5)
            size = max(0.01, size + rng.uniform(-0.05, 0.05))
            updated.append([f"{price:.2f}", f"{size:.4f}", timestamp])
        return updated

    payload["b"] = _update_levels(bids, direction=1)
    payload["a"] = _update_levels(asks, direction=-1)
    payload["timestamp"] = timestamp
    payload["sequence"] = int(payload.get("sequence") or 0) + 1
    return payload


def _load_local_depth_snapshot(symbol: str) -> Dict[str, Any]:
    rng = random.Random(symbol)
    state_file = _state_file_for_symbol(symbol)
    if state_file.exists():
        try:
            payload = json.loads(state_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = _seed_local_snapshot(symbol, rng)
    else:
        payload = _seed_local_snapshot(symbol, rng)

    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    payload = _apply_local_jitter(payload, rng, timestamp)
    state_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


async def _consume_via_local_snapshots(
    engine: Engine | None,
    producer: Producer | None,
    pairs: Sequence[str],
    *,
    iterations: int | None = None,
) -> None:
    LOGGER.warning(
        "Using local snapshot fallback for Kraken ingest; enable aiohttp for live order-book streaming",
    )
    loop_count = 0
    while True:
        for pair in pairs:
            payload = _load_local_depth_snapshot(pair)
            updates = flatten_updates(pair, payload)
            if not updates:
                continue
            persist_updates(engine, updates)
            publish_updates(producer, updates)
            await publish_to_nats(updates)
        loop_count += 1
        if iterations is not None and loop_count >= iterations:
            break
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)


def _backoff_seconds(attempt: int) -> float:
    """Return an exponential backoff capped at :data:`_RECONNECT_MAX_SECONDS`."""

    if attempt <= 1:
        return _RECONNECT_BASE_SECONDS
    exponent = min(attempt - 1, _RECONNECT_BACKOFF_CAP)
    return min(_RECONNECT_BASE_SECONDS * (2 ** exponent), _RECONNECT_MAX_SECONDS)


async def _stream_websocket(
    session: aiohttp.ClientSession,
    engine: Engine | None,
    producer: Producer | None,
    pairs: Sequence[str],
) -> None:
    try:
        ws = await subscribe(session, pairs)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
        raise _StreamRestart(f"subscription failed: {exc}") from exc

    try:
        async for message in ws:
            if message.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(message.data)
                if isinstance(data, dict) and data.get("event") == "subscriptionStatus":
                    LOGGER.info("Subscription update", extra=data)
                    continue
                if not isinstance(data, list) or len(data) < 4:
                    continue
                channel_data = data[1]
                if not isinstance(channel_data, dict):
                    continue
                market = data[3]
                updates = flatten_updates(market, channel_data)
                if not updates:
                    continue
                persist_updates(engine, updates)
                publish_updates(producer, updates)
                await publish_to_nats(updates)
                continue

            if message.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
            }:
                raise _StreamRestart("websocket closed by remote host")
            if message.type == aiohttp.WSMsgType.ERROR:
                raise _StreamRestart("websocket error frame received")
        raise _StreamRestart("websocket stream ended")
    except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as exc:
        raise _StreamRestart(f"websocket error: {exc}") from exc
    finally:
        try:
            await ws.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            LOGGER.exception("Failed to close Kraken WebSocket", exc_info=True)


async def consume(pairs: Sequence[str], *, max_cycles: int | None = None) -> None:
    _require_sqlalchemy()
    engine: Engine | None = None
    if _SQLALCHEMY_AVAILABLE and metadata is not None and orderbook_events_table is not None:
        database_url = _database_url_or_fallback()
        if database_url is not None:
            engine = create_engine(database_url, future=True)
    producer = kafka_producer()

    if aiohttp is None and _insecure_defaults_enabled():
        await _consume_via_local_snapshots(engine, producer, pairs, iterations=max_cycles)
        return

    _require_aiohttp()
    reconnect_attempt = 0
    cycles = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                await _stream_websocket(session, engine, producer, pairs)
        except _StreamRestart as exc:
            cycles += 1
            reconnect_attempt += 1
            if max_cycles is not None and cycles >= max_cycles:
                LOGGER.info(
                    "Kraken WebSocket stream finished after %s cycles (%s); stopping without reconnect.",
                    cycles,
                    exc,
                )
                break
            delay = _backoff_seconds(reconnect_attempt)
            LOGGER.warning(
                "Kraken WebSocket disconnected (%s); reconnecting in %.1f seconds.",
                exc,
                delay,
            )
            await asyncio.sleep(delay)
            continue
        except Exception as exc:
            cycles += 1
            reconnect_attempt += 1
            if max_cycles is not None and cycles >= max_cycles:
                LOGGER.error(
                    "Kraken WebSocket encountered unrecoverable error after %s cycles: %s",
                    cycles,
                    exc,
                )
                raise
            delay = _backoff_seconds(reconnect_attempt)
            LOGGER.exception(
                "Kraken WebSocket error; retrying in %.1f seconds.",
                delay,
                exc_info=exc,
            )
            await asyncio.sleep(delay)
            continue


def main() -> None:
    pairs = os.getenv("KRAKEN_PAIRS", "BTC/USD,ETH/USD").split(",")
    asyncio.run(consume(pairs))


if __name__ == "__main__":
    main()
