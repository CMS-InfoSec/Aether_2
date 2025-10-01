"""Adaptive routing and latency tracking for Kraken OMS transports."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional

from services.common.config import get_timescale_session
from services.oms.kraken_rest import KrakenRESTClient, KrakenRESTError
from services.oms.kraken_ws import KrakenWSClient, KrakenWSError, KrakenWSTimeout

try:  # pragma: no cover - optional dependency in CI
    import psycopg
    from psycopg import sql
except Exception:  # pragma: no cover - fallback when psycopg is unavailable
    psycopg = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


class LatencyRouter:
    """Track Kraken transport latency and select the preferred path."""

    _SCHEMA_INITIALISED: set[str] = set()

    def __init__(
        self,
        account_id: str,
        *,
        window: int = 50,
        probe_interval: float = 30.0,
    ) -> None:
        self.account_id = account_id
        self._window = window
        self._probe_interval = probe_interval
        self._samples: Dict[str, Deque[float]] = {
            "websocket": deque(maxlen=window),
            "rest": deque(maxlen=window),
        }
        self._ws_client: Optional[KrakenWSClient] = None
        self._rest_client: Optional[KrakenRESTClient] = None
        self._probe_template: Dict[str, str] = {
            "pair": "XBT/USD",
            "type": "buy",
            "ordertype": "limit",
            "price": "1",
            "volume": "0.0001",
        }
        self._probe_tasks: list[asyncio.Task[None]] = []
        self._pending_writes: set[asyncio.Task[None]] = set()
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def start(
        self,
        ws_client: Optional[KrakenWSClient] = None,
        rest_client: Optional[KrakenRESTClient] = None,
    ) -> None:
        if ws_client is not None:
            self._ws_client = ws_client
        if rest_client is not None:
            self._rest_client = rest_client
        if self._running or self._probe_interval <= 0:
            return
        self._running = True
        self._probe_tasks = [
            asyncio.create_task(self._probe_loop("websocket")),
            asyncio.create_task(self._probe_loop("rest")),
        ]

    async def stop(self) -> None:
        self._running = False
        tasks = list(self._probe_tasks)
        self._probe_tasks.clear()
        for task in tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        pending = list(self._pending_writes)
        self._pending_writes.clear()
        for task in pending:
            task.cancel()

    def record_latency(self, path: str, latency_ms: float) -> None:
        key = "websocket" if path == "websocket" else "rest"
        if key not in self._samples:
            self._samples[key] = deque(maxlen=self._window)
        self._samples[key].append(latency_ms)
        self._schedule_persist(key, latency_ms)

    def update_probe_template(self, payload: Dict[str, str]) -> None:
        template: Dict[str, str] = {}
        for key in ("pair", "type", "ordertype", "price", "volume", "oflags", "timeInForce"):
            value = payload.get(key)
            if value is not None:
                template[key] = str(value)
        if template:
            self._probe_template.update(template)

    @property
    def preferred_path(self) -> str:
        ws_avg = self._average("websocket")
        rest_avg = self._average("rest")
        if ws_avg is None and rest_avg is None:
            return "websocket"
        if rest_avg is None:
            return "websocket"
        if ws_avg is None:
            return "rest"
        return "websocket" if ws_avg <= rest_avg else "rest"

    def status(self) -> Dict[str, Optional[float] | str]:
        return {
            "ws_latency": self._average("websocket"),
            "rest_latency": self._average("rest"),
            "preferred_path": self.preferred_path,
        }

    # ------------------------------------------------------------------
    # Latency persistence
    # ------------------------------------------------------------------
    def _schedule_persist(self, path: str, latency_ms: float) -> None:
        if psycopg is None:
            return
        timestamp = datetime.now(timezone.utc)
        task = asyncio.create_task(self._persist_latency(path, latency_ms, timestamp))
        self._pending_writes.add(task)
        task.add_done_callback(self._pending_writes.discard)

    async def _persist_latency(self, path: str, latency_ms: float, ts: datetime) -> None:
        if psycopg is None:
            return
        session = get_timescale_session(self.account_id)

        def _sync() -> None:
            assert psycopg is not None and sql is not None  # nosec - guarded by caller
            with psycopg.connect(session.dsn, autocommit=True) as conn:
                self._ensure_schema(conn, session.account_schema)
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.{} (account_id, path, ms, ts)
                    VALUES (%(account_id)s, %(path)s, %(ms)s, %(ts)s)
                    ON CONFLICT DO NOTHING
                    """
                ).format(
                    sql.Identifier(session.account_schema),
                    sql.Identifier("oms_latency"),
                )
                params = {
                    "account_id": self.account_id,
                    "path": path,
                    "ms": float(latency_ms),
                    "ts": ts,
                }
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql, params)

        await asyncio.to_thread(_sync)

    @classmethod
    def _ensure_schema(cls, conn: "psycopg.Connection[Any]", schema: str) -> None:
        assert psycopg is not None and sql is not None  # nosec - guarded by caller
        if schema in cls._SCHEMA_INITIALISED:
            return
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema)
                )
            )
            cursor.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        account_id TEXT NOT NULL,
                        path TEXT NOT NULL,
                        ms DOUBLE PRECISION NOT NULL,
                        ts TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (account_id, path, ts)
                    )
                    """
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier("oms_latency"),
                )
            )
            cursor.execute(
                sql.SQL(
                    """
                    CREATE INDEX IF NOT EXISTS {} ON {}.{} (account_id, ts DESC)
                    """
                ).format(
                    sql.Identifier(f"{schema}_oms_latency_account_ts_idx"),
                    sql.Identifier(schema),
                    sql.Identifier("oms_latency"),
                )
            )
        cls._SCHEMA_INITIALISED.add(schema)

    # ------------------------------------------------------------------
    # Probe helpers
    # ------------------------------------------------------------------
    async def _probe_loop(self, path: str) -> None:
        while self._running:
            try:
                latency = await self._probe(path)
                if latency is not None:
                    self.record_latency(path, latency)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.debug("Latency probe for %s failed: %s", path, exc)
            await asyncio.sleep(self._probe_interval)

    async def _probe(self, path: str) -> Optional[float]:
        if path == "websocket":
            return await self._probe_websocket()
        return await self._probe_rest()

    async def _probe_websocket(self) -> Optional[float]:
        if self._ws_client is None:
            return None
        payload = self._build_probe_payload()
        start = time.perf_counter()
        try:
            await self._ws_client.add_order(payload)
        except (KrakenWSTimeout, KrakenWSError) as exc:
            LOGGER.debug("Websocket probe failed: %s", exc)
            return None
        return (time.perf_counter() - start) * 1000.0

    async def _probe_rest(self) -> Optional[float]:
        if self._rest_client is None:
            return None
        payload = self._build_probe_payload()
        start = time.perf_counter()
        try:
            await self._rest_client.add_order(payload)
        except KrakenRESTError as exc:
            LOGGER.debug("REST probe failed: %s", exc)
            return None
        return (time.perf_counter() - start) * 1000.0

    def _build_probe_payload(self) -> Dict[str, str]:
        payload = dict(self._probe_template)
        payload.setdefault("pair", "XBT/USD")
        payload.setdefault("type", "buy")
        payload.setdefault("ordertype", "limit")
        payload.setdefault("volume", "0.0001")
        if payload.get("ordertype") == "limit":
            payload.setdefault("price", "1")
        payload["validate"] = True
        payload["clientOrderId"] = f"latency-{uuid.uuid4().hex}"[:32]
        return payload

    def _average(self, key: str) -> Optional[float]:
        samples = self._samples.get(key)
        if not samples:
            return None
        return sum(samples) / len(samples)


__all__ = ["LatencyRouter"]
