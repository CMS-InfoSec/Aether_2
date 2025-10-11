
"""Batch ingestion job for CoinGecko metrics and whitelist publication."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Tuple
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency is required for CoinGecko ingest."""


try:  # pragma: no cover - optional dependency
    import requests
except Exception as exc:  # pragma: no cover - executed in lightweight environments
    requests = None  # type: ignore[assignment]
    _REQUESTS_IMPORT_ERROR = exc
else:
    _REQUESTS_IMPORT_ERROR = None

_SQLALCHEMY_AVAILABLE = True
_SQLALCHEMY_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency
    from sqlalchemy import Boolean, Column, DateTime, JSON, MetaData, Numeric, String, Table
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
    Boolean = Column = DateTime = JSON = Numeric = String = Table = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]

    def create_engine(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for CoinGecko ingest") from _SQLALCHEMY_IMPORT_ERROR

    def pg_insert(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for CoinGecko ingest") from _SQLALCHEMY_IMPORT_ERROR

    metadata = None
    whitelist_table = None
    metrics_table = None
else:
    metadata = MetaData()
    whitelist_table = Table(
        "universe_whitelist",
        metadata,
        Column("asset_id", String, primary_key=True),
        Column("as_of", DateTime(timezone=True), primary_key=True),
        Column("source", String, nullable=False),
        Column("approved", Boolean, nullable=False),
        Column("metadata", JSON, nullable=False),
    )
    metrics_table = Table(
        "features",
        metadata,
        Column("feature_name", String, primary_key=True),
        Column("entity_id", String, primary_key=True),
        Column("event_timestamp", DateTime(timezone=True), primary_key=True),
        Column("value", Numeric),
        Column("created_at", DateTime(timezone=True), nullable=False, default=dt.datetime.utcnow),
        Column("metadata", JSON, nullable=False),
    )

from shared.postgres import normalize_sqlalchemy_dsn

try:
    from nats.aio.client import Client as NATS
except ImportError:  # pragma: no cover - optional dependency
    NATS = None  # type: ignore

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

_INSECURE_DEFAULTS_FLAG = "COINGECKO_ALLOW_INSECURE_DEFAULTS"
_SQLITE_FALLBACK_FLAG = "COINGECKO_ALLOW_SQLITE_FOR_TESTS"
_STATE_DIR_ENV = "COINGECKO_STATE_DIR"


def _allow_insecure_defaults() -> bool:
    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1"


def _allow_sqlite_fallback() -> bool:
    return _allow_insecure_defaults() or os.getenv(_SQLITE_FALLBACK_FLAG) == "1"


def _state_dir() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, ".aether_state/coingecko"))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _json_default(value: object) -> object:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serialisable")


def _write_state_file(name: str, payload: object) -> Path:
    path = _state_dir() / name
    serialised = json.dumps(payload, indent=2, default=_json_default)
    path.write_text(serialised + "\n", encoding="utf-8")
    return path


def _require_requests() -> None:
    if requests is None:  # pragma: no cover - executed when requests missing
        raise MissingDependencyError("requests is required for CoinGecko ingest") from _REQUESTS_IMPORT_ERROR


def _require_sqlalchemy() -> None:
    if not _SQLALCHEMY_AVAILABLE or metadata is None or metrics_table is None or whitelist_table is None:
        raise MissingDependencyError("SQLAlchemy is required for CoinGecko ingest") from _SQLALCHEMY_IMPORT_ERROR

COINGECKO_API = os.getenv("COINGECKO_API", "https://api.coingecko.com/api/v3")


def _resolve_database_url() -> str:
    """Return the configured Timescale/PostgreSQL DSN for persistence."""

    raw_url = os.getenv("DATABASE_URL", "")
    if not raw_url.strip():
        if _allow_sqlite_fallback():
            fallback = _state_dir() / "coingecko_ingest.sqlite3"
            LOGGER.warning(
                "DATABASE_URL not configured; falling back to SQLite at %s because insecure defaults are enabled.",
                fallback,
            )
            return f"sqlite:///{fallback}"
        raise RuntimeError(
            "CoinGecko ingest requires DATABASE_URL to be set to a PostgreSQL/Timescale DSN."
        )

    allow_sqlite = _allow_sqlite_fallback()
    database_url = normalize_sqlalchemy_dsn(
        raw_url.strip(),
        allow_sqlite=allow_sqlite,
        label="CoinGecko ingest database URL",
    )

    if database_url.startswith("sqlite"):
        LOGGER.warning(
            "Using SQLite database '%s' for CoinGecko ingest; allowed only for tests.",
            database_url,
        )

    return database_url


_DATABASE_URL_ERROR: RuntimeError | None = None
if _allow_insecure_defaults():
    try:
        DATABASE_URL = _resolve_database_url()
    except RuntimeError as exc:  # pragma: no cover - exercised in insecure default environments
        DATABASE_URL = None
        _DATABASE_URL_ERROR = exc
else:
    DATABASE_URL = _resolve_database_url()
WHITELIST_TOPIC = os.getenv("WHITELIST_TOPIC", "universe.whitelist")
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222").split(",")


@dataclass
class AssetMetric:
    asset_id: str
    market_cap: float
    volume_24h: float
    price: float
    fetched_at: dt.datetime


class CoinGeckoClient:
    """Client responsible for pulling metrics from CoinGecko."""


    def __init__(self, session: requests.Session | None = None) -> None:
        if session is not None:
            if requests is None:
                raise MissingDependencyError(
                    "requests is required for CoinGecko ingest"
                ) from _REQUESTS_IMPORT_ERROR
            self.session = session
            self._use_requests = True
        elif requests is not None:
            self.session = requests.Session()
            self._use_requests = True
        elif _allow_insecure_defaults():
            LOGGER.warning(
                "requests is unavailable; falling back to urllib for CoinGecko metrics fetches because insecure defaults are enabled."
            )
            self.session = None
            self._use_requests = False
        else:
            raise MissingDependencyError(
                "requests is required for CoinGecko ingest"
            ) from _REQUESTS_IMPORT_ERROR


    def fetch_top_assets(self, vs_currency: str = "usd", limit: int = 100) -> List[AssetMetric]:
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1,
            "sparkline": "false",
        }
        url = f"{COINGECKO_API}/coins/markets"
        LOGGER.info("Fetching CoinGecko market data", extra={"url": url, "params": params})
        if self._use_requests:
            assert self.session is not None
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
        else:
            payload = _fallback_http_get_json(url, params, timeout=60)
        fetched_at = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        metrics = [
            AssetMetric(
                asset_id=item["symbol"].upper(),
                market_cap=float(item.get("market_cap", 0.0)),
                volume_24h=float(item.get("total_volume", 0.0)),
                price=float(item.get("current_price", 0.0)),
                fetched_at=fetched_at,
            )
            for item in payload
        ]
        LOGGER.info("Fetched %d CoinGecko assets", len(metrics))
        return metrics


def _fallback_http_get_json(
    url: str, params: Mapping[str, object], *, timeout: float = 60.0
) -> List[Mapping[str, Any]]:
    query = urllib_parse.urlencode(list(params.items()), doseq=True)
    request_url = f"{url}?{query}" if query else url
    request_obj = urllib_request.Request(
        request_url, headers={"Accept": "application/json"}
    )
    try:
        with urllib_request.urlopen(request_obj, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except urllib_error.HTTPError as exc:  # pragma: no cover - HTTP error handling
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"CoinGecko HTTP error {exc.code}: {body[:256]}"
        ) from exc
    except Exception as exc:  # pragma: no cover - network failure handling
        raise RuntimeError(f"CoinGecko request failed: {exc}") from exc

    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid payload guard
        raise RuntimeError("CoinGecko returned invalid JSON payload") from exc

    if not isinstance(decoded, list):  # pragma: no cover - defensive guard
        raise RuntimeError("CoinGecko response did not return a list of assets")
    return decoded  # type: ignore[return-value]


def _configure_persistence() -> Tuple[Engine | None, bool]:
    """Return the persistence backend and whether the local fallback is active."""

    if _SQLALCHEMY_AVAILABLE and DATABASE_URL:
        return create_engine(DATABASE_URL, future=True), False

    allow_fallback = _allow_insecure_defaults()

    if _SQLALCHEMY_AVAILABLE and _DATABASE_URL_ERROR is not None:
        if allow_fallback:
            LOGGER.warning(
                "DATABASE_URL unavailable (%s); persisting CoinGecko metrics to local state store.",
                _DATABASE_URL_ERROR,
            )
            return None, True
        raise _DATABASE_URL_ERROR

    if _SQLALCHEMY_AVAILABLE and DATABASE_URL:
        return create_engine(DATABASE_URL, future=True), False

    if _SQLALCHEMY_AVAILABLE and not allow_fallback:
        raise MissingDependencyError(
            "SQLAlchemy is required for CoinGecko ingest"
        ) from _SQLALCHEMY_IMPORT_ERROR

    if allow_fallback:
        if not _SQLALCHEMY_AVAILABLE:
            LOGGER.warning(
                "SQLAlchemy is unavailable; persisting CoinGecko metrics to local state store because insecure defaults are enabled."
            )
        return None, True

    raise MissingDependencyError(
        "SQLAlchemy is required for CoinGecko ingest"
    ) from _SQLALCHEMY_IMPORT_ERROR


def upsert_metrics(engine: Engine | None, metrics: Iterable[AssetMetric]) -> None:
    metrics_list = list(metrics)
    if not metrics_list:
        LOGGER.info('No CoinGecko metrics to persist')
        return
    if engine is None or not _SQLALCHEMY_AVAILABLE or metrics_table is None:
        snapshot = [
            {
                "asset_id": metric.asset_id,
                "market_cap": metric.market_cap,
                "volume_24h": metric.volume_24h,
                "price": metric.price,
                "fetched_at": metric.fetched_at,
            }
            for metric in metrics_list
        ]
        path = _write_state_file("metrics.json", {"metrics": snapshot})
        LOGGER.info(
            "Persisted CoinGecko metrics to local state store", extra={"path": str(path)}
        )
        return

    _require_sqlalchemy()
    with engine.begin() as connection:
        for metric in metrics_list:
            stmt = pg_insert(metrics_table).values(
                feature_name="coingecko_market_cap",
                entity_id=metric.asset_id,
                event_timestamp=metric.fetched_at,
                value=metric.market_cap,
                metadata={
                    "price": metric.price,
                    "volume_24h": metric.volume_24h,
                    "source": "coingecko",
                },
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    metrics_table.c.feature_name,
                    metrics_table.c.entity_id,
                    metrics_table.c.event_timestamp,
                ],
                set_={
                    "value": stmt.excluded.value,
                    "metadata": stmt.excluded.metadata,
                },
            )
            connection.execute(stmt)
        LOGGER.info("Persisted CoinGecko metrics", extra={"count": len(metrics_list)})


def publish_whitelist(
    engine: Engine | None, assets: Iterable[AssetMetric]
) -> List[Mapping[str, object]]:
    asset_list = list(assets)
    whitelist_records: List[Mapping[str, object]] = []
    if not asset_list:
        LOGGER.info('No assets to whitelist from CoinGecko')
        return []
    as_of = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    for asset in asset_list:
        record = {
            "asset_id": asset.asset_id,
            "as_of": as_of,
            "source": "coingecko",
            "approved": True,
            "metadata": {
                "market_cap": asset.market_cap,
                "price": asset.price,
            },
        }
        whitelist_records.append(record)

    if engine is None or not _SQLALCHEMY_AVAILABLE or whitelist_table is None:
        path = _write_state_file("whitelist.json", {"whitelist": whitelist_records})
        LOGGER.info(
            "Persisted whitelist snapshot to local state store", extra={"path": str(path)}
        )
        return whitelist_records

    _require_sqlalchemy()
    with engine.begin() as connection:
        for record in whitelist_records:
            stmt = pg_insert(whitelist_table).values(**record)
            stmt = stmt.on_conflict_do_update(
                index_elements=[whitelist_table.c.asset_id, whitelist_table.c.as_of],
                set_={"metadata": stmt.excluded.metadata, "approved": stmt.excluded.approved},
            )
            connection.execute(stmt)
    LOGGER.info("Updated whitelist", extra={'count': len(whitelist_records)})
    return whitelist_records


async def publish_to_nats(payloads: Iterable[Mapping[str, object]]) -> None:
    if NATS is None:
        LOGGER.warning("NATS client not available; skipping publication")
        return
    client = NATS()
    await client.connect(servers=NATS_SERVERS)
    try:
        for payload in payloads:
            await client.publish(WHITELIST_TOPIC, json.dumps(payload, default=str).encode("utf-8"))
            LOGGER.debug("Published whitelist asset", extra=payload)
    finally:
        await client.drain()


def main() -> None:
    LOGGER.info("Starting CoinGecko ingestion job")
    client = CoinGeckoClient()
    engine, using_local_store = _configure_persistence()
    metrics = client.fetch_top_assets()
    upsert_metrics(engine, metrics)
    whitelist_records = publish_whitelist(engine, metrics)
    if using_local_store:
        LOGGER.info(
            "CoinGecko ingest completed using local persistence under %s",
            _state_dir(),
        )
    try:
        import asyncio

        asyncio.run(publish_to_nats(whitelist_records))
    except RuntimeError:
        LOGGER.exception("Failed to publish whitelist to NATS")



if __name__ == "__main__":
    main()
