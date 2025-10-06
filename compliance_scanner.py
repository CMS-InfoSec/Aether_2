"""FastAPI service that maintains regulatory sanctions watchlists."""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
from fastapi import Depends, FastAPI
from sqlalchemy import delete
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from shared.postgres import normalize_sqlalchemy_dsn

from services.common.compliance import SanctionRecord, ensure_sanctions_schema
from services.common.security import require_admin_account


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


_REFRESH_INTERVAL = timedelta(days=1)
_REQUEST_TIMEOUT = float(os.getenv("COMPLIANCE_FETCH_TIMEOUT", "10"))

_DATABASE_ENV_KEYS = (
    "COMPLIANCE_DATABASE_URL",
    "RISK_DATABASE_URL",
    "TIMESCALE_DSN",
    "DATABASE_URL",
)

_DEFAULT_SOURCE_DATA: Dict[str, Dict[str, str]] = {
    "OFAC": {
        "RUSL": "sanctioned",
        "IRN": "sanctioned",
        "PRK": "sanctioned",
    },
    "FCA": {
        "XYZCN": "restricted",
        "DV8": "under_review",
    },
}


def _allow_sqlite() -> bool:
    """Return whether sqlite DSNs are permitted (pytest contexts only)."""

    return "pytest" in sys.modules


def _database_url() -> str:
    """Resolve the configured compliance database connection string."""

    allow_sqlite = _allow_sqlite()
    for env_key in _DATABASE_ENV_KEYS:
        raw_value = os.getenv(env_key)
        if not raw_value:
            continue
        return normalize_sqlalchemy_dsn(
            raw_value,
            allow_sqlite=allow_sqlite,
            label="Compliance scanner database URL",
        )

    if allow_sqlite:
        raise RuntimeError(
            "Compliance scanner database URL must be configured even under pytest; "
            "set COMPLIANCE_DATABASE_URL to a PostgreSQL/Timescale DSN or explicit "
            "sqlite URL for tests.",
        )

    raise RuntimeError(
        "Compliance scanner requires COMPLIANCE_DATABASE_URL or TIMESCALE_DSN "
        "to be configured with a PostgreSQL/Timescale DSN.",
    )


def _engine_options(url: str) -> Dict[str, object]:
    options: Dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


_DB_URL = _database_url()
ENGINE = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


app = FastAPI(title="Compliance Scanner", version="1.0.0")

_scan_lock = asyncio.Lock()
_background_task: Optional[asyncio.Task[None]] = None
_last_updated: Optional[datetime] = None
_symbols_checked: int = 0


async def _load_watchlist_from_url(
    client: httpx.AsyncClient, url: str
) -> List[Tuple[str, str]]:
    """Fetch a sanctions list from a configurable URL."""

    response = await client.get(url)
    response.raise_for_status()
    entries: List[Tuple[str, str]] = []
    content_type = response.headers.get("content-type", "")
    text = response.text
    if "json" in content_type:
        payload = response.json()
        entries.extend(_parse_json_payload(payload))
    else:
        entries.extend(_parse_delimited_payload(text))
    return entries


def _parse_json_payload(payload: object) -> List[Tuple[str, str]]:
    """Parse arbitrary JSON payloads into sanctions entries."""

    entries: List[Tuple[str, str]] = []
    if isinstance(payload, list):
        iterable: Iterable[object] = payload
    elif isinstance(payload, dict):
        iterable = payload.get("sanctions") or payload.get("results") or []
        if isinstance(iterable, dict):
            iterable = [
                {"symbol": key, "status": value}
                for key, value in iterable.items()
            ]
    else:
        iterable = []

    for item in iterable:
        if isinstance(item, dict):
            symbol = str(item.get("symbol") or item.get("ticker") or "").strip()
            status = str(item.get("status") or item.get("state") or "sanctioned").strip()
            if symbol:
                entries.append((symbol.upper(), status.lower()))
        elif isinstance(item, (list, tuple)) and len(item) >= 1:
            symbol = str(item[0]).strip()
            status = str(item[1]).strip() if len(item) > 1 else "sanctioned"
            if symbol:
                entries.append((symbol.upper(), status.lower()))
    return entries


def _parse_delimited_payload(text: str) -> List[Tuple[str, str]]:
    """Parse CSV or newline delimited text of sanctions entries."""

    entries: List[Tuple[str, str]] = []
    sample = text.strip().splitlines()
    if not sample:
        return entries
    reader = csv.reader(sample)
    for row in reader:
        if not row:
            continue
        symbol = row[0].strip()
        status = row[1].strip() if len(row) > 1 else "sanctioned"
        if symbol:
            entries.append((symbol.upper(), status.lower()))
    return entries


def _parse_inline_configuration(raw: Optional[str]) -> List[Tuple[str, str]]:
    entries: List[Tuple[str, str]] = []
    if not raw:
        return entries
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if ":" in token:
            symbol, status = token.split(":", 1)
        else:
            symbol, status = token, "sanctioned"
        symbol = symbol.strip()
        status = status.strip() or "sanctioned"
        if symbol:
            entries.append((symbol.upper(), status.lower()))
    return entries


async def _fetch_source_entries(
    client: httpx.AsyncClient, source: str
) -> List[Tuple[str, str]]:
    """Retrieve sanctions for the provided source."""

    url = os.getenv(f"{source}_SANCTIONS_URL")
    inline = os.getenv(f"{source}_SANCTIONS_SYMBOLS")
    entries: List[Tuple[str, str]] = []
    if url:
        try:
            entries.extend(await _load_watchlist_from_url(client, url))
        except Exception as exc:  # pragma: no cover - network resilience
            logger.warning("Failed to pull %s sanctions from %s: %s", source, url, exc)
    if not entries:
        entries.extend(_parse_inline_configuration(inline))
    if not entries:
        defaults = _DEFAULT_SOURCE_DATA.get(source, {})
        entries.extend((symbol, status) for symbol, status in defaults.items())
    return entries


def _upsert_sanctions(session: Session, source: str, entries: Sequence[Tuple[str, str]], ts: datetime) -> int:
    """Persist sanctions entries for the given source."""

    deleted = session.execute(
        delete(SanctionRecord).where(SanctionRecord.source == source)
    )
    logger.debug("Removed %s previous sanctions rows for %s", deleted.rowcount, source)

    count = 0
    for symbol, status in entries:
        record = SanctionRecord(symbol=symbol.upper(), status=status.lower(), source=source, ts=ts)
        session.add(record)
        count += 1
    return count


async def _refresh_watchlists() -> None:
    global _last_updated, _symbols_checked

    async with _scan_lock:
        logger.info("Starting sanctions refresh job")
        now = datetime.now(timezone.utc)
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
            ofac_entries = await _fetch_source_entries(client, "OFAC")
            fca_entries = await _fetch_source_entries(client, "FCA")
        symbols = {symbol for symbol, status in ofac_entries + fca_entries}
        with SessionLocal() as session:
            with session.begin():
                ofac_count = _upsert_sanctions(session, "OFAC", ofac_entries, now)
                fca_count = _upsert_sanctions(session, "FCA", fca_entries, now)
        _last_updated = now
        _symbols_checked = len(symbols)
        logger.info(
            "Sanctions refresh completed. OFAC=%s FCA=%s total_symbols=%s",
            ofac_count,
            fca_count,
            _symbols_checked,
        )


async def _refresh_periodically() -> None:
    while True:
        try:
            await _refresh_watchlists()
        except Exception:  # pragma: no cover - defensive scheduling
            logger.exception("Periodic sanctions refresh failed")
        await asyncio.sleep(_REFRESH_INTERVAL.total_seconds())


@app.on_event("startup")
async def _on_startup() -> None:
    global _background_task

    ensure_sanctions_schema(ENGINE)
    await _refresh_watchlists()
    _background_task = asyncio.create_task(_refresh_periodically())


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    global _background_task

    if _background_task:
        _background_task.cancel()
        try:
            await _background_task
        except asyncio.CancelledError:  # pragma: no cover - expected on shutdown
            pass
        _background_task = None


@app.get("/compliance/scan/status")
async def get_scan_status(
    _actor: str = Depends(require_admin_account),
) -> Dict[str, Optional[str | int]]:
    last_updated = _last_updated.isoformat() if _last_updated else None
    return {"symbols_checked": _symbols_checked, "last_updated": last_updated}


@app.post("/compliance/scan/run")
async def run_scan_now(
    _actor: str = Depends(require_admin_account),
) -> Dict[str, Optional[str | int]]:
    await _refresh_watchlists()
    return await get_scan_status()
