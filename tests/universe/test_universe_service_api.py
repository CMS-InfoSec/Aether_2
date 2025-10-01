from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

os.environ.setdefault("TIMESCALE_DATABASE_URI", "sqlite:///./universe_service_test.db")

from services.universe import universe_service
from services.universe.universe_service import (
    MANUAL_OVERRIDE_SOURCE,
    AuditLog,
    Feature,
    OhlcvBar,
    OverrideRequest,
    UniverseWhitelist,
    _kraken_volume_24h,
    _latest_manual_overrides,
    _normalize_market,
    _evaluate_universe,
    app,
)


@pytest.fixture
def session(tmp_path, monkeypatch) -> Session:
    db_path = tmp_path / "universe_test.db"
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    universe_service.Base.metadata.drop_all(bind=engine)
    universe_service.Base.metadata.create_all(bind=engine)

    TestSessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    monkeypatch.setattr(universe_service, "ENGINE", engine)
    monkeypatch.setattr(universe_service, "SessionLocal", TestSessionLocal)

    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def test_normalize_market_handles_kraken_aliases() -> None:
    assert _normalize_market("xbt/usd") == "BTC-USD"
    assert _normalize_market("btc") == "BTC-USD"
    assert _normalize_market("BTC-USD") == "BTC-USD"


def test_universe_api_returns_canonical_symbols_for_kraken_pairs(session: Session) -> None:
    now = datetime.now(timezone.utc)

    session.add_all(
        [
            Feature(
                feature_name="coingecko.market_cap",
                entity_id="btc",
                event_timestamp=now,
                value=2_500_000_000.0,
                attributes={},
            ),
            Feature(
                feature_name="coingecko.volatility_30d",
                entity_id="btc",
                event_timestamp=now,
                value=0.5,
                attributes={},
            ),
        ]
    )
    session.add(
        OhlcvBar(
            market="XBT/USD",
            bucket_start=now - timedelta(hours=1),
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=150_000_000.0,
        )
    )
    session.commit()

    volumes = _kraken_volume_24h(session)
    assert volumes["BTC-USD"] == pytest.approx(150_000_000.0)

    approved = _evaluate_universe(session)
    assert approved == ["BTC-USD"]

    client = TestClient(app)
    response = client.get("/universe/approved")
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbols"] == ["BTC-USD"]


def test_manual_override_migrates_legacy_symbols(session: Session) -> None:
    legacy_time = datetime.now(timezone.utc) - timedelta(days=1)

    session.add(
        UniverseWhitelist(
            asset_id="BTC",
            as_of=legacy_time,
            source=MANUAL_OVERRIDE_SOURCE,
            approved=False,
            details={},
        )
    )
    session.commit()

    request = OverrideRequest(symbol="xbt/usd", enabled=True, reason="Enable trading", actor="tester")
    response = universe_service.override_symbol(request, session=session)

    assert response.symbol == "BTC-USD"

    overrides = _latest_manual_overrides(session)
    assert list(overrides.keys()) == ["BTC-USD"]
    assert overrides["BTC-USD"].asset_id == "BTC-USD"

    audit_entries = session.query(AuditLog).all()
    assert audit_entries
    assert audit_entries[-1].entity_id == "BTC-USD"
@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_: JSONB, compiler, **kw) -> str:  # pragma: no cover - SQLAlchemy hook
    return "JSON"

