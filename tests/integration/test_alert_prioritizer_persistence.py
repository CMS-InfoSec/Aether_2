from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import importlib
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if TYPE_CHECKING:  # pragma: no cover - imported for static analysis only
    from alert_prioritizer import AlertPrioritizerService


def _install_security_stub() -> None:
    services_module = sys.modules.setdefault("services", types.ModuleType("services"))
    common_module = getattr(services_module, "common", types.ModuleType("services.common"))
    sys.modules.setdefault("services.common", common_module)
    setattr(services_module, "common", common_module)
    security_module = types.ModuleType("services.common.security")

    def _require_admin_account(*_: object, **__: object) -> str:
        return "test-account"

    security_module.require_admin_account = _require_admin_account  # type: ignore[attr-defined]
    sys.modules["services.common.security"] = security_module


try:  # pragma: no cover - prefer the real module when available
    importlib.import_module("services.common.security")
except ModuleNotFoundError:  # pragma: no cover - stub fallback for test environment
    _install_security_stub()


def _normalize_sql(query: str) -> str:
    return " ".join(query.strip().lower().split())


class _MemoryCursor:
    def __init__(self, store: List[Tuple[str, str, datetime]]) -> None:
        self._store = store

    def __enter__(self) -> "_MemoryCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        normalized = _normalize_sql(query)
        if normalized.startswith("create table"):
            return
        if normalized.startswith("create index"):
            return
        if normalized.startswith("set time zone"):
            return
        raise NotImplementedError(query)


class _MemoryConnection:
    def __init__(self, store: List[Tuple[str, str, datetime]]) -> None:
        self._store = store
        self.autocommit = False

    def cursor(self) -> _MemoryCursor:
        return _MemoryCursor(self._store)

    def close(self) -> None:
        return None


class _MemoryAsyncCursor:
    def __init__(self, store: List[Tuple[str, str, datetime]]) -> None:
        self._store = store
        self._result: Optional[Tuple[int]] = None

    async def __aenter__(self) -> "_MemoryAsyncCursor":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def execute(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        normalized = _normalize_sql(query)
        if normalized.startswith("set time zone"):
            return
        if normalized.startswith("select count(*)"):
            assert params is not None
            alert_id = params[0]
            count = sum(1 for record in self._store if record[0] == alert_id)
            self._result = (count,)
            return
        if normalized.startswith("insert into alert_log"):
            assert params is not None
            alert_id, severity, ts = params
            if not isinstance(ts, datetime):
                raise AssertionError("expected timestamp parameter to be datetime")
            self._store.append((str(alert_id), str(severity), ts))
            self._result = None
            return
        raise NotImplementedError(query)

    async def fetchone(self) -> Optional[Tuple[int]]:
        return self._result


class _MemoryAsyncConnection:
    def __init__(self, store: List[Tuple[str, str, datetime]]) -> None:
        self._store = store
        self.closed = False
        self.autocommit = True

    def cursor(self) -> _MemoryAsyncCursor:
        return _MemoryAsyncCursor(self._store)

    async def close(self) -> None:
        self.closed = True

    @classmethod
    async def connect(cls, dsn: str) -> "_MemoryAsyncConnection":
        del dsn
        return cls(cls._shared_store)  # type: ignore[attr-defined]


class _StubModel:
    def predict(self, _: Any) -> List[str]:
        return ["high"]


class _StubLabelEncoder:
    def inverse_transform(self, labels: List[str]) -> List[str]:
        return labels


class _MemoryPsycopg:
    def __init__(self) -> None:
        self._store: List[Tuple[str, str, datetime]] = []
        _MemoryAsyncConnection._shared_store = self._store  # type: ignore[attr-defined]

    def connect(self, dsn: str) -> _MemoryConnection:
        del dsn
        return _MemoryConnection(self._store)

    AsyncConnection = _MemoryAsyncConnection


def _build_service(db: _MemoryPsycopg) -> "AlertPrioritizerService":
    from alert_prioritizer import AlertPrioritizerService

    return AlertPrioritizerService(
        alertmanager_url="http://alertmanager",  # pragma: allowlist secret
        database_url="postgresql://shared/alerts",  # pragma: allowlist secret
        psycopg_module=db,
        model=_StubModel(),
        label_encoder=_StubLabelEncoder(),
    )


def _sample_alert(alert_id: str = "alert-1") -> Dict[str, Any]:
    return {
        "fingerprint": alert_id,
        "labels": {
            "service": "pricing",
            "alertname": "LatencyBreach",
        },
        "annotations": {
            "recent_pnl_impact": "-1200.5",
            "anomaly_count": "3",
        },
    }


@pytest.mark.asyncio
async def test_alert_classifications_persist_across_restarts() -> None:
    db = _MemoryPsycopg()
    service = _build_service(db)
    alert = _sample_alert("persist-alert")

    first = await service.classify_alert(alert)
    assert first["severity"] in {"low", "medium", "high"}

    await service.close()

    restarted = _build_service(db)
    try:
        frequency = await restarted._lookup_frequency(alert)
        assert frequency == 1.0
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_alert_classifications_visible_across_instances() -> None:
    db = _MemoryPsycopg()
    service_a = _build_service(db)
    service_b = _build_service(db)

    alert = _sample_alert("shared-alert")

    await service_a.classify_alert(alert)

    frequency_seen_by_b = await service_b._lookup_frequency(alert)
    assert frequency_seen_by_b == 1.0

    await service_b.classify_alert(alert)

    final_frequency = await service_a._lookup_frequency(alert)
    assert final_frequency == 2.0

    await service_a.close()
    await service_b.close()


def test_alert_prioritizer_rejects_sqlite_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    from alert_prioritizer import AlertPrioritizerService

    monkeypatch.delenv("ALERT_PRIORITIZER_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="PostgreSQL/Timescale"):
        AlertPrioritizerService(
            database_url="sqlite:///tmp/test.db",
            psycopg_module=_MemoryPsycopg(),
            model=_StubModel(),
            label_encoder=_StubLabelEncoder(),
        )
