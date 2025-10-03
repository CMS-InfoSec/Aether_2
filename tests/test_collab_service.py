import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Callable, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

tests_path = str(ROOT / "tests")
if tests_path in sys.path:
    sys.path.remove(tests_path)
    sys.path.append(tests_path)

import importlib.util


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise ImportError(f"Unable to load module {name} from {path}")
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_load_module("services", ROOT / "services" / "__init__.py")
_load_module("services.common", ROOT / "services" / "common" / "__init__.py")
_load_module("services.common.config", ROOT / "services" / "common" / "config.py")
_load_module("services.common.security", ROOT / "services" / "common" / "security.py")

import pytest

pytest.importorskip("fastapi")
from fastapi import status
from fastapi.testclient import TestClient

import collab_service
from services.common.security import require_admin_account


class _StubAuditLogger:
    def __init__(self) -> None:
        self.records: List[dict[str, object]] = []

    def record(self, **payload: object) -> None:
        self.records.append(dict(payload))


class _FakeCursor:
    def __init__(
        self,
        *,
        returning_row: Optional[dict[str, object]] = None,
        rows: Optional[List[dict[str, object]]] = None,
    ) -> None:
        self.returning_row = returning_row
        self.rows = rows or []
        self.executed_params: List[tuple[object, ...] | None] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, _query: str, params: Optional[tuple[object, ...]] = None) -> None:
        self.executed_params.append(params)

    def fetchone(self) -> Optional[dict[str, object]]:
        return self.returning_row

    def fetchall(self) -> List[dict[str, object]]:
        return list(self.rows)


class _FakeConnection:
    def __init__(self, cursor_factory: Callable[[], _FakeCursor]) -> None:
        self._cursor_factory = cursor_factory

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self, **_kwargs: object) -> _FakeCursor:
        return self._cursor_factory()


def _make_connection(cursor: _FakeCursor) -> _FakeConnection:
    return _FakeConnection(lambda: cursor)


@pytest.fixture(autouse=True)
def _reset_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_logger = _StubAuditLogger()
    monkeypatch.setattr(collab_service, "AUDIT_LOGGER", stub_logger, raising=False)
    collab_service.app.state.audit_logger = stub_logger
    monkeypatch.setattr(collab_service, "_ensure_tables", lambda: None)
    collab_service.app.dependency_overrides.clear()
    yield
    collab_service.app.dependency_overrides.clear()


def test_collab_comment_requires_authentication() -> None:
    with TestClient(collab_service.app) as client:
        response = client.post(
            "/collab/comment",
            json={"trade_id": "trade-1", "text": "This is important"},
        )
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_collab_comment_rejects_account_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_connect() -> None:
        raise AssertionError("connection should not be opened when scope mismatches")

    monkeypatch.setattr(collab_service, "_get_conn", _fail_connect)
    collab_service.app.dependency_overrides[require_admin_account] = lambda: "other-account"

    with TestClient(collab_service.app) as client:
        response = client.post(
            "/collab/comment",
            json={"trade_id": "trade-1", "text": "Out of scope"},
        )

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_collab_comment_persists_record_for_authorized_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    cursor = _FakeCursor(
        returning_row={
            "trade_id": "trade-1",
            "author": collab_service.ACCOUNT_ID,
            "text": "Leg filled",
            "ts": created_at,
        }
    )

    monkeypatch.setattr(collab_service, "_get_conn", lambda: _make_connection(cursor))
    collab_service.app.dependency_overrides[require_admin_account] = (
        lambda: collab_service.ACCOUNT_ID
    )

    with TestClient(collab_service.app) as client:
        response = client.post(
            "/collab/comment",
            json={"trade_id": "trade-1", "text": "Leg filled"},
        )

    assert response.status_code == status.HTTP_201_CREATED
    payload = response.json()
    assert payload["author"] == collab_service.ACCOUNT_ID
    assert payload["trade_id"] == "trade-1"
    assert payload["text"] == "Leg filled"
    assert cursor.executed_params[-1] == ("trade-1", collab_service.ACCOUNT_ID, "Leg filled")


def test_collab_comments_listing_requires_authentication() -> None:
    with TestClient(collab_service.app) as client:
        response = client.get("/collab/comments", params={"trade_id": "trade-1"})

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_collab_comments_listing_returns_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    retrieved_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
    cursor = _FakeCursor(
        rows=[
            {
                "trade_id": "trade-1",
                "author": collab_service.ACCOUNT_ID,
                "text": "Check spread",
                "ts": retrieved_at,
            }
        ]
    )

    monkeypatch.setattr(collab_service, "_get_conn", lambda: _make_connection(cursor))
    collab_service.app.dependency_overrides[require_admin_account] = (
        lambda: collab_service.ACCOUNT_ID
    )

    with TestClient(collab_service.app) as client:
        response = client.get("/collab/comments", params={"trade_id": "trade-1"})

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert payload[0]["author"] == collab_service.ACCOUNT_ID
    assert payload[0]["trade_id"] == "trade-1"
    assert payload[0]["text"] == "Check spread"


def test_collab_proposal_requires_authentication() -> None:
    with TestClient(collab_service.app) as client:
        response = client.post(
            "/collab/proposal",
            json={"key": "risk.limit", "new_value": 5, "reason": "Tighten"},
        )

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_collab_proposal_rejects_account_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_connect() -> None:
        raise AssertionError("connection should not be opened when scope mismatches")

    monkeypatch.setattr(collab_service, "_get_conn", _fail_connect)
    collab_service.app.dependency_overrides[require_admin_account] = lambda: "other-account"

    with TestClient(collab_service.app) as client:
        response = client.post(
            "/collab/proposal",
            json={"key": "risk.limit", "new_value": 5, "reason": "Tighten"},
        )

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_collab_proposal_returns_created_record(monkeypatch: pytest.MonkeyPatch) -> None:
    created_at = datetime(2024, 1, 3, tzinfo=timezone.utc)
    cursor = _FakeCursor(
        returning_row={
            "id": 42,
            "key": "risk.limit",
            "new_value": "5",
            "reason": "Tighten",
            "status": "pending",
            "ts": created_at,
        }
    )

    monkeypatch.setattr(collab_service, "_get_conn", lambda: _make_connection(cursor))
    collab_service.app.dependency_overrides[require_admin_account] = (
        lambda: collab_service.ACCOUNT_ID
    )

    with TestClient(collab_service.app) as client:
        response = client.post(
            "/collab/proposal",
            json={"key": "risk.limit", "new_value": 5, "reason": "Tighten"},
        )

    assert response.status_code == status.HTTP_201_CREATED
    payload = response.json()
    assert payload["id"] == 42
    assert payload["key"] == "risk.limit"
    assert payload["new_value"] == 5
    assert payload["reason"] == "Tighten"
    assert payload["status"] == "pending"


def test_collab_proposals_listing_requires_authentication() -> None:
    with TestClient(collab_service.app) as client:
        response = client.get("/collab/proposals")

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_collab_proposals_listing_returns_records(monkeypatch: pytest.MonkeyPatch) -> None:
    retrieved_at = datetime(2024, 1, 4, tzinfo=timezone.utc)
    cursor = _FakeCursor(
        rows=[
            {
                "id": 101,
                "key": "risk.limit",
                "new_value": "5",
                "reason": "Tighten",
                "status": "pending",
                "ts": retrieved_at,
            }
        ]
    )

    monkeypatch.setattr(collab_service, "_get_conn", lambda: _make_connection(cursor))
    collab_service.app.dependency_overrides[require_admin_account] = (
        lambda: collab_service.ACCOUNT_ID
    )

    with TestClient(collab_service.app) as client:
        response = client.get("/collab/proposals")

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert payload[0]["id"] == 101
    assert payload[0]["key"] == "risk.limit"
    assert payload[0]["new_value"] == 5
    assert payload[0]["reason"] == "Tighten"
    assert payload[0]["status"] == "pending"
