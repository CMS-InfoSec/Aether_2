from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from auth import service as auth_service
from shared.correlation import CorrelationContext


class _DummyCounter:
    def __init__(self) -> None:
        self.count = 0

    def labels(self, **kwargs):  # type: ignore[no-untyped-def]
        return self

    def inc(self) -> None:
        self.count += 1


class _EmptyRepository(auth_service.AdminRepositoryProtocol):  # type: ignore[misc]
    def add(self, admin: auth_service.AdminAccount) -> None:  # pragma: no cover
        raise NotImplementedError

    def delete(self, email: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def get_by_email(self, email: str):  # type: ignore[override]
        return None


class _DummySessionStore(auth_service.SessionStoreProtocol):  # type: ignore[misc]
    def create(self, admin_id: str):  # type: ignore[override]
        return SimpleNamespace(token="token", admin_id=admin_id)

    def get(self, token: str):  # pragma: no cover
        return None


@pytest.fixture(autouse=True)
def _stub_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        auth_service, "_LOGIN_FAILURE_COUNTER", _DummyCounter(), raising=False
    )
    monkeypatch.setattr(
        auth_service, "_MFA_DENIED_COUNTER", _DummyCounter(), raising=False
    )


def test_login_failure_logs_correlation_id(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Logging a failed login should include the correlation identifier."""

    service = auth_service.AuthService(_EmptyRepository(), _DummySessionStore())

    with CorrelationContext("corr-123"):
        with caplog.at_level(logging.WARNING):
            with pytest.raises(PermissionError):
                service.login(
                    email="missing@example.com",
                    password="irrelevant",
                    mfa_code="000000",
                    ip_address="203.0.113.42",
                )

    record = next(
        r
        for r in caplog.records
        if getattr(r, "auth_event", "") == "auth_login_failure"
    )
    assert record.correlation_id == "corr-123"
    assert record.auth_email == "missing@example.com"
    assert record.auth_ip == "203.0.113.42"
