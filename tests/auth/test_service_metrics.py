from __future__ import annotations

import sys
from types import ModuleType

import pytest
from prometheus_client import CollectorRegistry

try:  # pragma: no cover - favour real dependency when available
    import pyotp  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - install lightweight stub
    pyotp_stub = ModuleType("pyotp")

    class _TOTP:
        def __init__(self, secret: str) -> None:
            self._secret = secret

        def now(self) -> str:
            return "123456"

        def verify(self, code: str, valid_window: int = 0) -> bool:
            return code == self.now()

    pyotp_stub.TOTP = _TOTP  # type: ignore[attr-defined]
    pyotp_stub.random_base32 = lambda: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
    sys.modules["pyotp"] = pyotp_stub
    import pyotp  # type: ignore  # noqa: E402

from auth import service as auth_service


def _metric_value(counter, labels: dict[str, str] | None = None) -> float:
    labels = labels or {}
    try:
        labelled = counter.labels(**labels) if labels else counter
    except Exception:  # pragma: no cover - defensive for stubs without labels
        labelled = counter

    if hasattr(labelled, "_value"):
        return float(getattr(labelled, "_value", 0.0))

    try:
        for metric in counter.collect():
            for sample in metric.samples:
                if labels:
                    if sample.labels == labels:
                        return float(sample.value)
                else:
                    if not sample.labels:
                        return float(sample.value)
    except Exception:  # pragma: no cover - fallback when collection unsupported
        pass
    return 0.0


@pytest.fixture(autouse=True)
def _reset_metrics_registry() -> None:
    """Ensure each test sees isolated Prometheus metrics."""

    auth_service._init_metrics(registry=CollectorRegistry())


@pytest.fixture
def _service_with_admin():
    repository = auth_service.InMemoryAdminRepository()
    sessions = auth_service.InMemorySessionStore()
    service = auth_service.AuthService(repository, sessions)

    secret = pyotp.random_base32()
    admin = auth_service.AdminAccount(
        admin_id="admin-0001",
        email="admin@example.com",
        password_hash=auth_service.hash_password("CorrectHorseBatteryStaple"),
        mfa_secret=secret,
        allowed_ips=None,
    )
    repository.add(admin)
    return service, admin, secret


def test_invalid_credentials_increment_failure_counter(_service_with_admin):
    service, admin, secret = _service_with_admin
    valid_code = pyotp.TOTP(secret).now()

    with pytest.raises(PermissionError):
        service.login(
            email=admin.email,
            password="wrong-password",
            mfa_code=valid_code,
            ip_address=None,
        )

    assert (
        _metric_value(
            auth_service._LOGIN_FAILURE_COUNTER, {"reason": "invalid_credentials"}
        )
        == 1
    )
    assert _metric_value(auth_service._MFA_DENIED_COUNTER) == 0
    assert _metric_value(auth_service._LOGIN_SUCCESS_COUNTER) == 0


def test_mfa_failure_increments_failure_and_mfa_counters(_service_with_admin):
    service, admin, _secret = _service_with_admin

    with pytest.raises(PermissionError):
        service.login(
            email=admin.email,
            password="CorrectHorseBatteryStaple",
            mfa_code="000000",
            ip_address=None,
        )

    assert (
        _metric_value(
            auth_service._LOGIN_FAILURE_COUNTER, {"reason": "mfa_required"}
        )
        == 1
    )
    assert _metric_value(auth_service._MFA_DENIED_COUNTER) == 1
    assert _metric_value(auth_service._LOGIN_SUCCESS_COUNTER) == 0


def test_successful_login_increments_success_counter(_service_with_admin):
    service, admin, secret = _service_with_admin
    valid_code = pyotp.TOTP(secret).now()

    session = service.login(
        email=admin.email,
        password="CorrectHorseBatteryStaple",
        mfa_code=valid_code,
        ip_address=None,
    )

    assert session.admin_id == admin.admin_id
    assert session.token

    assert _metric_value(auth_service._LOGIN_SUCCESS_COUNTER) == 1
    assert _metric_value(
        auth_service._LOGIN_FAILURE_COUNTER, {"reason": "invalid_credentials"}
    ) == 0
    assert _metric_value(auth_service._MFA_DENIED_COUNTER) == 0
