from __future__ import annotations


import hashlib
import logging
import sys
from pathlib import Path

import importlib.util
import sysconfig

stdlib_path = Path(sysconfig.get_paths()["stdlib"]) / "secrets.py"
spec = importlib.util.spec_from_file_location("secrets", stdlib_path)
if spec and spec.loader:
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["secrets"] = module


import pyotp
import pytest


from auth import service as auth_service_module

AdminAccount = auth_service_module.AdminAccount
AdminRepository = auth_service_module.AdminRepository
AuthService = auth_service_module.AuthService
SessionStore = auth_service_module.SessionStore
hash_password = auth_service_module.hash_password
from shared.correlation import CorrelationContext


def _metric_value(counter, labels: dict[str, str] | None = None) -> float:
    labels = labels or {}
    try:
        labelled = counter.labels(**labels) if labels else counter
    except Exception:  # pragma: no cover - defensive for stubs without labels
        labelled = counter

    # Prometheus test stubs expose `_value` directly.
    if hasattr(labelled, "_value"):
        return float(getattr(labelled, "_value", 0.0))

    # Fallback to collecting samples from the real client.
    try:
        for metric in counter.collect():
            for sample in metric.samples:
                if labels:
                    if sample.labels == labels:
                        return float(sample.value)
                else:
                    if not sample.labels:
                        return float(sample.value)
    except Exception:  # pragma: no cover - in case collection is unsupported
        pass
    return 0.0


def test_login_enforces_mfa_and_ip_allow_list():
    repository = AdminRepository()
    sessions = SessionStore()
    service = AuthService(repository, sessions)


    secret = pyotp.random_base32()
    admin = AdminAccount(
        admin_id="admin-1",
        email="admin@example.com",
        password_hash=hash_password("P@ssw0rd"),
        mfa_secret=secret,
        allowed_ips={"203.0.113.10"},
    )
    repository.add(admin)

    # Missing or invalid MFA should fail

    mfa_failure_before = _metric_value(
        auth_service_module._LOGIN_FAILURE_COUNTER, {"reason": "mfa_required"}
    )
    mfa_denied_before = _metric_value(auth_service_module._MFA_DENIED_COUNTER)
    with pytest.raises(PermissionError) as exc:

        service.login(
            email=admin.email,
            password="P@ssw0rd",
            mfa_code="000000",
            ip_address="203.0.113.10",
        )

    assert str(exc.value) == "mfa_required"
    assert (
        _metric_value(
            auth_service_module._LOGIN_FAILURE_COUNTER, {"reason": "mfa_required"}
        )
        == mfa_failure_before + 1
    )
    assert (
        _metric_value(auth_service_module._MFA_DENIED_COUNTER)
        == mfa_denied_before + 1
    )

    # Valid MFA and IP allow-list should succeed
    valid_code = pyotp.TOTP(secret).now()
    success_before = _metric_value(auth_service_module._LOGIN_SUCCESS_COUNTER)

    session = service.login(
        email=admin.email,
        password="P@ssw0rd",
        mfa_code=valid_code,
        ip_address="203.0.113.10",
    )
    assert session.admin_id == admin.admin_id
    assert session.token

    assert (
        _metric_value(auth_service_module._LOGIN_SUCCESS_COUNTER)
        == success_before + 1
    )


def test_auth_service_failure_branches_emit_structured_logs_and_metrics(caplog):
    repository = AdminRepository()
    sessions = SessionStore()
    service = AuthService(repository, sessions)

    secret = pyotp.random_base32()
    admin = AdminAccount(
        admin_id="admin-42",
        email="admin@example.com",
        password_hash=hash_password("S3cret!"),
        mfa_secret=secret,
        allowed_ips={"203.0.113.10"},
    )
    repository.add(admin)

    def expect_failure(
        expected_reason: str,
        *,
        email: str,
        password: str,
        mfa_code: str,
        ip_address: str | None,
        admin_id_expected: bool,
    ) -> None:
        failure_before = _metric_value(
            auth_service_module._LOGIN_FAILURE_COUNTER, {"reason": expected_reason}
        )
        mfa_before = (
            _metric_value(auth_service_module._MFA_DENIED_COUNTER)
            if expected_reason == "mfa_required"
            else None
        )
        caplog.clear()
        correlation = f"corr-{expected_reason}"
        with CorrelationContext(correlation):
            with caplog.at_level(logging.WARNING):
                with pytest.raises(PermissionError):
                    service.login(
                        email=email,
                        password=password,
                        mfa_code=mfa_code,
                        ip_address=ip_address,
                    )

        record = next(
            r
            for r in caplog.records
            if getattr(r, "auth_event", "") == "auth_login_failure"
            and getattr(r, "auth_reason", "") == expected_reason
        )
        assert record.correlation_id == correlation
        assert getattr(record, "auth_email") == email
        if admin_id_expected:
            assert getattr(record, "auth_admin_id") == admin.admin_id
        else:
            assert not hasattr(record, "auth_admin_id")
        assert _metric_value(
            auth_service_module._LOGIN_FAILURE_COUNTER, {"reason": expected_reason}
        ) == failure_before + 1
        if expected_reason == "mfa_required" and mfa_before is not None:
            assert (
                _metric_value(auth_service_module._MFA_DENIED_COUNTER)
                == mfa_before + 1
            )

    # Unknown administrator
    expect_failure(
        "invalid_credentials",
        email="unknown@example.com",
        password="whatever",
        mfa_code="",
        ip_address="198.51.100.5",
        admin_id_expected=False,
    )

    valid_code = pyotp.TOTP(secret).now()

    # IP allow list failure
    expect_failure(
        "ip_not_allowed",
        email=admin.email,
        password="S3cret!",
        mfa_code=valid_code,
        ip_address="198.51.100.5",
        admin_id_expected=True,
    )

    # Password failure
    expect_failure(
        "invalid_credentials",
        email=admin.email,
        password="wrong",
        mfa_code=valid_code,
        ip_address="203.0.113.10",
        admin_id_expected=True,
    )

    # MFA failure
    expect_failure(
        "mfa_required",
        email=admin.email,
        password="S3cret!",
        mfa_code="",
        ip_address="203.0.113.10",
        admin_id_expected=True,
    )


def test_hash_password_delegates_to_module_hasher(monkeypatch):
    captured: dict[str, str] = {}

    class StubHasher:
        def hash(self, password: str) -> str:
            captured["password"] = password
            return "argon-hash"

    monkeypatch.setattr(auth_service_module, "_ARGON2_HASHER", StubHasher())

    result = auth_service_module.hash_password("hunter2")

    assert result == "argon-hash"
    assert captured["password"] == "hunter2"


def test_verify_password_with_argon2_hash(monkeypatch):
    stored_hash = "$argon2id$v=19$m=65536,t=3,p=4$abc$def"
    admin = AdminAccount(
        admin_id="admin-argon",
        email="argon@example.com",
        password_hash=stored_hash,
        mfa_secret=pyotp.random_base32(),
    )

    class StubHasher:
        def __init__(self) -> None:
            self.needs_update_called = False

        def hash(self, password: str) -> str:
            return "rehashed"

        def verify(self, hashed: str, password: str) -> bool:
            assert hashed == stored_hash
            assert password == "correcthorsebatterystaple"
            return True

        def needs_update(self, hashed: str) -> bool:
            assert hashed == stored_hash
            self.needs_update_called = True
            return True

    hasher = StubHasher()
    monkeypatch.setattr(auth_service_module, "_ARGON2_HASHER", hasher)

    service = AuthService(AdminRepository(), SessionStore())

    assert service._verify_password(admin, "correcthorsebatterystaple") is True
    assert admin.password_hash == "rehashed"
    assert hasher.needs_update_called is True


def test_verify_password_with_legacy_sha256_hash(monkeypatch):
    password = "legacy-secret"
    stored_hash = hashlib.sha256(password.encode()).hexdigest()
    admin = AdminAccount(
        admin_id="admin-legacy",
        email="legacy@example.com",
        password_hash=stored_hash,
        mfa_secret=pyotp.random_base32(),
    )

    class StubHasher:
        def __init__(self) -> None:
            self.hash_calls: list[str] = []

        def hash(self, password: str) -> str:
            self.hash_calls.append(password)
            return "argon-upgrade"

    hasher = StubHasher()
    monkeypatch.setattr(auth_service_module, "_ARGON2_HASHER", hasher)

    service = AuthService(AdminRepository(), SessionStore())

    assert service._verify_password(admin, password) is True
    assert admin.password_hash == "argon-upgrade"
    assert hasher.hash_calls == [password]

