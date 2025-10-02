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


if not hasattr(auth_service_module, "_ARGON2_HASHER"):
    class _DeterministicHasher:
        def hash(self, password: str) -> str:
            digest = hashlib.sha256(password.encode()).hexdigest()
            return f"$argon2${digest}"

        def verify(self, password: str, stored_hash: str) -> bool:
            return stored_hash == self.hash(password)

        def needs_update(self, stored_hash: str) -> bool:
            del stored_hash
            return False

    auth_service_module._ARGON2_HASHER = _DeterministicHasher()


if not hasattr(auth_service_module, "_LOGIN_FAILURE_COUNTER"):
    class _MetricCounter:
        def __init__(self) -> None:
            self._value = 0.0
            self._label_values: dict[tuple[tuple[str, str], ...], float] = {}
            self._labelled_views: dict[
                tuple[tuple[str, str], ...], "_LabelledCounter"
            ] = {}

        def labels(self, **labels: str) -> "_LabelledCounter":
            key = tuple(sorted(labels.items()))
            view = self._labelled_views.get(key)
            if view is None:
                view = _LabelledCounter(self, key)
                view._value = self._label_values.get(key, 0.0)
                self._labelled_views[key] = view
            return view

        def inc(self, amount: float = 1.0) -> None:
            self._value += amount

    class _LabelledCounter:
        def __init__(
            self, parent: _MetricCounter, key: tuple[tuple[str, str], ...]
        ) -> None:
            self._parent = parent
            self._key = key
            self._value = 0.0

        def inc(self, amount: float = 1.0) -> None:
            self._value += amount
            self._parent._label_values[self._key] = self._value

    auth_service_module._LOGIN_FAILURE_COUNTER = _MetricCounter()
    auth_service_module._MFA_DENIED_COUNTER = _MetricCounter()
    auth_service_module._LOGIN_SUCCESS_COUNTER = _MetricCounter()


class _RecordingPostgresRepository:
    def __init__(self, admin: AdminAccount) -> None:
        self._admin = admin
        self.saved_hashes: list[str] = []

    def add(self, admin: AdminAccount) -> None:
        self._admin = admin
        self.saved_hashes.append(admin.password_hash)

    def get_by_email(self, email: str) -> AdminAccount | None:
        if self._admin.email == email:
            return self._admin
        return None


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



def test_login_upgrades_legacy_hash_and_persists() -> None:
    secret = pyotp.random_base32()
    legacy_hash = hashlib.sha256("Outdated!".encode()).hexdigest()
    admin = AdminAccount(
        admin_id="legacy-1",
        email="legacy@example.com",
        password_hash=legacy_hash,
        mfa_secret=secret,
    )
    repository = _RecordingPostgresRepository(admin)
    sessions = SessionStore()
    service = AuthService(repository, sessions)

    code = pyotp.TOTP(secret).now()

    service.login(
        email=admin.email,
        password="Outdated!",
        mfa_code=code,
        ip_address=None,
    )

    assert len(repository.saved_hashes) == 1
    persisted_hash = repository.saved_hashes[0]
    assert persisted_hash.startswith("$argon2")
    assert persisted_hash != legacy_hash
    assert repository.get_by_email(admin.email).password_hash == persisted_hash


def test_login_refreshes_outdated_argon_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = pyotp.random_base32()
    admin = AdminAccount(
        admin_id="argon-1",
        email="argon@example.com",
        password_hash="$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$c29tZWhhc2g",
        mfa_secret=secret,
    )
    repository = _RecordingPostgresRepository(admin)
    sessions = SessionStore()
    service = AuthService(repository, sessions)

    class _StubHasher:
        def verify(self, password: str, stored_hash: str) -> bool:
            assert password == "Updatable!"
            assert stored_hash == admin.password_hash
            return True

        def needs_update(self, stored_hash: str) -> bool:
            assert stored_hash == admin.password_hash
            return True

        def hash(self, password: str) -> str:
            assert password == "Updatable!"
            return "$argon2id$v=19$m=65536,t=3,p=4$bmV3c2FsdA$bmV3aGFzaA"

    monkeypatch.setattr(auth_service_module, "_ARGON2_HASHER", _StubHasher())

    code = pyotp.TOTP(secret).now()

    service.login(
        email=admin.email,
        password="Updatable!",
        mfa_code=code,
        ip_address=None,
    )

    assert repository.saved_hashes == ["$argon2id$v=19$m=65536,t=3,p=4$bmV3c2FsdA$bmV3aGFzaA"]
    assert (
        repository.get_by_email(admin.email).password_hash
        == "$argon2id$v=19$m=65536,t=3,p=4$bmV3c2FsdA$bmV3aGFzaA"
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

