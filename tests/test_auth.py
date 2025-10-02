from __future__ import annotations

import hashlib

import pyotp
import pytest

from auth.service import (
    AdminAccount,
    AdminRepository,
    AuthService,
    SessionStore,
    hash_password,
)


def _make_service() -> tuple[AuthService, AdminRepository]:
    repository = AdminRepository()
    sessions = SessionStore()
    service = AuthService(repository, sessions)
    return service, repository


def test_login_enforces_mfa_and_ip_allow_list():
    service, repository = _make_service()

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
    with pytest.raises(PermissionError) as excinfo:
        service.login(
            email=admin.email,
            password="P@ssw0rd",
            mfa_code="000000",
            ip_address="203.0.113.10",
        )
    assert str(excinfo.value) == "mfa_required"

    # Valid MFA and IP allow-list should succeed
    valid_code = pyotp.TOTP(secret).now()
    session = service.login(
        email=admin.email,
        password="P@ssw0rd",
        mfa_code=valid_code,
        ip_address="203.0.113.10",
    )
    assert session.admin_id == admin.admin_id
    assert session.token


def test_hash_password_uses_argon2id() -> None:
    hashed = hash_password("super-secret")
    assert hashed.startswith("$argon2id$")


def test_login_upgrades_legacy_hashes() -> None:
    service, repository = _make_service()

    secret = pyotp.random_base32()
    password = "LegacyP@ssw0rd"
    legacy_hash = hashlib.sha256(password.encode()).hexdigest()
    admin = AdminAccount(
        admin_id="admin-legacy",
        email="legacy@example.com",
        password_hash=legacy_hash,
        mfa_secret=secret,
        allowed_ips=None,
    )
    repository.add(admin)

    valid_code = pyotp.TOTP(secret).now()
    session = service.login(
        email=admin.email,
        password=password,
        mfa_code=valid_code,
        ip_address=None,
    )

    assert session.admin_id == admin.admin_id
    assert admin.password_hash.startswith("$argon2id$")


def test_login_rejects_tampered_password_hash() -> None:
    service, repository = _make_service()

    secret = pyotp.random_base32()
    password = "P@ssw0rd"
    admin = AdminAccount(
        admin_id="admin-tampered",
        email="tampered@example.com",
        password_hash=hash_password(password),
        mfa_secret=secret,
        allowed_ips=None,
    )
    admin.password_hash = admin.password_hash[:-1] + (
        "0" if admin.password_hash[-1] != "0" else "1"
    )
    repository.add(admin)

    valid_code = pyotp.TOTP(secret).now()
    with pytest.raises(PermissionError) as excinfo:
        service.login(
            email=admin.email,
            password=password,
            mfa_code=valid_code,
            ip_address=None,
        )

    assert str(excinfo.value) == "invalid credentials"
