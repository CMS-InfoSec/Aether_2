from __future__ import annotations

import pyotp
from fastapi.testclient import TestClient

from app import create_app
from auth.service import AdminAccount, hash_password


def test_login_enforces_mfa_and_ip_allow_list():
    app = create_app()
    client = TestClient(app)

    secret = pyotp.random_base32()
    admin = AdminAccount(
        admin_id="admin-1",
        email="admin@example.com",
        password_hash=hash_password("P@ssw0rd"),
        mfa_secret=secret,
        allowed_ips={"203.0.113.10"},
    )
    app.state.admin_repository.add(admin)

    # Missing or invalid MFA should fail
    response = client.post(
        "/auth/login",
        json={
            "email": admin.email,
            "password": "P@ssw0rd",
            "mfa_code": "000000",
            "ip_address": "203.0.113.10",
        },
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "mfa_required"

    # Valid MFA and IP allow-list should succeed
    valid_code = pyotp.TOTP(secret).now()
    response = client.post(
        "/auth/login",
        json={
            "email": admin.email,
            "password": "P@ssw0rd",
            "mfa_code": valid_code,
            "ip_address": "203.0.113.10",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["admin_id"] == admin.admin_id
    assert body["token"]
