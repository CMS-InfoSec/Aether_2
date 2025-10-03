from __future__ import annotations

import json
from pathlib import Path

import pytest

from auth.service import AdminAccount, InMemoryAdminRepository

from ops.migrate_admin_repository import _normalize_dsn, load_admin_accounts, migrate_accounts


class RecordingRepository(InMemoryAdminRepository):
    def __init__(self) -> None:
        super().__init__()
        self.added: list[AdminAccount] = []

    def add(self, admin: AdminAccount) -> None:
        super().add(admin)
        self.added.append(admin)


def _write_export(path: Path, payload: list[dict[str, object]]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_admin_accounts_supports_password_hash_and_plain(tmp_path: Path) -> None:
    export = tmp_path / "admins.json"
    _write_export(
        export,
        [
            {
                "email": "hashed@example.com",
                "admin_id": "hashed",
                "password_hash": "legacy-hash",
                "mfa_secret": "JBSWY3DPEHPK3PXP",
                "allowed_ips": ["10.0.0.1", " 10.0.0.2 "],
            },
            {
                "email": "plain@example.com",
                "admin_id": "plain",
                "password": "S3cure!",
                "mfa_secret": "NB2WY3DPEHPK3PXP",
            },
        ],
    )

    accounts = load_admin_accounts(export)
    assert len(accounts) == 2
    hashed, plain = accounts

    assert hashed.email == "hashed@example.com"
    assert hashed.password_hash == "legacy-hash"
    assert hashed.allowed_ips == {"10.0.0.1", "10.0.0.2"}

    assert plain.email == "plain@example.com"
    assert plain.password_hash != "S3cure!"
    assert plain.allowed_ips is None


def test_migrate_accounts_persists_records(tmp_path: Path) -> None:
    export = tmp_path / "admins.json"
    _write_export(
        export,
        [
            {
                "email": "a@example.com",
                "admin_id": "a",
                "password": "pw1",
                "mfa_secret": "JBSWY3DPEHPK3PXP",
            }
        ],
    )
    accounts = load_admin_accounts(export)

    repository = RecordingRepository()
    migrated = migrate_accounts(accounts, repository)

    assert migrated == 1
    assert repository.added[0].email == "a@example.com"


@pytest.mark.parametrize(
    "input_dsn,expected",
    [
        ("postgres://user:pass@host/db", "postgresql://user:pass@host/db"),
        ("postgresql://user:pass@host/db", "postgresql://user:pass@host/db"),
        ("timescale://user:pass@host/db", "timescale://user:pass@host/db"),
    ],
)
def test_normalize_dsn_accepts_postgres_and_timescale(input_dsn: str, expected: str) -> None:
    assert _normalize_dsn(input_dsn) == expected


def test_normalize_dsn_rejects_non_postgres() -> None:
    with pytest.raises(ValueError):
        _normalize_dsn("sqlite:///not-allowed.db")
