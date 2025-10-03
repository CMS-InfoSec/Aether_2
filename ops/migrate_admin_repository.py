"""Migrate administrator accounts into the shared Postgres/Timescale repository."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable, Sequence

from auth.service import AdminAccount, AdminRepositoryProtocol, PostgresAdminRepository, hash_password


def _coerce_allowed_ips(raw: Iterable[object] | None) -> set[str] | None:
    if raw is None:
        return None
    cleaned: set[str] = set()
    for value in raw:
        text = str(value).strip()
        if text:
            cleaned.add(text)
    return cleaned or None


def load_admin_accounts(source: Path) -> Sequence[AdminAccount]:
    """Load administrator records from a JSON file."""

    with source.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        raise ValueError("Admin export must be a list of objects")

    accounts: list[AdminAccount] = []
    for index, entry in enumerate(payload):
        if not isinstance(entry, dict):
            raise ValueError(f"Admin entry #{index + 1} must be an object")

        try:
            email = entry["email"]
            admin_id = entry.get("admin_id") or entry["id"]
            mfa_secret = entry["mfa_secret"]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Admin entry #{index + 1} missing required field: {exc.args[0]}") from exc

        password_hash = entry.get("password_hash")
        if not password_hash:
            password = entry.get("password")
            if not password:
                raise ValueError(
                    f"Admin entry '{email}' requires either 'password_hash' or 'password'"
                )
            password_hash = hash_password(str(password))

        allowed_ips_raw = entry.get("allowed_ips")
        allowed_ips = _coerce_allowed_ips(allowed_ips_raw)

        accounts.append(
            AdminAccount(
                admin_id=str(admin_id),
                email=str(email),
                password_hash=str(password_hash),
                mfa_secret=str(mfa_secret),
                allowed_ips=allowed_ips,
            )
        )
    return accounts


def migrate_accounts(accounts: Sequence[AdminAccount], repository: AdminRepositoryProtocol) -> int:
    """Persist the provided accounts into the repository."""

    migrated = 0
    for account in accounts:
        repository.add(account)
        migrated += 1
    return migrated


def _normalize_dsn(dsn: str) -> str:
    normalized = dsn.lower()
    if normalized.startswith("postgres://"):
        dsn = "postgresql://" + dsn.split("://", 1)[1]
        normalized = dsn.lower()

    allowed_prefixes = (
        "postgresql://",
        "postgresql+psycopg://",
        "postgresql+psycopg2://",
        "timescale://",
    )
    if not normalized.startswith(allowed_prefixes):
        raise ValueError(
            "Admin repository requires a Postgres/Timescale DSN; "
            f"received '{dsn}'."
        )
    return dsn


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        help="Postgres/Timescale DSN for the admin repository. Defaults to ADMIN_POSTGRES_DSN.",
    )
    parser.add_argument(
        "--source",
        required=True,
        type=Path,
        help="Path to a JSON export of administrator accounts.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    dsn = args.dsn or os.getenv("ADMIN_POSTGRES_DSN")
    if not dsn:
        raise SystemExit(
            "ADMIN_POSTGRES_DSN must be set or provided via --dsn when migrating admin accounts."
        )
    dsn = _normalize_dsn(dsn)

    accounts = load_admin_accounts(args.source)
    repository = PostgresAdminRepository(dsn)
    migrated = migrate_accounts(accounts, repository)
    print(f"Migrated {migrated} administrator account(s).")


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()
