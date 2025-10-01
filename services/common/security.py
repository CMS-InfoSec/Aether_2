"""Security and permission helpers shared across services."""
from __future__ import annotations

import os
from typing import Iterable, Tuple

from fastapi import Header, HTTPException


_DEFAULT_ACCOUNTS: Tuple[str, ...] = ("admin-alpha", "admin-beta", "admin-gamma")


def _load_admin_accounts() -> Tuple[str, ...]:
    accounts_env = os.getenv("AETHER_ADMIN_ACCOUNTS")
    if not accounts_env:
        return _DEFAULT_ACCOUNTS
    accounts: Iterable[str] = (acct.strip() for acct in accounts_env.split(","))
    normalized = tuple(account for account in accounts if account)
    return normalized or _DEFAULT_ACCOUNTS


def require_admin_account(account_id: str = Header(..., alias="X-Account-Id")) -> str:
    allowed = _load_admin_accounts()
    if account_id not in allowed:
        raise HTTPException(status_code=403, detail="Account is not permitted to call this service")
    return account_id


__all__ = ["require_admin_account"]
