from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, Optional, Callable

from fastapi import Header, HTTPException, Request, status
from fastapi.applications import FastAPI


@contextmanager
def override_admin_auth(
    app: FastAPI,
    dependency: Callable[..., str],
    account_id: str,
    *,
    token: str = "test-admin-token",
) -> Iterator[Dict[str, str]]:
    """Temporarily override an admin authentication dependency for tests."""

    expected = f"Bearer {token}"

    def _dependency(
        _request: Request,
        authorization: Optional[str] = Header(None, alias="Authorization"),
        x_account_id: Optional[str] = Header(None, alias="X-Account-ID"),
    ) -> str:
        if authorization != expected:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing Authorization header.",
            )
        if x_account_id:
            header_account = x_account_id.strip()
            if header_account and header_account.lower() != account_id.lower():
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Account header does not match authenticated session.",
                )
        return account_id

    app.dependency_overrides[dependency] = _dependency
    try:
        yield {"Authorization": expected}
    finally:
        app.dependency_overrides.pop(dependency, None)
