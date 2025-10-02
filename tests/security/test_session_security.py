import pytest
fastapi = pytest.importorskip("fastapi")
from fastapi import HTTPException
from starlette.requests import Request
from types import SimpleNamespace

from auth.service import InMemorySessionStore

from services.common.security import (
    require_admin_account,
    require_authenticated_principal,
    require_dual_director_confirmation,
    require_mfa_context,
)


def _build_request(store: InMemorySessionStore, headers: dict[str, str] | None = None) -> Request:
    headers = headers or {}
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [
            (key.lower().encode("latin-1"), value.encode("latin-1"))
            for key, value in headers.items()
        ],
        "app": SimpleNamespace(state=SimpleNamespace(session_store=store)),
    }
    return Request(scope)


def test_require_authenticated_principal_requires_token() -> None:
    store = InMemorySessionStore()
    request = _build_request(store)
    with pytest.raises(HTTPException) as exc:
        require_authenticated_principal(request, authorization=None)
    assert exc.value.status_code == 401


def test_require_admin_account_accepts_valid_session() -> None:
    store = InMemorySessionStore()
    session = store.create("company")
    request = _build_request(store)
    account = require_admin_account(
        request,
        authorization=f"Bearer {session.token}",
        x_account_id="company",
    )
    assert account == "company"


def test_require_admin_account_rejects_header_mismatch() -> None:
    store = InMemorySessionStore()
    session = store.create("company")
    request = _build_request(store)
    with pytest.raises(HTTPException) as exc:
        require_admin_account(
            request,
            authorization=f"Bearer {session.token}",
            x_account_id="shadow",
        )
    assert exc.value.status_code == 403


def test_require_admin_account_rejects_non_admin_session() -> None:
    store = InMemorySessionStore()
    session = store.create("guest")
    request = _build_request(store)
    with pytest.raises(HTTPException) as exc:
        require_admin_account(request, authorization=f"Bearer {session.token}")
    assert exc.value.status_code == 403


def test_require_mfa_context_uses_session_state() -> None:
    store = InMemorySessionStore()
    session = store.create("director-1")
    request = _build_request(store)
    account = require_mfa_context(request, authorization=f"Bearer {session.token}")
    assert account == "director-1"


def test_require_dual_director_confirmation_success() -> None:
    store = InMemorySessionStore()
    caller = store.create("company")
    director_one = store.create("director-1")
    director_two = store.create("director-2")
    request = _build_request(store)
    approvals = require_dual_director_confirmation(
        request,
        authorization=f"Bearer {caller.token}",
        x_director_approvals=f"{director_one.token},{director_two.token}",
    )
    assert set(approvals) == {"director-1", "director-2"}


def test_require_dual_director_confirmation_requires_distinct_sessions() -> None:
    store = InMemorySessionStore()
    caller = store.create("company")
    director = store.create("director-1")
    request = _build_request(store)
    with pytest.raises(HTTPException) as exc:
        require_dual_director_confirmation(
            request,
            authorization=f"Bearer {caller.token}",
            x_director_approvals=f"{director.token},{director.token}",
        )
    assert exc.value.status_code == 403


def test_require_dual_director_confirmation_rejects_invalid_tokens() -> None:
    store = InMemorySessionStore()
    caller = store.create("company")
    request = _build_request(store)
    with pytest.raises(HTTPException) as exc:
        require_dual_director_confirmation(
            request,
            authorization=f"Bearer {caller.token}",
            x_director_approvals="invalid-token,another-token",
        )
    assert exc.value.status_code == 401
