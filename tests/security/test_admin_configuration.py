import os
from types import SimpleNamespace

import pytest

fastapi = pytest.importorskip("fastapi")
from starlette.requests import Request

from auth.service import InMemorySessionStore
from services.common import security


def _build_request(store: InMemorySessionStore) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [],
        "app": SimpleNamespace(state=SimpleNamespace(session_store=store)),
    }
    return Request(scope)


@pytest.fixture
def configure_admin_accounts(monkeypatch):
    original_admin_env = os.environ.get("ADMIN_ALLOWLIST")
    original_director_env = os.environ.get("DIRECTOR_ALLOWLIST")
    original_admin_accounts = set(security.ADMIN_ACCOUNTS)
    original_director_accounts = set(security.DIRECTOR_ACCOUNTS)

    def _apply(admin_value: str, director_value: str | None = None) -> None:
        monkeypatch.setenv("ADMIN_ALLOWLIST", admin_value)
        monkeypatch.setenv(
            "DIRECTOR_ALLOWLIST", director_value if director_value is not None else admin_value
        )
        security.reload_admin_accounts()

    yield _apply

    if original_admin_env is None:
        monkeypatch.delenv("ADMIN_ALLOWLIST", raising=False)
    else:
        monkeypatch.setenv("ADMIN_ALLOWLIST", original_admin_env)

    if original_director_env is None:
        monkeypatch.delenv("DIRECTOR_ALLOWLIST", raising=False)
    else:
        monkeypatch.setenv("DIRECTOR_ALLOWLIST", original_director_env)

    security.reload_admin_accounts(
        original_admin_accounts,
        director_source=original_director_accounts,
    )


def test_configured_admin_account_is_accepted(configure_admin_accounts) -> None:
    configure_admin_accounts(
        "company,director-1,director-2,Admin-Ops",
        director_value="director-1,director-2",
    )

    store = InMemorySessionStore()
    session = store.create("ADMIN-OPS")
    request = _build_request(store)

    account = security.require_admin_account(request, authorization=f"Bearer {session.token}")
    assert account == "ADMIN-OPS"


def test_director_configuration_honors_case_normalization(configure_admin_accounts) -> None:
    configure_admin_accounts(
        "Company, Director-Alpha, director-beta",
        director_value="Director-Alpha, director-beta",
    )


def test_allowlist_env_variables_are_required(monkeypatch) -> None:
    monkeypatch.delenv("ADMIN_ALLOWLIST", raising=False)
    monkeypatch.setenv("DIRECTOR_ALLOWLIST", "director-1")

    with pytest.raises(RuntimeError):
        security.reload_admin_accounts()

    monkeypatch.setenv("ADMIN_ALLOWLIST", "company,director-1,director-2")
    security.reload_admin_accounts()


def test_missing_director_allowlist_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_ALLOWLIST", "company,director-1")
    monkeypatch.delenv("DIRECTOR_ALLOWLIST", raising=False)

    with pytest.raises(RuntimeError):
        security.reload_admin_accounts()

    monkeypatch.setenv("DIRECTOR_ALLOWLIST", "director-1,director-2")
    security.reload_admin_accounts()


def test_director_allowlist_can_be_configured_independently(configure_admin_accounts) -> None:
    configure_admin_accounts(
        "company,director-1",
        director_value="director-2,director-3",
    )

    assert security.ADMIN_ACCOUNTS == {"company", "director-1"}
    assert security.DIRECTOR_ACCOUNTS == {"director-2", "director-3"}

    store = InMemorySessionStore()
    caller = store.create("company")
    director_one = store.create("DIRECTOR-2")
    director_two = store.create("director-3")
    request = _build_request(store)

    approvals = security.require_dual_director_confirmation(
        request,
        authorization=f"Bearer {caller.token}",
        x_director_approvals=f"{director_one.token},{director_two.token}",
    )

    assert set(approvals) == {"DIRECTOR-2", "director-3"}

    store = InMemorySessionStore()
    caller = store.create("company")
    director_one = store.create("DIRECTOR-ALPHA")
    director_two = store.create("director-beta")
    request = _build_request(store)

    with pytest.raises(fastapi.HTTPException):
        security.require_dual_director_confirmation(
            request,
            authorization=f"Bearer {caller.token}",
            x_director_approvals=f"{director_one.token},{director_two.token}",
        )
