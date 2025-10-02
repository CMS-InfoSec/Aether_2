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
    original_env = os.environ.get("AETHER_ADMIN_ACCOUNTS")
    original_accounts = set(security.ADMIN_ACCOUNTS)

    def _apply(config_value: str) -> None:
        monkeypatch.setenv("AETHER_ADMIN_ACCOUNTS", config_value)
        security.reload_admin_accounts()

    yield _apply

    if original_env is None:
        monkeypatch.delenv("AETHER_ADMIN_ACCOUNTS", raising=False)
    else:
        monkeypatch.setenv("AETHER_ADMIN_ACCOUNTS", original_env)

    security.reload_admin_accounts(original_accounts)


def test_configured_admin_account_is_accepted(configure_admin_accounts) -> None:
    configure_admin_accounts("company,director-1,director-2,Admin-Ops")

    store = InMemorySessionStore()
    session = store.create("ADMIN-OPS")
    request = _build_request(store)

    account = security.require_admin_account(request, authorization=f"Bearer {session.token}")
    assert account == "ADMIN-OPS"


def test_director_configuration_honors_case_normalization(configure_admin_accounts) -> None:
    configure_admin_accounts("Company, Director-Alpha, director-beta")

    store = InMemorySessionStore()
    caller = store.create("company")
    director_one = store.create("DIRECTOR-ALPHA")
    director_two = store.create("director-beta")
    request = _build_request(store)

    approvals = security.require_dual_director_confirmation(
        request,
        authorization=f"Bearer {caller.token}",
        x_director_approvals=f"{director_one.token},{director_two.token}",
    )
    assert set(approvals) == {"DIRECTOR-ALPHA", "director-beta"}
