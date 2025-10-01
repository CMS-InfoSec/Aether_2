from __future__ import annotations

from typing import Any, Dict

import pytest

from services.oms.kraken_client import KrakenWSClient
from shared.k8s import KrakenSecretStore


@pytest.fixture(autouse=True)
def reset_secret_store() -> None:
    KrakenSecretStore.reset()
    yield
    KrakenSecretStore.reset()


def test_kraken_ws_client_loads_seeded_credentials() -> None:
    account_id = "company"
    api_key = "fixture-key"
    api_secret = "fixture-secret"

    store = KrakenSecretStore()
    store.write_credentials(account_id, api_key=api_key, api_secret=api_secret)

    captured: Dict[str, str] = {}

    class DummySession:
        def __init__(self, creds: Dict[str, str]) -> None:
            captured.update(creds)

        def request(self, channel: str, payload: Dict[str, Any], timeout: float | None = None) -> Dict[str, Any]:
            if channel == "openOrders":
                return {"open": []}
            return {"status": "ok"}

        def close(self) -> None:
            return None

    client = KrakenWSClient(account_id, session_factory=lambda creds: DummySession(creds))

    response = client.open_orders()
    assert response == {"open": []}
    assert captured["api_key"] == api_key
    assert captured["api_secret"] == api_secret
    assert captured.get("metadata", {}).get("secret_name") == "kraken-keys-company"
