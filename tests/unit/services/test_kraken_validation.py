from typing import Any, Dict

import pytest
from fastapi import HTTPException

from services.oms.kraken_rest import KrakenRESTError
from services.secrets import secrets_service


class _StubRESTClient:
    def __init__(self, *, credential_getter, **_: Any) -> None:
        self._credential_getter = credential_getter
        self.calls = 0
        self.closed = False

    async def balance(self) -> Dict[str, Any]:
        self.calls += 1
        return {"result": {"ZUSD": "1.0"}}

    async def close(self) -> None:
        self.closed = True


class _FailingRESTClient(_StubRESTClient):
    def __init__(self, error: Exception, *, credential_getter, **kwargs: Any) -> None:
        super().__init__(credential_getter=credential_getter, **kwargs)
        self._error = error

    async def balance(self) -> Dict[str, Any]:  # type: ignore[override]
        raise self._error


def test_validate_kraken_credentials_success(monkeypatch: pytest.MonkeyPatch) -> None:
    created: Dict[str, _StubRESTClient] = {}

    def factory(**kwargs: Any) -> _StubRESTClient:
        client = _StubRESTClient(**kwargs)
        created["client"] = client
        return client

    monkeypatch.setattr(secrets_service, "KrakenRESTClient", factory)

    assert secrets_service.validate_kraken_credentials(
        "ABCDEF", "U0VDUkVU", account_id="acct"
    )
    client = created["client"]
    assert client.calls == 1
    assert client.closed is True


def test_validate_kraken_credentials_invalid_secret() -> None:
    with pytest.raises(HTTPException) as excinfo:
        secrets_service.validate_kraken_credentials(
            "ABCDEF",
            "not-base64",
            account_id="acct",
        )
    assert excinfo.value.status_code == 400


def test_validate_kraken_credentials_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    error = KrakenRESTError("Kraken API error: ['EAPI:Invalid key']")
    created: Dict[str, _FailingRESTClient] = {}

    def factory(**kwargs: Any) -> _FailingRESTClient:
        client = _FailingRESTClient(error, **kwargs)
        created["client"] = client
        return client

    monkeypatch.setattr(secrets_service, "KrakenRESTClient", factory)

    with pytest.raises(HTTPException) as excinfo:
        secrets_service.validate_kraken_credentials(
            "ABCDEF", "U0VDUkVU", account_id="acct"
        )
    assert excinfo.value.status_code == 400
    assert created["client"].closed is True


def test_validate_kraken_credentials_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = KrakenRESTError("REST request failed: Cannot connect")
    created: Dict[str, _FailingRESTClient] = {}

    def factory(**kwargs: Any) -> _FailingRESTClient:
        client = _FailingRESTClient(error, **kwargs)
        created["client"] = client
        return client

    monkeypatch.setattr(secrets_service, "KrakenRESTClient", factory)

    with pytest.raises(HTTPException) as excinfo:
        secrets_service.validate_kraken_credentials(
            "ABCDEF", "U0VDUkVU", account_id="acct"
        )
    assert excinfo.value.status_code == 502
    assert created["client"].closed is True
