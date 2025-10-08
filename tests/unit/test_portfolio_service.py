from __future__ import annotations

from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from typing import Any, Iterable, Mapping

import importlib
import sys

import pytest


class _HttpxResponse:
    def __init__(self, payload: Mapping[str, Any] | None = None, status_code: int = 200) -> None:
        self._payload = dict(payload or {})
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            raise _HttpxStatusError("error", response=SimpleNamespace(status_code=self.status_code))

    def json(self) -> Mapping[str, Any]:
        return dict(self._payload)


class _HttpxAsyncClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def __aenter__(self) -> "_HttpxAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(self, *args: Any, **kwargs: Any) -> _HttpxResponse:
        return _HttpxResponse()


class _HttpxError(Exception):
    pass


class _HttpxStatusError(_HttpxError):
    def __init__(self, message: str, *, response: Any | None = None) -> None:
        super().__init__(message)
        self.response = response or SimpleNamespace(status_code=500)


@pytest.fixture(autouse=True)
def _stub_httpx(monkeypatch: pytest.MonkeyPatch) -> None:
    if "httpx" in sys.modules:
        return
    stub = SimpleNamespace(
        AsyncClient=_HttpxAsyncClient,
        HTTPError=_HttpxError,
        HTTPStatusError=_HttpxStatusError,
    )
    monkeypatch.setitem(sys.modules, "httpx", stub)

    from services.common import fastapi_stub

    if "fastapi" not in sys.modules:
        fastapi_module = ModuleType("fastapi")
        for name in getattr(fastapi_stub, "__all__", []):
            setattr(fastapi_module, name, getattr(fastapi_stub, name))
        # Ensure status codes are available even if omitted from __all__
        if not hasattr(fastapi_module, "status"):
            fastapi_module.status = fastapi_stub.status
        monkeypatch.setitem(sys.modules, "fastapi", fastapi_module)

    if "fastapi.responses" not in sys.modules:
        responses_module = ModuleType("fastapi.responses")
        responses_module.JSONResponse = fastapi_stub.JSONResponse
        responses_module.StreamingResponse = fastapi_stub.StreamingResponse
        monkeypatch.setitem(sys.modules, "fastapi.responses", responses_module)

    if "pydantic" not in sys.modules:
        pydantic_module = ModuleType("pydantic")

        class _BaseModel:
            def __init__(self, **data: Any) -> None:
                for key, value in data.items():
                    setattr(self, key, value)

            @classmethod
            def parse_obj(cls, data: Mapping[str, Any]) -> "_BaseModel":
                return cls(**dict(data))

        class _EmailStr(str):
            @classmethod
            def __get_validators__(cls):  # pragma: no cover - compatibility hook
                yield cls.validate

            @classmethod
            def validate(cls, value: Any) -> "_EmailStr":
                if isinstance(value, str):
                    return cls(value)
                raise TypeError("EmailStr requires a string value")

        pydantic_module.BaseModel = _BaseModel
        pydantic_module.EmailStr = _EmailStr
        monkeypatch.setitem(sys.modules, "pydantic", pydantic_module)

    # Provide a lightweight portfolio balance reader to avoid importing optional
    # dependencies like httpx or prometheus for these unit tests.
    if "services.portfolio.balance_reader" not in sys.modules:
        balance_reader_module = ModuleType("services.portfolio.balance_reader")

        class _BalanceReaderStub:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            def invalidate(self, account_id: str | None = None) -> None:  # pragma: no cover - noop
                return None

        class _BalanceRetrievalError(Exception):
            pass

        balance_reader_module.BalanceReader = _BalanceReaderStub
        balance_reader_module.BalanceRetrievalError = _BalanceRetrievalError
        monkeypatch.setitem(sys.modules, "services.portfolio.balance_reader", balance_reader_module)


def _connection_stub(rows: Iterable[Mapping[str, Any]]):
    class _CursorStub:
        def __init__(self, items: Iterable[Mapping[str, Any]]) -> None:
            self._rows = [dict(row) for row in items]

        def __enter__(self) -> "_CursorStub":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        def execute(self, query: str, params: Mapping[str, Any]) -> None:
            self.query = query
            self.params = params

        def fetchall(self) -> list[Mapping[str, Any]]:
            return [dict(row) for row in self._rows]

    class _ConnectionStub:
        def __init__(self, items: Iterable[Mapping[str, Any]]) -> None:
            self._rows = [dict(row) for row in items]

        def __enter__(self) -> "_ConnectionStub":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        def cursor(self) -> _CursorStub:
            return _CursorStub(self._rows)

    return _ConnectionStub(rows)


def test_compute_daily_return_pct_prefers_mid_price(monkeypatch: pytest.MonkeyPatch) -> None:
    portfolio_rows = [
        {
            "nav_open_usd_mark": 1_000_000.0,
            "nav_close_usd_mark": 1_020_000.0,
            "nav_open_usd_mid": 1_000_000.0,
            "nav_close_usd_mid": 1_050_000.0,
            "curve_ts": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        }
    ]

    import portfolio_service

    monkeypatch.setattr(portfolio_service, "_connect", lambda: _connection_stub(portfolio_rows))

    pct = portfolio_service.compute_daily_return_pct(
        "acct-123", datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    )
    assert pct == pytest.approx(5.0)


def test_compute_daily_return_pct_zero_nav_open_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    portfolio_rows = [
        {
            "nav_open_usd": 0.0,
            "nav_close_usd": 1_000_000.0,
            "curve_ts": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        }
    ]

    import portfolio_service

    monkeypatch.setattr(portfolio_service, "_connect", lambda: _connection_stub(portfolio_rows))

    with caplog.at_level("WARNING"):
        pct = portfolio_service.compute_daily_return_pct(
            "acct-456", datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
        )

    assert pct == 0.0
    assert any("nav_open_usd" in record.message for record in caplog.records)


def test_compute_daily_return_pct_uses_nav_series(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "nav": 1_000_000.0,
            "curve_ts": datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc),
        },
        {
            "nav": 1_040_000.0,
            "curve_ts": datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        },
    ]

    import portfolio_service

    monkeypatch.setattr(portfolio_service, "_connect", lambda: _connection_stub(rows))

    pct = portfolio_service.compute_daily_return_pct(
        "acct-789", datetime(2024, 1, 1, 23, 59, tzinfo=timezone.utc)
    )
    assert pct == pytest.approx(4.0)


def test_query_positions_filters_derivatives(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    rows = [
        {"account_id": "company", "symbol": "BTC/USD", "notional": 1_000_000.0},
        {"account_id": "company", "instrument": "ETH-PERP", "notional": 100_000.0},
        {"account_id": "company", "pair": "eth_usd", "notional": 500_000.0},
    ]

    import portfolio_service

    monkeypatch.setattr(portfolio_service, "_connect", lambda: _connection_stub(rows))

    with caplog.at_level("WARNING"):
        result = portfolio_service.query_positions(account_scopes=["company"], limit=10)

    assert result == [
        {"account_id": "company", "symbol": "BTC-USD", "notional": 1_000_000.0},
        {"account_id": "company", "pair": "ETH-USD", "notional": 500_000.0},
    ]
    assert any("non-spot" in record.message for record in caplog.records)


def test_parse_datetime_accepts_trailing_z() -> None:
    import portfolio_service

    importlib.reload(portfolio_service)

    parsed = portfolio_service._parse_datetime("2024-01-01T00:00:00Z")
    assert parsed == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_parse_datetime_assumes_utc_for_naive_values() -> None:
    import portfolio_service

    importlib.reload(portfolio_service)

    parsed = portfolio_service._parse_datetime("2024-01-01T12:30:00")
    assert parsed == datetime(2024, 1, 1, 12, 30, tzinfo=timezone.utc)

