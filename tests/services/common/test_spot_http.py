"""Unit tests for HTTP-facing USD spot symbol validators."""

import pytest
from fastapi import HTTPException

from services.common.spot import require_spot_http


def test_require_spot_http_accepts_usd_pair() -> None:
    assert require_spot_http("eth/usd") == "ETH-USD"


def test_require_spot_http_rejects_missing_symbol() -> None:
    with pytest.raises(HTTPException) as exc:
        require_spot_http(" ")

    assert exc.value.status_code == 422
    assert "must be provided" in exc.value.detail


def test_require_spot_http_rejects_derivative_symbol() -> None:
    with pytest.raises(HTTPException) as exc:
        require_spot_http("BTC-PERP")

    assert exc.value.status_code == 422
    assert "not a supported USD spot market instrument" in exc.value.detail
