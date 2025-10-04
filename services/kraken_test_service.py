"""Helpers for validating Kraken API credentials."""

from __future__ import annotations

import time
from typing import Dict, List


class KrakenConnectionError(RuntimeError):
    """Raised when Kraken connectivity validation fails."""


def test_kraken_connection(api_key: str, api_secret: str) -> Dict[str, object]:
    """Simulate a Kraken connectivity test.

    The real deployment issues a ``/Balance`` private API request.  For unit
    testing purposes we simply ensure both credentials are present and return a
    fabricated latency measurement.  Callers may monkeypatch this helper with a
    fully-fledged implementation when running integration tests against Kraken.
    """

    if not api_key or not api_secret:
        raise KrakenConnectionError("Kraken API credentials are required")

    start = time.perf_counter()
    # Simulate minimal processing time
    time.sleep(0.001)
    latency_ms = (time.perf_counter() - start) * 1000
    return {
        "status": "ok",
        "permissions": ["trade", "query_funds"],
        "latency_ms": round(latency_ms, 3),
    }


__all__ = ["test_kraken_connection", "KrakenConnectionError"]

