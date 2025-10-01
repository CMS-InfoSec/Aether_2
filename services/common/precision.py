"""Precision metadata for Kraken trading pairs."""

from __future__ import annotations

from typing import Dict

# Representative subset of Kraken precision metadata. The defaults favour
# conservative rounding so the policy service never exceeds venue limits.
KRAKEN_PRECISION: Dict[str, Dict[str, float]] = {
    "BTC-USD": {"tick": 0.1, "lot": 0.0001},
    "ETH-USD": {"tick": 0.05, "lot": 0.0001},
    "SOL-USD": {"tick": 0.01, "lot": 0.001},
}

__all__ = ["KRAKEN_PRECISION"]

