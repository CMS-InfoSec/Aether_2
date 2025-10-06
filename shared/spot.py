"""Utilities for validating and normalising spot trading instruments."""

from __future__ import annotations

import logging
import re
from typing import Iterable, List, Optional, Sequence, Set

__all__ = [
    "normalize_spot_symbol",
    "is_spot_symbol",
    "filter_spot_symbols",
]


_SPOT_PAIR_PATTERN = re.compile(r"^[A-Z0-9]{2,}-[A-Z0-9]{2,}$")
_NON_SPOT_KEYWORDS: Sequence[str] = ("PERP", "FUT", "FUTURE", "MARGIN", "SWAP", "OPTION", "DERIV")
_LEVERAGE_SUFFIXES: Sequence[str] = ("UP", "DOWN")
_LEVERAGE_PATTERN = re.compile(r"\d+(?:X|L|S)$")


def normalize_spot_symbol(symbol: object) -> str:
    """Return a canonical, uppercase representation of a spot symbol.

    The function accepts inputs such as ``"btc-usd"`` or ``"BTC/USD"`` and returns
    ``"BTC-USD"``.  Invalid values result in an empty string to simplify validation
    flows that treat falsy results as missing data.
    """

    if symbol is None:
        return ""

    candidate = str(symbol).strip()
    if not candidate:
        return ""

    normalized = candidate.replace("/", "-").replace("_", "-").upper()
    return normalized


def is_spot_symbol(symbol: object) -> bool:
    """Return ``True`` when *symbol* represents a spot market trading pair."""

    normalized = normalize_spot_symbol(symbol)
    if not normalized:
        return False

    if any(keyword in normalized for keyword in _NON_SPOT_KEYWORDS):
        return False

    if not _SPOT_PAIR_PATTERN.match(normalized):
        return False

    base, _ = normalized.split("-", maxsplit=1)

    if any(base.endswith(suffix) for suffix in _LEVERAGE_SUFFIXES):
        return False

    if _LEVERAGE_PATTERN.search(base):
        return False

    return True


def filter_spot_symbols(
    symbols: Iterable[object], *, logger: Optional[logging.Logger] = None
) -> List[str]:
    """Return the subset of *symbols* that represent spot market pairs.

    Symbols are normalised via :func:`normalize_spot_symbol` and deduplicated while
    preserving input order.  Any non-spot instruments are optionally logged via the
    supplied ``logger``.
    """

    filtered: List[str] = []
    seen: Set[str] = set()

    for symbol in symbols:
        normalized = normalize_spot_symbol(symbol)
        if not normalized:
            continue

        if not is_spot_symbol(normalized):
            if logger is not None:
                logger.warning(
                    "Ignoring non-spot instrument '%s' in spot-only context", symbol
                )
            continue

        if normalized in seen:
            continue

        filtered.append(normalized)
        seen.add(normalized)

    return filtered
