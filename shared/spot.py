"""Utilities for validating and normalising USD spot trading instruments."""

from __future__ import annotations

import logging
import re
from typing import Iterable, List, Optional, Sequence, Set

__all__ = [
    "normalize_spot_symbol",
    "is_spot_symbol",
    "filter_spot_symbols",
    "require_spot_symbol",
]


_SPOT_PAIR_PATTERN = re.compile(r"^[A-Z0-9]{2,}-[A-Z0-9]{2,}$")
_NON_SPOT_KEYWORDS: Sequence[str] = ("PERP", "FUT", "FUTURE", "MARGIN", "SWAP", "OPTION", "DERIV")
_LEVERAGE_SUFFIXES: Sequence[str] = ("UP", "DOWN")
_LEVERAGE_PATTERN = re.compile(r"\d+(?:X|L|S)$")
_ALLOWED_QUOTES: Sequence[str] = ("USD",)


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
    """Return ``True`` when *symbol* represents a USD-quoted spot market pair."""

    normalized = normalize_spot_symbol(symbol)
    if not normalized:
        return False

    if any(keyword in normalized for keyword in _NON_SPOT_KEYWORDS):
        return False

    if not _SPOT_PAIR_PATTERN.match(normalized):
        return False

    base, quote = normalized.split("-", maxsplit=1)

    if quote not in _ALLOWED_QUOTES:
        return False

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


def require_spot_symbol(symbol: object, *, message: str | None = None) -> str:
    """Return *symbol* normalised when it represents a USD spot market pair.

    ``ValueError`` is raised when the supplied *symbol* is missing or does not
    correspond to an allowed spot instrument.  A custom *message* may be
    provided to tailor the error for different call sites.
    """

    normalized = normalize_spot_symbol(symbol)
    if not normalized or not is_spot_symbol(normalized):
        raise ValueError(message or "Only spot market instruments are supported.")
    return normalized
