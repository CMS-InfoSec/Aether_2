"""HTTP-facing helpers for enforcing USD spot-only trading symbols."""

from __future__ import annotations

import logging
from typing import Callable

from fastapi import HTTPException, status

from shared import spot as _spot

normalize_spot_symbol: Callable[[object], str] = _spot.normalize_spot_symbol
require_spot_symbol: Callable[[object], str] = _spot.require_spot_symbol

LOGGER = logging.getLogger(__name__)

__all__ = ["require_spot_http"]


def require_spot_http(
    symbol: object,
    *,
    param: str = "symbol",
    logger: logging.Logger | None = None,
) -> str:
    """Return ``symbol`` normalised when it represents a USD spot market pair.

    The helper wraps :func:`shared.spot.require_spot_symbol` so HTTP handlers can
    surface consistent ``422`` errors when callers supply derivatives, leveraged
    tokens, or missing instruments.  A module level logger is used by default to
    emit a warning for auditability.
    """

    try:
        return require_spot_symbol(symbol)
    except ValueError as exc:
        normalized = normalize_spot_symbol(symbol)
        detail: str
        if not normalized:
            detail = f"{param} must be provided as a USD spot market instrument"
        else:
            detail = f"{param} '{normalized}' is not a supported USD spot market instrument"

        log = logger or LOGGER
        log.warning("Rejected non-spot instrument for %s", param, extra={"symbol": symbol})
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail) from exc
