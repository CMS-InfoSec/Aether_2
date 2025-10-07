"""System health endpoint aggregating portfolio, return, and mode status."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

try:  # pragma: no cover - FastAPI is optional in some unit tests
    from fastapi import APIRouter, Depends, Query
except ImportError:  # pragma: no cover - fallback when FastAPI is stubbed out
    from services.common.fastapi_stub import (  # type: ignore[misc]
        APIRouter,
        Depends,
        Query,
    )

from services.common.security import require_admin_account
from services.reports.report_service import compute_daily_return_pct
from services.risk import portfolio_risk
from shared.spot import is_spot_symbol, normalize_spot_symbol


LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/system", tags=["system"])

portfolio_aggregator = portfolio_risk.aggregator


def _ensure_safe_state_path(path: Path, *, env_var: str) -> Path:
    """Validate and return a filesystem path for simulation state tracking."""

    if not path.is_absolute():
        raise ValueError(f"{env_var} must be an absolute path")

    if any(part in {".", ".."} for part in path.parts[1:]):
        raise ValueError(f"{env_var} must not contain path traversal sequences")

    for ancestor in (path,) + tuple(path.parents):
        if ancestor.is_symlink():
            raise ValueError(f"{env_var} must not reference symlinks")

    if path.exists() and not path.is_file():
        raise ValueError(f"{env_var} must reference a regular file")

    return path


def _resolve_sim_mode_state_path() -> Path:
    """Return a sanitized path for the simulation mode state file."""

    raw = os.getenv("SIM_MODE_STATE_PATH")
    if raw:
        candidate = Path(raw).expanduser()
        return _ensure_safe_state_path(candidate, env_var="SIM_MODE_STATE_PATH")

    default_path = Path.cwd() / "simulation_mode_state.json"
    return _ensure_safe_state_path(default_path, env_var="SIM_MODE_STATE_PATH default")


_SIM_MODE_STATE_PATH = _resolve_sim_mode_state_path()


def _coerce_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _normalize_flags(breaches: Sequence[Any]) -> Sequence[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for entry in breaches:
        if isinstance(entry, Mapping):
            constraint = entry.get("constraint")
            limit = entry.get("limit")
            value = entry.get("value")
            detail = _coerce_mapping(entry.get("detail"))
        else:
            constraint = getattr(entry, "constraint", None)
            limit = getattr(entry, "limit", None)
            value = getattr(entry, "value", None)
            detail = _coerce_mapping(getattr(entry, "detail", None))
        normalized.append(
            {
                "constraint": constraint,
                "limit": float(limit) if isinstance(limit, (int, float)) else None,
                "value": float(value) if isinstance(value, (int, float)) else None,
                "detail": dict(detail),
            }
        )
    return normalized


def _canonical_spot_exposures(exposures: Mapping[str, Any]) -> Dict[str, float]:
    """Return canonical spot exposures aggregated by symbol."""

    normalized: Dict[str, float] = {}
    dropped: set[str] = set()

    for instrument, raw_value in exposures.items():
        normalized_symbol = normalize_spot_symbol(instrument)
        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            dropped.add(str(instrument))
            continue

        try:
            exposure_value = float(raw_value)
        except (TypeError, ValueError):
            continue

        normalized[normalized_symbol] = normalized.get(normalized_symbol, 0.0) + exposure_value

    if dropped:
        LOGGER.warning(
            "Dropping non-spot instruments from system health exposures",
            extra={"symbols": sorted(dropped)},
        )

    return normalized


def _top_exposures(exposures: Mapping[str, Any], *, limit: int = 5) -> Sequence[Dict[str, Any]]:
    sortable = []
    for instrument, raw_value in exposures.items():
        try:
            sortable.append((str(instrument), abs(float(raw_value)), float(raw_value)))
        except (TypeError, ValueError):
            continue
    sortable.sort(key=lambda item: item[1], reverse=True)
    return [
        {"instrument": instrument, "exposure": exposure}
        for instrument, _, exposure in sortable[:limit]
    ]


def _diversification_summary() -> Dict[str, Any]:
    summary = {"top_assets": [], "flags": [], "correlation_note": "ok"}
    aggregator = portfolio_aggregator
    if aggregator is None:
        return summary
    try:
        status = aggregator.portfolio_status()
    except Exception:  # pragma: no cover - defensive guard for runtime issues
        LOGGER.exception("Failed to fetch portfolio diversification status")
        return summary

    totals = getattr(status, "totals", None)
    exposures = {}
    max_correlation = 0.0
    if totals is not None:
        raw_exposures = getattr(totals, "instrument_exposure", {})
        if isinstance(raw_exposures, Mapping):
            exposures = _canonical_spot_exposures(raw_exposures)
        max_correlation = float(getattr(totals, "max_correlation", 0.0) or 0.0)
    summary["top_assets"] = _top_exposures(exposures)

    breaches = getattr(status, "breaches", [])
    if isinstance(breaches, Sequence):
        summary["flags"] = list(_normalize_flags(breaches))
    correlation_limit = float(getattr(aggregator, "correlation_limit", 0.0) or 0.0)
    summary["correlation_note"] = "elevated" if correlation_limit and max_correlation >= correlation_limit else "ok"
    return summary


def _load_sim_mode_file() -> Optional[Dict[str, Any]]:
    if not _SIM_MODE_STATE_PATH.exists():
        return None
    if _SIM_MODE_STATE_PATH.is_symlink():
        LOGGER.warning(
            "Refusing to read simulation mode state from symlink",
            extra={"path": str(_SIM_MODE_STATE_PATH)},
        )
        return None
    try:
        payload = json.loads(_SIM_MODE_STATE_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("Failed to read simulation mode state", extra={"error": str(exc)})
        return None
    if not isinstance(payload, Mapping):
        return None
    active = bool(payload.get("active", False))
    reason_raw = payload.get("reason")
    reason = str(reason_raw) if isinstance(reason_raw, str) and reason_raw else None
    return {"active": active, "reason": reason}


def _simulation_status() -> Dict[str, Any]:
    file_state = _load_sim_mode_file()
    if file_state is not None:
        return file_state

    env_value = os.getenv("SIMULATION_MODE", "").strip()
    env_reason = os.getenv("SIMULATION_MODE_REASON", "").strip()
    truthy = {"1", "true", "yes", "on", "enabled", "active"}
    falsy = {"0", "false", "no", "off", "disabled", "inactive"}
    lowered = env_value.lower()
    if lowered in truthy:
        active = True
    elif lowered in falsy or not env_value:
        active = False
    else:
        # If the value is a free-form reason assume simulation is active.
        active = True
        if not env_reason:
            env_reason = env_value
    reason = env_reason or None
    return {"active": active, "reason": reason}


def _daily_return(account_id: Optional[str]) -> Optional[float]:
    try:
        return compute_daily_return_pct(account_id=account_id)
    except Exception:  # pragma: no cover - defensive guard around external dependency
        LOGGER.exception("Failed to compute daily return percentage", extra={"account_id": account_id})
        return None


def build_health_snapshot(*, account_id: Optional[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    payload["daily_return_pct"] = _daily_return(account_id)
    payload["diversification"] = _diversification_summary()
    payload["simulation"] = _simulation_status()
    return payload


@router.get("/health")
async def get_system_health(
    account_id: Optional[str] = Query(default=None, description="Account identifier for account-scoped metrics"),
    _: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    """Return aggregated system health metadata for dashboards."""

    return build_health_snapshot(account_id=account_id)


__all__ = ["router", "build_health_snapshot"]
