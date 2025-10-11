"""Runtime safeguards for production deployments.

These helpers enforce that dangerous development-only toggles are not
accidentally enabled when the platform starts in a live environment.  The
checks are intentionally lightweight so they can run during import without
pulling in heavy dependencies.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable

from yaml import safe_load


_GLOBAL_ALLOW_FLAG = "AETHER_ALLOW_INSECURE_DEFAULTS"
_SYSTEM_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "system.yaml"
_ENV_FLAG = "ENV"
_REQUIRED_ALLOWLIST_ENV = ("ADMIN_ALLOWLIST", "DIRECTOR_ALLOWLIST")


def _is_test_environment() -> bool:
    """Return ``True`` when pytest or explicit overrides are active."""

    if "pytest" in sys.modules:
        return True
    return os.getenv(_GLOBAL_ALLOW_FLAG) == "1"


def is_production_environment() -> bool:
    """Return ``True`` when the process identifies itself as production."""

    for key in _PRODUCTION_ENV_KEYS:
        value = os.getenv(key)
        if value and value.strip().lower() in _PRODUCTION_VALUES:
            return True

    for key in _PRODUCTION_NAMESPACE_ENV_KEYS:
        namespace = os.getenv(key)
        if namespace and namespace.strip().lower().endswith("prod"):
            return True

    return False


def ensure_insecure_default_flag_disabled(flag_name: str) -> None:
    """Raise when ``flag_name`` enables insecure defaults in production."""

    if _is_test_environment():
        return

    if not is_production_environment():
        return

    value = os.getenv(flag_name)
    if value is None:
        return

    if value.strip().lower() in _TRUTHY:
        raise RuntimeError(
            f"{flag_name} is enabled but production deployments must use secure defaults. "
            "Unset the flag or override the environment classification to continue."
        )


def assert_insecure_defaults_disabled(
    *, allowlist: Iterable[str] | None = None
) -> None:
    """Raise when insecure-default feature flags are enabled in production.

    The majority of services expose ``*_ALLOW_INSECURE_DEFAULTS`` toggles that
    ease local development by swapping durable dependencies (TimescaleDB,
    Redis, etc.) with ephemeral on-disk stores.  Accidentally enabling these in
    production would silently route traffic to non-hardened backends.  The
    guard fails fast when any of the toggles are set to ``"1"`` unless pytest is
    driving the process or ``AETHER_ALLOW_INSECURE_DEFAULTS=1`` explicitly
    requests an override.
    """

    if _is_test_environment():
        return

    permitted = set(flag.upper() for flag in (allowlist or ()))
    offenders = sorted(
        key
        for key, value in os.environ.items()
        if key.endswith("_ALLOW_INSECURE_DEFAULTS")
        and value.strip() == "1"
        and key.upper() not in permitted
    )

    if not offenders:
        return

    formatted = ", ".join(offenders)
    raise RuntimeError(
        "Insecure fallbacks are enabled via environment variables: "
        f"{formatted}. Unset them or set {_GLOBAL_ALLOW_FLAG}=1 to explicitly "
        "acknowledge the risk in non-production environments."
    )


def assert_account_allowlists_configured() -> None:
    """Ensure administrator and director allowlists are configured via secrets."""

    if _is_test_environment():
        return

    missing = []
    for variable in _REQUIRED_ALLOWLIST_ENV:
        value = os.getenv(variable)
        if value is None or not value.strip():
            missing.append(variable)

    if missing:
        formatted = ", ".join(sorted(missing))
        raise RuntimeError(
            "Required account allowlists are not configured. Set the following "
            f"environment variables via Kubernetes secrets: {formatted}."
        )


def _load_system_config() -> dict[str, object]:
    """Return the parsed ``system.yaml`` configuration."""

    try:
        with _SYSTEM_CONFIG_PATH.open("r", encoding="utf-8") as handle:
            loaded = safe_load(handle) or {}
    except FileNotFoundError:
        return {}
    except Exception as exc:  # pragma: no cover - defensive guard
        raise RuntimeError(f"Failed to load {_SYSTEM_CONFIG_PATH}") from exc

    if isinstance(loaded, dict):
        return loaded

    raise RuntimeError(
        f"{_SYSTEM_CONFIG_PATH} must contain a top-level mapping, received {type(loaded)!r}."
    )


def assert_simulation_disabled_in_production() -> None:
    """Prevent enabling global simulation mode when ``ENV=production``."""

    environment = (os.getenv(_ENV_FLAG) or "").strip().lower()
    if environment != "production":
        return

    simulation_config = _load_system_config().get("simulation", {})
    if isinstance(simulation_config, dict):
        enabled = bool(simulation_config.get("enabled"))
    else:
        enabled = bool(simulation_config)

    if enabled:
        raise RuntimeError(
            "Simulation mode is enabled in system.yaml, but production deployments "
            "require it to remain disabled."
        )


__all__ = [
    "assert_account_allowlists_configured",
    "assert_insecure_defaults_disabled",
    "assert_simulation_disabled_in_production",
]

