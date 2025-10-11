"""Runtime safeguards for production deployments.

These helpers enforce that dangerous development-only toggles are not
accidentally enabled when the platform starts in a live environment.  The
checks are intentionally lightweight so they can run during import without
pulling in heavy dependencies.
"""

from __future__ import annotations

import os
import sys
from typing import Iterable


_GLOBAL_ALLOW_FLAG = "AETHER_ALLOW_INSECURE_DEFAULTS"


def _is_test_environment() -> bool:
    """Return ``True`` when pytest or explicit overrides are active."""

    if "pytest" in sys.modules:
        return True
    return os.getenv(_GLOBAL_ALLOW_FLAG) == "1"


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

