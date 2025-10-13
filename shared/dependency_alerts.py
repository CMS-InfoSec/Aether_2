"""Helpers for emitting dependency fallback alerts without breaking local tests."""

from __future__ import annotations

import logging
import os
import sys
from typing import Mapping

try:  # pragma: no cover - alerts module may be unavailable in limited environments
    from alerts import AlertPushError, push_dependency_fallback
except Exception:  # pragma: no cover - fallback when alerts transport isn't usable
    push_dependency_fallback = None  # type: ignore[assignment]

    class AlertPushError(RuntimeError):
        """Placeholder error raised when alert push operations fail."""

LOGGER = logging.getLogger("dependency_alerts")


def notify_dependency_fallback(
    *,
    component: str,
    dependency: str,
    fallback: str,
    reason: str,
    severity: str = "critical",
    environment: str | None = None,
    metadata: Mapping[str, str] | None = None,
) -> None:
    """Emit a structured alert when a critical dependency fallback is triggered."""

    metadata_payload = {key: str(value) for key, value in (metadata or {}).items()}
    metadata_payload.setdefault("reason", reason)
    context = {
        "component": component,
        "dependency": dependency,
        "fallback": fallback,
        **metadata_payload,
    }

    if "pytest" in sys.modules and os.getenv("AETHER_ENABLE_ALERTS_IN_TESTS") != "1":
        LOGGER.info(
            "Skipping dependency fallback alert in test environment",
            extra=context,
        )
        return

    if environment is None:
        environment = (
            os.getenv("ENV")
            or os.getenv("ENVIRONMENT")
            or os.getenv("AETHER_ENV")
            or os.getenv("DEPLOYMENT_ENV")
        )

    if push_dependency_fallback is None:
        LOGGER.error(
            "Dependency fallback activated but alert transport is unavailable",
            extra={**context, "environment": environment},
        )
        return

    try:
        push_dependency_fallback(
            component=component,
            dependency=dependency,
            fallback=fallback,
            severity=severity,
            environment=environment,
            details=metadata_payload,
        )
    except AlertPushError as exc:
        LOGGER.error(
            "Failed to push dependency fallback alert",
            extra={**context, "environment": environment, "error": str(exc)},
        )
    except Exception:  # pragma: no cover - defensive guard
        LOGGER.exception(
            "Unexpected error while attempting to push dependency fallback alert",
            extra={**context, "environment": environment},
        )
