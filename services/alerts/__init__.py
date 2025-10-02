"""Alert service package providing deduplication utilities."""

from .alert_dedupe import (
    AlertDedupeMetrics,
    AlertDedupeService,
    AlertPolicy,
    configure_alert_dedupe_service,
    get_alert_dedupe_service,
    router,
    setup_alert_dedupe,
)

__all__ = [
    "AlertDedupeMetrics",
    "AlertDedupeService",
    "AlertPolicy",
    "configure_alert_dedupe_service",
    "get_alert_dedupe_service",
    "router",
    "setup_alert_dedupe",
]
