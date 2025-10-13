"""Lightweight accessors for Kafka/NATS publishing adapters with fallbacks."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, ClassVar, Dict, List

from common.utils.tracing import attach_correlation

LOGGER = logging.getLogger(__name__)


def _normalize_account_id(value: str | None) -> str:
    """Mirror the normalisation performed by the production adapter."""

    if value is None:
        return "default"
    candidate = value.strip()
    return candidate or "default"


try:  # pragma: no cover - depends on optional services.common.adapters package
    from services.common.adapters import KafkaNATSAdapter as _KafkaNATSAdapter
except Exception as exc:  # pragma: no cover - exercised when dependency unavailable
    _KafkaNATSAdapter = None  # type: ignore[assignment]
    _KAFKA_IMPORT_ERROR = exc
else:  # pragma: no cover - executed when dependency is available
    _KAFKA_IMPORT_ERROR = None


if _KafkaNATSAdapter is None:
    class KafkaNATSAdapter:  # type: ignore[no-redef]
        """Fallback Kafka/NATS publisher used when the common adapters are unavailable."""

        _event_store: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}

        def __init__(self, account_id: str, **_: object) -> None:
            self.account_id = _normalize_account_id(account_id)

        async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
            enriched = attach_correlation(payload)
            correlation_id = enriched.get("correlation_id")
            record = {
                "topic": topic,
                "payload": enriched,
                "timestamp": datetime.now(timezone.utc),
                "correlation_id": correlation_id,
                "delivered": True,
                "partial_delivery": False,
            }
            self._event_store.setdefault(self.account_id, []).append(record)

        def history(self, correlation_id: str | None = None) -> List[Dict[str, Any]]:
            records = list(self._event_store.get(self.account_id, []))
            if correlation_id is not None:
                return [r for r in records if r.get("correlation_id") == correlation_id]
            return records

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:
            if account_id is None:
                cls._event_store.clear()
            else:
                normalized = _normalize_account_id(account_id)
                cls._event_store.pop(normalized, None)

        @classmethod
        async def flush_events(cls) -> Dict[str, int]:
            drained = {
                account: len(events)
                for account, events in cls._event_store.items()
                if events
            }
            cls._event_store.clear()
            return drained

        @classmethod
        def shutdown(cls) -> None:
            cls._event_store.clear()

    if _KAFKA_IMPORT_ERROR is not None:
        LOGGER.warning(
            "KafkaNATSAdapter unavailable; using in-memory fallback",
            exc_info=_KAFKA_IMPORT_ERROR,
        )
else:
    KafkaNATSAdapter = _KafkaNATSAdapter


__all__ = ["KafkaNATSAdapter"]
