"""Lightweight accessors for Kafka/NATS publishing adapters with fallbacks."""

from __future__ import annotations

from datetime import datetime, timezone
from importlib import import_module
import sys
import logging
from typing import Any, ClassVar, Dict, List, Tuple

from common.utils.tracing import attach_correlation
from shared.correlation import CorrelationContext

LOGGER = logging.getLogger(__name__)


def _normalize_account_id(value: str | None) -> str:
    """Mirror the normalisation performed by the production adapter."""

    if value is None:
        return "default"

    candidate = value.strip()
    if not candidate:
        return "default"

    normalized = candidate.lower().replace(" ", "-")
    return normalized or "default"


def _adapter_dependencies_ready() -> tuple[bool, BaseException | None]:
    """Return ``True`` when runtime dependencies for the real adapter exist."""

    try:
        import httpx  # type: ignore
    except Exception as exc:  # pragma: no cover - exercised when httpx missing
        return False, exc

    required = ("AsyncClient", "RequestError", "HTTPStatusError")
    missing = [name for name in required if not hasattr(httpx, name)]
    if missing:
        return False, AttributeError(
            f"httpx is missing required attributes: {', '.join(sorted(missing))}"
        )

    return True, None


_adapters_module = sys.modules.get("services.common.adapters")
_missing_adapter_stub = bool(
    _adapters_module is not None
    and not hasattr(_adapters_module, "KafkaNATSAdapter")
)
_force_fallback = _missing_adapter_stub

if _adapters_module is None:
    try:  # pragma: no cover - depends on optional services.common.adapters package
        _adapters_module = import_module("services.common.adapters")
    except Exception as exc:  # pragma: no cover - exercised when dependency unavailable
        _KafkaNATSAdapter = None  # type: ignore[assignment]
        _KAFKA_IMPORT_ERROR = exc
    else:  # pragma: no cover - executed when dependency is available
        _KafkaNATSAdapter = getattr(_adapters_module, "KafkaNATSAdapter", None)
        if _KafkaNATSAdapter is None or _missing_adapter_stub:
            _KAFKA_IMPORT_ERROR = AttributeError(
                "services.common.adapters lacks KafkaNATSAdapter"
            )
            _KafkaNATSAdapter = None  # type: ignore[assignment]
            _force_fallback = True
        else:
            ready, error = _adapter_dependencies_ready()
            if ready:
                _KAFKA_IMPORT_ERROR = None
            else:
                _KafkaNATSAdapter = None  # type: ignore[assignment]
                _KAFKA_IMPORT_ERROR = error
else:
    if _missing_adapter_stub:
        _KafkaNATSAdapter = None  # type: ignore[assignment]
        _KAFKA_IMPORT_ERROR = AttributeError(
            "services.common.adapters lacks KafkaNATSAdapter"
        )
        _force_fallback = True
    else:
        _KafkaNATSAdapter = getattr(_adapters_module, "KafkaNATSAdapter", None)
        if _KafkaNATSAdapter is None:
            _KAFKA_IMPORT_ERROR = AttributeError(
                "services.common.adapters lacks KafkaNATSAdapter"
            )
        else:
            ready, error = _adapter_dependencies_ready()
            if ready:
                _KAFKA_IMPORT_ERROR = None
            else:
                _KafkaNATSAdapter = None  # type: ignore[assignment]
                _KAFKA_IMPORT_ERROR = error


_delegate_cls = _KafkaNATSAdapter if not _force_fallback else None

if _force_fallback:
    _KafkaNATSAdapter = None  # type: ignore[assignment]


class KafkaNATSAdapter:  # type: ignore[no-redef]
    """Resilient Kafka/NATS publisher that falls back to in-memory delivery."""

    _event_store: ClassVar[Dict[str, List[Tuple[Dict[str, Any], bool]]]] = {}

    def __init__(self, account_id: str, **kwargs: object) -> None:
        self.account_id = _normalize_account_id(account_id)
        self._delegate = None
        if _delegate_cls is not None:
            try:
                self._delegate = _delegate_cls(account_id=account_id, **kwargs)  # type: ignore[misc]
            except Exception as exc:  # pragma: no cover - exercised in tests when adapters partially stubbed
                LOGGER.warning(
                    "Falling back to in-memory Kafka adapter due to delegate initialisation failure",
                    exc_info=exc,
                )

    def _record_local_event(
        self,
        topic: str,
        payload: Dict[str, Any],
        *,
        delivered: bool,
        partial: bool,
        delegate_recorded: bool,
    ) -> Dict[str, Any]:
        enriched = attach_correlation(payload)
        record = {
            "topic": topic,
            "payload": enriched,
            "timestamp": datetime.now(timezone.utc),
            "correlation_id": enriched.get("correlation_id"),
            "delivered": delivered,
            "partial_delivery": partial,
        }
        self._event_store.setdefault(self.account_id, []).append((record, delegate_recorded))
        return record

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        if self._delegate is None:
            self._record_local_event(
                topic,
                payload,
                delivered=True,
                partial=False,
                delegate_recorded=False,
            )
            return

        record = self._record_local_event(
            topic,
            payload,
            delivered=False,
            partial=False,
            delegate_recorded=True,
        )
        delegate_payload = dict(record["payload"])

        try:
            with CorrelationContext(record["correlation_id"]):
                await self._delegate.publish(topic, delegate_payload)
        except Exception as exc:
            LOGGER.warning(
                "Delegate publish failed; retaining event in in-memory fallback",
                exc_info=exc,
            )
            record["partial_delivery"] = True
            record["delivered"] = False
            # Mark the event as locally managed so flushes account for it.
            self._event_store[self.account_id][-1] = (record, False)
            return

        record["delivered"] = True
        delegate_records: List[Dict[str, Any]] | None = None
        try:
            delegate_records = self._delegate.history(record["correlation_id"])
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning(
                "Delegate history lookup failed; retaining shimmed record",
                exc_info=exc,
            )

        if delegate_records:
            delegate_record = delegate_records[-1]
            delivered = bool(delegate_record.get("delivered"))
            partial = bool(delegate_record.get("partial_delivery"))
            if not delivered or partial:
                record["delivered"] = delivered or not partial
                record["partial_delivery"] = partial
                self._event_store[self.account_id][-1] = (record, False)
                return

    def history(self, correlation_id: str | None = None) -> List[Dict[str, Any]]:
        stored = self._event_store.get(self.account_id, [])
        records = [record for record, _ in stored]
        if correlation_id is not None:
            return [r for r in records if r.get("correlation_id") == correlation_id]
        return records

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        if _delegate_cls is not None:
            try:
                _delegate_cls.reset(account_id)
            except Exception as exc:  # pragma: no cover - delegate cleanup failed
                LOGGER.warning(
                    "Delegate reset failed; retaining shimmed in-memory state",
                    exc_info=exc,
                )

        if account_id is None:
            cls._event_store.clear()
        else:
            normalized = _normalize_account_id(account_id)
            cls._event_store.pop(normalized, None)

    @classmethod
    async def flush_events(cls) -> Dict[str, int]:
        drained: Dict[str, int] = {}
        if _delegate_cls is not None:
            try:
                drained.update(await _delegate_cls.flush_events())
            except Exception as exc:  # pragma: no cover - delegate flush failure
                LOGGER.warning(
                    "Delegate flush failed; falling back to shimmed in-memory counts",
                    exc_info=exc,
                )

        local_counts: Dict[str, int] = {}
        for account, events in cls._event_store.items():
            count = sum(1 for _, delegate_recorded in events if not delegate_recorded)
            if count:
                local_counts[account] = count
        cls._event_store.clear()
        for account, count in local_counts.items():
            drained[account] = drained.get(account, 0) + count
        return drained

    @classmethod
    def shutdown(cls) -> None:
        if _delegate_cls is not None:
            try:
                _delegate_cls.shutdown()
            except Exception as exc:  # pragma: no cover - delegate shutdown failure
                LOGGER.warning(
                    "Delegate shutdown failed; clearing shimmed in-memory store", exc_info=exc
                )
        cls._event_store.clear()


if _delegate_cls is None and _KAFKA_IMPORT_ERROR is not None:
    LOGGER.warning(
        "KafkaNATSAdapter unavailable; using in-memory fallback",
        exc_info=_KAFKA_IMPORT_ERROR,
    )


__all__ = ["KafkaNATSAdapter"]
