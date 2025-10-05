"""Persistence helpers for sharing circuit breaker state across replicas."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Optional

from common.utils.redis import create_redis_from_url


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CircuitBreakerPersistedState:
    """Serializable representation of a halted instrument."""

    instrument: str
    reason: str
    expires_at: Optional[float]

    def to_dict(self) -> Dict[str, object]:
        return {
            "instrument": self.instrument,
            "reason": self.reason,
            "expires_at": self.expires_at,
        }

    @staticmethod
    def from_dict(payload: Dict[str, object]) -> Optional["CircuitBreakerPersistedState"]:
        instrument = payload.get("instrument")
        reason = payload.get("reason")
        expires_raw = payload.get("expires_at")

        if not isinstance(instrument, str) or not instrument:
            return None
        if not isinstance(reason, str) or not reason:
            return None

        expires_at: Optional[float]
        if isinstance(expires_raw, (int, float)):
            expires_at = float(expires_raw)
        else:
            expires_at = None

        return CircuitBreakerPersistedState(
            instrument=instrument,
            reason=reason,
            expires_at=expires_at,
        )


class CircuitBreakerStateStore:
    """Persist circuit breaker halts in Redis for crash recovery."""

    _DEFAULT_KEY = "oms:circuit-breakers:halts"

    def __init__(
        self,
        redis_client: Optional[Any] = None,
        *,
        key: str = _DEFAULT_KEY,
    ) -> None:
        self._lock = Lock()
        self._key = key
        self._redis = redis_client or self._create_default_client()

    @staticmethod
    def _create_default_client() -> Any:
        redis_url = os.getenv("OMS_CIRCUIT_BREAKER_REDIS_URL", "redis://localhost:6379/0")
        client, _ = create_redis_from_url(redis_url, decode_responses=True, logger=LOGGER)
        return client

    def _load_payload(self) -> Dict[str, Dict[str, object]]:
        try:
            raw_state = self._redis.get(self._key)
        except Exception as exc:  # pragma: no cover - operational resilience
            LOGGER.warning("Failed to load circuit breaker state: %s", exc)
            return {}

        if raw_state is None:
            return {}

        if isinstance(raw_state, bytes):
            try:
                raw_state = raw_state.decode("utf-8")
            except UnicodeDecodeError:
                return {}

        try:
            payload = json.loads(raw_state)
        except (TypeError, json.JSONDecodeError):
            return {}

        if not isinstance(payload, dict):
            return {}

        normalized: Dict[str, Dict[str, object]] = {}
        for instrument, entry in payload.items():
            if isinstance(instrument, str) and isinstance(entry, dict):
                normalized[instrument] = dict(entry)
        return normalized

    def _persist_payload(self, payload: Dict[str, Dict[str, object]]) -> None:
        try:
            if payload:
                serialized = json.dumps(payload)
                self._redis.set(self._key, serialized)
            else:
                self._redis.delete(self._key)
        except Exception as exc:  # pragma: no cover - operational resilience
            LOGGER.warning("Failed to persist circuit breaker state: %s", exc)

    def load_all(self) -> Dict[str, CircuitBreakerPersistedState]:
        with self._lock:
            payload = self._load_payload()

        result: Dict[str, CircuitBreakerPersistedState] = {}
        for instrument, raw_state in payload.items():
            persisted = CircuitBreakerPersistedState.from_dict({
                "instrument": instrument,
                **raw_state,
            })
            if persisted is not None:
                result[instrument] = persisted
        return result

    def save(self, state: CircuitBreakerPersistedState) -> None:
        with self._lock:
            payload = self._load_payload()
            payload[state.instrument] = state.to_dict()
            self._persist_payload(payload)

    def delete(self, instrument: str) -> None:
        with self._lock:
            payload = self._load_payload()
            if instrument in payload:
                payload.pop(instrument, None)
                self._persist_payload(payload)

    def clear(self) -> None:
        with self._lock:
            try:
                self._redis.delete(self._key)
            except Exception as exc:  # pragma: no cover - operational resilience
                LOGGER.warning("Failed to clear circuit breaker state: %s", exc)


__all__ = [
    "CircuitBreakerPersistedState",
    "CircuitBreakerStateStore",
]
