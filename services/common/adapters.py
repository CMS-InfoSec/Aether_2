from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List


@dataclass
class KafkaNATSAdapter:
    account_id: str

    _event_store: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}

    def __post_init__(self) -> None:
        self._event_store.setdefault(self.account_id, [])

    def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        record = {"topic": topic, "payload": payload, "timestamp": datetime.now(timezone.utc)}
        self._event_store[self.account_id].append(record)

    def history(self) -> List[Dict[str, Any]]:
        return list(self._event_store.get(self.account_id, []))

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        if account_id is None:
            cls._event_store.clear()
            return
        cls._event_store.pop(account_id, None)


@dataclass
class TimescaleAdapter:
    account_id: str

    _metrics: ClassVar[Dict[str, Dict[str, float]]] = {}
    _events: ClassVar[Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}

    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})
        self._events.setdefault(self.account_id, {"acks": [], "fills": []})

    def record_usage(self, notional: float) -> None:
        self._metrics[self.account_id]["usage"] += notional

    def check_limits(self, notional: float) -> bool:
        projected = self._metrics[self.account_id]["usage"] + notional
        return projected <= self._metrics[self.account_id]["limit"]

    def record_ack(self, payload: Dict[str, Any]) -> None:
        event = dict(payload)
        event.setdefault("timestamp", datetime.now(timezone.utc))
        self._events[self.account_id]["acks"].append(event)

    def record_fill(self, payload: Dict[str, Any]) -> None:
        event = dict(payload)
        event.setdefault("timestamp", datetime.now(timezone.utc))
        self._events[self.account_id]["fills"].append(event)

    def events(self) -> Dict[str, List[Dict[str, Any]]]:
        stored = self._events.get(self.account_id, {"acks": [], "fills": []})
        return {"acks": list(stored["acks"]), "fills": list(stored["fills"])}

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        if account_id is None:
            cls._metrics.clear()
            cls._events.clear()
            return
        cls._metrics.pop(account_id, None)
        cls._events.pop(account_id, None)


@dataclass
class RedisFeastAdapter:
    account_id: str

    _features: ClassVar[Dict[str, Dict[str, Any]]] = {
        "admin-eu": {"approved": ["BTC-USD", "ETH-USD"], "fees": {"BTC-USD": {"maker": 0.1, "taker": 0.2}}},
        "admin-us": {"approved": ["SOL-USD"], "fees": {}},
        "admin-apac": {"approved": ["BTC-USDT", "ETH-USDT"], "fees": {}},
    }

    def approved_instruments(self) -> List[str]:
        return list(self._features.get(self.account_id, {}).get("approved", []))

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        return self._features.get(self.account_id, {}).get("fees", {}).get(instrument)


@dataclass
class KrakenSecretManager:
    account_id: str

    _secrets: ClassVar[Dict[str, Dict[str, str]]] = {
        "admin-eu": {"api_key": "eu-key", "api_secret": "eu-secret"},
        "admin-us": {"api_key": "us-key", "api_secret": "us-secret"},
        "admin-apac": {"api_key": "apac-key", "api_secret": "apac-secret"},
    }

    def get_credentials(self) -> Dict[str, str]:
        if self.account_id not in self._secrets:
            raise PermissionError("Account has no Kraken credentials")
        return dict(self._secrets[self.account_id])


def default_fee(currency: str = "USD") -> Dict[str, float | str]:
    return {"currency": currency, "maker": 0.1, "taker": 0.2}
