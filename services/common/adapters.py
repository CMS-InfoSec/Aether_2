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


@dataclass
class TimescaleAdapter:
    account_id: str

    _metrics: ClassVar[Dict[str, Dict[str, float]]] = {}
    _telemetry: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}

    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})
        self._telemetry.setdefault(self.account_id, [])

    def record_usage(self, notional: float) -> None:
        self._metrics[self.account_id]["usage"] += notional

    def check_limits(self, notional: float) -> bool:
        projected = self._metrics[self.account_id]["usage"] + notional
        return projected <= self._metrics[self.account_id]["limit"]

    def record_decision(self, order_id: str, payload: Dict[str, Any]) -> None:
        entry = {
            "order_id": order_id,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc),
        }
        self._telemetry[self.account_id].append(entry)

    def telemetry(self) -> List[Dict[str, Any]]:
        return list(self._telemetry.get(self.account_id, []))


@dataclass
class RedisFeastAdapter:
    account_id: str

    _features: ClassVar[Dict[str, Dict[str, Any]]] = {
        "admin-eu": {"approved": ["BTC-USD", "ETH-USD"], "fees": {"BTC-USD": {"maker": 0.1, "taker": 0.2}}},
        "admin-us": {"approved": ["SOL-USD"], "fees": {}},
        "admin-apac": {"approved": ["BTC-USDT", "ETH-USDT"], "fees": {}},
    }
    _online_feature_store: ClassVar[Dict[str, Dict[str, Dict[str, Any]]]] = {
        "admin-eu": {
            "BTC-USD": {
                "features": [18.0, 4.0, -2.0],
                "book_snapshot": {"mid_price": 30_000.0, "spread_bps": 3.0, "imbalance": 0.15},
                "state": {
                    "regime": "neutral",
                    "volatility": 0.35,
                    "liquidity_score": 0.75,
                    "conviction": 0.6,
                },
                "expected_edge_bps": 14.0,
                "take_profit_bps": 25.0,
                "stop_loss_bps": 10.0,
                "confidence": {
                    "model_confidence": 0.62,
                    "state_confidence": 0.58,
                    "execution_confidence": 0.64,
                },
            }
        }
    }

    def approved_instruments(self) -> List[str]:
        return list(self._features.get(self.account_id, {}).get("approved", []))

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        return self._features.get(self.account_id, {}).get("fees", {}).get(instrument)

    def fetch_online_features(self, instrument: str) -> Dict[str, Any]:
        account_store = self._online_feature_store.get(self.account_id, {})
        feature_payload = account_store.get(instrument, {})
        return dict(feature_payload)


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
