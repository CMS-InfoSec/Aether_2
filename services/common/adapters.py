from __future__ import annotations


from copy import deepcopy
from dataclasses import dataclass

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from shared.k8s import KubernetesSecretClient

from services.universe.repository import UniverseRepository


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

    _telemetry: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}


    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})
        self._telemetry.setdefault(self.account_id, [])

        self._events.setdefault(self.account_id, {"acks": [], "fills": []})


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
        return self._repository.approved_universe()

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        return self._repository.fee_override(instrument)

    def fetch_online_features(self, instrument: str) -> Dict[str, Any]:
        account_store = self._online_feature_store.get(self.account_id, {})
        feature_payload = account_store.get(instrument, {})
        return dict(feature_payload)


@dataclass
class KrakenSecretManager:
    account_id: str
    namespace: str = "aether-secrets"
    k8s_client: Optional[KubernetesSecretClient] = None
    timescale: Optional[TimescaleAdapter] = None

    secret_prefix: ClassVar[str] = "kraken"

    def __post_init__(self) -> None:
        if self.k8s_client is None:
            self.k8s_client = KubernetesSecretClient(namespace=self.namespace)
        if self.timescale is None:
            self.timescale = TimescaleAdapter(account_id=self.account_id)

    @property
    def secret_name(self) -> str:
        return f"{self.secret_prefix}-{self.account_id}"

    def rotate_credentials(self, *, api_key: str, api_secret: str) -> Dict[str, Any]:
        assert self.k8s_client is not None  # for type checkers
        assert self.timescale is not None

        before_secret = self.k8s_client.get_secret(self.secret_name)
        payload = {"api_key": api_key, "api_secret": api_secret}
        self.k8s_client.patch_secret(self.secret_name, payload)

        rotated_at = datetime.now(timezone.utc)
        self.timescale.record_credential_rotation(secret_name=self.secret_name, rotated_at=rotated_at)

        return {
            "secret_name": self.secret_name,
            "rotated_at": rotated_at,
            "before": {
                "secret_name": self.secret_name,
                "material_present": bool(before_secret),
            },
            "after": {
                "secret_name": self.secret_name,
                "material_present": True,
            },
        }

    def status(self) -> Optional[Dict[str, Any]]:
        assert self.timescale is not None
        return self.timescale.credential_rotation_status()


def default_fee(currency: str = "USD") -> Dict[str, float | str]:
    return {"currency": currency, "maker": 0.1, "taker": 0.2}
