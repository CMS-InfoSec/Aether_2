from __future__ import annotations


from copy import deepcopy
from dataclasses import dataclass, field

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from shared.k8s import KrakenSecretStore

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
    _daily_usage: ClassVar[Dict[str, Dict[str, float]]] = {}
    _instrument_exposure: ClassVar[Dict[str, Dict[str, float]]] = {}
    _events: ClassVar[Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}
    _risk_configs: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _credential_rotations: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _telemetry: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}

    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})
        self._daily_usage.setdefault(self.account_id, {"loss": 0.0, "fee": 0.0})
        self._instrument_exposure.setdefault(self.account_id, {})
        self._telemetry.setdefault(self.account_id, [])
        self._events.setdefault(self.account_id, {"acks": [], "fills": [], "risk": []})
        self._credential_rotations.setdefault(self.account_id, {})
        self._risk_configs.setdefault(
            self.account_id,
            {
                "nav": 2_500_000.0,
                "loss_cap": 150_000.0,
                "fee_cap": 50_000.0,
                "max_nav_percent": 0.25,
                "var_limit": 120_000.0,
                "spread_limit_bps": 50.0,
                "latency_limit_ms": 250.0,
                "diversification_rules": {"max_single_instrument_percent": 0.35},
                "kill_switch": False,
            },
        )


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
        stored = self._events.setdefault(self.account_id, {"acks": [], "fills": [], "risk": []})
        return {bucket: [deepcopy(event) for event in events] for bucket, events in stored.items()}

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        if account_id is None:
            cls._metrics.clear()
            cls._daily_usage.clear()
            cls._instrument_exposure.clear()
            cls._events.clear()
            cls._risk_configs.clear()
            cls._credential_rotations.clear()
            cls._telemetry.clear()
            return
        cls._metrics.pop(account_id, None)
        cls._daily_usage.pop(account_id, None)
        cls._instrument_exposure.pop(account_id, None)
        cls._events.pop(account_id, None)
        cls._risk_configs.pop(account_id, None)
        cls._credential_rotations.pop(account_id, None)
        cls._telemetry.pop(account_id, None)


    def load_risk_config(self) -> Dict[str, Any]:
        return deepcopy(self._risk_configs[self.account_id])

    def update_risk_config(self, **overrides: Any) -> Dict[str, Any]:
        config = self._risk_configs[self.account_id]
        config.update(overrides)
        return deepcopy(config)

    def get_daily_usage(self) -> Dict[str, float]:
        return deepcopy(self._daily_usage[self.account_id])

    def record_daily_usage(self, projected_loss: float, projected_fee: float) -> Dict[str, float]:
        usage = self._daily_usage[self.account_id]
        usage["loss"] += float(projected_loss)
        usage["fee"] += float(projected_fee)
        return deepcopy(usage)

    def record_instrument_exposure(self, instrument: str, notional: float) -> Dict[str, float]:
        exposure = self._instrument_exposure[self.account_id]
        exposure[instrument] = exposure.get(instrument, 0.0) + float(notional)
        return deepcopy(exposure)

    def instrument_exposure(self, instrument: str) -> float:
        exposure = self._instrument_exposure[self.account_id]
        return float(exposure.get(instrument, 0.0))

    def record_event(self, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        event = {
            "type": event_type,
            "payload": deepcopy(payload),
            "timestamp": datetime.now(timezone.utc),
        }
        self._events[self.account_id]["risk"].append(event)
        return deepcopy(event)


    def record_decision(self, order_id: str, payload: Dict[str, Any]) -> None:
        entry = {
            "order_id": order_id,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc),
        }
        self._telemetry[self.account_id].append(entry)

    def telemetry(self) -> List[Dict[str, Any]]:
        return list(self._telemetry.get(self.account_id, []))

    # ------------------------------------------------------------------
    # Credential rotation metadata helpers
    # ------------------------------------------------------------------
    def record_credential_rotation(self, *, secret_name: str) -> Dict[str, Any]:
        rotated_at = datetime.now(timezone.utc)
        record = self._credential_rotations[self.account_id]
        if not record:
            record.update({
                "secret_name": secret_name,
                "created_at": rotated_at,
                "rotated_at": rotated_at,
            })
        else:
            record["secret_name"] = secret_name
            record["rotated_at"] = rotated_at
            record.setdefault("created_at", rotated_at)
        return deepcopy(record)

    def credential_rotation_status(self) -> Optional[Dict[str, Any]]:
        record = self._credential_rotations.get(self.account_id)
        if not record:
            return None
        return deepcopy(record)

    @classmethod
    def reset_rotation_state(cls) -> None:
        cls._credential_rotations.clear()



@dataclass
class RedisFeastAdapter:
    account_id: str
    _repository: UniverseRepository = field(init=False)


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


    def __post_init__(self) -> None:
        self._repository = UniverseRepository(account_id=self.account_id)

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
    secret_store: Optional[KrakenSecretStore] = None
    timescale: Optional[TimescaleAdapter] = None

    def __post_init__(self) -> None:
        if self.secret_store is None:
            self.secret_store = KrakenSecretStore(namespace=self.namespace)
        if self.timescale is None:
            self.timescale = TimescaleAdapter(account_id=self.account_id)

    @property
    def secret_name(self) -> str:
        assert self.secret_store is not None
        return self.secret_store.secret_name(self.account_id)

    def rotate_credentials(self, *, api_key: str, api_secret: str) -> Dict[str, Any]:
        assert self.secret_store is not None  # for type checkers
        assert self.timescale is not None

        before_status = self.timescale.credential_rotation_status()
        self.secret_store.write_credentials(
            self.account_id,
            api_key=api_key,
            api_secret=api_secret,
        )
        metadata = self.timescale.record_credential_rotation(secret_name=self.secret_name)

        return {
            "metadata": metadata,
            "before": before_status,
        }

    def get_credentials(self) -> Dict[str, str]:
        assert self.secret_store is not None
        credentials = self.secret_store.read_credentials(self.account_id)
        if credentials:
            return credentials
        return {"api_key": f"demo-key-{self.account_id}", "api_secret": f"demo-secret-{self.account_id}"}

    def status(self) -> Optional[Dict[str, Any]]:
        assert self.timescale is not None
        return self.timescale.credential_rotation_status()


def default_fee(currency: str = "USD") -> Dict[str, float | str]:
    return {"currency": currency, "maker": 0.1, "taker": 0.2}
