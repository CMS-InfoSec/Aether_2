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


@dataclass
class TimescaleAdapter:
    account_id: str

    _metrics: ClassVar[Dict[str, Dict[str, float]]] = {}

    _risk_configs: ClassVar[Dict[str, Dict[str, Any]]] = {
        "admin-eu": {
            "nav": 5_000_000.0,
            "loss_cap": 250_000.0,
            "fee_cap": 75_000.0,
            "max_nav_percent": 0.25,
            "var_limit": 200_000.0,
            "spread_limit_bps": 50.0,
            "latency_limit_ms": 250.0,
            "diversification_rules": {"max_single_instrument_percent": 0.35},
            "kill_switch": False,
        },
        "admin-us": {
            "nav": 3_500_000.0,
            "loss_cap": 200_000.0,
            "fee_cap": 60_000.0,
            "max_nav_percent": 0.3,
            "var_limit": 150_000.0,
            "spread_limit_bps": 45.0,
            "latency_limit_ms": 200.0,
            "diversification_rules": {"max_single_instrument_percent": 0.3},
            "kill_switch": False,
        },
        "admin-apac": {
            "nav": 4_000_000.0,
            "loss_cap": 225_000.0,
            "fee_cap": 70_000.0,
            "max_nav_percent": 0.28,
            "var_limit": 180_000.0,
            "spread_limit_bps": 60.0,
            "latency_limit_ms": 275.0,
            "diversification_rules": {"max_single_instrument_percent": 0.4},
            "kill_switch": False,
        },
    }
    _daily_usage: ClassVar[Dict[str, Dict[str, float]]] = {}
    _instrument_exposure: ClassVar[Dict[str, Dict[str, float]]] = {}
    _events: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}


    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})
        self._daily_usage.setdefault(self.account_id, {"loss": 0.0, "fee": 0.0})
        self._instrument_exposure.setdefault(self.account_id, {})
        self._events.setdefault(self.account_id, [])

    def record_usage(self, notional: float) -> None:
        self._metrics[self.account_id]["usage"] += notional

    def check_limits(self, notional: float) -> bool:
        projected = self._metrics[self.account_id]["usage"] + notional
        return projected <= self._metrics[self.account_id]["limit"]


    def load_risk_config(self) -> Dict[str, Any]:
        config = self._risk_configs.setdefault(
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
        return deepcopy(config)

    def get_daily_usage(self) -> Dict[str, float]:
        usage = self._daily_usage.setdefault(self.account_id, {"loss": 0.0, "fee": 0.0})
        return dict(usage)

    def record_daily_usage(self, loss: float, fee: float) -> None:
        usage = self._daily_usage.setdefault(self.account_id, {"loss": 0.0, "fee": 0.0})
        usage["loss"] += loss
        usage["fee"] += fee

    def instrument_exposure(self, instrument: str) -> float:
        exposure = self._instrument_exposure.setdefault(self.account_id, {})
        return exposure.get(instrument, 0.0)

    def record_instrument_exposure(self, instrument: str, notional: float) -> None:
        exposure = self._instrument_exposure.setdefault(self.account_id, {})
        exposure[instrument] = exposure.get(instrument, 0.0) + notional

    def record_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        event = {
            "type": event_type,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc),
        }
        self._events.setdefault(self.account_id, []).append(event)

    def events(self) -> List[Dict[str, Any]]:
        return list(self._events.get(self.account_id, []))



@dataclass
class RedisFeastAdapter:
    account_id: str

    _repository: UniverseRepository = field(init=False)

    def __post_init__(self) -> None:
        self._repository = UniverseRepository(account_id=self.account_id)

    def approved_instruments(self) -> List[str]:
        return self._repository.approved_universe()

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        return self._repository.fee_override(instrument)


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
