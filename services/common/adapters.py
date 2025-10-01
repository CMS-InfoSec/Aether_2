from __future__ import annotations


import base64
import logging
from copy import deepcopy
from dataclasses import dataclass, field

from datetime import datetime, timezone
from typing import Any, Callable, ClassVar, Dict, List, Optional

from shared.k8s import KrakenSecretStore

from services.universe.repository import UniverseRepository


logger = logging.getLogger(__name__)


def _mask_secret(value: str) -> str:
    if not value:
        return "***"
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"


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
    _events: ClassVar[Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}
    _credential_events: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}

    _events: ClassVar[Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}
    _risk_configs: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _credential_rotations: ClassVar[Dict[str, Dict[str, Any]]] = {}

    _risk_configs: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _daily_usage: ClassVar[Dict[str, Dict[str, Dict[str, float]]]] = {}
    _instrument_exposures: ClassVar[Dict[str, Dict[str, float]]] = {}

    _default_risk_config: ClassVar[Dict[str, Any]] = {
        "kill_switch": False,
        "loss_cap": 50_000.0,
        "fee_cap": 5_000.0,
        "nav": 1_000_000.0,
        "max_nav_percent": 0.20,
        "var_limit": 250_000.0,
        "spread_limit_bps": 25.0,

        "latency_limit_ms": 250.0,
        "diversification_rules": {"max_single_instrument_percent": 0.35},
        "kill_switch": False,
    }

    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})
        self._daily_usage.setdefault(self.account_id, {})
        self._instrument_exposures.setdefault(self.account_id, {})
        self._telemetry.setdefault(self.account_id, [])

        account_events = self._events.setdefault(
            self.account_id,
            {"acks": [], "fills": [], "events": [], "credential_rotations": []},
        )
        account_events.setdefault("acks", [])
        account_events.setdefault("fills", [])
        account_events.setdefault("events", [])
        account_events.setdefault("credential_rotations", [])

        self._credential_events.setdefault(self.account_id, [])
        self._credential_rotations.setdefault(self.account_id, {})

        self._risk_configs.setdefault(self.account_id, deepcopy(self._default_risk_config))

    # ------------------------------------------------------------------
    # OMS-inspired metrics
    # ------------------------------------------------------------------
    def record_usage(self, notional: float) -> None:
        self._metrics[self.account_id]["usage"] += float(notional)

    def check_limits(self, notional: float) -> bool:
        projected = self._metrics[self.account_id]["usage"] + float(notional)
        return projected <= self._metrics[self.account_id]["limit"]

    def record_ack(self, payload: Dict[str, Any]) -> None:
        record = {"payload": deepcopy(payload), "recorded_at": datetime.now(timezone.utc)}
        self._events[self.account_id]["acks"].append(record)

    def record_fill(self, payload: Dict[str, Any]) -> None:
        record = {"payload": deepcopy(payload), "recorded_at": datetime.now(timezone.utc)}
        self._events[self.account_id]["fills"].append(record)

    def events(self) -> Dict[str, List[Dict[str, Any]]]:

        stored = self._events.get(
            self.account_id,
            {"acks": [], "fills": [], "events": [], "credential_rotations": []},
        )
        return {
            "acks": [
                {**deepcopy(entry["payload"]), "recorded_at": entry["recorded_at"]}
                for entry in stored.get("acks", [])
            ],
            "fills": [
                {**deepcopy(entry["payload"]), "recorded_at": entry["recorded_at"]}
                for entry in stored.get("fills", [])
            ],
            "events": [deepcopy(entry) for entry in stored.get("events", [])],
            "credential_rotations": [
                deepcopy(entry) for entry in stored.get("credential_rotations", [])
            ],

        }

    # ------------------------------------------------------------------
    # Timescale-inspired risk state helpers
    # ------------------------------------------------------------------

    def load_risk_config(self) -> Dict[str, Any]:
        config = self._risk_configs.setdefault(
            self.account_id, deepcopy(self._default_risk_config)
        )
        return deepcopy(config)

    def get_daily_usage(self) -> Dict[str, float]:
        date_key = datetime.now(timezone.utc).date().isoformat()
        usage = self._daily_usage.setdefault(self.account_id, {})
        day_usage = usage.setdefault(date_key, {"loss": 0.0, "fee": 0.0})
        return deepcopy(day_usage)

    def record_daily_usage(self, loss: float, fee: float) -> None:
        date_key = datetime.now(timezone.utc).date().isoformat()
        usage = self._daily_usage.setdefault(self.account_id, {})
        day_usage = usage.setdefault(date_key, {"loss": 0.0, "fee": 0.0})
        day_usage["loss"] += float(loss)
        day_usage["fee"] += float(fee)

    def record_instrument_exposure(self, instrument: str, notional: float) -> None:
        exposures = self._instrument_exposures.setdefault(self.account_id, {})
        exposures[instrument] = exposures.get(instrument, 0.0) + float(notional)

    def instrument_exposure(self, instrument: str) -> float:
        exposures = self._instrument_exposures.get(self.account_id, {})
        return float(exposures.get(instrument, 0.0))

    def record_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        entry = {
            "type": event_type,
            "payload": deepcopy(payload),
            "timestamp": datetime.now(timezone.utc),
        }
        self._events[self.account_id]["events"].append(entry)

    # ------------------------------------------------------------------
    # Telemetry helpers
    # ------------------------------------------------------------------
    def record_decision(self, order_id: str, payload: Dict[str, Any]) -> None:
        entry = {
            "order_id": order_id,
            "payload": deepcopy(payload),
            "timestamp": datetime.now(timezone.utc),
        }
        self._telemetry[self.account_id].append(entry)

    def telemetry(self) -> List[Dict[str, Any]]:
        return [deepcopy(entry) for entry in self._telemetry.get(self.account_id, [])]

    # ------------------------------------------------------------------
    # Credential rotation tracking
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Credential rotation tracking & test helpers
    # ------------------------------------------------------------------
    def record_credential_rotation(
        self, *, secret_name: str, rotated_at: Optional[datetime] = None
    ) -> Dict[str, Any]:
        if rotated_at is None:
            rotated_at = datetime.now(timezone.utc)

        rotation_state = self._credential_rotations.setdefault(self.account_id, {})

        created_at = rotation_state.get("created_at") or rotated_at
        rotation_state.update(
            {
                "secret_name": secret_name,
                "created_at": created_at,
                "rotated_at": rotated_at,
            }
        )

        payload = {
            "event": "rotation",
            "secret_name": secret_name,
            "rotated_at": rotated_at,
            "timestamp": rotated_at,
            "created_at": created_at,
        }
        events = self._credential_events.setdefault(self.account_id, [])
        events.append(deepcopy(payload))

        return deepcopy(rotation_state)

    def record_credential_access(self, *, secret_name: str, metadata: Dict[str, Any]) -> None:
        sanitized = deepcopy(metadata)
        for key in ("api_key", "api_secret"):
            if key in sanitized:
                sanitized[key] = "***"
        payload = {
            "event": "access",
            "secret_name": secret_name,
            "metadata": sanitized,
            "timestamp": datetime.now(timezone.utc),
        }
        events = self._credential_events.setdefault(self.account_id, [])
        events.append(deepcopy(payload))

    def credential_rotation_status(self) -> Optional[Dict[str, Any]]:
        metadata = self._credential_rotations.get(self.account_id)
        if not metadata:
            return None
        return deepcopy(metadata)

    def credential_events(self) -> List[Dict[str, Any]]:
        return [deepcopy(event) for event in self._credential_events.get(self.account_id, [])]

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        if account_id is None:
            cls._metrics.clear()
            cls._telemetry.clear()
            cls._risk_configs.clear()
            cls._daily_usage.clear()
            cls._instrument_exposures.clear()
            cls._events.clear()
            cls._credential_events.clear()
            cls._credential_rotations.clear()
            return

        cls._metrics.pop(account_id, None)
        cls._telemetry.pop(account_id, None)
        cls._risk_configs.pop(account_id, None)
        cls._daily_usage.pop(account_id, None)
        cls._instrument_exposures.pop(account_id, None)
        cls._events.pop(account_id, None)
        cls._credential_events.pop(account_id, None)
        cls._credential_rotations.pop(account_id, None)

    @classmethod
    def reset_rotation_state(cls, account_id: str | None = None) -> None:
        if account_id is None:
            cls._credential_events.clear()
            cls._credential_rotations.clear()
            return

        cls._credential_events.pop(account_id, None)
        cls._credential_rotations.pop(account_id, None)



@dataclass
class RedisFeastAdapter:
    account_id: str
    repository_factory: Callable[[str], UniverseRepository] = field(
        default=UniverseRepository, repr=False
    )


    _repository: UniverseRepository = field(init=False, repr=False)


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

        self._repository = self.repository_factory(self.account_id)


    def approved_instruments(self) -> List[str]:
        assert self._repository is not None
        return self._repository.approved_universe()

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        assert self._repository is not None
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


    def get_credentials(self) -> Dict[str, str]:
        assert self.secret_store is not None
        return self.secret_store.read_credentials(self.account_id)


    def get_credentials(self) -> Dict[str, str]:
        assert self.k8s_client is not None
        assert self.timescale is not None

        secret = self.k8s_client.get_secret(self.secret_name)
        if not secret:
            raise RuntimeError(
                f"Credential secret '{self.secret_name}' not found in namespace '{self.namespace}'"
            )

        missing = [field for field in ("api_key", "api_secret") if not secret.get(field)]
        if missing:
            formatted = ", ".join(sorted(missing))
            raise RuntimeError(
                f"Credential secret '{self.secret_name}' is missing required field(s): {formatted}"
            )

        credentials = {"api_key": secret["api_key"], "api_secret": secret["api_secret"]}

        audit_metadata = {
            "api_key": credentials["api_key"],
            "api_secret": credentials["api_secret"],
            "material_present": True,
        }
        self.timescale.record_credential_access(secret_name=self.secret_name, metadata=audit_metadata)

        return credentials

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

    def get_credentials(self) -> Dict[str, Any]:
        """Load Kraken API credentials for the account from Kubernetes."""

        assert self.secret_store is not None

        secret_payload = self.secret_store.get_secret(self.secret_name)
        if not secret_payload:
            raise RuntimeError(
                f"Kraken credentials secret '{self.secret_name}' not found in namespace "
                f"'{self.namespace}'."
            )

        data: Dict[str, Any] = secret_payload.get("data") or {}
        api_key = data.get("api_key")
        api_secret = data.get("api_secret")
        if not api_key or not api_secret:
            raise RuntimeError(
                "Kraken credentials are missing required fields 'api_key' or 'api_secret'."
            )

        metadata = deepcopy(secret_payload.get("metadata") or {})
        for sensitive_field in ("api_key", "api_secret"):
            if sensitive_field in metadata:
                metadata[sensitive_field] = "***"
        metadata.setdefault("secret_name", self.secret_name)
        metadata.setdefault("namespace", self.namespace)

        return {"api_key": api_key, "api_secret": api_secret, "metadata": metadata}


def default_fee(currency: str = "USD") -> Dict[str, float | str]:
    return {"currency": currency, "maker": 0.1, "taker": 0.2}

