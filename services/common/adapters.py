from __future__ import annotations


import base64
import logging
from copy import deepcopy
from dataclasses import dataclass, field

from datetime import datetime, timezone
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Mapping, Optional

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
    _risk_configs: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _credential_rotations: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _daily_usage: ClassVar[Dict[str, Dict[str, Dict[str, float]]]] = {}
    _instrument_exposures: ClassVar[Dict[str, Dict[str, float]]] = {}
    _rolling_volume: ClassVar[Dict[str, Dict[str, Dict[str, Any]]]] = {}

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

        self._daily_usage.setdefault(self.account_id, {"loss": 0.0, "fee": 0.0})
        self._instrument_exposures.setdefault(self.account_id, {})
        self._telemetry.setdefault(self.account_id, [])



        self._telemetry.setdefault(self.account_id, [])
        self._credential_events.setdefault(self.account_id, [])


        account_events = self._events.setdefault(
            self.account_id,
            {"acks": [], "fills": [], "events": [], "credential_rotations": []},
        )
        account_events.setdefault("acks", [])
        account_events.setdefault("fills", [])
        account_events.setdefault("events", [])
        account_events.setdefault("credential_rotations", [])


        self._risk_configs.setdefault(self.account_id, deepcopy(self._default_risk_config))
        self._daily_usage.setdefault(self.account_id, {})
        self._instrument_exposures.setdefault(self.account_id, {})
        self._credential_rotations.setdefault(self.account_id, {})
        self._rolling_volume.setdefault(self.account_id, {})


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

    # ------------------------------------------------------------------
    # Rolling volume helpers
    # ------------------------------------------------------------------
    def rolling_volume(self, pair: str) -> Dict[str, Any]:
        account_payload = self._rolling_volume.setdefault(self.account_id, {})
        volume_entry = account_payload.get(pair)
        if volume_entry is None:
            volume_entry = {
                "notional": 0.0,
                "basis_ts": datetime.now(timezone.utc),
            }
            account_payload[pair] = deepcopy(volume_entry)
        return {
            "notional": float(volume_entry.get("notional", 0.0)),
            "basis_ts": volume_entry.get("basis_ts", datetime.now(timezone.utc)),
        }

    @classmethod
    def seed_rolling_volume(
        cls, payload: Mapping[str, Mapping[str, Dict[str, Any]]]
    ) -> None:
        cls._rolling_volume = {
            account: {
                pair: {
                    "notional": float(entry.get("notional", 0.0)),
                    "basis_ts": entry.get("basis_ts", datetime.now(timezone.utc)),
                }
                for pair, entry in pairs.items()
            }
            for account, pairs in payload.items()
        }

    def record_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        entry = {
            "type": event_type,
            "event_type": event_type,
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
    # Credential rotation tracking & test helpers
    # ------------------------------------------------------------------
    def record_credential_rotation(
        self, *, secret_name: str, rotated_at: datetime
    ) -> Dict[str, Any]:
        existing = self._credential_rotations.get(self.account_id) or {}
        created_at = existing.get("created_at", rotated_at)

        metadata = {
            "secret_name": secret_name,
            "created_at": created_at,
            "rotated_at": rotated_at,
        }

        self._credential_rotations[self.account_id] = deepcopy(metadata)

        events = self._credential_events.setdefault(self.account_id, [])
        events.append(
            {
                "event": "rotation",
                "event_type": "kraken.credentials.rotation",
                "secret_name": secret_name,
                "metadata": deepcopy(metadata),
                "timestamp": rotated_at,
            }
        )

        return deepcopy(metadata)

    def credential_rotation_status(self) -> Optional[Dict[str, Any]]:
        record = self._credential_rotations.get(self.account_id)
        if not record:
            return None
        return deepcopy(record)


    # ------------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------------
    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        caches = (
            cls._metrics,
            cls._telemetry,
            cls._risk_configs,
            cls._daily_usage,
            cls._instrument_exposures,
            cls._events,
            cls._credential_rotations,
            cls._credential_events,
        )

        if account_id is None:
            for cache in caches:
                cache.clear()
            return

        for cache in caches:
            cache.pop(account_id, None)

    @classmethod
    def reset_rotation_state(cls, account_id: str | None = None) -> None:
        cls.reset(account_id=account_id)




    def record_credential_access(self, *, secret_name: str, metadata: Dict[str, Any]) -> None:
        sanitized = deepcopy(metadata)
        for key in ("api_key", "api_secret"):
            if key in sanitized and sanitized[key]:
                sanitized[key] = "***"
        if "material_present" not in sanitized:
            sanitized["material_present"] = bool(
                sanitized.get("api_key") and sanitized.get("api_secret")
            )
        payload = {
            "event": "access",
            "event_type": "kraken.credentials.access",
            "secret_name": secret_name,
            "metadata": sanitized,
            "timestamp": datetime.now(timezone.utc),
        }
        events = self._credential_events.setdefault(self.account_id, [])
        events.append(deepcopy(payload))


    def credential_events(self) -> List[Dict[str, Any]]:
        return [deepcopy(event) for event in self._credential_events.get(self.account_id, [])]


@dataclass
class RedisFeastAdapter:
    account_id: str
    repository_factory: Callable[[str], UniverseRepository] = field(
        default=UniverseRepository, repr=False
    )
    repository: UniverseRepository | None = field(default=None, repr=False)


    _features: ClassVar[Dict[str, Dict[str, Any]]] = {
        "admin-eu": {
            "approved": ["BTC-USD", "ETH-USD"],
            "fees": {"BTC-USD": {"maker": 0.1, "taker": 0.2}},
        },
        "admin-us": {"approved": ["SOL-USD"], "fees": {}},
        "admin-apac": {"approved": ["BTC-USD", "ETH-USD"], "fees": {}},
    }
    _fee_tiers: ClassVar[Dict[str, List[Dict[str, Any]]]] = {
        "default": [
            {
                "tier_id": "base",
                "volume_threshold": 0.0,
                "maker_bps": 1.0,
                "taker_bps": 1.5,
                "currency": "USD",
            }
        ]
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

        repository = self.repository
        if repository is not None:
            self._repository = repository
        else:
            self._repository = self.repository_factory(account_id=self.account_id)



    def approved_instruments(self) -> List[str]:

        assert self._repository is not None

        instruments = self._repository.approved_universe()
        if not instruments:
            fallback = self._features.get(self.account_id, {}).get("approved", [])
            instruments = list(fallback)

        return [symbol for symbol in instruments if symbol.endswith("-USD")]


    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        override = self._repository.fee_override(instrument)
        if override:
            return dict(override)
        account_config = self._features.get(self.account_id, {})
        fees = account_config.get("fees", {})
        if instrument not in fees:
            return None
        return dict(fees[instrument])

    def fee_tiers(self, pair: str) -> List[Dict[str, Any]]:
        tiers = self._fee_tiers.get(pair) or self._fee_tiers.get("default", [])
        return [dict(tier) for tier in tiers]

    @classmethod
    def seed_fee_tiers(cls, tiers: Mapping[str, Iterable[Mapping[str, Any]]]) -> None:
        cls._fee_tiers = {
            pair: [dict(entry) for entry in entries]
            for pair, entries in tiers.items()
        }


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
        rotated_at = datetime.now(timezone.utc)
        self.secret_store.write_credentials(
            self.account_id,
            api_key=api_key,
            api_secret=api_secret,
        )
        metadata = self.timescale.record_credential_rotation(
            secret_name=self.secret_name,
            rotated_at=rotated_at,
        )
        metadata.setdefault("secret_name", self.secret_name)
        metadata.setdefault("created_at", rotated_at)
        metadata.setdefault("rotated_at", rotated_at)

        return {
            "metadata": metadata,
            "before": before_status,
        }

    def status(self) -> Optional[Dict[str, Any]]:
        assert self.timescale is not None
        return self.timescale.credential_rotation_status()

    def get_credentials(self) -> Dict[str, Any]:
        """Load Kraken API credentials for the account from the secret store."""

        assert self.secret_store is not None
        assert self.timescale is not None

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
        metadata.setdefault("secret_name", self.secret_name)
        metadata.setdefault("namespace", self.namespace)
        metadata["material_present"] = True

        sanitized_metadata = deepcopy(metadata)
        for sensitive_field in ("api_key", "api_secret"):
            if sensitive_field in sanitized_metadata and sanitized_metadata[sensitive_field]:
                sanitized_metadata[sensitive_field] = "***"

        self.timescale.record_credential_access(
            secret_name=self.secret_name,
            metadata=metadata,
        )

        self.timescale.record_event(
            "kraken.credentials.access",
            {
                "secret_name": self.secret_name,
                "metadata": sanitized_metadata,
            },
        )

        return {
            "api_key": api_key,
            "api_secret": api_secret,
            "metadata": sanitized_metadata,
        }


def default_fee(currency: str = "USD") -> Dict[str, float | str]:
    return {"currency": currency, "maker": 0.1, "taker": 0.2}

