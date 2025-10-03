from __future__ import annotations


import base64
import logging
import os
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Mapping, Optional, Tuple

from common.utils.tracing import attach_correlation, current_correlation_id
from shared.k8s import ANNOTATION_ROTATED_AT as K8S_ROTATED_AT, KrakenSecretStore
from services.secrets.secure_secrets import (
    EncryptedSecretEnvelope,
    EnvelopeEncryptor,
)
from services.common.config import get_feast_client, get_redis_client
from services.universe.repository import UniverseRepository

try:
    from feast import FeatureStore
except Exception:  # pragma: no cover - Feast is optional during testing
    FeatureStore = None  # type: ignore[assignment]

def _normalize_account_id(account_id: str) -> str:
    """Convert human readable admin labels into canonical keys."""

    return account_id.strip().lower().replace(" ", "-")


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
        enriched = attach_correlation(payload)
        record = {
            "topic": topic,
            "payload": enriched,
            "timestamp": datetime.now(timezone.utc),
            "correlation_id": enriched.get("correlation_id") or current_correlation_id(),
        }
        self._event_store[self.account_id].append(record)

    def history(self, correlation_id: str | None = None) -> List[Dict[str, Any]]:
        records = list(self._event_store.get(self.account_id, []))
        if correlation_id:
            return [record for record in records if record.get("correlation_id") == correlation_id]
        return records

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        if account_id is None:
            cls._event_store.clear()
            return
        cls._event_store.pop(account_id, None)

    @classmethod
    def flush_events(cls) -> Dict[str, int]:
        """Flush buffered publish events and return per-account counts."""

        counts = {account: len(events) for account, events in cls._event_store.items()}
        for account, count in counts.items():
            logger.info(
                "Flushing Kafka/NATS buffer", extra={"account_id": account, "event_count": count}
            )
        cls._event_store.clear()
        return counts


@dataclass
class TimescaleAdapter:
    account_id: str

    _metrics: ClassVar[Dict[str, Dict[str, float]]] = {}

    _telemetry: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _events: ClassVar[Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}
    _audit_logs: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _credential_events: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _risk_configs: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _credential_rotations: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _kill_events: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _daily_usage: ClassVar[Dict[str, Dict[str, Dict[str, float]]]] = {}
    _instrument_exposures: ClassVar[Dict[str, Dict[str, float]]] = {}
    _rolling_volume: ClassVar[Dict[str, Dict[str, Dict[str, Any]]]] = {}

    _cvar_results: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _nav_forecasts: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}


    _default_risk_config: ClassVar[Dict[str, Any]] = {
        "kill_switch": False,
        "safe_mode": False,
        "loss_cap": 50_000.0,
        "fee_cap": 5_000.0,
        "nav": 1_000_000.0,
        "max_nav_percent": 0.20,
        "var_limit": 250_000.0,
        "spread_limit_bps": 25.0,

        "latency_limit_ms": 250.0,
        "diversification_rules": {"max_single_instrument_percent": 0.35},
        "kill_switch": False,
        "volatility_overrides": {},
        "correlation_matrix": {},
        "circuit_breakers": {},
        "position_sizer": {
            "max_trade_risk_pct_nav": 0.1,
            "max_trade_risk_pct_cash": 0.5,
            "volatility_floor": 0.05,
            "slippage_bps": 2.0,
            "safety_margin_bps": 5.0,
            "min_trade_notional": 10.0,
        },
    }

    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})

        self._daily_usage.setdefault(self.account_id, {"loss": 0.0, "fee": 0.0})
        self._instrument_exposures.setdefault(self.account_id, {})
        self._telemetry.setdefault(self.account_id, [])



        self._telemetry.setdefault(self.account_id, [])
        self._credential_events.setdefault(self.account_id, [])
        self._kill_events.setdefault(self.account_id, [])

        self._cvar_results.setdefault(self.account_id, [])
        self._nav_forecasts.setdefault(self.account_id, [])


        account_events = self._events.setdefault(
            self.account_id,
            {
                "acks": [],
                "fills": [],
                "events": [],
                "credential_rotations": [],
                "shadow_fills": [],
            },
        )
        account_events.setdefault("acks", [])
        account_events.setdefault("fills", [])
        account_events.setdefault("events", [])
        account_events.setdefault("credential_rotations", [])
        account_events.setdefault("shadow_fills", [])


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

    def record_shadow_fill(self, payload: Dict[str, Any]) -> None:
        record = {"payload": deepcopy(payload), "recorded_at": datetime.now(timezone.utc)}
        self._events[self.account_id]["shadow_fills"].append(record)

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        """Persist an audit log entry scoped to this adapter's account."""

        stored = deepcopy(dict(record))
        stored.setdefault("id", uuid.uuid4())
        stored.setdefault("created_at", datetime.now(timezone.utc))
        entries = self._audit_logs.setdefault(self.account_id, [])
        entries.append(stored)

    def audit_logs(self) -> List[Dict[str, Any]]:
        """Return recorded audit logs for the adapter's account."""

        entries = self._audit_logs.get(self.account_id, [])
        return [deepcopy(entry) for entry in entries]

    def events(self) -> Dict[str, List[Dict[str, Any]]]:

        stored = self._events.get(
            self.account_id,
            {
                "acks": [],
                "fills": [],
                "events": [],
                "credential_rotations": [],
                "shadow_fills": [],
            },
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
            "shadow_fills": [
                {**deepcopy(entry["payload"]), "recorded_at": entry["recorded_at"]}
                for entry in stored.get("shadow_fills", [])
            ],
            "events": [deepcopy(entry) for entry in stored.get("events", [])],
            "credential_rotations": [
                deepcopy(entry) for entry in stored.get("credential_rotations", [])
            ],

        }

    @classmethod
    def flush_event_buffers(cls) -> Dict[str, Dict[str, int]]:
        """Flush in-memory telemetry/event buffers and return a summary."""

        summary: Dict[str, Dict[str, int]] = {}

        def _merge(account: str, bucket: str, count: int) -> None:
            if count <= 0:
                return
            account_summary = summary.setdefault(account, {})
            account_summary[bucket] = account_summary.get(bucket, 0) + count

        for account, buckets in cls._events.items():
            for channel, events in buckets.items():
                _merge(account, f"events_{channel}", len(events))
                events.clear()

        for account, entries in cls._telemetry.items():
            _merge(account, "telemetry", len(entries))
            entries.clear()

        for account, entries in cls._credential_events.items():
            _merge(account, "credential_events", len(entries))
            entries.clear()

        for account, entries in cls._credential_rotations.items():
            _merge(account, "credential_rotations", len(entries))
            entries.clear()

        for account, entries in cls._kill_events.items():
            _merge(account, "kill_events", len(entries))
            entries.clear()

        for account, entries in cls._cvar_results.items():
            _merge(account, "cvar_results", len(entries))
            entries.clear()

        for account, entries in cls._nav_forecasts.items():
            _merge(account, "nav_forecasts", len(entries))
            entries.clear()

        for account, entries in cls._rolling_volume.items():
            bucket_total = sum(len(records) for records in entries.values())
            _merge(account, "rolling_volume", bucket_total)
            for records in entries.values():
                records.clear()

        if summary:
            for account, buckets in summary.items():
                logger.info(
                    "Flushing Timescale adapter buffers",
                    extra={"account_id": account, "buckets": buckets},
                )

        return summary

    # ------------------------------------------------------------------
    # Timescale-inspired risk state helpers
    # ------------------------------------------------------------------

    def load_risk_config(self) -> Dict[str, Any]:
        config = self._risk_configs.setdefault(
            self.account_id, deepcopy(self._default_risk_config)
        )
        return deepcopy(config)

    def set_kill_switch(
        self,
        *,
        engaged: bool,
        reason: str | None = None,
        actor: str | None = None,
    ) -> None:
        """Engage or release the kill switch for the account."""

        config = self._risk_configs.setdefault(
            self.account_id, deepcopy(self._default_risk_config)
        )
        config["kill_switch"] = bool(engaged)

        event_payload: Dict[str, Any] = {"state": "engaged" if engaged else "released"}
        if reason:
            event_payload["reason"] = reason
        if actor:
            event_payload["actor"] = actor

        event_type = "kill_switch_engaged" if engaged else "kill_switch_released"
        self.record_event(event_type, event_payload)

    def set_safe_mode(
        self,
        *,
        engaged: bool,
        reason: str | None = None,
        actor: str | None = None,
    ) -> None:
        """Engage or release safe mode controls for the account."""

        config = self._risk_configs.setdefault(
            self.account_id, deepcopy(self._default_risk_config)
        )
        config["safe_mode"] = bool(engaged)

        event_payload: Dict[str, Any] = {"state": "engaged" if engaged else "released"}
        if reason:
            event_payload["reason"] = reason
        if actor:
            event_payload["actor"] = actor

        event_type = "safe_mode_engaged" if engaged else "safe_mode_released"
        self.record_event(event_type, event_payload)

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

    def open_positions(self) -> Dict[str, float]:
        exposures = self._instrument_exposures.get(self.account_id, {})
        return {symbol: float(notional) for symbol, notional in exposures.items()}

    def record_cvar_result(
        self,
        *,
        horizon: str,
        var95: float,
        cvar95: float,
        prob_cap_hit: float,
        timestamp: datetime | None = None,
    ) -> Dict[str, Any]:
        ts = timestamp or datetime.now(timezone.utc)
        record = {
            "account_id": self.account_id,
            "horizon": horizon,
            "var95": float(var95),
            "cvar95": float(cvar95),
            "prob_cap_hit": float(prob_cap_hit),
            "ts": ts,
        }
        history = self._cvar_results.setdefault(self.account_id, [])
        history.append(record)
        return dict(record)

    def cvar_results(self) -> List[Dict[str, Any]]:
        records = self._cvar_results.get(self.account_id, [])
        return [dict(entry) for entry in records]

    def record_nav_forecast(
        self,
        *,
        horizon: str,
        metrics: Mapping[str, float],
        timestamp: datetime | None = None,
    ) -> Dict[str, Any]:
        ts = timestamp or datetime.now(timezone.utc)
        normalized_metrics = {key: float(value) for key, value in metrics.items()}
        record = {
            "account_id": self.account_id,
            "horizon": horizon,
            "metrics_json": normalized_metrics,
            "ts": ts,
        }
        history = self._nav_forecasts.setdefault(self.account_id, [])
        history.append(record)
        return dict(record)

    def nav_forecasts(self) -> List[Dict[str, Any]]:
        records = self._nav_forecasts.get(self.account_id, [])
        return [dict(entry) for entry in records]

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

    def record_kill_event(
        self,
        *,
        reason_code: str,
        triggered_at: datetime,
        channels_sent: Iterable[str],
    ) -> Dict[str, Any]:
        payload = {
            "account_id": self.account_id,
            "reason": reason_code,
            "ts": triggered_at,
            "channels_sent": list(channels_sent),
        }
        events = self._kill_events.setdefault(self.account_id, [])
        events.append(deepcopy(payload))
        return deepcopy(payload)

    def kill_events(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        entries = list(self._kill_events.get(self.account_id, []))
        entries.sort(key=lambda entry: entry["ts"], reverse=True)
        if limit is not None:
            entries = entries[: int(limit)]
        return [deepcopy(entry) for entry in entries]

    @classmethod
    def all_kill_events(
        cls, *, account_id: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        if account_id is not None:
            entries = list(cls._kill_events.get(account_id, []))
        else:
            entries = [
                entry for events in cls._kill_events.values() for entry in events
            ]
        entries.sort(key=lambda entry: entry["ts"], reverse=True)
        if limit is not None:
            entries = entries[: int(limit)]
        return [deepcopy(entry) for entry in entries]

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
        self,
        *,
        secret_name: str,
        rotated_at: datetime,
        kms_key_id: str | None = None,
    ) -> Dict[str, Any]:
        existing = self._credential_rotations.get(self.account_id) or {}
        created_at = existing.get("created_at", rotated_at)

        metadata = {
            "secret_name": secret_name,
            "created_at": created_at,
            "rotated_at": rotated_at,
        }
        if kms_key_id is not None:
            metadata["kms_key_id"] = kms_key_id

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
            cls._audit_logs,
            cls._credential_rotations,
            cls._credential_events,
            cls._rolling_volume,

            cls._cvar_results,

        )

        if account_id is None:
            for cache in caches:
                cache.clear()
            return

        for cache in caches:
            cache.pop(account_id, None)

    @classmethod
    def reset_rotation_state(cls, account_id: str | None = None) -> None:
        rotation_caches = (
            cls._credential_rotations,
            cls._credential_events,
        )

        if account_id is None:
            for cache in rotation_caches:
                cache.clear()

            for events in cls._events.values():
                rotations = events.get("credential_rotations")
                if isinstance(rotations, list):
                    rotations.clear()
                elif rotations is not None:
                    events["credential_rotations"] = []
            return

        for cache in rotation_caches:
            cache.pop(account_id, None)

        account_events = cls._events.get(account_id)
        if account_events is not None and "credential_rotations" in account_events:
            rotations = account_events["credential_rotations"]
            if isinstance(rotations, list):
                rotations.clear()
            else:
                account_events["credential_rotations"] = []




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
    feast_client_factory: Callable[[str], "FeastClient"] = field(
        default=get_feast_client, repr=False
    )
    redis_client_factory: Callable[[str], "RedisClient"] = field(
        default=get_redis_client, repr=False
    )
    feature_store_factory: Callable[["FeastClient", "RedisClient"], Any] | None = field(
        default=None, repr=False
    )
    cache_ttl: int = 60

    _features: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _fee_tiers: ClassVar[Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}
    _online_feature_store: ClassVar[Dict[str, Dict[str, Dict[str, Any]]]] = {}
    _feature_expirations: ClassVar[Dict[str, datetime]] = {}
    _fee_tier_expirations: ClassVar[Dict[str, datetime]] = {}
    _online_feature_expirations: ClassVar[Dict[str, Dict[str, datetime]]] = {}

    _APPROVED_FIELDS: ClassVar[Tuple[str, ...]] = ("instrument", "approved")
    _FEE_OVERRIDE_FIELDS: ClassVar[Tuple[str, ...]] = (
        "instrument",
        "maker_bps",
        "taker_bps",
        "currency",
    )
    _FEE_TIER_FIELDS: ClassVar[Tuple[str, ...]] = (
        "pair",
        "tier",
        "maker_bps",
        "taker_bps",
        "notional_threshold",
    )
    _ONLINE_FIELDS: ClassVar[Tuple[str, ...]] = (
        "features",
        "book_snapshot",
        "state",
        "expected_edge_bps",
        "take_profit_bps",
        "stop_loss_bps",
        "confidence",
    )

    def __post_init__(self) -> None:
        repository = self.repository
        if repository is not None:
            self._repository = repository
        else:
            self._repository = self.repository_factory(account_id=self.account_id)

        feast_client = self.feast_client_factory(self.account_id)
        redis_client = self.redis_client_factory(self.account_id)
        factory = self.feature_store_factory or self._default_feature_store_factory
        self._feast_client = feast_client
        self._redis_client = redis_client
        self._store = factory(feast_client, redis_client)

    def approved_instruments(self) -> List[str]:
        cached = self._features.get(self.account_id)
        expires = self._feature_expirations.get(self.account_id)
        if cached and cached.get("approved") and (
            expires is None or not self._cache_expired(expires)
        ):
            return [str(symbol) for symbol in cached["approved"]]

        records = self._load_historical_records(
            view_suffix="approved_instruments",
            fields=self._APPROVED_FIELDS,
            window_minutes=60,
        )

        approved = {
            str(record["instrument"]): record
            for record in records
            if record.get("approved")
        }
        if not approved:
            raise RuntimeError(
                "Feast returned no approved instruments for account '%s'" % self.account_id
            )

        instruments = sorted(approved.keys())
        payload = self._features.setdefault(self.account_id, {})
        payload["approved"] = instruments
        self._feature_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return instruments

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        cache_key = instrument.upper()
        cached_account = self._features.setdefault(self.account_id, {})
        cached_fees = cached_account.setdefault("fees", {})
        expires = self._feature_expirations.get(self.account_id)
        if cache_key in cached_fees and (
            expires is None or not self._cache_expired(expires)
        ):
            return dict(cached_fees[cache_key])

        records = self._load_historical_records(
            view_suffix="fee_overrides",
            fields=self._FEE_OVERRIDE_FIELDS,
            window_minutes=240,
        )

        matched = None
        for record in sorted(
            records,
            key=lambda item: item.get("event_timestamp", datetime.min),
            reverse=True,
        ):
            if str(record.get("instrument", "")).upper() == cache_key:
                matched = record
                break

        if matched is None:
            raise RuntimeError(
                f"Feast returned no fee override data for {instrument}"
            )

        override = {
            "currency": matched.get("currency", "USD"),
            "maker": float(matched.get("maker_bps", matched.get("maker", 0.0))),
            "taker": float(matched.get("taker_bps", matched.get("taker", 0.0))),
        }
        cached_fees[cache_key] = dict(override)
        self._feature_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return override

    def fee_tiers(self, pair: str) -> List[Dict[str, Any]]:
        account_cache = self._fee_tiers.setdefault(self.account_id, {})
        expires = self._fee_tier_expirations.get(self.account_id)
        if pair in account_cache and (
            expires is None or not self._cache_expired(expires)
        ):
            return [dict(entry) for entry in account_cache[pair]]

        records = self._load_historical_records(
            view_suffix="fee_tiers",
            fields=self._FEE_TIER_FIELDS,
            window_minutes=240,
        )

        tiers: List[Dict[str, Any]] = []
        for record in records:
            record_pair = str(record.get("pair", "")).upper()
            if record_pair not in {pair.upper(), "DEFAULT"}:
                continue
            tiers.append(
                {
                    "tier": record.get("tier"),
                    "maker": float(record.get("maker_bps", 0.0)),
                    "taker": float(record.get("taker_bps", 0.0)),
                    "notional_threshold": float(record.get("notional_threshold", 0.0)),
                }
            )

        if not tiers:
            raise RuntimeError(f"Feast returned no fee tiers for {pair}")

        tiers.sort(key=lambda item: item.get("notional_threshold", 0.0))
        account_cache[pair] = [dict(entry) for entry in tiers]
        self._fee_tier_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return tiers

    @classmethod
    def seed_fee_tiers(
        cls, tiers: Mapping[str, Mapping[str, Iterable[Mapping[str, Any]]]]
    ) -> None:
        cls._fee_tiers = {
            account: {
                pair: [dict(entry) for entry in entries]
                for pair, entries in account_tiers.items()
            }
            for account, account_tiers in tiers.items()
        }
        cls._fee_tier_expirations.clear()

    def fetch_online_features(self, instrument: str) -> Dict[str, Any]:
        account_cache = self._online_feature_store.setdefault(self.account_id, {})
        instrument_key = instrument.upper()
        expires = self._online_feature_expirations.setdefault(self.account_id, {})
        cached = account_cache.get(instrument_key)
        if cached:
            cached_expiry = expires.get(instrument_key)
            if cached_expiry is None or not self._cache_expired(cached_expiry):
                return dict(cached)

        features = self._online_feature_refs()
        entity_rows = [
            {
                "account_id": self._feast_client.account_namespace,
                "instrument": instrument,
            }
        ]
        try:
            response = self._store.get_online_features(
                features=features,
                entity_rows=entity_rows,
            )
        except Exception as exc:  # pragma: no cover - safety net
            raise RuntimeError("Failed to fetch online features from Feast") from exc

        payload = self._coerce_online_payload(response)
        if not payload:
            raise RuntimeError(
                f"Feast returned empty online feature payload for {instrument}"
            )

        account_cache[instrument_key] = dict(payload)
        expires[instrument_key] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return dict(payload)

    def _default_feature_store_factory(
        self, feast_client: "FeastClient", redis_client: "RedisClient"
    ) -> Any:
        if FeatureStore is None:
            raise RuntimeError(
                "feast.FeatureStore is unavailable; install Feast to use RedisFeastAdapter"
            )
        repo_path = Path(os.getenv("FEAST_REPO_PATH", "data/feast"))
        if repo_path.exists():
            return FeatureStore(repo_path=str(repo_path))
        try:
            from feast.infra.online_stores.redis import RedisOnlineStoreConfig
            from feast.repo_config import RepoConfig
        except Exception as exc:  # pragma: no cover - depends on Feast install
            raise RuntimeError("Unable to construct Feast configuration") from exc

        config = RepoConfig(
            project=feast_client.project,
            provider="local",
            registry=str(repo_path / "registry.db"),
            online_store=RedisOnlineStoreConfig(
                connection_string=redis_client.dsn,
            ),
            entity_key_serialization_version=2,
        )
        return FeatureStore(config=config)

    def _load_historical_records(
        self,
        *,
        view_suffix: str,
        fields: Tuple[str, ...],
        window_minutes: int,
    ) -> List[Dict[str, Any]]:
        feature_refs = [f"{self._view_name(view_suffix)}:{field}" for field in fields]
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=window_minutes)
        entity_rows = [
            {
                "account_id": self._feast_client.account_namespace,
            }
        ]
        try:
            job = self._store.get_historical_features(
                entity_rows=entity_rows,
                feature_refs=feature_refs,
                start_date=start,
                end_date=end,
            )
        except Exception as exc:
            raise RuntimeError("Failed to query Feast historical store") from exc

        records = self._coerce_records(job)
        if not records:
            raise RuntimeError(
                f"Feast returned no data for feature view '{self._view_name(view_suffix)}'"
            )
        return records

    def _view_name(self, suffix: str) -> str:
        return f"{self._feast_client.account_namespace}__{suffix}"

    def _coerce_records(self, job: Any) -> List[Dict[str, Any]]:
        if hasattr(job, "to_dicts"):
            return [dict(record) for record in job.to_dicts()]
        if hasattr(job, "to_df"):
            frame = job.to_df()
            if hasattr(frame, "to_dict"):
                records = frame.to_dict(orient="records")  # type: ignore[arg-type]
                return [dict(record) for record in records]
        if isinstance(job, Iterable):
            return [dict(record) for record in job]
        return []

    def _coerce_online_payload(self, payload: Any) -> Dict[str, Any]:
        data: Mapping[str, Any]
        if hasattr(payload, "to_dict"):
            data = payload.to_dict()
        elif isinstance(payload, Mapping):
            data = payload
        else:
            try:
                data = dict(payload)
            except Exception:  # pragma: no cover - fallback
                return {}

        normalized: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(key, str) and ":" in key:
                normalized_key = key.split(":", 1)[-1]
            else:
                normalized_key = str(key)
            normalized[normalized_key] = value
        return normalized

    def _online_feature_refs(self) -> List[str]:
        view = self._view_name("instrument_features")
        return [f"{view}:{field}" for field in self._ONLINE_FIELDS]

    def _cache_expired(self, expires: Optional[datetime]) -> bool:
        if expires is None:
            return True
        return datetime.now(timezone.utc) >= expires


def _extract_secret_rotated_at(metadata: Mapping[str, Any]) -> Optional[str]:
    rotated = metadata.get("rotated_at") or metadata.get("last_rotated_at")
    if rotated:
        return str(rotated)
    annotations = metadata.get("annotations")
    if isinstance(annotations, Mapping):
        value = annotations.get(K8S_ROTATED_AT)
        if value:
            return str(value)
    return None


@dataclass
class KrakenSecretManager:
    account_id: str
    namespace: str = "aether-secrets"
    secret_store: Optional[KrakenSecretStore] = None
    timescale: Optional[TimescaleAdapter] = None
    encryptor: Optional[EnvelopeEncryptor] = None

    def __post_init__(self) -> None:
        if self.secret_store is None:
            self.secret_store = KrakenSecretStore(namespace=self.namespace)
        if self.timescale is None:
            self.timescale = TimescaleAdapter(account_id=self.account_id)
        if self.encryptor is None:
            self.encryptor = EnvelopeEncryptor()

    @property
    def secret_name(self) -> str:
        assert self.secret_store is not None
        return self.secret_store.secret_name(self.account_id)


    def rotate_credentials(self, *, api_key: str, api_secret: str) -> Dict[str, Any]:
        assert self.secret_store is not None  # for type checkers
        assert self.timescale is not None

        before_status = self.timescale.credential_rotation_status()
        rotated_at = datetime.now(timezone.utc)
        assert self.encryptor is not None
        envelope = self.encryptor.encrypt_credentials(
            self.account_id,
            api_key=api_key,
            api_secret=api_secret,
        )
        assert self.secret_store is not None
        if hasattr(self.secret_store, "write_encrypted_secret"):
            self.secret_store.write_encrypted_secret(
                self.account_id,
                envelope=envelope,
            )
        else:  # pragma: no cover - compatibility guard
            self.secret_store.write_credentials(
                self.account_id,
                api_key=api_key,
                api_secret=api_secret,
            )
        metadata = self.timescale.record_credential_rotation(
            secret_name=self.secret_name,
            rotated_at=rotated_at,
            kms_key_id=envelope.kms_key_id,
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
        api_key: Optional[str]
        api_secret: Optional[str]
        if {
            "encrypted_api_key",
            "encrypted_api_key_nonce",
            "encrypted_api_secret",
            "encrypted_api_secret_nonce",
            "encrypted_data_key",
        }.issubset(data):
            envelope = EncryptedSecretEnvelope.from_secret_data(data)
            assert self.encryptor is not None
            decrypted = self.encryptor.decrypt_credentials(
                self.account_id,
                envelope,
            )
            api_key = decrypted.api_key
            api_secret = decrypted.api_secret
        else:
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

        rotated_at = _extract_secret_rotated_at(metadata)
        if rotated_at is None:
            raise RuntimeError(
                "Kraken credentials metadata missing rotation timestamp; cannot determine freshness."
            )

        sanitized_metadata = deepcopy(metadata)
        for sensitive_field in (
            "api_key",
            "api_secret",
            "encrypted_api_key",
            "encrypted_api_secret",
        ):
            if sensitive_field in sanitized_metadata and sanitized_metadata[sensitive_field]:
                sanitized_metadata[sensitive_field] = "***"
        sanitized_metadata.setdefault("api_key", "***")
        sanitized_metadata.setdefault("api_secret", "***")
        annotations = sanitized_metadata.get("annotations")
        if isinstance(annotations, Mapping):
            annotations = dict(annotations)
        else:
            annotations = {}
        annotations.setdefault(K8S_ROTATED_AT, rotated_at)
        sanitized_metadata["annotations"] = annotations
        sanitized_metadata["rotated_at"] = rotated_at

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

