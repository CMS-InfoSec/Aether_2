from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from shared.k8s import KubernetesSecretClient


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
    _credential_rotations: ClassVar[Dict[str, Dict[str, Any]]] = {}

    def __post_init__(self) -> None:
        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})

    def record_usage(self, notional: float) -> None:
        self._metrics[self.account_id]["usage"] += notional

    def check_limits(self, notional: float) -> bool:
        projected = self._metrics[self.account_id]["usage"] + notional
        return projected <= self._metrics[self.account_id]["limit"]

    def record_credential_rotation(self, *, secret_name: str, rotated_at: datetime) -> None:
        record = self._credential_rotations.setdefault(
            self.account_id,
            {"secret_name": secret_name, "created_at": rotated_at, "rotated_at": rotated_at},
        )
        record.update({"secret_name": secret_name, "rotated_at": rotated_at})
        record.setdefault("created_at", rotated_at)

    def credential_rotation_status(self) -> Optional[Dict[str, Any]]:
        record = self._credential_rotations.get(self.account_id)
        if not record:
            return None
        return dict(record)

    @classmethod
    def reset_rotation_state(cls) -> None:
        cls._credential_rotations.clear()


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
