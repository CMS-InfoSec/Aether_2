"""Pydantic models representing shared event contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Type, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T", bound="MessageModel")


class MessageModel(BaseModel):
    """Base model that provides serialization helpers for messaging buses."""

    model_config = ConfigDict(populate_by_name=True)

    def to_kafka(self) -> bytes:
        """Serialize the model to a UTF-8 JSON payload for Kafka topics."""

        return self.model_dump_json(by_alias=True).encode("utf-8")

    @classmethod
    def from_kafka(cls: Type[T], payload: Union[str, bytes]) -> T:
        """Deserialize a Kafka payload (bytes or string) into the model."""

        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return cls.model_validate_json(payload)

    def to_nats(self) -> bytes:
        """Serialize the model to a UTF-8 JSON payload for NATS subjects."""

        return self.to_kafka()

    @classmethod
    def from_nats(cls: Type[T], payload: Union[str, bytes]) -> T:
        """Deserialize a NATS payload (bytes or string) into the model."""

        return cls.from_kafka(payload)


class IntentEvent(MessageModel):
    """Event representing a trading intent for a particular account and symbol."""

    account_id: str = Field(..., min_length=1, max_length=64)
    symbol: str = Field(..., min_length=1, max_length=64)
    intent: Dict[str, Any] = Field(..., description="Raw intent payload")
    ts: datetime = Field(..., description="Event timestamp")


class RiskDecisionEvent(MessageModel):
    """Event capturing a risk service decision for an intent."""

    account_id: str = Field(..., min_length=1, max_length=64)
    symbol: str = Field(..., min_length=1, max_length=64)
    decision: Dict[str, Any] = Field(..., description="Risk decision payload")
    ts: datetime = Field(..., description="Event timestamp")


class OrderEvent(MessageModel):
    """Order lifecycle update event."""

    account_id: str = Field(..., min_length=1, max_length=64)
    symbol: str = Field(..., min_length=1, max_length=64)
    order_id: str = Field(..., min_length=1, max_length=64)
    status: str = Field(..., min_length=1, max_length=64)
    ts: datetime = Field(..., description="Event timestamp")


class FillEvent(MessageModel):
    """Execution fill event."""

    account_id: str = Field(..., min_length=1, max_length=64)
    symbol: str = Field(..., min_length=1, max_length=64)
    qty: float = Field(..., description="Executed quantity")
    price: float = Field(..., description="Execution price")
    fee: float = Field(..., ge=0.0, description="Fees applied to the execution")
    liquidity: str = Field(..., min_length=1, max_length=32, description="Maker/Taker flag")
    ts: datetime = Field(..., description="Event timestamp")


class SimModeEvent(MessageModel):
    """Event capturing transitions into or out of platform simulation mode."""

    account_id: str = Field(..., min_length=1, max_length=128, description="Trading account impacted by the transition")
    active: bool = Field(..., description="True when the platform is in simulation mode")
    reason: str | None = Field(default=None, description="Human readable reason for the state change")
    ts: datetime = Field(..., description="Timestamp of the state change")
    actor: str = Field(..., min_length=1, max_length=64, description="Actor responsible for the change")


class AnomalyEvent(MessageModel):
    """Anomaly detection event."""

    account_id: str = Field(..., min_length=1, max_length=64)
    anomaly_type: str = Field(..., min_length=1, max_length=64)
    details: Dict[str, Any] = Field(default_factory=dict, description="Anomaly metadata")
    ts: datetime = Field(..., description="Event timestamp")


class ConfigChangeEvent(MessageModel):
    """Event describing a configuration change."""

    actor: str = Field(..., min_length=1, max_length=64)
    key: str = Field(..., min_length=1, max_length=128)
    value: Any = Field(..., description="New configuration value")
    version: int = Field(..., ge=0, description="Configuration version")
    ts: datetime = Field(..., description="Event timestamp")


__all__ = [
    "MessageModel",
    "IntentEvent",
    "RiskDecisionEvent",
    "OrderEvent",
    "FillEvent",
    "SimModeEvent",
    "AnomalyEvent",
    "ConfigChangeEvent",
]
