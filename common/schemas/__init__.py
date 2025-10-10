"""Shared Pydantic schema exports for legacy imports.

Historically the project treated ``common.schemas`` as a regular package that
exposed the trading intent and contract models at the top level.  When the
repository was trimmed down for dependency-light environments the
``__init__`` module was dropped which left ``from common.schemas import ...``
imports broken throughout the codebase and test-suite.  Restoring the package
initializer keeps those imports working without forcing every caller to reach
into the individual modules.
"""

from .contracts import (  # noqa: F401 - re-exported for convenience
    AnomalyEvent,
    ConfigChangeEvent,
    FillEvent,
    IntentEvent,
    MessageModel,
    OrderEvent,
    RiskDecisionEvent,
    SimModeEvent,
)
from .intents import (  # noqa: F401 - re-exported for convenience
    Fill,
    Intent,
    Order,
    RiskResult,
)

__all__ = [
    "AnomalyEvent",
    "ConfigChangeEvent",
    "Fill",
    "FillEvent",
    "Intent",
    "IntentEvent",
    "MessageModel",
    "Order",
    "OrderEvent",
    "RiskDecisionEvent",
    "RiskResult",
    "SimModeEvent",
]

