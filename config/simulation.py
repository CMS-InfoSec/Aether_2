"""Simulation configuration for OMS components."""
from __future__ import annotations

from typing import Dict

from pydantic import BaseModel, Field


class SimulationConfig(BaseModel):
    """Tunable parameters for order simulation."""

    base_bps: float = Field(
        5.0,
        ge=0.0,
        description="Baseline slippage applied to simulated executions in basis points.",
    )
    vol_multiplier: float = Field(
        1.0,
        ge=0.0,
        description="Multiplier applied to volatility (in bps) when computing slippage.",
    )


# Stub configuration keyed by account identifier.
_SIMULATION_CONFIG: Dict[str, SimulationConfig] = {
    "ACC-DEFAULT": SimulationConfig(),
}


def get_simulation_config(account_id: str) -> SimulationConfig:
    """Return the simulation configuration for *account_id*.

    The current implementation provides an in-memory stub so tests and local
    development can run without an external configuration service.
    """

    return _SIMULATION_CONFIG.get(account_id, SimulationConfig())


__all__ = ["SimulationConfig", "get_simulation_config"]
