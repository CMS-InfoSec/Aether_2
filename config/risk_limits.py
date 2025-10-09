"""Stub configuration module providing account risk limits."""

from __future__ import annotations

from typing import Dict

from pydantic import BaseModel, Field, PositiveFloat


class ConfigError(RuntimeError):
    """Raised when account specific configuration cannot be loaded."""


class AccountRiskLimits(BaseModel):
    """Container for risk limits applied to an account."""

    daily_loss_limit: PositiveFloat = Field(
        ..., description="Maximum tolerable realized loss for the current trading day."
    )
    fee_budget: PositiveFloat = Field(
        ..., description="Maximum fees that can be spent before trading is halted."
    )
    max_nav_pct: PositiveFloat = Field(
        ..., description="Maximum portion of NAV that can be deployed as notional exposure.",
    )
    notional_cap: PositiveFloat = Field(
        ..., description="Absolute cap on notional exposure regardless of NAV."
    )
    cooldown_minutes: int = Field(
        60,
        ge=0,
        description="Default cooldown period, in minutes, applied after hard risk breaches.",
    )


# In-memory stub configuration for demonstration purposes only.
_STUB_LIMITS: Dict[str, AccountRiskLimits] = {
    "ACC-DEFAULT": AccountRiskLimits(
        daily_loss_limit=50_000,
        fee_budget=10_000,
        max_nav_pct=0.25,
        notional_cap=1_000_000,
        cooldown_minutes=120,
    ),
    "company": AccountRiskLimits(
        daily_loss_limit=150_000,
        fee_budget=35_000,
        max_nav_pct=0.35,
        notional_cap=7_500_000,
        cooldown_minutes=60,
    ),
    "director-1": AccountRiskLimits(
        daily_loss_limit=50_000,
        fee_budget=10_000,
        max_nav_pct=0.25,
        notional_cap=1_500_000,
        cooldown_minutes=120,
    ),
    "director-2": AccountRiskLimits(
        daily_loss_limit=100_000,
        fee_budget=20_000,
        max_nav_pct=0.3,
        notional_cap=3_500_000,
        cooldown_minutes=90,
    ),
}


def get_account_limits(account_id: str) -> AccountRiskLimits:
    """Retrieve risk limits for *account_id*.

    In production this function would retrieve the values from a configuration
    service or database. For now a small in-memory store is used.
    """

    try:
        return _STUB_LIMITS[account_id]
    except KeyError as exc:
        raise ConfigError(f"No risk limits configured for account '{account_id}'.") from exc
