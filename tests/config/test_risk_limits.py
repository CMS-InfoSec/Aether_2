"""Regression coverage for the risk limits stub configuration."""

from __future__ import annotations

import pytest

from config.risk_limits import AccountRiskLimits, ConfigError, get_account_limits


@pytest.mark.parametrize(
    "account_id,expected",
    (
        ("company", AccountRiskLimits),
        ("director-1", AccountRiskLimits),
        ("director-2", AccountRiskLimits),
        ("ACC-DEFAULT", AccountRiskLimits),
    ),
)
def test_stub_limits_exposes_production_accounts(account_id: str, expected: type[AccountRiskLimits]) -> None:
    """All production accounts should resolve to concrete limit payloads."""

    limits = get_account_limits(account_id)
    assert isinstance(limits, expected)

    numeric_fields = (
        limits.daily_loss_limit,
        limits.fee_budget,
        limits.max_nav_pct,
        limits.notional_cap,
    )
    assert all(value > 0 for value in numeric_fields)


def test_unknown_account_raises_config_error() -> None:
    """Requests for unmapped accounts should raise a configuration error."""

    with pytest.raises(ConfigError):
        get_account_limits("shadow")
