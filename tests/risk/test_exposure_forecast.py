from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys
import types
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # pragma: no cover - exercised in environments with real package path
    from services.common import config as _config  # type: ignore  # noqa: F401
    from services.common import security as _security  # type: ignore  # noqa: F401
except (ImportError, ModuleNotFoundError):  # pragma: no cover - fallback for isolated test execution
    services_module = types.ModuleType("services")
    common_module = types.ModuleType("services.common")
    config_module = types.ModuleType("services.common.config")
    config_module.get_timescale_session = lambda account_id: SimpleNamespace(
        dsn="postgresql://localhost/test", account_schema="public"
    )
    security_module = types.ModuleType("services.common.security")

    def _require_admin_account(account_id: str) -> str:  # pragma: no cover - stub
        return account_id

    security_module.require_admin_account = _require_admin_account
    services_module.common = common_module
    sys.modules.setdefault("services", services_module)
    sys.modules["services.common"] = common_module
    sys.modules["services.common.config"] = config_module
    sys.modules["services.common.security"] = security_module

from exposure_forecast import ExposureForecaster, ForecastResult


def _population_std(values: list[Decimal]) -> Decimal:
    if len(values) <= 1:
        return Decimal("0")
    mean = sum(values, Decimal("0")) / Decimal(len(values))
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
    return variance.sqrt()


def test_nav_forecast_preserves_decimal_precision() -> None:
    forecaster = ExposureForecaster(account_id="ACC-PRECISION")
    nav_rows = [
        {"nav": "12345.6789012345"},
        {"nav": "12345.6789024695"},
        {"nav": "12345.6789037045"},
        {"nav": "12345.6789049395"},
    ]

    result = forecaster._forecast_nav_volatility(nav_rows)

    nav_values = [Decimal(row["nav"]) for row in nav_rows]
    returns = []
    for previous, current in zip(nav_values, nav_values[1:]):
        returns.append((current - previous) / previous)

    span = min(20, len(returns))
    alpha = Decimal(2) / Decimal(span + 1)
    complement = Decimal(1) - alpha
    ewma_var = returns[0] ** 2
    for ret in returns[1:]:
        ewma_var = alpha * (ret**2) + complement * ewma_var

    daily_vol = ewma_var.sqrt()
    horizon_days = 7
    horizon_factor = Decimal(horizon_days).sqrt()
    expected_value = daily_vol * horizon_factor

    window_vols = []
    for idx in range(span, len(returns) + 1):
        slice_returns = returns[idx - span : idx]
        if not slice_returns:
            continue
        variance = sum(ret**2 for ret in slice_returns) / Decimal(len(slice_returns))
        window_vols.append(variance.sqrt() * horizon_factor)
    if not window_vols:
        window_vols = [abs(ret) * horizon_factor for ret in returns]

    std_dev = _population_std(window_vols)
    samples = max(len(window_vols), 1)
    if samples <= 1 or std_dev <= 0:
        expected_lower = expected_upper = expected_value
    else:
        margin = Decimal("1.96") * (std_dev / Decimal(samples).sqrt())
        expected_lower = max(expected_value - margin, Decimal("0"))
        expected_upper = expected_value + margin

    assert result.value == expected_value
    assert result.lower == expected_lower
    assert result.upper == expected_upper

    serialised = result.as_dict()
    quant = result.quantization
    assert Decimal(str(serialised["value"])) == expected_value.quantize(quant)
    assert Decimal(str(serialised["confidence_interval"][0])) == expected_lower.quantize(quant)
    assert Decimal(str(serialised["confidence_interval"][1])) == expected_upper.quantize(quant)


def test_fee_forecast_preserves_decimal_precision() -> None:
    forecaster = ExposureForecaster(account_id="ACC-PRECISION")
    fee_rows = [
        {"notional": "6.1728395061728395", "fees": "0.00308641975308642"},
        {"notional": "7.2839506172839506", "fees": "0.00364197530864197"},
        {"notional": "8.3950617283950617", "fees": "0.00419753086419753"},
    ]

    result = forecaster._forecast_fee_spend(fee_rows)

    notionals = [Decimal(row["notional"]) for row in fee_rows]
    fees = [Decimal(row["fees"]) for row in fee_rows]

    total_notional = sum(notionals, Decimal("0"))
    total_fees = sum(fees, Decimal("0"))
    observations = len(fee_rows)
    horizon_days = Decimal(7)

    avg_daily_volume = total_notional / Decimal(observations)
    avg_fee_rate = total_fees / total_notional
    expected_volume = avg_daily_volume * horizon_days
    expected_value = expected_volume * avg_fee_rate

    std_dev_daily = _population_std(fees)
    std_dev_horizon = std_dev_daily * horizon_days.sqrt()
    if observations <= 1 or std_dev_horizon <= 0:
        expected_lower = expected_upper = expected_value
    else:
        margin = Decimal("1.96") * (std_dev_horizon / Decimal(observations).sqrt())
        expected_lower = max(expected_value - margin, Decimal("0"))
        expected_upper = expected_value + margin

    assert result.value == expected_value
    assert result.lower == expected_lower
    assert result.upper == expected_upper

    serialised = result.as_dict()
    quant = result.quantization
    assert Decimal(str(serialised["value"])) == expected_value.quantize(quant)
    assert Decimal(str(serialised["confidence_interval"][0])) == expected_lower.quantize(quant)
    assert Decimal(str(serialised["confidence_interval"][1])) == expected_upper.quantize(quant)


def test_margin_usage_high_precision_inputs() -> None:
    base_forecast = ForecastResult(
        value=Decimal("0.00001234"),
        lower=Decimal("0.00000678"),
        upper=Decimal("0.00001890"),
        horizon_days=7,
    )

    forecaster = ExposureForecaster(account_id="ACC-PRECISION")
    positions = [
        {"quantity": "0.00012345", "entry_price": "50789.123456"},
        {"quantity": "-0.00023456", "entry_price": "25001.987654"},
    ]

    result = forecaster._forecast_margin_usage(positions, base_forecast)

    exposure = sum(
        abs(Decimal(position["quantity"]) * Decimal(position["entry_price"]))
        for position in positions
    )
    expected_value = exposure * (Decimal(1) + base_forecast.value)
    expected_lower = exposure * (Decimal(1) + base_forecast.lower)
    expected_upper = exposure * (Decimal(1) + base_forecast.upper)

    assert result.value == expected_value
    assert result.lower == min(expected_lower, expected_upper)
    assert result.upper == max(expected_lower, expected_upper)

    serialised = result.as_dict()
    quant = result.quantization
    assert Decimal(str(serialised["value"])) == expected_value.quantize(quant)
    assert Decimal(str(serialised["confidence_interval"][0])) == min(
        expected_lower, expected_upper
    ).quantize(quant)
    assert Decimal(str(serialised["confidence_interval"][1])) == max(
        expected_lower, expected_upper
    ).quantize(quant)
