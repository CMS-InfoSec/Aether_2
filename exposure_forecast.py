"""Risk exposure forecasting FastAPI endpoints."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterator, List, Mapping, Sequence, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, status
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from services.common.config import get_timescale_session
from services.common.security import require_admin_account


PNL_CURVE_QUERY = """
SELECT
    as_of,
    COALESCE(nav, 0) AS nav
FROM pnl_curves
WHERE account_id = %(account_id)s
  AND as_of >= %(start)s
  AND as_of < %(end)s
ORDER BY as_of
"""


DAILY_FEES_AND_VOLUME_QUERY = """
SELECT
    DATE_TRUNC('day', f.fill_time) AS day,
    COALESCE(SUM(ABS(f.quantity * f.price)), 0) AS notional,
    COALESCE(SUM(f.fee), 0) AS fees
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE o.account_id = %(account_id)s
  AND f.fill_time >= %(start)s
  AND f.fill_time < %(end)s
GROUP BY day
ORDER BY day
"""


LATEST_POSITIONS_QUERY = """
WITH latest_positions AS (
    SELECT DISTINCT ON (market)
        market,
        quantity,
        COALESCE(entry_price, 0) AS entry_price,
        as_of
    FROM positions
    WHERE account_id = %(account_id)s
    ORDER BY market, as_of DESC
)
SELECT market, quantity, entry_price
FROM latest_positions
"""


DecimalLike = Decimal | float | int | str

ZERO = Decimal("0")
CONFIDENCE_Z_SCORE = Decimal("1.96")
DEFAULT_QUANTIZATION = Decimal("0.00000001")


def _as_decimal(value: DecimalLike) -> Decimal:
    """Convert arbitrary numeric inputs to :class:`~decimal.Decimal`."""

    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        return Decimal(value)
    if isinstance(value, int):
        return Decimal(value)
    return Decimal(str(value))


def _population_std(values: Sequence[Decimal]) -> Decimal:
    """Return the population standard deviation of *values* as a decimal."""

    if len(values) <= 1:
        return ZERO

    mean = sum(values, ZERO) / Decimal(len(values))
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
    return variance.sqrt()


@dataclass(slots=True)
class ForecastResult:
    """Structured response describing a forecast value and its confidence interval."""

    value: Decimal
    lower: Decimal
    upper: Decimal
    horizon_days: int
    quantization: Decimal = field(default=DEFAULT_QUANTIZATION)

    def _quantize(self, value: Decimal) -> Decimal:
        return value.quantize(self.quantization, rounding=ROUND_HALF_UP)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "value": float(self._quantize(self.value)),
            "confidence_interval": [
                float(self._quantize(self.lower)),
                float(self._quantize(self.upper)),
            ],
            "horizon_days": self.horizon_days,
        }


class ExposureForecaster:
    """Domain service encapsulating the exposure forecasting logic."""

    def __init__(self, *, account_id: str, window_days: int = 90) -> None:
        self._account_id = account_id
        self._window_days = window_days
        self._timescale = get_timescale_session(account_id)

    @contextmanager
    def _session(self) -> Iterator[RealDictCursor]:
        import psycopg2

        conn = psycopg2.connect(self._timescale.dsn)
        try:
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(self._timescale.account_schema)
                    )
                )
                yield cursor
        finally:
            conn.close()

    def _fetch(self, cursor: RealDictCursor, query: str, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def forecast(self) -> Dict[str, Any]:
        """Generate the NAV, fee, and margin forecasts for the account."""

        now = datetime.now(timezone.utc)
        start = now - timedelta(days=self._window_days)

        with self._session() as cursor:
            nav_rows = self._fetch(
                cursor,
                PNL_CURVE_QUERY,
                {"account_id": self._account_id, "start": start, "end": now},
            )
            if not nav_rows:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No NAV history available for account",
                )

            fees_rows = self._fetch(
                cursor,
                DAILY_FEES_AND_VOLUME_QUERY,
                {"account_id": self._account_id, "start": start, "end": now},
            )
            positions_rows = self._fetch(cursor, LATEST_POSITIONS_QUERY, {"account_id": self._account_id})

        nav_forecast = self._forecast_nav_volatility(nav_rows)
        fee_forecast = self._forecast_fee_spend(fees_rows)
        margin_forecast = self._forecast_margin_usage(positions_rows, nav_forecast)

        return {
            "account_id": self._account_id,
            "as_of": now.isoformat(),
            "projected_nav_volatility": nav_forecast.as_dict(),
            "projected_fee_spend": fee_forecast.as_dict(),
            "projected_margin_usage": margin_forecast.as_dict(),
        }

    def _forecast_nav_volatility(self, rows: Sequence[Mapping[str, Any]]) -> ForecastResult:
        nav_values = [_as_decimal(row.get("nav", ZERO) or ZERO) for row in rows]
        if len(nav_values) < 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Not enough NAV observations to compute volatility",
            )

        returns: List[Decimal] = []
        for previous, current in zip(nav_values, nav_values[1:]):
            if previous != ZERO:
                returns.append((current - previous) / previous)
            else:
                returns.append(ZERO)

        if not returns:
            return ForecastResult(value=ZERO, lower=ZERO, upper=ZERO, horizon_days=7)

        span = min(20, len(returns))
        alpha = Decimal(2) / Decimal(span + 1)
        complement = Decimal(1) - alpha
        ewma_var = returns[0] ** 2
        for ret in returns[1:]:
            ewma_var = alpha * (ret**2) + complement * ewma_var

        daily_vol = max(ewma_var, ZERO).sqrt()
        horizon_days = 7
        horizon_vol = daily_vol * Decimal(horizon_days).sqrt()

        window_vols = self._rolling_window_vols(
            returns, window=span, horizon_days=horizon_days
        )
        std_dev = _population_std(window_vols)
        lower, upper = self._confidence_interval(
            horizon_vol, std_dev, max(len(window_vols), 1)
        )

        return ForecastResult(
            value=horizon_vol,
            lower=lower,
            upper=upper,
            horizon_days=horizon_days,
            quantization=Decimal("0.00000001"),
        )

    def _forecast_fee_spend(self, rows: Sequence[Mapping[str, Any]]) -> ForecastResult:
        if not rows:
            return ForecastResult(value=ZERO, lower=ZERO, upper=ZERO, horizon_days=7)

        notional_totals = [_as_decimal(row.get("notional", ZERO) or ZERO) for row in rows]
        fee_totals = [_as_decimal(row.get("fees", ZERO) or ZERO) for row in rows]

        total_notional = sum(notional_totals, ZERO)
        total_fees = sum(fee_totals, ZERO)
        observations = len(rows)

        avg_daily_volume = (
            total_notional / Decimal(observations) if observations else ZERO
        )
        avg_fee_rate = (total_fees / total_notional) if total_notional else ZERO

        horizon_days = 7
        horizon_decimal = Decimal(horizon_days)
        projected_volume = avg_daily_volume * horizon_decimal
        projected_fees = projected_volume * avg_fee_rate

        std_dev_daily = _population_std(fee_totals)
        std_dev_horizon = std_dev_daily * horizon_decimal.sqrt()
        lower, upper = self._confidence_interval(
            projected_fees, std_dev_horizon, max(observations, 1)
        )

        return ForecastResult(
            value=projected_fees,
            lower=lower,
            upper=upper,
            horizon_days=horizon_days,
        )

    def _forecast_margin_usage(
        self,
        rows: Sequence[Mapping[str, Any]],
        nav_volatility: ForecastResult,
    ) -> ForecastResult:
        if not rows:
            return ForecastResult(value=ZERO, lower=ZERO, upper=ZERO, horizon_days=7)

        exposure = ZERO
        for row in rows:
            quantity = _as_decimal(row.get("quantity", ZERO) or ZERO)
            entry_price = _as_decimal(row.get("entry_price", ZERO) or ZERO)
            exposure += abs(quantity * entry_price)

        horizon_days = nav_volatility.horizon_days
        projected_usage = exposure * (Decimal(1) + nav_volatility.value)

        lower_usage = exposure * (Decimal(1) + nav_volatility.lower)
        upper_usage = exposure * (Decimal(1) + nav_volatility.upper)

        return ForecastResult(
            value=projected_usage,
            lower=min(lower_usage, upper_usage),
            upper=max(lower_usage, upper_usage),
            horizon_days=horizon_days,
        )

    @staticmethod
    def _rolling_window_vols(
        returns: Sequence[Decimal], *, window: int, horizon_days: int
    ) -> List[Decimal]:
        if window <= 1:
            horizon_factor = Decimal(horizon_days).sqrt()
            return [abs(r) * horizon_factor for r in returns]

        vols: List[Decimal] = []
        horizon_factor = Decimal(horizon_days).sqrt()
        for idx in range(window, len(returns) + 1):
            window_slice = returns[idx - window : idx]
            if not window_slice:
                continue
            variance = sum(ret**2 for ret in window_slice) / Decimal(len(window_slice))
            vols.append(max(variance, ZERO).sqrt() * horizon_factor)
        return vols or [abs(r) * horizon_factor for r in returns]

    @staticmethod
    def _confidence_interval(
        value: Decimal, std_dev: Decimal, samples: int
    ) -> Tuple[Decimal, Decimal]:
        if samples <= 1 or std_dev <= ZERO:
            lower = upper = max(value, ZERO)
            return lower, upper

        margin = CONFIDENCE_Z_SCORE * (
            std_dev / Decimal(samples).sqrt()
        )
        lower = max(value - margin, ZERO)
        upper = value + margin
        return lower, upper


router = APIRouter(prefix="/risk", tags=["risk"])


@router.get("/forecast")
def get_exposure_forecast(
    requested_account_id: str = Query(..., alias="account_id", description="Account identifier"),
    authorized_account_id: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    """Return projected NAV volatility, fee spend, and margin usage for the next week."""

    if requested_account_id != authorized_account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between credential and requested account",
        )

    forecaster = ExposureForecaster(account_id=requested_account_id)
    return forecaster.forecast()


__all__ = ["router", "ExposureForecaster", "ForecastResult"]

