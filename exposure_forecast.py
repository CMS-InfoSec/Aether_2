"""Risk exposure forecasting FastAPI endpoints."""

from __future__ import annotations

import math
import statistics
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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


@dataclass(slots=True)
class ForecastResult:
    """Structured response describing a forecast value and its confidence interval."""

    value: float
    lower: float
    upper: float
    horizon_days: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "confidence_interval": [self.lower, self.upper],
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
        nav_values = [float(row.get("nav", 0.0)) for row in rows]
        if len(nav_values) < 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Not enough NAV observations to compute volatility",
            )

        returns: List[float] = []
        for previous, current in zip(nav_values, nav_values[1:]):
            if previous:
                returns.append((current - previous) / previous)
            else:
                returns.append(0.0)

        if not returns:
            return ForecastResult(value=0.0, lower=0.0, upper=0.0, horizon_days=7)

        span = min(20, len(returns))
        alpha = 2.0 / (span + 1)
        ewma_var = returns[0] ** 2
        for ret in returns[1:]:
            ewma_var = alpha * (ret**2) + (1.0 - alpha) * ewma_var

        daily_vol = math.sqrt(max(ewma_var, 0.0))
        horizon_days = 7
        horizon_vol = daily_vol * math.sqrt(horizon_days)

        window_vols = self._rolling_window_vols(returns, window=span, horizon_days=horizon_days)
        std_dev = statistics.pstdev(window_vols) if len(window_vols) > 1 else 0.0
        lower, upper = self._confidence_interval(horizon_vol, std_dev, max(len(window_vols), 1))

        return ForecastResult(value=horizon_vol, lower=lower, upper=upper, horizon_days=horizon_days)

    def _forecast_fee_spend(self, rows: Sequence[Mapping[str, Any]]) -> ForecastResult:
        if not rows:
            return ForecastResult(value=0.0, lower=0.0, upper=0.0, horizon_days=7)

        notional_totals = [float(row.get("notional", 0.0) or 0.0) for row in rows]
        fee_totals = [float(row.get("fees", 0.0) or 0.0) for row in rows]

        total_notional = sum(notional_totals)
        total_fees = sum(fee_totals)
        observations = len(rows)

        avg_daily_volume = total_notional / observations if observations else 0.0
        avg_fee_rate = (total_fees / total_notional) if total_notional else 0.0

        horizon_days = 7
        projected_volume = avg_daily_volume * horizon_days
        projected_fees = projected_volume * avg_fee_rate

        std_dev_daily = statistics.pstdev(fee_totals) if observations > 1 else 0.0
        std_dev_horizon = std_dev_daily * math.sqrt(horizon_days)
        lower, upper = self._confidence_interval(projected_fees, std_dev_horizon, max(observations, 1))

        return ForecastResult(value=projected_fees, lower=lower, upper=upper, horizon_days=horizon_days)

    def _forecast_margin_usage(
        self,
        rows: Sequence[Mapping[str, Any]],
        nav_volatility: ForecastResult,
    ) -> ForecastResult:
        if not rows:
            return ForecastResult(value=0.0, lower=0.0, upper=0.0, horizon_days=7)

        exposure = 0.0
        for row in rows:
            quantity = float(row.get("quantity", 0.0) or 0.0)
            entry_price = float(row.get("entry_price", 0.0) or 0.0)
            exposure += abs(quantity * entry_price)

        horizon_days = nav_volatility.horizon_days
        projected_usage = exposure * (1.0 + nav_volatility.value)

        lower_usage = exposure * (1.0 + nav_volatility.lower)
        upper_usage = exposure * (1.0 + nav_volatility.upper)

        return ForecastResult(
            value=projected_usage,
            lower=min(lower_usage, upper_usage),
            upper=max(lower_usage, upper_usage),
            horizon_days=horizon_days,
        )

    @staticmethod
    def _rolling_window_vols(
        returns: Sequence[float], *, window: int, horizon_days: int
    ) -> List[float]:
        if window <= 1:
            return [abs(r) * math.sqrt(horizon_days) for r in returns]

        vols: List[float] = []
        for idx in range(window, len(returns) + 1):
            window_slice = returns[idx - window : idx]
            if not window_slice:
                continue
            variance = sum(ret**2 for ret in window_slice) / len(window_slice)
            vols.append(math.sqrt(max(variance, 0.0)) * math.sqrt(horizon_days))
        return vols or [abs(r) * math.sqrt(horizon_days) for r in returns]

    @staticmethod
    def _confidence_interval(value: float, std_dev: float, samples: int) -> Tuple[float, float]:
        if samples <= 1 or std_dev <= 0:
            lower = upper = max(value, 0.0)
            return lower, upper

        margin = 1.96 * (std_dev / math.sqrt(samples))
        lower = max(value - margin, 0.0)
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

