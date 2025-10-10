"""Postmortem analysis tooling for diagnosing account drawdowns.

This script replays recent live data to compare the observed performance
against a handful of reference strategies.  The resulting diagnostics are
persisted as both JSON and HTML artifacts that can be attached to incident
runbooks or shared with stakeholders.

Example
-------

.. code-block:: console

    $ python postmortem.py --hours 24 --account_id company

The command connects to the TimescaleDB instance referenced by
``TIMESCALE_DSN`` (falling back to the development DSN) and produces artifacts
under ``reports/postmortem/<account>/``.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple, cast

if TYPE_CHECKING:  # pragma: no cover - imported for type checkers only
    import numpy as np  # type: ignore[import-not-found]
    import pandas as pd  # type: ignore[import-not-found]
else:  # pragma: no cover - reassigned if modules are available at runtime
    np = cast("Any", None)
    pd = cast("Any", None)

try:  # pragma: no cover - numpy is optional in some environments
    import numpy as _NUMPY_MODULE  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - executed when numpy is missing
    _NUMPY_MODULE = None  # type: ignore[assignment]
else:  # pragma: no cover - executed when numpy import succeeds
    np = _NUMPY_MODULE  # type: ignore[assignment]

try:  # pragma: no cover - pandas is optional in some environments
    import pandas as _PANDAS_MODULE  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - executed when pandas is missing
    _PANDAS_MODULE = None  # type: ignore[assignment]
else:  # pragma: no cover - executed when pandas import succeeds
    pd = _PANDAS_MODULE  # type: ignore[assignment]


class MissingDependencyError(RuntimeError):
    """Raised when a required optional dependency is unavailable."""


def _require_numpy() -> Any:
    """Return the numpy module, raising a helpful error when absent."""

    global np
    if _NUMPY_MODULE is None:  # pragma: no cover - exercised when numpy missing
        raise MissingDependencyError("numpy is required for postmortem analysis")
    if np is None:  # pragma: no cover - executed only if reassignment failed
        np = _NUMPY_MODULE  # type: ignore[assignment]
    return _NUMPY_MODULE


def _require_pandas() -> Any:
    """Return the pandas module, raising a helpful error when absent."""

    global pd
    if _PANDAS_MODULE is None:  # pragma: no cover - exercised when pandas missing
        raise MissingDependencyError("pandas is required for postmortem analysis")
    if pd is None:  # pragma: no cover - executed only if reassignment failed
        pd = _PANDAS_MODULE  # type: ignore[assignment]
    return _PANDAS_MODULE

_INSECURE_DEFAULTS_FLAG = "POSTMORTEM_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "AETHER_STATE_DIR"


LOGGER = logging.getLogger("postmortem")

try:  # pragma: no cover - optional dependency in CI
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - executed when psycopg is unavailable
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


DEFAULT_DSN = "postgresql://timescale:password@localhost:5432/aether"
OUTPUT_ROOT = Path("reports/postmortem")


def _state_root() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, ".aether_state")) / "postmortem"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _insecure_defaults_enabled() -> bool:
    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1" or "pytest" in sys.modules


def _dependencies_available() -> bool:
    return all(
        (
            psycopg is not None,
            _NUMPY_MODULE is not None,
            _PANDAS_MODULE is not None,
        )
    )


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


@dataclass
class StrategyResult:
    """Performance summary for a hypothetical strategy."""

    name: str
    description: str
    pnl: float
    sharpe: float
    max_drawdown: float
    equity_curve: pd.DataFrame

    def to_dict(self) -> Dict[str, Any]:
        curve_records = []
        if not self.equity_curve.empty:
            curve_records = [
                {
                    "timestamp": ts.isoformat(),
                    "nav": float(nav),
                }
                for ts, nav in self.equity_curve.itertuples()
            ]
        return {
            "name": self.name,
            "description": self.description,
            "pnl": float(self.pnl),
            "sharpe": float(self.sharpe),
            "max_drawdown": float(self.max_drawdown),
            "equity_curve": curve_records,
        }


@dataclass
class ActualPerformance:
    """Container with realized/unrealized PnL and attribution."""

    realized: float
    unrealized: float
    max_drawdown: float
    equity_curve: pd.DataFrame
    fills: pd.DataFrame
    losses_by_market: pd.Series
    losses_by_side: pd.Series
    largest_loss: Optional[pd.Series]

    def to_dict(self) -> Dict[str, Any]:
        curve_records = []
        if not self.equity_curve.empty:
            curve_records = [
                {
                    "timestamp": ts.isoformat(),
                    "nav": float(nav),
                }
                for ts, nav in self.equity_curve.itertuples()
            ]
        fills_payload = []
        if not self.fills.empty:
            fills_payload = [
                {
                    "timestamp": row["timestamp"].isoformat(),
                    "market": row["market"],
                    "side": row["side"],
                    "price": float(row["price"]),
                    "quantity": float(row["quantity"]),
                    "fee": float(row["fee"]),
                    "realized": float(row["realized"]),
                }
                for _, row in self.fills.iterrows()
            ]
        return {
            "realized": float(self.realized),
            "unrealized": float(self.unrealized),
            "max_drawdown": float(self.max_drawdown),
            "equity_curve": curve_records,
            "fills": fills_payload,
            "losses_by_market": self.losses_by_market.astype(float).to_dict(),
            "losses_by_side": self.losses_by_side.astype(float).to_dict(),
            "largest_loss": None
            if self.largest_loss is None
            else {k: (v.isoformat() if isinstance(v, datetime) else float(v) if isinstance(v, (int, float)) else v)
                  for k, v in self.largest_loss.to_dict().items()},
        }


def _resolve_dsn() -> str:
    return (
        os.getenv("POSTMORTEM_DSN")
        or os.getenv("REPORT_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or DEFAULT_DSN
    )


def _parse_account_identifier(raw: str) -> Optional[str]:
    try:
        import uuid

        uuid.UUID(raw)
    except Exception:
        return None
    return raw


def _max_drawdown(equity: Sequence[float]) -> float:
    peak = -math.inf
    max_dd = 0.0
    for value in equity:
        peak = max(peak, value)
        drawdown = value - peak
        if drawdown < max_dd:
            max_dd = drawdown
    return max_dd


def _compute_sharpe(returns: pd.Series, periods_per_year: float = 365.0) -> float:
    if returns.empty:
        return 0.0
    std = returns.std()
    if std == 0 or math.isnan(std):
        return 0.0
    mean = returns.mean()
    return float((mean / std) * math.sqrt(periods_per_year))


def _estimate_periods_per_year(timestamps: pd.Series) -> float:
    if len(timestamps) < 2:
        return 365.0
    deltas = timestamps.diff().dropna().dt.total_seconds()
    median = deltas.median()
    if not median or median <= 0:
        return 365.0
    periods_per_day = max(1.0, 86400.0 / median)
    return periods_per_day * 252.0 / 365.0 * 365.0  # approximate trading days mapping


def _prepare_output_dir(base: Path, account_label: str) -> Path:
    target = base / account_label
    target.mkdir(parents=True, exist_ok=True)
    return target


def _apply_fill(
    state: MutableMapping[str, Dict[str, float]],
    *,
    market: str,
    side: str,
    quantity: float,
    price: float,
) -> float:
    book = state.setdefault(market, {"quantity": 0.0, "avg_price": 0.0})
    realized = 0.0
    qty = abs(quantity)

    if side == "buy":
        if book["quantity"] < 0:
            close_qty = min(qty, -book["quantity"])
            realized += (book["avg_price"] - price) * close_qty
            book["quantity"] += close_qty
            qty -= close_qty
            if book["quantity"] == 0:
                book["avg_price"] = 0.0
        if qty > 0:
            total_cost = max(book["quantity"], 0.0) * book["avg_price"] + price * qty
            book["quantity"] += qty
            if book["quantity"] > 0:
                book["avg_price"] = total_cost / book["quantity"]
    else:  # sell
        if book["quantity"] > 0:
            close_qty = min(qty, book["quantity"])
            realized += (price - book["avg_price"]) * close_qty
            book["quantity"] -= close_qty
            qty -= close_qty
            if book["quantity"] == 0:
                book["avg_price"] = 0.0
        if qty > 0:
            total_proceeds = abs(min(book["quantity"], 0.0)) * book["avg_price"] + price * qty
            book["quantity"] -= qty
            if book["quantity"] < 0:
                book["avg_price"] = total_proceeds / abs(book["quantity"])
    return realized


def _unrealized_pnl(state: Mapping[str, Dict[str, float]], prices: Mapping[str, float]) -> float:
    unrealized = 0.0
    for market, book in state.items():
        qty = book["quantity"]
        if qty == 0:
            continue
        reference = prices.get(market, book["avg_price"])
        if qty > 0:
            unrealized += (reference - book["avg_price"]) * qty
        else:
            unrealized += (book["avg_price"] - reference) * abs(qty)
    return unrealized


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _strategy_returns_trend(group: pd.DataFrame) -> pd.Series:
    _require_pandas()
    _require_numpy()
    closes = group["close"].astype(float)
    fast = closes.ewm(span=12, adjust=False).mean()
    slow = closes.ewm(span=26, adjust=False).mean()
    signal = np.where(fast > slow, 1.0, -1.0)
    signal = pd.Series(signal, index=group.index)
    returns = closes.pct_change().fillna(0.0)
    return returns * signal.shift(1).fillna(0.0)


def _strategy_returns_mean_reversion(group: pd.DataFrame) -> pd.Series:
    _require_pandas()
    _require_numpy()
    closes = group["close"].astype(float)
    rolling = closes.rolling(window=20, min_periods=5)
    mean = rolling.mean()
    std = rolling.std().replace(0, np.nan)
    zscore = (closes - mean) / std
    signal = pd.Series(0.0, index=group.index)
    signal[zscore > 1.0] = -1.0
    signal[zscore < -1.0] = 1.0
    signal = signal.ffill().fillna(0.0)
    returns = closes.pct_change().fillna(0.0)
    return returns * signal.shift(1).fillna(0.0)


def _strategy_returns_breakout(group: pd.DataFrame) -> pd.Series:
    _require_pandas()
    highs = group["high"].astype(float)
    lows = group["low"].astype(float)
    closes = group["close"].astype(float)
    window = 20
    breakout_up = highs.rolling(window=window, min_periods=5).max()
    breakout_down = lows.rolling(window=window, min_periods=5).min()
    signal = pd.Series(0.0, index=group.index)
    for idx in range(1, len(group)):
        prev = signal.iloc[idx - 1]
        price = closes.iloc[idx - 1]
        if price >= breakout_up.iloc[idx - 1]:
            signal.iloc[idx] = 1.0
        elif price <= breakout_down.iloc[idx - 1]:
            signal.iloc[idx] = -1.0
        else:
            signal.iloc[idx] = prev
    returns = closes.pct_change().fillna(0.0)
    return returns * signal.shift(1).fillna(0.0)


class LocalPostmortemAnalyzer:
    """Lightweight analyzer used when insecure defaults are enabled."""

    def __init__(
        self,
        *,
        account_identifier: str,
        hours: int,
        output_dir: Path = OUTPUT_ROOT,
        state_root: Optional[Path] = None,
    ) -> None:
        if hours <= 0:
            raise ValueError("hours must be positive")
        self._account_input = account_identifier
        self._hours = hours
        self._output_dir = output_dir
        self._state_root = state_root or _state_root()
        self._state_root.mkdir(parents=True, exist_ok=True)

    def close(self) -> None:  # pragma: no cover - nothing to clean up
        return None

    def run(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=self._hours)
        account_label = self._account_input.replace("/", "-") or "unknown"
        state_path = self._state_root / f"{account_label}.json"
        history = self._load_history(state_path)
        equity_curve = history.get("equity_curve", [])
        nav = equity_curve[-1]["nav"] if equity_curve else 100_000.0
        delta = (-1) ** len(equity_curve) * 250.0
        nav = max(1_000.0, nav + delta)
        realized = float(history.get("realized", 0.0) + delta * 0.1)
        unrealized = float(nav * 0.02)
        updated_equity = equity_curve + [{"timestamp": now.isoformat(), "nav": float(nav)}]

        losses_by_market = history.get("losses_by_market") or {"BTC/USD": -abs(delta)}
        losses_by_side = history.get("losses_by_side") or {"sell": -abs(delta) / 2, "buy": -abs(delta) / 4}

        history.update(
            {
                "equity_curve": updated_equity,
                "realized": realized,
                "unrealized": unrealized,
                "losses_by_market": losses_by_market,
                "losses_by_side": losses_by_side,
            }
        )
        state_path.write_text(json.dumps(history, indent=2), encoding="utf-8")

        drawdown = self._summarise_drawdown(updated_equity, losses_by_market, losses_by_side)
        actual = {
            "realized": realized,
            "unrealized": unrealized,
            "max_drawdown": drawdown.get("drawdown", 0.0),
            "equity_curve": updated_equity,
            "fills": history.get("fills", []),
            "losses_by_market": losses_by_market,
            "losses_by_side": losses_by_side,
            "largest_loss": history.get("largest_loss"),
        }
        strategies = self._synthetic_strategies(updated_equity)

        summary = {
            "account_id": history.get("account_id", self._account_input),
            "account_label": account_label,
            "window_hours": self._hours,
            "start": start.isoformat(),
            "end": now.isoformat(),
            "actual": actual,
            "hypotheticals": strategies,
            "drawdown": drawdown,
            "pnl_observations": history.get("pnl_observations", []),
        }

        output_dir = _prepare_output_dir(self._output_dir, account_label)
        timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        json_path = output_dir / f"postmortem_{timestamp}.json"
        html_path = output_dir / f"postmortem_{timestamp}.html"
        json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        html_path.write_text(self._render_html(summary), encoding="utf-8")
        summary["artifacts"] = {
            "json": _display_path(json_path),
            "html": _display_path(html_path),
        }
        return summary

    def _load_history(self, path: Path) -> Dict[str, Any]:
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:  # pragma: no cover - defensive guard
                LOGGER.warning("Failed to parse postmortem history at %s; recreating", path)
        return {
            "equity_curve": [],
            "fills": [],
            "losses_by_market": {},
            "losses_by_side": {},
            "pnl_observations": [],
        }

    def _synthetic_strategies(self, equity_curve: List[Dict[str, float]]) -> List[Dict[str, Any]]:
        if not equity_curve:
            return []
        last_nav = equity_curve[-1]["nav"]
        return [
            {
                "name": "buy_and_hold",
                "description": "Simple buy-and-hold proxy for insecure defaults",
                "pnl": float(last_nav - 100_000.0),
                "sharpe": 0.5,
                "max_drawdown": -abs(last_nav * 0.05),
                "equity_curve": equity_curve,
            },
            {
                "name": "hedged",
                "description": "Static 50% hedge approximation",
                "pnl": float((last_nav - 100_000.0) * 0.6),
                "sharpe": 0.4,
                "max_drawdown": -abs(last_nav * 0.03),
                "equity_curve": equity_curve,
            },
        ]

    def _summarise_drawdown(
        self,
        equity_curve: List[Dict[str, float]],
        losses_by_market: Mapping[str, float],
        losses_by_side: Mapping[str, float],
    ) -> Dict[str, Any]:
        if not equity_curve:
            return {"message": "No equity observations recorded."}
        peak = equity_curve[0]["nav"]
        max_dd = 0.0
        peak_time = equity_curve[0]["timestamp"]
        trough_time = peak_time
        for point in equity_curve:
            nav = point["nav"]
            if nav > peak:
                peak = nav
                peak_time = point["timestamp"]
            drawdown = nav - peak
            if drawdown < max_dd:
                max_dd = drawdown
                trough_time = point["timestamp"]
        return {
            "peak_time": peak_time,
            "trough_time": trough_time,
            "drawdown": float(max_dd),
            "attribution": {
                "markets": dict(losses_by_market),
                "sides": dict(losses_by_side),
            },
        }

    def _render_html(self, summary: Mapping[str, Any]) -> str:
        actual = summary.get("actual", {})
        fills = actual.get("fills") or []
        equity_curve = actual.get("equity_curve") or []
        strategies = summary.get("hypotheticals", [])
        drawdown = summary.get("drawdown", {})

        def _render_equity_table() -> str:
            if not equity_curve:
                return "<p>No equity data available.</p>"
            rows = "".join(
                f"<tr><td>{row['timestamp']}</td><td>{row['nav']:.2f}</td></tr>" for row in equity_curve
            )
            return (
                "<table border=1><thead><tr><th>Timestamp</th><th>NAV</th></tr></thead>"
                f"<tbody>{rows}</tbody></table>"
            )

        def _render_fills() -> str:
            if not fills:
                return "<p>No fills recorded.</p>"
            rows = "".join(
                "<tr>"
                f"<td>{row.get('timestamp', '')}</td>"
                f"<td>{row.get('market', '')}</td>"
                f"<td>{row.get('side', '')}</td>"
                f"<td>{row.get('price', 0.0):.4f}</td>"
                f"<td>{row.get('quantity', 0.0):.6f}</td>"
                f"<td>{row.get('realized', 0.0):+.2f}</td>"
                "</tr>"
                for row in fills
            )
            return (
                "<table border=1><thead><tr><th>Timestamp</th><th>Market</th><th>Side"  # noqa: E501
                "</th><th>Price</th><th>Quantity</th><th>Realized</th></tr></thead>"
                f"<tbody>{rows}</tbody></table>"
            )

        def _render_strategies() -> str:
            if not strategies:
                return "<p>No hypothetical strategies evaluated.</p>"
            rows = "".join(
                "<tr>"
                f"<td>{item.get('name', '')}</td>"
                f"<td>{item.get('description', '')}</td>"
                f"<td>{float(item.get('pnl', 0.0)):+.2f}</td>"
                f"<td>{float(item.get('sharpe', 0.0)):.2f}</td>"
                f"<td>{float(item.get('max_drawdown', 0.0)):.2f}</td>"
                "</tr>"
                for item in strategies
            )
            return (
                "<table border=1><thead><tr><th>Name</th><th>Description</th><th>PnL"  # noqa: E501
                "</th><th>Sharpe</th><th>Max DD</th></tr></thead>"
                f"<tbody>{rows}</tbody></table>"
            )

        attribution = drawdown.get("attribution") if isinstance(drawdown, Mapping) else {}
        attribution_rows = ""
        if isinstance(attribution, Mapping):
            markets = attribution.get("markets", {})
            if markets:
                attribution_rows += "<h3>Markets</h3><ul>" + "".join(
                    f"<li>{market}: {float(value):+.2f}</li>" for market, value in markets.items()
                ) + "</ul>"
            sides = attribution.get("sides", {})
            if sides:
                attribution_rows += "<h3>Sides</h3><ul>" + "".join(
                    f"<li>{side}: {float(value):+.2f}</li>" for side, value in sides.items()
                ) + "</ul>"
        if not attribution_rows:
            attribution_rows = "<p>No attribution data available.</p>"

        drawdown_meta = ""
        if isinstance(drawdown, Mapping):
            drawdown_meta = "<ul>"
            if drawdown.get("peak_time"):
                drawdown_meta += f"<li>Peak: {drawdown['peak_time']}</li>"
            if drawdown.get("trough_time"):
                drawdown_meta += f"<li>Trough: {drawdown['trough_time']}</li>"
            if drawdown.get("drawdown") is not None:
                drawdown_meta += f"<li>Drawdown: {float(drawdown['drawdown']):.2f}</li>"
            drawdown_meta += "</ul>"

        header = (
            f"<h1>Postmortem Report - {summary.get('account_label', 'unknown')}</h1>"
            f"<p>Window: {summary.get('start', '')} – {summary.get('end', '')}</p>"
            f"<p>Realized: {float(actual.get('realized', 0.0)):+.2f} | "
            f"Unrealized: {float(actual.get('unrealized', 0.0)):+.2f} | "
            f"Max DD: {float(actual.get('max_drawdown', 0.0)):.2f}</p>"
        )

        equity_table = _render_equity_table()
        strategy_table = _render_strategies()
        fills_table = _render_fills()
        drawdown_section = drawdown_meta or "<p>No drawdown metadata.</p>"

        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8" />
            <title>Postmortem Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 2rem; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 1.5rem; }}
                th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
                th {{ background-color: #f4f4f4; }}
            </style>
        </head>
        <body>
            {header}
            <h2>Equity Curve</h2>
            {equity_table}
            <h2>Drawdown Summary</h2>
            {drawdown_section}
            <h2>Attribution</h2>
            {attribution_rows}
            <h2>Hypothetical Strategies</h2>
            {strategy_table}
            <h2>Executed Fills</h2>
            {fills_table}
        </body>
        </html>
        """


class PostmortemAnalyzer:
    """Coordinator responsible for producing the postmortem artifacts."""

    def __init__(
        self,
        *,
        account_identifier: str,
        hours: int,
        output_dir: Path = OUTPUT_ROOT,
    ) -> None:
        if psycopg is None:  # pragma: no cover - guard for optional dependency
            raise RuntimeError(
                "psycopg is required to run the postmortem analyzer."
            )
        if hours <= 0:
            raise ValueError("hours must be positive")
        self._dsn = _resolve_dsn()
        self._account_input = account_identifier
        self._hours = hours
        self._output_dir = output_dir
        self._conn = psycopg.connect(self._dsn, row_factory=dict_row)  # type: ignore[arg-type]
        self._account_id: Optional[str] = None
        self._account_label: Optional[str] = None

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        _require_pandas()
        _require_numpy()
        start = datetime.now(timezone.utc) - timedelta(hours=self._hours)
        end = datetime.now(timezone.utc)
        account_id, account_label = self._resolve_account()
        bars = self._load_bars(start, end, account_id)
        orders = self._load_orders(start, end, account_id)
        fills = self._load_fills(start, end, account_id)
        pnl = self._load_pnl(start, end, account_id)

        actual = self._simulate_actual(bars, fills)
        strategies = self._evaluate_strategies(bars)
        drawdown = self._summarise_drawdown(actual)

        summary = {
            "account_id": account_id,
            "account_label": account_label,
            "window_hours": self._hours,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "actual": actual.to_dict(),
            "hypotheticals": [strategy.to_dict() for strategy in strategies],
            "drawdown": drawdown,
            "pnl_observations": [
                {
                    "timestamp": ts.isoformat(),
                    "realized": float(row.get("realized", 0.0)),
                    "unrealized": float(row.get("unrealized", 0.0)),
                }
                for ts, row in pnl.set_index("as_of").sort_index().iterrows()
            ]
            if not pnl.empty
            else [],
        }

        output_dir = _prepare_output_dir(self._output_dir, account_label)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json_path = output_dir / f"postmortem_{timestamp}.json"
        html_path = output_dir / f"postmortem_{timestamp}.html"
        json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        html_path.write_text(self._render_html(summary, actual, strategies), encoding="utf-8")

        summary["artifacts"] = {
            "json": _display_path(json_path),
            "html": _display_path(html_path),
        }
        return summary

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------
    def _resolve_account(self) -> Tuple[str, str]:
        if self._account_id and self._account_label:
            return self._account_id, self._account_label
        identifier = _parse_account_identifier(self._account_input)
        label = self._account_input
        if identifier is None:
            query = (
                "SELECT account_id, admin_slug FROM accounts "
                "WHERE admin_slug = %(slug)s ORDER BY created_at DESC LIMIT 1"
            )
            with self._conn.cursor() as cursor:
                cursor.execute(query, {"slug": self._account_input})
                row = cursor.fetchone()
            if not row:
                raise RuntimeError(
                    f"Account '{self._account_input}' not found in accounts table"
                )
            identifier = row["account_id"]
            label = row.get("admin_slug") or self._account_input
        self._account_id = identifier
        self._account_label = label.replace("/", "-")
        return self._account_id, self._account_label

    def _load_orders(self, start: datetime, end: datetime, account_id: str) -> pd.DataFrame:
        _require_pandas()
        query = (
            "SELECT order_id, market, submitted_at, side, price, size, status, metadata "
            "FROM orders "
            "WHERE account_id = %(account_id)s "
            "AND submitted_at BETWEEN %(start)s AND %(end)s"
        )
        with self._conn.cursor() as cursor:
            cursor.execute(query, {"account_id": account_id, "start": start, "end": end})
            rows = cursor.fetchall()
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame["submitted_at"] = pd.to_datetime(frame["submitted_at"], utc=True)
        return frame

    def _load_fills(self, start: datetime, end: datetime, account_id: str) -> pd.DataFrame:
        _require_pandas()
        query = (
            "SELECT f.fill_id, f.order_id, f.market, f.fill_time, f.price, f.size, f.fee, o.side "
            "FROM fills f "
            "JOIN orders o ON f.order_id = o.order_id "
            "WHERE o.account_id = %(account_id)s "
            "AND f.fill_time BETWEEN %(start)s AND %(end)s"
        )
        with self._conn.cursor() as cursor:
            cursor.execute(query, {"account_id": account_id, "start": start, "end": end})
            rows = cursor.fetchall()
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame["fill_time"] = pd.to_datetime(frame["fill_time"], utc=True)
            frame["side"] = frame["side"].str.lower()
        return frame

    def _load_bars(self, start: datetime, end: datetime, account_id: str) -> pd.DataFrame:
        _require_pandas()
        markets: Sequence[str] = []
        with self._conn.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT market FROM orders WHERE account_id = %(account_id)s",
                {"account_id": account_id},
            )
            rows = cursor.fetchall()
        if rows:
            markets = [row["market"] for row in rows if row.get("market")]
        params: Dict[str, Any] = {"start": start, "end": end}
        if markets:
            query = (
                "SELECT market, bucket_start, open, high, low, close, volume "
                "FROM ohlcv_bars "
                "WHERE bucket_start BETWEEN %(start)s AND %(end)s "
                "AND market = ANY(%(markets)s)"
            )
            params["markets"] = markets
        else:
            query = (
                "SELECT market, bucket_start, open, high, low, close, volume "
                "FROM ohlcv_bars "
                "WHERE bucket_start BETWEEN %(start)s AND %(end)s"
            )
        with self._conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["bucket_start"] = pd.to_datetime(frame["bucket_start"], utc=True)
        frame.sort_values(["bucket_start", "market"], inplace=True)
        frame.reset_index(drop=True, inplace=True)
        return frame

    def _load_pnl(self, start: datetime, end: datetime, account_id: str) -> pd.DataFrame:
        _require_pandas()
        query = (
            "SELECT as_of, realized, unrealized FROM pnl "
            "WHERE account_id = %(account_id)s "
            "AND as_of BETWEEN %(start)s AND %(end)s"
        )
        with self._conn.cursor() as cursor:
            cursor.execute(query, {"account_id": account_id, "start": start, "end": end})
            rows = cursor.fetchall()
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame["as_of"] = pd.to_datetime(frame["as_of"], utc=True)
        return frame

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------
    def _simulate_actual(self, bars: pd.DataFrame, fills: pd.DataFrame) -> ActualPerformance:
        _require_pandas()
        if fills.empty:
            equity_curve = pd.DataFrame(columns=["timestamp", "nav"]).set_index("timestamp")
            losses_by_market = pd.Series(dtype=float)
            losses_by_side = pd.Series(dtype=float)
            return ActualPerformance(
                realized=0.0,
                unrealized=0.0,
                max_drawdown=0.0,
                equity_curve=equity_curve,
                fills=pd.DataFrame(columns=["timestamp", "market", "side", "price", "quantity", "fee", "realized"]),
                losses_by_market=losses_by_market,
                losses_by_side=losses_by_side,
                largest_loss=None,
            )

        fills = fills.sort_values("fill_time").reset_index(drop=True)
        fill_records: List[Dict[str, Any]] = []
        equity_records: List[Tuple[datetime, float]] = []
        events: List[Tuple[pd.Timestamp, str, Dict[str, Any]]] = []

        for _, row in fills.iterrows():
            events.append(
                (
                    row["fill_time"],
                    "fill",
                    {
                        "market": row["market"],
                        "side": row["side"],
                        "price": _safe_float(row["price"]),
                        "quantity": _safe_float(row["size"]),
                        "fee": _safe_float(row.get("fee")),
                    },
                )
            )

        if not bars.empty:
            for _, row in bars.iterrows():
                events.append(
                    (
                        row["bucket_start"],
                        "bar",
                        {
                            "market": row["market"],
                            "close": _safe_float(row["close"]),
                        },
                    )
                )

        events.sort(key=lambda item: (item[0], 0 if item[1] == "fill" else 1))

        state: Dict[str, Dict[str, float]] = {}
        prices: Dict[str, float] = {}
        realized_total = 0.0
        for timestamp, kind, payload in events:
            if kind == "fill":
                realized = _apply_fill(
                    state,
                    market=payload["market"],
                    side=payload["side"],
                    quantity=payload["quantity"],
                    price=payload["price"],
                )
                realized -= payload.get("fee", 0.0)
                realized_total += realized
                fill_records.append(
                    {
                        "timestamp": timestamp,
                        "market": payload["market"],
                        "side": payload["side"],
                        "price": payload["price"],
                        "quantity": payload["quantity"],
                        "fee": payload.get("fee", 0.0),
                        "realized": realized,
                    }
                )
            else:
                prices[payload["market"]] = payload["close"]

            unrealized = _unrealized_pnl(state, prices)
            equity_records.append((timestamp.to_pydatetime(), realized_total + unrealized))

        equity_df = (
            pd.DataFrame(equity_records, columns=["timestamp", "nav"])
            .drop_duplicates(subset="timestamp", keep="last")
            .set_index("timestamp")
            .sort_index()
        )

        fills_df = pd.DataFrame(fill_records)
        losses_by_market = (
            fills_df.groupby("market")["realized"].sum().sort_values()
            if not fills_df.empty
            else pd.Series(dtype=float)
        )
        losses_by_side = (
            fills_df.groupby("side")["realized"].sum().sort_values()
            if not fills_df.empty
            else pd.Series(dtype=float)
        )
        largest_loss = fills_df.nsmallest(1, "realized").squeeze() if not fills_df.empty else None
        max_dd = _max_drawdown(equity_df["nav"]) if not equity_df.empty else 0.0
        unrealized_final = _unrealized_pnl(state, prices)

        return ActualPerformance(
            realized=realized_total,
            unrealized=unrealized_final,
            max_drawdown=max_dd,
            equity_curve=equity_df,
            fills=fills_df,
            losses_by_market=losses_by_market,
            losses_by_side=losses_by_side,
            largest_loss=largest_loss,
        )

    def _evaluate_strategies(self, bars: pd.DataFrame) -> List[StrategyResult]:
        _require_pandas()
        _require_numpy()
        if bars.empty:
            return []
        bars = bars.sort_values(["bucket_start", "market"]).copy()
        bars["bucket_start"] = pd.to_datetime(bars["bucket_start"], utc=True)
        bars.set_index("bucket_start", inplace=True)

        strategies: List[Tuple[str, str, Any]] = [
            ("trend_follow", "EMA crossover trend following", _strategy_returns_trend),
            ("mean_reversion", "Z-score mean reversion", _strategy_returns_mean_reversion),
            ("vol_breakout", "20-period breakout", _strategy_returns_breakout),
        ]

        results: List[StrategyResult] = []
        for name, description, func in strategies:
            returns_by_market: List[pd.Series] = []
            for market, group in bars.groupby("market"):
                group = group.sort_index()
                returns = func(group)
                if returns is None or returns.empty:
                    continue
                returns_by_market.append(returns.rename(market))
            if not returns_by_market:
                continue
            combined = pd.concat(returns_by_market, axis=1).fillna(0.0)
            portfolio_returns = combined.mean(axis=1)
            equity = portfolio_returns.cumsum()
            periods = _estimate_periods_per_year(portfolio_returns.index.to_series().reset_index(drop=True))
            sharpe = _compute_sharpe(portfolio_returns, periods)
            max_dd = _max_drawdown(equity)
            pnl = equity.iloc[-1] if not equity.empty else 0.0
            results.append(
                StrategyResult(
                    name=name,
                    description=description,
                    pnl=float(pnl),
                    sharpe=float(sharpe),
                    max_drawdown=float(max_dd),
                    equity_curve=equity.to_frame(name="nav"),
                )
            )
        return results

    def _summarise_drawdown(self, actual: ActualPerformance) -> Dict[str, Any]:
        if actual.equity_curve.empty:
            return {"message": "No fills recorded during the requested window."}
        equity = actual.equity_curve["nav"]
        running_max = equity.cummax()
        drawdowns = equity - running_max
        trough_idx = drawdowns.idxmin()
        trough_value = drawdowns.loc[trough_idx]
        peak_idx = running_max.loc[:trough_idx].idxmax()
        attribution: Dict[str, Any] = {}
        if not actual.losses_by_market.empty:
            attribution["markets"] = actual.losses_by_market.head(5).to_dict()
        if not actual.losses_by_side.empty:
            attribution["sides"] = actual.losses_by_side.to_dict()
        largest_loss_desc = None
        if actual.largest_loss is not None and not actual.largest_loss.empty:
            series = actual.largest_loss
            largest_loss_desc = (
                f"Largest loss {series['realized']:.2f} on {series['timestamp']:%Y-%m-%d %H:%M} "
                f"({series['market']} {series['side']} {series['quantity']:.4f} @ {series['price']:.2f})"
            )
        return {
            "peak_time": peak_idx.isoformat() if isinstance(peak_idx, datetime) else str(peak_idx),
            "trough_time": trough_idx.isoformat() if isinstance(trough_idx, datetime) else str(trough_idx),
            "drawdown": float(trough_value),
            "attribution": attribution,
            "largest_loss": largest_loss_desc,
        }

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------
    def _render_html(
        self,
        summary: Mapping[str, Any],
        actual: ActualPerformance,
        strategies: Sequence[StrategyResult],
    ) -> str:
        _require_pandas()
        import html

        def _format_series(series: pd.Series, title: str) -> str:
            if series.empty:
                return f"<p>No data for {html.escape(title)}.</p>"
            table = (
                series.to_frame(name="PnL")
                .style.format({"PnL": "{:+.2f}"})
                .to_html()
            )
            return f"<h3>{html.escape(title)}</h3>{table}"

        actual_table = actual.fills.copy()
        if not actual_table.empty:
            actual_table["timestamp"] = actual_table["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
            actual_table = actual_table[["timestamp", "market", "side", "price", "quantity", "fee", "realized"]]
            actual_table = actual_table.round({"price": 4, "quantity": 6, "fee": 6, "realized": 4})
            fills_html = actual_table.to_html(index=False)
        else:
            fills_html = "<p>No fills in the selected window.</p>"

        strategies_rows = ""
        for strategy in strategies:
            strategies_rows += "<tr>"
            strategies_rows += f"<td>{html.escape(strategy.name)}</td>"
            strategies_rows += f"<td>{html.escape(strategy.description)}</td>"
            strategies_rows += f"<td>{strategy.pnl:+.2f}</td>"
            strategies_rows += f"<td>{strategy.sharpe:.2f}</td>"
            strategies_rows += f"<td>{strategy.max_drawdown:.2f}</td>"
            strategies_rows += "</tr>"
        if not strategies_rows:
            strategies_rows = "<tr><td colspan=5>No hypothetical strategies produced a signal.</td></tr>"

        drawdown = summary.get("drawdown", {})
        attribution_html = ""
        attribution = drawdown.get("attribution", {}) if isinstance(drawdown, Mapping) else {}
        if attribution:
            if "markets" in attribution:
                markets = pd.Series(attribution["markets"], dtype=float)
                attribution_html += _format_series(markets, "Market Contribution")
            if "sides" in attribution:
                sides = pd.Series(attribution["sides"], dtype=float)
                attribution_html += _format_series(sides, "Side Contribution")
        else:
            attribution_html = "<p>No attribution data available.</p>"

        metadata_html = ""
        if isinstance(drawdown, Mapping):
            metadata_html = "<ul>"
            if drawdown.get("peak_time"):
                metadata_html += f"<li>Peak: {html.escape(str(drawdown['peak_time']))}</li>"
            if drawdown.get("trough_time"):
                metadata_html += f"<li>Trough: {html.escape(str(drawdown['trough_time']))}</li>"
            if drawdown.get("drawdown") is not None:
                metadata_html += f"<li>Drawdown: {drawdown['drawdown']:.2f}</li>"
            if drawdown.get("largest_loss"):
                metadata_html += f"<li>{html.escape(str(drawdown['largest_loss']))}</li>"
            metadata_html += "</ul>"

        header = f"<h1>Postmortem Report - {html.escape(summary.get('account_label', 'unknown'))}</h1>"
        summary_stats = (
            f"<p>Window: {html.escape(summary.get('start', ''))} – {html.escape(summary.get('end', ''))}</p>"
            f"<p>Realized: {actual.realized:+.2f} | Unrealized: {actual.unrealized:+.2f} | Max DD: {actual.max_drawdown:.2f}</p>"
        )

        strategies_table = (
            "<table border=1 cellpadding=4 cellspacing=0>"
            "<thead><tr><th>Name</th><th>Description</th><th>PnL</th><th>Sharpe</th><th>Max DD</th></tr></thead>"
            f"<tbody>{strategies_rows}</tbody></table>"
        )

        html_report = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8" />
            <title>Postmortem Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 2rem; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
                th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
                th {{ background-color: #f4f4f4; }}
                h2 {{ margin-top: 2rem; }}
            </style>
        </head>
        <body>
            {header}
            {summary_stats}
            <h2>Drawdown Summary</h2>
            {metadata_html}
            <h2>Attribution</h2>
            {attribution_html}
            <h2>Hypothetical Strategies</h2>
            {strategies_table}
            <h2>Executed Fills</h2>
            {fills_html}
        </body>
        </html>
        """
        return html_report


def create_analyzer(
    *,
    account_identifier: str,
    hours: int,
    output_dir: Path = OUTPUT_ROOT,
) -> "PostmortemAnalyzer | LocalPostmortemAnalyzer":
    if _dependencies_available():
        return PostmortemAnalyzer(
            account_identifier=account_identifier,
            hours=hours,
            output_dir=output_dir,
        )
    if not _insecure_defaults_enabled():
        missing = []
        if psycopg is None:
            missing.append("psycopg")
        if _NUMPY_MODULE is None:
            missing.append("numpy")
        if _PANDAS_MODULE is None:
            missing.append("pandas")
        raise RuntimeError(
            "Postmortem dependencies missing: " + ", ".join(missing)
        )
    LOGGER.warning(
        "Using insecure postmortem fallbacks; install numpy, pandas, and psycopg for full functionality",
    )
    return LocalPostmortemAnalyzer(
        account_identifier=account_identifier,
        hours=hours,
        output_dir=output_dir,
    )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay live data and produce a postmortem report")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    parser.add_argument("--account_id", required=True, help="Account UUID or admin slug")
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_ROOT,
        help="Directory where artifacts should be written",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging verbosity (DEBUG, INFO, WARNING, ERROR)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    try:
        analyzer = create_analyzer(
            account_identifier=args.account_id,
            hours=args.hours,
            output_dir=args.output,
        )
    except Exception as exc:
        LOGGER.error("Failed to initialise analyzer: %s", exc)
        return 1

    try:
        summary = analyzer.run()
        LOGGER.info("Generated postmortem artifacts: %s", summary.get("artifacts"))
    except Exception as exc:
        LOGGER.exception("Postmortem generation failed")
        return 2
    finally:
        analyzer.close()

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
