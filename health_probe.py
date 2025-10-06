"""Operational health probe for the trading decision pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Mapping, Optional

import httpx
from prometheus_client import Gauge, start_http_server


DEFAULT_ACCOUNT_ID = "ACC-DEFAULT"
DEFAULT_SYMBOL = "BTC-USD"
DEFAULT_INTERVAL = 300.0
DEFAULT_ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
DEFAULT_WS_BOOK_HEALTH_URL = os.getenv("WS_BOOK_HEALTH_URL")
WS_BOOK_DELAY_THRESHOLD_SECONDS = float(os.getenv("WS_BOOK_DELAY_THRESHOLD_SECONDS", "2.0"))
ALERTMANAGER_ENDPOINT = "/api/v2/alerts"
LATENCY_BUDGET_MS = 500.0


logger = logging.getLogger("health_probe")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


PIPELINE_LATENCY_GAUGE = Gauge(
    "pipeline_latency_ms",
    "Latency in milliseconds for the dummy Intent->Policy->Risk->OMS pipeline.",
)

WS_BOOK_STALENESS_GAUGE = Gauge(
    "ws_book_lag_ms",
    "Age of the latest websocket book update in milliseconds.",
)

TRADING_BLOCK_GAUGE = Gauge(
    "trading_block_active",
    "1 when new trades are blocked due to stale market data.",
)
TRADING_BLOCK_GAUGE.set(0.0)


@dataclass
class LatencyState:
    """Holds the most recent latency observations for HTTP exposure."""

    pipeline_latency_ms: float = math.nan
    ws_book_lag_ms: float = math.nan
    last_ws_book_timestamp: Optional[float] = None
    last_updated_epoch: float = 0.0
    trading_blocked: bool = False
    trading_block_reason: Optional[str] = None
    trading_block_since: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        """Serialise state to JSON-safe primitives."""

        def _safe_float(value: Optional[float]) -> Optional[float]:
            if value is None:
                return None
            if isinstance(value, float) and math.isnan(value):
                return None
            return value

        def _to_iso(timestamp: Optional[float]) -> Optional[str]:
            if timestamp is None:
                return None
            try:
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
            return dt.isoformat().replace("+00:00", "Z")

        return {
            "pipeline_latency_ms": _safe_float(self.pipeline_latency_ms),
            "ws_book_lag_ms": _safe_float(self.ws_book_lag_ms),
            "last_ws_book_timestamp": _to_iso(self.last_ws_book_timestamp),
            "last_updated": _to_iso(self.last_updated_epoch),
            "trading_blocked": self.trading_blocked,
            "trading_block_reason": self.trading_block_reason,
            "trading_block_since": _to_iso(self.trading_block_since),
        }


LATENCY_STATE = LatencyState()
_LATENCY_STATE_LOCK = threading.Lock()


class TradingBlocker:
    """Tracks whether trading should be blocked due to stale data."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._blocked = False
        self._reason: Optional[str] = None
        self._since: Optional[float] = None

    def block(self, reason: str) -> None:
        with self._lock:
            now = time.time()
            if not self._blocked:
                logger.warning("Blocking new trades: %s", reason)
            elif reason != self._reason:
                logger.warning(
                    "Updating trade block reason from %s to %s", self._reason, reason
                )
            self._blocked = True
            self._reason = reason
            self._since = now
            TRADING_BLOCK_GAUGE.set(1.0)

    def unblock(self) -> None:
        with self._lock:
            if self._blocked:
                logger.info("Unblocking new trades; market data latency recovered")
            self._blocked = False
            self._reason = None
            self._since = None
            TRADING_BLOCK_GAUGE.set(0.0)

    def snapshot(self) -> tuple[bool, Optional[str], Optional[float]]:
        with self._lock:
            return self._blocked, self._reason, self._since


TRADING_BLOCKER = TradingBlocker()


def _update_latency_state(
    pipeline_latency_ms: float,
    ws_book_timestamp: Optional[float],
    ws_book_lag_ms: float,
) -> None:
    blocked, reason, since = TRADING_BLOCKER.snapshot()
    with _LATENCY_STATE_LOCK:
        LATENCY_STATE.pipeline_latency_ms = pipeline_latency_ms
        LATENCY_STATE.ws_book_lag_ms = ws_book_lag_ms
        LATENCY_STATE.last_ws_book_timestamp = ws_book_timestamp
        LATENCY_STATE.last_updated_epoch = time.time()
        LATENCY_STATE.trading_blocked = blocked
        LATENCY_STATE.trading_block_reason = reason
        LATENCY_STATE.trading_block_since = since


class _LatencyHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler exposing latency state."""

    server_version = "HealthProbeLatency/1.0"

    def do_GET(self) -> None:  # pragma: no cover - exercised via integration tests
        path = self.path.split("?", 1)[0]
        if path != "/health/latency":
            self.send_error(404, "Not Found")
            return

        with _LATENCY_STATE_LOCK:
            payload = json.dumps(LATENCY_STATE.as_dict()).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover - noise
        logger.info("HTTP %s - %s", self.address_string(), format % args)


def _start_health_server(port: int) -> ThreadingHTTPServer:
    """Start the latency health HTTP server on the provided port."""

    server = ThreadingHTTPServer(("0.0.0.0", port), _LatencyHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="latency-health-server",
        daemon=True,
    )
    thread.start()
    logger.info("Latency health endpoint exposed on port %s", port)
    return server


@dataclass
class DummyIntent:
    """Simplified intent object produced by the dummy policy layer."""

    account_id: str
    symbol: str
    features: Dict[str, float]
    desired_qty: float
    side: str


@dataclass
class PolicyDecision:
    """Simplified policy decision used for the downstream checks."""

    account_id: str
    symbol: str
    side: str
    quantity: float
    order_type: str
    venue: str


@dataclass
class RiskAssessment:
    """Result of the dummy risk validation stage."""

    approved: bool
    reasons: Optional[str] = None


@dataclass
class OMSAcknowledgement:
    """Represents the OMS paper trade acknowledgement."""

    status: str


class PipelineError(RuntimeError):
    """Raised when any stage of the dummy pipeline fails."""


class MarketDataError(PipelineError):
    """Base class for websocket market data failures."""

    def __init__(
        self,
        message: str,
        *,
        timestamp: Optional[float] = None,
        lag_ms: float = math.nan,
    ) -> None:
        super().__init__(message)
        self.timestamp = timestamp
        self.lag_ms = lag_ms


class MarketDataUnavailableError(MarketDataError):
    """Raised when websocket book data cannot be fetched."""


class MarketDataStaleError(MarketDataError):
    """Raised when websocket book data is too old."""


def _generate_dummy_intent(account_id: str, symbol: str) -> DummyIntent:
    """Generate a deterministic yet slightly noisy intent payload."""

    base_qty = 10.0
    noise = random.uniform(-2.0, 2.0)
    side = random.choice(["buy", "sell"])
    features = {
        "edge_bps": random.uniform(5.0, 25.0),
        "imbalance": random.uniform(-0.5, 0.5),
        "volatility": random.uniform(0.1, 1.5),
    }
    desired_qty = max(1.0, base_qty + noise)
    return DummyIntent(
        account_id=account_id,
        symbol=symbol,
        features=features,
        desired_qty=desired_qty,
        side=side,
    )


def _evaluate_policy(intent: DummyIntent) -> PolicyDecision:
    """Convert an intent into a simplified policy decision."""

    # Simulate some lightweight computation for latency realism.
    time.sleep(random.uniform(0.005, 0.02))
    edge = intent.features.get("edge_bps", 0.0)
    if edge <= 0:
        raise PipelineError("Policy rejected intent due to non-positive edge")

    order_type = "limit" if edge > 15 else "market"
    venue = "maker" if order_type == "limit" else "taker"
    return PolicyDecision(
        account_id=intent.account_id,
        symbol=intent.symbol,
        side=intent.side,
        quantity=round(intent.desired_qty, 4),
        order_type=order_type,
        venue=venue,
    )


def _run_risk_checks(decision: PolicyDecision) -> RiskAssessment:
    """Perform a deterministic risk approval using simple heuristics."""

    time.sleep(random.uniform(0.003, 0.015))
    max_position = 100.0
    if decision.quantity > max_position:
        return RiskAssessment(approved=False, reasons="Quantity exceeds paper limit")

    if decision.order_type == "market" and decision.quantity > 50.0:
        return RiskAssessment(approved=False, reasons="Market order size above throttle")

    return RiskAssessment(approved=True)


def _submit_to_oms(decision: PolicyDecision, risk: RiskAssessment) -> OMSAcknowledgement:
    """Pretend to submit the order to the OMS in paper trading mode."""

    time.sleep(random.uniform(0.002, 0.01))
    if not risk.approved:
        raise PipelineError(risk.reasons or "Risk rejected order")

    return OMSAcknowledgement(status="accepted")


def simulate_pipeline(account_id: str, symbol: str) -> float:
    """Run the full dummy pipeline and return latency in milliseconds."""

    start = time.perf_counter()
    intent = _generate_dummy_intent(account_id, symbol)
    decision = _evaluate_policy(intent)
    risk = _run_risk_checks(decision)
    ack = _submit_to_oms(decision, risk)

    if ack.status != "accepted":
        raise PipelineError(f"Unexpected OMS status: {ack.status}")

    end = time.perf_counter()
    return (end - start) * 1000.0


def _emit_alert(message: str, alertmanager_url: Optional[str]) -> None:
    """Send a notification to Alertmanager if configured."""

    if not alertmanager_url:
        logger.warning("Alertmanager URL not configured; skipping alert: %s", message)
        return

    base_url = alertmanager_url.rstrip("/")
    if base_url.endswith(ALERTMANAGER_ENDPOINT):
        endpoint = base_url
    else:
        endpoint = base_url + ALERTMANAGER_ENDPOINT
    payload = {
        "labels": {
            "alertname": "HealthProbeFailure",
            "severity": "critical",
        },
        "annotations": {
            "summary": "Health probe detected a failure",
            "description": message,
        },
    }

    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.post(endpoint, json=[payload])
            response.raise_for_status()
    except Exception as exc:  # pragma: no cover - network issues
        logger.exception("Failed to push alert to Alertmanager: %s", exc)


def _extract_timestamp(payload: Mapping[str, Any]) -> Optional[float]:
    """Extract an epoch timestamp from a generic payload."""

    candidates = (
        "last_book_update",
        "latest_book_ts",
        "latest_book_timestamp",
        "timestamp",
        "ts",
        "last_update",
    )
    for key in candidates:
        if key not in payload:
            continue
        value = payload[key]
        if value is None:
            continue
        if isinstance(value, (int, float)):
            if value > 1_000_000_000_000:  # assume milliseconds
                return float(value) / 1000.0
            return float(value)
        if isinstance(value, str):
            try:
                numeric = float(value)
            except ValueError:
                cleaned = value.replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(cleaned)
                except ValueError:
                    continue
                return dt.timestamp()
            else:
                if numeric > 1_000_000_000_000:
                    return numeric / 1000.0
                return numeric
    return None


def _fetch_latest_ws_book_timestamp(url: str) -> float:
    """Query the websocket health endpoint for the last book timestamp."""

    with httpx.Client(timeout=5.0) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()

    if isinstance(payload, Mapping):
        timestamp = _extract_timestamp(payload)
        if timestamp is None:
            raise ValueError("Response missing recognised timestamp fields")
        return timestamp

    raise ValueError("Unexpected payload structure from websocket health endpoint")


def _assess_ws_book_staleness(
    url: Optional[str], threshold_seconds: float
) -> tuple[float, float]:
    """Return the timestamp and lag (ms) for the latest websocket book update."""

    if not url:
        message = "Websocket book health URL not configured"
        WS_BOOK_STALENESS_GAUGE.set(math.nan)
        TRADING_BLOCKER.block(message)
        raise MarketDataUnavailableError(message)

    try:
        timestamp = _fetch_latest_ws_book_timestamp(url)
    except Exception as exc:
        message = f"Failed to fetch websocket book timestamp: {exc}"
        WS_BOOK_STALENESS_GAUGE.set(math.nan)
        TRADING_BLOCKER.block(message)
        raise MarketDataUnavailableError(message) from exc

    now = time.time()
    lag_ms = max(0.0, (now - timestamp) * 1000.0)
    WS_BOOK_STALENESS_GAUGE.set(lag_ms)

    if lag_ms > threshold_seconds * 1000.0:
        message = (
            f"Websocket book data stale: lag {lag_ms / 1000.0:.3f}s exceeds "
            f"threshold {threshold_seconds:.3f}s"
        )
        TRADING_BLOCKER.block(message)
        raise MarketDataStaleError(message, timestamp=timestamp, lag_ms=lag_ms)

    TRADING_BLOCKER.unblock()
    return timestamp, lag_ms


def _run_once(
    account_id: str,
    symbol: str,
    alertmanager_url: Optional[str],
    ws_book_health_url: Optional[str],
    ws_delay_threshold: float,
) -> float:
    """Execute the pipeline once, record metrics, and handle failures."""

    pipeline_latency_ms = math.nan
    try:
        pipeline_latency_ms = simulate_pipeline(account_id, symbol)
        PIPELINE_LATENCY_GAUGE.set(pipeline_latency_ms)
    except Exception as exc:
        PIPELINE_LATENCY_GAUGE.set(math.nan)
        message = f"Pipeline execution failed: {exc}"
        logger.error(message)
        TRADING_BLOCKER.block(message)
        with _LATENCY_STATE_LOCK:
            last_ws_ts = LATENCY_STATE.last_ws_book_timestamp
            last_ws_lag = LATENCY_STATE.ws_book_lag_ms
        _update_latency_state(math.nan, last_ws_ts, last_ws_lag)
        _emit_alert(message, alertmanager_url)
        raise

    try:
        timestamp, lag_ms = _assess_ws_book_staleness(
            ws_book_health_url, ws_delay_threshold
        )
    except MarketDataError as exc:
        _update_latency_state(pipeline_latency_ms, exc.timestamp, exc.lag_ms)
        logger.error("%s", exc)
        _emit_alert(str(exc), alertmanager_url)
        raise
    except Exception as exc:
        message = f"Unexpected error while evaluating websocket book latency: {exc}"
        logger.exception(message)
        TRADING_BLOCKER.block(message)
        WS_BOOK_STALENESS_GAUGE.set(math.nan)
        _update_latency_state(pipeline_latency_ms, None, math.nan)
        _emit_alert(message, alertmanager_url)
        raise PipelineError(message)

    if pipeline_latency_ms >= LATENCY_BUDGET_MS:
        message = (
            f"Pipeline latency breach: observed {pipeline_latency_ms:.2f}ms >= "
            f"budget {LATENCY_BUDGET_MS:.2f}ms"
        )
        logger.error(message)
        _update_latency_state(pipeline_latency_ms, timestamp, lag_ms)
        _emit_alert(message, alertmanager_url)
        raise PipelineError(message)

    _update_latency_state(pipeline_latency_ms, timestamp, lag_ms)

    blocked, _, _ = TRADING_BLOCKER.snapshot()
    logger.info(
        "Pipeline healthy for account=%s symbol=%s latency=%.2fms ws_lag=%.2fms blocked=%s",
        account_id,
        symbol,
        pipeline_latency_ms,
        lag_ms,
        blocked,
    )
    return pipeline_latency_ms


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dummy health probe for the trading pipeline")
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="Interval in seconds between successive probes (default: %(default)s)",
    )
    parser.add_argument(
        "--account-id",
        default=DEFAULT_ACCOUNT_ID,
        help="Account identifier used for the dummy pipeline",
    )
    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="Symbol used for the dummy pipeline",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="Optional port to expose Prometheus metrics",
    )
    parser.add_argument(
        "--alertmanager-url",
        default=DEFAULT_ALERTMANAGER_URL,
        help="Alertmanager base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=None,
        help="Optional port to expose /health/latency endpoint",
    )
    parser.add_argument(
        "--ws-book-health-url",
        default=DEFAULT_WS_BOOK_HEALTH_URL,
        help="URL returning the latest websocket book timestamp",
    )
    parser.add_argument(
        "--ws-delay-threshold",
        type=float,
        default=WS_BOOK_DELAY_THRESHOLD_SECONDS,
        help="Maximum acceptable websocket book delay in seconds",
    )
    return parser.parse_args(argv)


def _install_signal_handlers() -> None:
    def _handler(signum, _frame):
        logger.info("Received signal %s, shutting down health probe", signum)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    interval = max(1.0, float(args.interval))

    if args.metrics_port is not None:
        start_http_server(args.metrics_port)
        logger.info("Prometheus metrics exposed on port %s", args.metrics_port)

    if args.health_port is not None:
        _start_health_server(args.health_port)

    _install_signal_handlers()

    next_run = time.perf_counter()
    alertmanager_url = args.alertmanager_url
    ws_book_health_url = args.ws_book_health_url
    ws_delay_threshold = max(0.0, float(args.ws_delay_threshold))

    if not ws_book_health_url:
        logger.warning(
            "WS book health URL not configured; websocket latency checks will trigger blocks"
        )

    while True:
        try:
            _run_once(
                args.account_id,
                args.symbol,
                alertmanager_url,
                ws_book_health_url,
                ws_delay_threshold,
            )
        except PipelineError:
            logger.debug("Continuing after pipeline error")
        except SystemExit:
            raise
        except Exception:
            logger.debug("Continuing after unexpected error", exc_info=True)

        next_run += interval
        sleep_for = next_run - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_run = time.perf_counter()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit as exc:
        raise
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.exception("Health probe terminated due to unexpected error: %s", exc)
        sys.exit(1)
