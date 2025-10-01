"""Operational health probe for the trading decision pipeline."""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional

import httpx
from prometheus_client import Gauge, start_http_server


DEFAULT_ACCOUNT_ID = "ACC-DEFAULT"
DEFAULT_SYMBOL = "AAPL"
DEFAULT_INTERVAL = 300.0
DEFAULT_ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
ALERTMANAGER_ENDPOINT = "/api/v2/alerts"
LATENCY_BUDGET_MS = 500.0


logger = logging.getLogger("health_probe")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


PIPELINE_LATENCY_GAUGE = Gauge(
    "pipeline_latency_ms",
    "Latency in milliseconds for the dummy Intent->Policy->Risk->OMS pipeline.",
)


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


def _run_once(account_id: str, symbol: str, alertmanager_url: Optional[str]) -> float:
    """Execute the pipeline once, record metrics, and handle failures."""

    try:
        latency_ms = simulate_pipeline(account_id, symbol)
        PIPELINE_LATENCY_GAUGE.set(latency_ms)
    except Exception as exc:
        PIPELINE_LATENCY_GAUGE.set(math.nan)
        message = f"Pipeline execution failed: {exc}"
        logger.error(message)
        _emit_alert(message, alertmanager_url)
        raise

    if latency_ms >= LATENCY_BUDGET_MS:
        message = (
            f"Pipeline latency breach: observed {latency_ms:.2f}ms >= "
            f"budget {LATENCY_BUDGET_MS:.2f}ms"
        )
        logger.error(message)
        _emit_alert(message, alertmanager_url)
        raise PipelineError(message)

    logger.info(
        "Pipeline healthy for account=%s symbol=%s latency=%.2fms",
        account_id,
        symbol,
        latency_ms,
    )
    return latency_ms


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

    _install_signal_handlers()

    next_run = time.perf_counter()
    alertmanager_url = args.alertmanager_url

    while True:
        try:
            _run_once(args.account_id, args.symbol, alertmanager_url)
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
