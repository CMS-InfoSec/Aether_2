from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

fastapi_spec = importlib.util.find_spec("fastapi")
if fastapi_spec is not None:
    fastapi_module = importlib.import_module("fastapi")
    FastAPI = fastapi_module.FastAPI
    HTTPException = fastapi_module.HTTPException
    Depends = fastapi_module.Depends
else:  # pragma: no cover - FastAPI optional dependency
    FastAPI = None  # type: ignore[assignment]
    Depends = None  # type: ignore[assignment]

    class HTTPException(RuntimeError):  # type: ignore[no-redef]
        def __init__(self, status_code: int, detail: str) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

from pydantic import BaseModel, Field

from auth.service import InMemorySessionStore, RedisSessionStore, SessionStoreProtocol
import services.common.security as security
from services.common.schemas import (
    BookSnapshot,
    ConfidenceMetrics,
    FeeBreakdown,
    PolicyDecisionPayload,
    PolicyDecisionRequest,
    PolicyDecisionResponse,
    PolicyState,
    PortfolioState,
    RiskIntentMetrics,
    RiskIntentPayload,
    RiskValidationRequest,
    RiskValidationResponse,
)
from services.common.security import require_admin_account
from services.policy.main import decide_policy
from services.risk.engine import RiskEngine
from services.oms.shadow_oms import shadow_oms

LOG = logging.getLogger("snapshot_replay")

DEFAULT_OUTPUT_DIR = Path(os.getenv("SNAPSHOT_REPLAY_OUTPUT", "reports/snapshots"))
DEFAULT_STORAGE_PATH = Path(os.getenv("SNAPSHOT_REPLAY_STORAGE", "reports/replay_inputs"))


@dataclass(slots=True)
class SnapshotReplayConfig:
    """Configuration for a snapshot replay run."""

    start: datetime
    end: datetime
    account_id: str
    output_dir: Path = field(default_factory=lambda: DEFAULT_OUTPUT_DIR)
    storage_path: Path = field(default_factory=lambda: DEFAULT_STORAGE_PATH)

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            self.start = self.start.replace(tzinfo=timezone.utc)
        else:
            self.start = self.start.astimezone(timezone.utc)

        if self.end.tzinfo is None:
            self.end = self.end.replace(tzinfo=timezone.utc)
        else:
            self.end = self.end.astimezone(timezone.utc)

        if self.end <= self.start:
            raise ValueError("End timestamp must be after start timestamp")

        self.account_id = self.account_id.strip().lower()
        self.output_dir = self.output_dir.expanduser().resolve()
        self.storage_path = self.storage_path.expanduser().resolve()


@dataclass(slots=True)
class ReplayEvent:
    """Historical trading intent captured for replay."""

    timestamp: datetime
    order_id: str
    instrument: str
    side: str
    quantity: float
    price: float
    fee: FeeBreakdown
    features: Sequence[float]
    book_snapshot: BookSnapshot
    state: PolicyState
    confidence: ConfidenceMetrics | None
    expected_edge_bps: Optional[float]
    actual_mid_price: float
    actual_volume: float

    def to_policy_request(self, account_id: str) -> PolicyDecisionRequest:
        return PolicyDecisionRequest(
            account_id=account_id,
            order_id=self.order_id,
            instrument=self.instrument,
            side=self.side,
            quantity=self.quantity,
            price=self.price,
            fee=self.fee,
            features=list(self.features),
            book_snapshot=self.book_snapshot,
            state=self.state,
            confidence=self.confidence,
            expected_edge_bps=self.expected_edge_bps,
        )


@dataclass(slots=True)
class ReplayDecision:
    """Container for the deterministic replay outputs."""

    event: ReplayEvent
    policy: PolicyDecisionResponse
    risk: RiskValidationResponse
    risk_result: bool
    risk_reasons: List[str]
    shadow_fills: List[Dict[str, Any]]
    market_move_bps: float
    favorable: bool
    divergence: Dict[str, Any]


@dataclass(slots=True)
class ReplayResult:
    run_id: str
    config: SnapshotReplayConfig
    generated_at: datetime
    decisions: List[ReplayDecision]
    summary: Dict[str, Any]
    json_report: Path
    html_report: Path


class HistoricalDataLoader:
    """Load historical events/market context from TimescaleDB or object storage."""

    def __init__(self, config: SnapshotReplayConfig) -> None:
        self._config = config

    def load_events(self) -> List[ReplayEvent]:
        events = list(self._load_from_object_storage())
        if events:
            LOG.info("Loaded %d replay events from object storage", len(events))
            return events

        LOG.warning("Falling back to synthetic events for snapshot replay")
        return self._generate_synthetic_events()

    # ------------------------------------------------------------------
    # Object storage loader
    # ------------------------------------------------------------------
    def _load_from_object_storage(self) -> Iterable[ReplayEvent]:
        path = self._config.storage_path
        if path.is_file():
            files = [path]
        elif path.is_dir():
            files = sorted(p for p in path.glob("*.json")) + sorted(path.glob("*.jsonl"))
        else:
            return []

        for file_path in files:
            try:
                yield from self._parse_file(file_path)
            except Exception as exc:  # pragma: no cover - defensive logging
                LOG.warning("Failed to parse replay input %s: %s", file_path, exc)

    def _parse_file(self, file_path: Path) -> Iterable[ReplayEvent]:
        with file_path.open("r", encoding="utf-8") as handle:
            if file_path.suffix == ".jsonl":
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    payload = json.loads(line)
                    event = self._event_from_payload(payload)
                    if event is not None:
                        yield event
            else:
                payload = json.load(handle)
                if isinstance(payload, dict):
                    payload = [payload]
                if not isinstance(payload, list):
                    raise ValueError("Replay input must be a JSON object or array")
                for entry in payload:
                    event = self._event_from_payload(entry)
                    if event is not None:
                        yield event

    def _event_from_payload(self, payload: Dict[str, Any]) -> ReplayEvent | None:
        try:
            timestamp = _coerce_datetime(payload.get("timestamp"))
            if not (self._config.start <= timestamp <= self._config.end):
                return None
            fee_payload = payload.get("fee") or {"currency": "USD", "maker": 0.1, "taker": 0.2}
            fee = FeeBreakdown(**fee_payload)
            book_snapshot_payload = payload.get("book_snapshot") or {
                "mid_price": payload.get("price", 0.0),
                "spread_bps": 5.0,
                "imbalance": 0.0,
            }
            book_snapshot = BookSnapshot(**book_snapshot_payload)
            state_payload = payload.get("state") or {
                "regime": "neutral",
                "volatility": 0.25,
                "liquidity_score": 0.5,
                "conviction": 0.5,
            }
            state = PolicyState(**state_payload)
            confidence_payload = payload.get("confidence")
            confidence = ConfidenceMetrics(**confidence_payload) if confidence_payload else None
            raw_features = payload.get("features") or []
            if isinstance(raw_features, dict):
                features = list(raw_features.values())
            elif isinstance(raw_features, Sequence) and not isinstance(raw_features, (str, bytes)):
                features = list(raw_features)
            else:
                raise TypeError("features must be a sequence or mapping")
            actual_payload = payload.get("actual") or {}
            actual_mid_price = float(actual_payload.get("mid_price", book_snapshot.mid_price))
            actual_volume = float(actual_payload.get("volume", 0.0))
            expected_edge_bps = payload.get("expected_edge_bps")
            if expected_edge_bps is not None:
                expected_edge_bps = float(expected_edge_bps)

            return ReplayEvent(
                timestamp=timestamp,
                order_id=str(payload.get("order_id") or uuid.uuid4()),
                instrument=str(payload.get("instrument") or "BTC-USD"),
                side=str(payload.get("side") or "BUY").upper(),
                quantity=float(payload.get("quantity") or 0.1),
                price=float(payload.get("price") or book_snapshot.mid_price),
                fee=fee,
                features=list(features),
                book_snapshot=book_snapshot,
                state=state,
                confidence=confidence,
                expected_edge_bps=expected_edge_bps,
                actual_mid_price=actual_mid_price,
                actual_volume=actual_volume,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            LOG.error("Invalid replay payload: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Synthetic data
    # ------------------------------------------------------------------
    def _generate_synthetic_events(self) -> List[ReplayEvent]:
        base_time = self._config.start
        duration = self._config.end - self._config.start
        steps = max(int(duration / timedelta(minutes=5)), 1)
        events: List[ReplayEvent] = []
        for index in range(steps):
            timestamp = base_time + timedelta(minutes=5 * index)
            mid_price = 30_000.0 + 100.0 * index
            book_snapshot = BookSnapshot(mid_price=mid_price, spread_bps=4.0, imbalance=0.1)
            state = PolicyState(
                regime="trend" if index % 2 == 0 else "range",
                volatility=0.3,
                liquidity_score=0.7,
                conviction=0.6,
            )
            fee = FeeBreakdown(currency="USD", maker=0.12, taker=0.24)
            features = [float(index) * 0.1 + 1.0, 0.5, -0.2]
            events.append(
                ReplayEvent(
                    timestamp=timestamp,
                    order_id=f"SYN-{index:04d}",
                    instrument="BTC-USD",
                    side="BUY" if index % 2 == 0 else "SELL",
                    quantity=0.25,
                    price=mid_price,
                    fee=fee,
                    features=features,
                    book_snapshot=book_snapshot,
                    state=state,
                    confidence=None,
                    expected_edge_bps=None,
                    actual_mid_price=mid_price * (1.0 + (0.001 if index % 2 == 0 else -0.001)),
                    actual_volume=10.0 + index,
                )
            )
        return events


class SnapshotReplayer:
    """Replay the policy -> risk -> shadow OMS pipeline deterministically."""

    def __init__(self, config: SnapshotReplayConfig, loader: Optional[HistoricalDataLoader] = None) -> None:
        self.config = config
        self.loader = loader or HistoricalDataLoader(config)

    def run(self) -> ReplayResult:
        events = self.loader.load_events()
        if not events:
            raise RuntimeError("No historical events available for replay")

        decisions: List[ReplayDecision] = []
        for event in events:
            decision = self._replay_event(event)
            decisions.append(decision)

        summary = self._build_summary(decisions)
        run_id = uuid.uuid4().hex
        generated_at = datetime.now(timezone.utc)
        json_report, html_report = self._persist_reports(run_id, generated_at, decisions, summary)

        return ReplayResult(
            run_id=run_id,
            config=self.config,
            generated_at=generated_at,
            decisions=decisions,
            summary=summary,
            json_report=json_report,
            html_report=html_report,
        )

    # ------------------------------------------------------------------
    # Replay helpers
    # ------------------------------------------------------------------
    def _replay_event(self, event: ReplayEvent) -> ReplayDecision:
        policy_request = event.to_policy_request(self.config.account_id)
        policy_response = decide_policy(policy_request, account_id=self.config.account_id)

        risk_response = self._run_risk(policy_request, policy_response, event)
        risk_valid = risk_response.valid
        risk_reasons = list(risk_response.reasons)
        shadow_fills = self._run_shadow(event, policy_request, policy_response)
        market_move_bps, favorable = self._evaluate_market_outcome(event, policy_response)
        divergence = self._compute_divergence(event, policy_response, risk_valid, risk_reasons)

        return ReplayDecision(
            event=event,
            policy=policy_response,
            risk=risk_response,
            risk_result=risk_valid,
            risk_reasons=risk_reasons,
            shadow_fills=shadow_fills,
            market_move_bps=market_move_bps,
            favorable=favorable,
            divergence=divergence,
        )

    def _run_risk(
        self,
        request: PolicyDecisionRequest,
        response: PolicyDecisionResponse,
        event: ReplayEvent,
    ) -> RiskValidationResponse:
        metrics = self._build_risk_metrics(event, response)
        intent_payload = RiskIntentPayload(
            policy_decision=PolicyDecisionPayload(request=request, response=response),
            metrics=metrics,
        )
        portfolio_state = PortfolioState(
            nav=1_000_000.0,
            loss_to_date=10_000.0,
            fee_to_date=2_500.0,
            instrument_exposure={event.instrument: 100_000.0},
            metadata={"source": "snapshot"},
        )
        risk_request = RiskValidationRequest(
            account_id=self.config.account_id,
            intent=intent_payload,
            portfolio_state=portfolio_state,
        )
        engine = RiskEngine(account_id=self.config.account_id)
        return engine.validate(risk_request)

    def _build_risk_metrics(
        self, event: ReplayEvent, response: PolicyDecisionResponse
    ) -> RiskIntentMetrics:
        direction = 1.0 if event.side.upper() == "BUY" else -1.0
        gross_notional = abs(event.price * event.quantity)
        net_exposure = direction * gross_notional
        projected_loss = gross_notional * 0.01
        projected_fee = gross_notional * (response.effective_fee.maker / 10_000.0)
        var_95 = gross_notional * 0.05
        latency_ms = 75.0
        spread_bps = event.book_snapshot.spread_bps
        return RiskIntentMetrics(
            net_exposure=net_exposure,
            gross_notional=gross_notional,
            projected_loss=projected_loss,
            projected_fee=projected_fee,
            var_95=var_95,
            spread_bps=spread_bps,
            latency_ms=latency_ms,
        )

    def _run_shadow(
        self, event: ReplayEvent, request: PolicyDecisionRequest, response: PolicyDecisionResponse
    ) -> List[Dict[str, Any]]:
        if not response.approved or response.selected_action == "abstain":
            return []
        fills = shadow_oms.generate_shadow_fills(
            account_id=request.account_id,
            symbol=request.instrument,
            side=request.side,
            quantity=request.quantity,
            price=request.price,
            timestamp=event.timestamp,
        )
        return fills

    def _evaluate_market_outcome(
        self, event: ReplayEvent, response: PolicyDecisionResponse
    ) -> tuple[float, bool]:
        price = event.price
        outcome_price = event.actual_mid_price
        direction = 1.0 if event.side.upper() == "BUY" else -1.0
        move = ((outcome_price - price) / price) * 10_000.0 * direction
        favorable = move >= 0
        return move, favorable

    def _compute_divergence(
        self,
        event: ReplayEvent,
        response: PolicyDecisionResponse,
        risk_valid: Optional[bool],
        risk_reasons: Sequence[str],
    ) -> Dict[str, Any]:
        divergence: Dict[str, Any] = {
            "policy_edge_delta": None,
            "risk_mismatch": None,
        }
        live_decision = getattr(event, "live_decision", None)
        if isinstance(live_decision, dict):
            live_edge = float(live_decision.get("fee_adjusted_edge_bps", 0.0))
            divergence["policy_edge_delta"] = round(
                response.fee_adjusted_edge_bps - live_edge, 4
            )
            divergence["risk_mismatch"] = bool(live_decision.get("risk_valid", True) != bool(risk_valid))
        else:
            divergence["risk_mismatch"] = bool(risk_reasons)
        return divergence

    # ------------------------------------------------------------------
    # Reporting helpers
    # ------------------------------------------------------------------
    def _build_summary(self, decisions: Sequence[ReplayDecision]) -> Dict[str, Any]:
        total = len(decisions)
        approvals = sum(1 for item in decisions if item.policy.approved)
        favorable = sum(1 for item in decisions if item.favorable)
        average_move = sum(item.market_move_bps for item in decisions) / float(total)
        return {
            "events": total,
            "approved": approvals,
            "approval_rate": approvals / float(total),
            "favorable_moves": favorable,
            "favorable_rate": favorable / float(total),
            "avg_move_bps": average_move,
        }

    def _persist_reports(
        self,
        run_id: str,
        generated_at: datetime,
        decisions: Sequence[ReplayDecision],
        summary: Dict[str, Any],
    ) -> tuple[Path, Path]:
        output_dir = self.config.output_dir / run_id
        output_dir.mkdir(parents=True, exist_ok=True)

        json_path = output_dir / "report.json"
        html_path = output_dir / "report.html"

        payload = {
            "run_id": run_id,
            "generated_at": generated_at.isoformat(),
            "account_id": self.config.account_id,
            "start": self.config.start.isoformat(),
            "end": self.config.end.isoformat(),
            "summary": summary,
            "decisions": [self._serialize_decision(entry) for entry in decisions],
        }
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        html_path.write_text(self._render_html(payload), encoding="utf-8")
        return json_path, html_path

    def _serialize_decision(self, decision: ReplayDecision) -> Dict[str, Any]:
        return {
            "timestamp": decision.event.timestamp.isoformat(),
            "order_id": decision.event.order_id,
            "instrument": decision.event.instrument,
            "side": decision.event.side,
            "quantity": decision.event.quantity,
            "price": decision.event.price,
            "policy": decision.policy.model_dump(),
            "risk_valid": decision.risk_result,
            "risk_reasons": list(decision.risk_reasons),
            "shadow_fills": list(decision.shadow_fills),
            "market_move_bps": decision.market_move_bps,
            "favorable": decision.favorable,
            "divergence": decision.divergence,
        }

    def _render_html(self, payload: Dict[str, Any]) -> str:
        rows = []
        for entry in payload["decisions"]:
            rows.append(
                "<tr>"
                f"<td>{entry['timestamp']}</td>"
                f"<td>{entry['order_id']}</td>"
                f"<td>{entry['instrument']}</td>"
                f"<td>{entry['side']}</td>"
                f"<td>{entry['quantity']:.6f}</td>"
                f"<td>{entry['price']:.2f}</td>"
                f"<td>{entry['policy']['fee_adjusted_edge_bps']:.2f}</td>"
                f"<td>{entry['market_move_bps']:.2f}</td>"
                f"<td>{'✅' if entry['favorable'] else '❌'}</td>"
                f"<td>{entry['divergence'].get('policy_edge_delta')}</td>"
                f"<td>{', '.join(entry['risk_reasons']) if entry['risk_reasons'] else '—'}</td>"
                "</tr>"
            )
        rows_html = "\n".join(rows)
        summary = payload["summary"]
        return f"""
<!DOCTYPE html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <title>Snapshot Replay {payload['run_id']}</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 2rem; }}
      table {{ border-collapse: collapse; width: 100%; }}
      th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: right; }}
      th {{ background-color: #f5f5f5; }}
      td:first-child, th:first-child {{ text-align: left; }}
    </style>
  </head>
  <body>
    <h1>Snapshot Replay Report</h1>
    <section>
      <h2>Metadata</h2>
      <p><strong>Run ID:</strong> {payload['run_id']}</p>
      <p><strong>Account:</strong> {payload['account_id']}</p>
      <p><strong>Window:</strong> {payload['start']} → {payload['end']}</p>
      <p><strong>Generated:</strong> {payload['generated_at']}</p>
    </section>
    <section>
      <h2>Summary</h2>
      <ul>
        <li>Events: {summary['events']}</li>
        <li>Approved: {summary['approved']} ({summary['approval_rate']:.2%})</li>
        <li>Favorable moves: {summary['favorable_moves']} ({summary['favorable_rate']:.2%})</li>
        <li>Average move (bps): {summary['avg_move_bps']:.2f}</li>
      </ul>
    </section>
    <section>
      <h2>Decisions</h2>
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Order ID</th>
            <th>Instrument</th>
            <th>Side</th>
            <th>Qty</th>
            <th>Price</th>
            <th>Edge (bps)</th>
            <th>Move (bps)</th>
            <th>Outcome</th>
            <th>Edge Δ</th>
            <th>Risk Reasons</th>
          </tr>
        </thead>
        <tbody>
          {rows_html}
        </tbody>
      </table>
    </section>
  </body>
</html>
"""


# ----------------------------------------------------------------------
# Utility helpers
# ----------------------------------------------------------------------

def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    else:
        raise TypeError(f"Unsupported datetime value: {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _parse_datetime(value: str) -> datetime:
    try:
        return _coerce_datetime(value)
    except Exception as exc:  # pragma: no cover - argparse error path
        raise argparse.ArgumentTypeError(str(exc)) from exc


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay historical policy/risk decisions deterministically")
    parser.add_argument("--from", dest="start", type=_parse_datetime, required=True, help="ISO8601 start timestamp (inclusive)")
    parser.add_argument("--to", dest="end", type=_parse_datetime, required=True, help="ISO8601 end timestamp (exclusive)")
    parser.add_argument("--account", dest="account_id", required=True, help="Account identifier to replay")
    parser.add_argument("--output", dest="output_dir", default=None, help="Output directory for reports")
    parser.add_argument("--storage", dest="storage_path", default=None, help="Optional storage path containing replay inputs")
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> SnapshotReplayConfig:
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_DIR
    storage_path = Path(args.storage_path) if args.storage_path else DEFAULT_STORAGE_PATH
    return SnapshotReplayConfig(start=args.start, end=args.end, account_id=args.account_id, output_dir=output_dir, storage_path=storage_path)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)
    config = build_config(args)
    runner = SnapshotReplayer(config)
    result = runner.run()
    LOG.info("Replay %s completed. JSON report: %s", result.run_id, result.json_report)
    return 0


# ----------------------------------------------------------------------
# FastAPI endpoint (optional)
# ----------------------------------------------------------------------

if FastAPI is not None:  # pragma: no branch - simple module guard
    app = FastAPI(title="Snapshot Replay")
    _SESSION_DSN_ENV_VARS = ("SESSION_REDIS_URL", "SESSION_STORE_URL", "SESSION_BACKEND_DSN")

    def _resolve_session_store_dsn() -> str:
        for env_var in _SESSION_DSN_ENV_VARS:
            value = os.getenv(env_var)
            if value:
                return value
        joined = ", ".join(_SESSION_DSN_ENV_VARS)
        raise RuntimeError(
            "Session store misconfigured: configure one of "
            f"{joined} so the snapshot replay service can authenticate callers.",
        )

    def _configure_session_store(application: FastAPI) -> SessionStoreProtocol:
        existing = getattr(application.state, "session_store", None)
        if isinstance(existing, SessionStoreProtocol):
            store = existing
        else:
            dsn = _resolve_session_store_dsn()
            ttl_minutes = int(os.getenv("SESSION_TTL_MINUTES", "60"))
            if dsn.startswith("memory://"):
                store = InMemorySessionStore(ttl_minutes=ttl_minutes)
            else:
                try:  # pragma: no cover - optional dependency for production deployments
                    import redis  # type: ignore[import-not-found]
                except ImportError as exc:  # pragma: no cover - surfaced when redis missing locally
                    raise RuntimeError(
                        "redis package is required when configuring the snapshot replay session store via SESSION_REDIS_URL.",
                    ) from exc

                client = redis.Redis.from_url(dsn)
                store = RedisSessionStore(client, ttl_minutes=ttl_minutes)

            application.state.session_store = store

        security.set_default_session_store(store)
        return store

    SESSION_STORE = _configure_session_store(app)

    class ReplayRunRequest(BaseModel):
        start: datetime = Field(..., alias="from")
        end: datetime = Field(..., alias="to")
        account_id: str
        output_dir: Optional[Path] = None
        storage_path: Optional[Path] = None

    class ReplayRunResponse(BaseModel):
        run_id: str
        report_json: str
        report_html: str

    @app.post("/replay/run", response_model=ReplayRunResponse)
    def trigger_replay(
        request: ReplayRunRequest,
        actor_account: str = Depends(require_admin_account),
    ) -> ReplayRunResponse:
        request_account = request.account_id.strip()
        if request_account.lower() != actor_account.strip().lower():
            raise HTTPException(
                status_code=403,
                detail="Account mismatch between authenticated session and replay request.",
            )

        config = SnapshotReplayConfig(
            start=request.start,
            end=request.end,
            account_id=request_account,
            output_dir=request.output_dir or DEFAULT_OUTPUT_DIR,
            storage_path=request.storage_path or DEFAULT_STORAGE_PATH,
        )
        runner = SnapshotReplayer(config)
        try:
            result = runner.run()
        except Exception as exc:  # pragma: no cover - FastAPI error path
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return ReplayRunResponse(
            run_id=result.run_id,
            report_json=str(result.json_report),
            report_html=str(result.html_report),
        )
else:
    app = None  # type: ignore[assignment]


if __name__ == "__main__":  # pragma: no cover - manual execution
    raise SystemExit(main())

