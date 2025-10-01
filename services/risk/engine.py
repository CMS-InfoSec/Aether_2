"""Risk engine responsible for validating trading intents against policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

from compliance_filter import COMPLIANCE_REASON, compliance_filter
from esg_filter import ESG_REASON, esg_filter
from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.schemas import RiskValidationRequest, RiskValidationResponse


@dataclass
class RiskEngine:
    """Encapsulates policy evaluation for trading intents."""

    account_id: str
    timescale: TimescaleAdapter | None = None
    universe_source: RedisFeastAdapter | None = None

    def __post_init__(self) -> None:
        if self.timescale is None:
            self.timescale = TimescaleAdapter(account_id=self.account_id)
        else:
            self.timescale.account_id = self.account_id
        self._config = self.timescale.load_risk_config()
        if self.universe_source is None:
            self.universe_source = RedisFeastAdapter(account_id=self.account_id)

    def validate(self, request: RiskValidationRequest) -> RiskValidationResponse:
        if self.timescale is None:
            raise RuntimeError("Timescale adapter not initialised")

        reasons: List[str] = []
        context = {
            "instrument": request.instrument,
            "gross_notional": request.gross_notional,
            "var_95": request.var_95,
            "spread_bps": request.spread_bps,
            "latency_ms": request.latency_ms,
            "loss_to_date": request.portfolio_state.loss_to_date,
            "fee_to_date": request.portfolio_state.fee_to_date,
        }

        if self._config.get("kill_switch", False):
            reason = "Risk kill switch engaged for account"
            self._record_event("kill_switch_triggered", reason, context)
            return RiskValidationResponse(valid=False, reasons=[reason], fee=request.fee)

        self._validate_compliance(request, reasons, context)
        self._validate_esg(request, reasons, context)
        if reasons:
            self._record_event(
                "risk_rejected",
                "Intent rejected by risk engine",
                {"reasons": list(reasons), **context},
            )
            return RiskValidationResponse(valid=False, reasons=reasons, fee=request.fee)

        self._validate_universe(request, reasons, context)
        self._validate_caps(request, reasons, context)
        self._validate_market_health(request, reasons, context)
        self._validate_diversification(request, reasons, context)

        if reasons:
            self._record_event("risk_rejected", "Intent rejected by risk engine", {"reasons": list(reasons), **context})
            return RiskValidationResponse(valid=False, reasons=reasons, fee=request.fee)

        self.timescale.record_daily_usage(request.projected_loss, request.projected_fee)
        self.timescale.record_instrument_exposure(request.instrument, abs(request.gross_notional))
        return RiskValidationResponse(valid=True, reasons=[], fee=request.fee)

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------
    def _validate_compliance(
        self,
        request: RiskValidationRequest,
        reasons: List[str],
        context: Dict[str, Any],
    ) -> None:
        allowed, entry = compliance_filter.evaluate(request.instrument)
        if allowed:
            return

        note = entry.reason if entry is not None else None
        message = f"Instrument {request.instrument} is restricted by compliance"
        reasons.append(message)

        enriched_context = {**context, "reason": COMPLIANCE_REASON}
        if note:
            enriched_context["compliance_note"] = note
        self._record_event("compliance_rejection", message, enriched_context)
        compliance_filter.log_rejection(self.account_id, request.instrument, entry)

    def _validate_esg(
        self,
        request: RiskValidationRequest,
        reasons: List[str],
        context: Dict[str, Any],
    ) -> None:
        allowed, entry = esg_filter.evaluate(request.instrument)
        if allowed:
            return

        message = f"Instrument {request.instrument} is blocked by ESG policy"
        reasons.append(message)

        enriched_context = {**context, "reason": ESG_REASON}
        if entry is not None:
            enriched_context["esg_flag"] = entry.flag
            enriched_context["esg_score"] = entry.score
            if entry.reason:
                enriched_context["esg_note"] = entry.reason

        self._record_event("esg_rejection", message, enriched_context)
        esg_filter.log_rejection(self.account_id, request.instrument, entry)

    def _validate_universe(
        self,
        request: RiskValidationRequest,
        reasons: List[str],
        context: Dict[str, Any],
    ) -> None:
        approved = set(self._approved_instruments())
        if request.instrument not in approved:
            reason = f"Instrument {request.instrument} is not part of the approved trading universe"
            reasons.append(reason)
            self._record_event("universe_rejection", reason, context)
            return

        if not request.instrument.endswith("-USD"):
            reason = f"Instrument {request.instrument} is not approved for USD settlement"
            reasons.append(reason)
            self._record_event("currency_rejection", reason, context)

    def _validate_caps(
        self,
        request: RiskValidationRequest,
        reasons: List[str],
        context: Dict[str, Any],
    ) -> None:
        loss_cap = float(self._config.get("loss_cap", 0.0))
        projected_loss = request.portfolio_state.loss_to_date + request.projected_loss
        if loss_cap and projected_loss > loss_cap:
            reason = (
                f"Projected daily loss {projected_loss:,.2f} exceeds configured cap {loss_cap:,.2f}"
            )
            reasons.append(reason)
            self._record_event("loss_cap_breach", reason, {**context, "projected_loss": projected_loss})

        fee_cap = float(self._config.get("fee_cap", 0.0))
        projected_fee = request.portfolio_state.fee_to_date + request.projected_fee
        if fee_cap and projected_fee > fee_cap:
            reason = (
                f"Projected daily fee usage {projected_fee:,.2f} exceeds configured cap {fee_cap:,.2f}"
            )
            reasons.append(reason)
            self._record_event("fee_cap_breach", reason, {**context, "projected_fee": projected_fee})

        nav = max(float(request.portfolio_state.nav), 1.0)
        max_nav_percent = float(self._config.get("max_nav_percent", 1.0))
        exposure_ratio = abs(request.net_exposure) / nav
        if exposure_ratio > max_nav_percent:
            reason = (
                f"Net exposure ratio {exposure_ratio:.2%} exceeds NAV limit {max_nav_percent:.2%}"
            )
            reasons.append(reason)
            self._record_event("nav_limit_breach", reason, {**context, "exposure_ratio": exposure_ratio})

        var_limit = float(self._config.get("var_limit", 0.0))
        if var_limit and request.var_95 > var_limit:
            reason = f"Projected VaR {request.var_95:,.2f} exceeds limit {var_limit:,.2f}"
            reasons.append(reason)
            self._record_event("var_breach", reason, {**context, "var_95": request.var_95})

    def _validate_market_health(
        self,
        request: RiskValidationRequest,
        reasons: List[str],
        context: Dict[str, Any],
    ) -> None:
        spread_limit = self._config.get("spread_limit_bps")
        if spread_limit is not None and request.spread_bps > float(spread_limit):
            reason = (
                f"Observed spread {request.spread_bps:.2f} bps exceeds circuit breaker {float(spread_limit):.2f} bps"
            )
            reasons.append(reason)
            self._record_event("spread_circuit_breaker", reason, context)

        latency_limit = self._config.get("latency_limit_ms")
        if latency_limit is not None and request.latency_ms > float(latency_limit):
            reason = (
                f"Observed latency {request.latency_ms:.2f} ms exceeds circuit breaker {float(latency_limit):.2f} ms"
            )
            reasons.append(reason)
            self._record_event("latency_circuit_breaker", reason, context)

    def _validate_diversification(
        self,
        request: RiskValidationRequest,
        reasons: List[str],
        context: Dict[str, Any],
    ) -> None:
        rules: Dict[str, Any] = self._config.get("diversification_rules", {})
        limit_percent = rules.get("max_single_instrument_percent")
        if limit_percent is None:
            return

        nav = max(float(request.portfolio_state.nav), 1.0)
        current_exposure = float(
            request.portfolio_state.instrument_exposure.get(request.instrument, 0.0)
        )
        projected_exposure = current_exposure + abs(request.gross_notional)
        exposure_ratio = projected_exposure / nav
        if exposure_ratio > float(limit_percent):
            reason = (
                "Projected concentration for {instrument} at {ratio:.2%} exceeds limit {limit:.2%}".format(
                    instrument=request.instrument,
                    ratio=exposure_ratio,
                    limit=float(limit_percent),
                )
            )
            reasons.append(reason)
            self._record_event(
                "diversification_breach",
                reason,
                {**context, "projected_exposure": projected_exposure, "exposure_ratio": exposure_ratio},
            )

    def _approved_instruments(self) -> Sequence[str]:
        if self.universe_source is None:
            return []
        return self.universe_source.approved_instruments()

    def _record_event(self, event_type: str, reason: str, context: Dict[str, Any]) -> None:
        payload = {"reason": reason, **context}
        self.timescale.record_event(event_type, payload)
