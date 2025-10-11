"""Risk engine responsible for validating trading intents against policy."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from compliance_filter import COMPLIANCE_REASON, compliance_filter
from esg_filter import ESG_REASON, esg_filter
from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.schemas import RiskValidationRequest, RiskValidationResponse
from shared.exits import compute_exit_targets
from services.risk.circuit_breakers import CircuitBreakerDecision, get_circuit_breaker
from shared.spot import normalize_spot_symbol


LOGGER = logging.getLogger(__name__)


@dataclass
class RiskEngine:
    """Encapsulates policy evaluation for trading intents."""

    account_id: str
    timescale: TimescaleAdapter | None = None
    universe_source: RedisFeastAdapter | None = None
    balance_loader: Callable[[str], Tuple[Mapping[str, float], Optional[float]]] | None = None

    def __post_init__(self) -> None:
        if self.timescale is None:
            self.timescale = TimescaleAdapter(account_id=self.account_id)
        else:
            self.timescale.account_id = self.account_id
        self._config = self.timescale.load_risk_config()
        if self.universe_source is None:
            self.universe_source = RedisFeastAdapter(account_id=self.account_id)
        self._circuit_breaker = get_circuit_breaker(
            self.account_id, timescale=self.timescale
        )
        if self.balance_loader is None:
            self.balance_loader = self._default_balance_loader

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

        self._apply_live_balances(request, context)

        decision_payload = request.intent.policy_decision
        decision_request = decision_payload.request
        decision_response = decision_payload.response
        take_profit_bps = None
        stop_loss_bps = None
        if decision_response is not None:
            take_profit_bps = decision_response.take_profit_bps
            stop_loss_bps = decision_response.stop_loss_bps
        else:
            take_profit_bps = decision_request.take_profit_bps
            stop_loss_bps = decision_request.stop_loss_bps
        exit_take_profit, exit_stop_loss = compute_exit_targets(
            price=decision_request.price,
            side=decision_request.side,
            take_profit_bps=take_profit_bps,
            stop_loss_bps=stop_loss_bps,
        )

        adjusted_stop_loss = self._apply_stop_loss_cash_guard(
            side=decision_request.side,
            price=decision_request.price,
            quantity=decision_request.quantity,
            instrument=decision_request.instrument,
            stop_loss=exit_stop_loss,
            available_cash=request.portfolio_state.available_cash,
        )
        if adjusted_stop_loss != exit_stop_loss:
            context["stop_loss_adjusted_for_cash"] = True
            exit_stop_loss = adjusted_stop_loss

        if self._config.get("kill_switch", False):
            reason = "Risk kill switch engaged for account"
            self._record_event("kill_switch_triggered", reason, context)
            return RiskValidationResponse(
                valid=False,
                reasons=[reason],
                fee=request.fee,
                take_profit=exit_take_profit,
                stop_loss=exit_stop_loss,
            )

        self._validate_compliance(request, reasons, context)
        self._validate_esg(request, reasons, context)
        if reasons:
            self._record_event(
                "risk_rejected",
                "Intent rejected by risk engine",
                {"reasons": list(reasons), **context},
            )
            return RiskValidationResponse(
                valid=False,
                reasons=reasons,
                fee=request.fee,
                take_profit=exit_take_profit,
                stop_loss=exit_stop_loss,
            )

        self._validate_universe(request, reasons, context)
        self._validate_caps(request, reasons, context)
        self._validate_market_health(request, reasons, context)
        self._validate_circuit_breakers(request, reasons, context)
        self._validate_diversification(request, reasons, context)

        if reasons:
            self._record_event("risk_rejected", "Intent rejected by risk engine", {"reasons": list(reasons), **context})
            return RiskValidationResponse(
                valid=False,
                reasons=reasons,
                fee=request.fee,
                take_profit=exit_take_profit,
                stop_loss=exit_stop_loss,
            )

        self.timescale.record_daily_usage(request.projected_loss, request.projected_fee)
        self.timescale.record_instrument_exposure(request.instrument, abs(request.gross_notional))
        return RiskValidationResponse(
            valid=True,
            reasons=[],
            fee=request.fee,
            take_profit=exit_take_profit,
            stop_loss=exit_stop_loss,
        )

    # ------------------------------------------------------------------
    # Live balance helpers
    # ------------------------------------------------------------------
    def _default_balance_loader(
        self, account_id: str
    ) -> Tuple[Mapping[str, float], Optional[float]]:
        if self.timescale is None:
            return {}, None
        exposures: Mapping[str, float]
        try:
            exposures = self.timescale.open_positions()
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Failed to load live exposures for %s: %s", account_id, exc)
            exposures = {}

        available_cash: Optional[float] = None
        cash_attr = getattr(self.timescale, "available_cash", None)
        if callable(cash_attr):  # pragma: no cover - exercised via integration tests
            try:
                available_cash = cash_attr()
            except Exception as exc:  # pragma: no cover - defensive guard
                LOGGER.warning("Failed to fetch available cash for %s: %s", account_id, exc)
        elif isinstance(cash_attr, (int, float)):
            available_cash = float(cash_attr)

        return exposures, available_cash

    def _apply_live_balances(
        self,
        request: RiskValidationRequest,
        context: Dict[str, Any],
    ) -> None:
        if self.balance_loader is None:
            return
        try:
            exposures, available_cash = self.balance_loader(self.account_id)
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Balance loader failed for %s: %s", self.account_id, exc)
            exposures, available_cash = {}, None

        normalized_exposures: Dict[str, float] = {}
        for symbol, notional in exposures.items():
            normalized_symbol = normalize_spot_symbol(symbol) or str(symbol)
            try:
                normalized_exposures[normalized_symbol] = abs(float(notional))
            except (TypeError, ValueError):  # pragma: no cover - defensive guard
                continue

        if normalized_exposures:
            request.portfolio_state.instrument_exposure.update(normalized_exposures)
            context["live_exposures"] = dict(normalized_exposures)

        if available_cash is not None:
            try:
                request.portfolio_state.available_cash = max(float(available_cash), 0.0)
            except (TypeError, ValueError):  # pragma: no cover - defensive guard
                request.portfolio_state.available_cash = None
            else:
                context["live_available_cash"] = request.portfolio_state.available_cash

        instrument = request.instrument or request.intent.policy_decision.request.instrument
        normalized_instrument = normalize_spot_symbol(instrument) or str(instrument)
        current_exposure = float(
            abs(
                request.portfolio_state.instrument_exposure.get(normalized_instrument, 0.0)
            )
        )
        gross_notional = abs(
            float(
                request.gross_notional
                if request.gross_notional is not None
                else request.intent.metrics.gross_notional or 0.0
            )
        )
        request.net_exposure = current_exposure + gross_notional
        context["live_instrument_exposure"] = current_exposure

    def _apply_stop_loss_cash_guard(
        self,
        *,
        side: str,
        price: float,
        quantity: float,
        instrument: Optional[str],
        stop_loss: Optional[float],
        available_cash: Optional[float],
    ) -> Optional[float]:
        if stop_loss is None or available_cash is None:
            return stop_loss

        side_normalized = (side or "").upper()
        try:
            price_value = float(price)
            quantity_value = float(quantity)
        except (TypeError, ValueError):  # pragma: no cover - defensive guard
            return stop_loss

        if quantity_value <= 0 or price_value <= 0:
            return stop_loss

        available = max(float(available_cash), 0.0)
        if available <= 0:
            return stop_loss

        if side_normalized == "BUY":
            potential_loss_per_unit = max(price_value - stop_loss, 0.0)
        else:
            potential_loss_per_unit = max(stop_loss - price_value, 0.0)

        potential_loss = potential_loss_per_unit * quantity_value
        if potential_loss <= available or potential_loss_per_unit <= 0:
            return stop_loss

        max_loss_per_unit = available / quantity_value
        if side_normalized == "BUY":
            adjusted_stop = max(price_value - max_loss_per_unit, 0.0)
        else:
            adjusted_stop = price_value + max_loss_per_unit

        LOGGER.info(
            "Adjusted stop loss to respect live cash balance",
            extra={
                "instrument": instrument,
                "original_stop": stop_loss,
                "adjusted_stop": adjusted_stop,
                "available_cash": available,
            },
        )
        return round(adjusted_stop, 8)

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

    def _validate_circuit_breakers(
        self,
        request: RiskValidationRequest,
        reasons: List[str],
        context: Dict[str, Any],
    ) -> None:
        decision: CircuitBreakerDecision | None = self._circuit_breaker.evaluate(request)
        if decision is None:
            return
        message = (
            f"Intent blocked by circuit breaker for {decision.symbol}: {decision.reason}"
        )
        reasons.append(message)
        payload = {
            **context,
            "symbol": decision.symbol,
            "spread_bps": decision.spread_bps,
            "realized_volatility": decision.volatility,
            "triggered_at": decision.triggered_at,
            "safe_mode": decision.safe_mode_engaged,
        }
        self._record_event("intent_circuit_breaker", message, payload)

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
