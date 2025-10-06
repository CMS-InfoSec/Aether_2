from types import SimpleNamespace

from services.risk.pretrade_sanity import (
    OrderContext,
    PretradeSanityChecker,
)


def _make_request(symbol: str = "BTC-USD", *, gross_notional: float = 10_000.0):
    policy_request = SimpleNamespace(instrument=symbol, side="BUY")
    policy_decision = SimpleNamespace(request=policy_request, response=None)
    metrics = SimpleNamespace(gross_notional=gross_notional)
    intent = SimpleNamespace(
        policy_decision=policy_decision,
        metrics=metrics,
        book_snapshot=None,
    )
    return SimpleNamespace(
        account_id="company",
        instrument=symbol,
        gross_notional=gross_notional,
        spread_bps=None,
        intent=intent,
    )


def test_pretrade_check_rejects_non_spot_symbol():
    checker = PretradeSanityChecker()
    context = OrderContext(
        account_id="company",
        symbol="BTC-PERP",
        side="BUY",
        notional=5_000.0,
    )

    decision = checker.check(context)

    assert decision.permitted is False
    assert decision.action == "reject"
    assert any("spot trading" in reason for reason in decision.reasons)

    snapshot = checker.status("company")
    assert snapshot.counts.get(PretradeSanityChecker.SPOT_ELIGIBILITY, 0) == 1
    assert snapshot.recent_failures[0].symbol == "BTC-PERP"


def test_pretrade_evaluate_rejects_non_spot_validation_request():
    checker = PretradeSanityChecker()
    request = _make_request("ETH-PERP", gross_notional=25_000.0)

    decision = checker.evaluate_validation_request(request)

    assert decision.permitted is False
    assert decision.action == "reject"
    assert any("spot trading" in reason for reason in decision.reasons)

    snapshot = checker.status("company")
    assert snapshot.counts.get(PretradeSanityChecker.SPOT_ELIGIBILITY, 0) == 1
    assert snapshot.recent_failures[0].symbol == "ETH-PERP"


def test_pretrade_normalizes_spot_symbols():
    checker = PretradeSanityChecker()
    context = OrderContext(
        account_id="company",
        symbol="eth/usd",
        side="SELL",
        notional=1_500.0,
    )

    decision = checker.check(context)

    assert decision.permitted is True
    assert decision.action == "proceed"
    status = checker.status("company")
    assert PretradeSanityChecker.SPOT_ELIGIBILITY not in status.counts
