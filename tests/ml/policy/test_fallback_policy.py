import pytest

from ml.policy.fallback_policy import FallbackPolicy
from services.common.schemas import PolicyDecisionRequest
from tests.factories import policy_decision_request


def test_fallback_policy_filters_non_spot_symbols() -> None:
    policy = FallbackPolicy(top_symbols=["btc-usd", "ETH-PERP", "eth-usd"])

    assert policy.top_symbols == ("BTC-USD", "ETH-USD")


def test_fallback_policy_requires_spot_symbols() -> None:
    with pytest.raises(ValueError, match="requires at least one top liquidity spot symbol"):
        FallbackPolicy(top_symbols=["btc-perp", "ethdown-usd"])


def test_fallback_policy_rejects_non_spot_request() -> None:
    policy = FallbackPolicy(top_symbols=["BTC-USD"])
    base_request = policy_decision_request()

    non_spot_request = PolicyDecisionRequest.model_construct(
        account_id=base_request.account_id,
        order_id=base_request.order_id,
        instrument="BTC-PERP",
        side=base_request.side,
        quantity=base_request.quantity,
        price=base_request.price,
        fee=base_request.fee,
        features=list(base_request.features),
        book_snapshot=base_request.book_snapshot,
        state=base_request.state,
        expected_edge_bps=base_request.expected_edge_bps,
        slippage_bps=base_request.slippage_bps,
        take_profit_bps=base_request.take_profit_bps,
        stop_loss_bps=base_request.stop_loss_bps,
        confidence=base_request.confidence,
    )

    decision = policy.evaluate(
        request=non_spot_request,
        book_snapshot=base_request.book_snapshot,
        reason="fallback engaged",
    )

    assert decision.response.approved is False
    assert decision.response.selected_action == "abstain"
    assert decision.response.reason == "Instrument is not a supported spot market pair"
