"""Unit tests ensuring the watchdog filters non-spot instruments."""

from datetime import datetime, timezone

from watchdog import WatchdogCoordinator


class _StubDetector:
    def evaluate(self, intent, decision):  # pragma: no cover - not exercised
        return None


class _StubRepository:
    def record(self, veto):  # pragma: no cover - not exercised
        return True


def _coordinator() -> WatchdogCoordinator:
    return WatchdogCoordinator(detector=_StubDetector(), repository=_StubRepository())


def test_intent_event_rejects_non_spot_symbol() -> None:
    coordinator = _coordinator()
    payload = {
        "intent_id": "abc123",
        "symbol": "BTC-PERP",
        "qty": "1",
    }

    envelope = coordinator._parse_intent_event(  # type: ignore[attr-defined]
        "company",
        payload,
        datetime.now(timezone.utc),
    )

    assert envelope is None


def test_intent_event_normalizes_spot_symbol() -> None:
    coordinator = _coordinator()
    payload = {
        "intent_id": "abc123",
        "symbol": "eth/usd",
        "qty": "1",
    }

    envelope = coordinator._parse_intent_event(  # type: ignore[attr-defined]
        "company",
        payload,
        datetime.now(timezone.utc),
    )

    assert envelope is not None
    assert envelope.symbol == "ETH-USD"


def test_policy_event_rejects_non_spot_instrument() -> None:
    coordinator = _coordinator()
    payload = {
        "order_id": "abc123",
        "instrument": "ETH-PERP",
        "approved": True,
    }

    envelope = coordinator._parse_policy_event(  # type: ignore[attr-defined]
        "company",
        payload,
        datetime.now(timezone.utc),
    )

    assert envelope is None


def test_policy_event_normalizes_spot_instrument() -> None:
    coordinator = _coordinator()
    payload = {
        "order_id": "abc123",
        "instrument": "btc/usd",
        "approved": False,
        "confidence": {"overall_confidence": 0.5},
    }

    envelope = coordinator._parse_policy_event(  # type: ignore[attr-defined]
        "company",
        payload,
        datetime.now(timezone.utc),
    )

    assert envelope is not None
    assert envelope.instrument == "BTC-USD"
