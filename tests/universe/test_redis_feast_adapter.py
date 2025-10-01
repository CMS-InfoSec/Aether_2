from __future__ import annotations

from services.common.adapters import RedisFeastAdapter


class StubUniverseRepository:
    def __init__(self) -> None:
        self.approved_calls = 0
        self.fee_override_calls: list[str] = []
        self._approved = ["BTC-USD"]
        self._fees = {"BTC-USD": {"currency": "USD", "maker": 0.1, "taker": 0.2}}

    def approved_universe(self) -> list[str]:
        self.approved_calls += 1
        return list(self._approved)

    def fee_override(self, instrument: str) -> dict[str, float | str] | None:
        self.fee_override_calls.append(instrument)
        if instrument in self._fees:
            return dict(self._fees[instrument])
        return None


def test_adapter_delegates_to_injected_repository() -> None:
    repository = StubUniverseRepository()
    adapter = RedisFeastAdapter(account_id="company", repository=repository)

    instruments = adapter.approved_instruments()
    assert instruments == ["BTC-USD"]
    assert repository.approved_calls == 1

    fee = adapter.fee_override("BTC-USD")
    assert fee == {"currency": "USD", "maker": 0.1, "taker": 0.2}
    assert repository.fee_override_calls == ["BTC-USD"]

    # Ensure delegation uses the shared repository for subsequent calls as well.
    adapter.approved_instruments()
    assert repository.approved_calls == 2
    adapter.fee_override("ETH-USD")
    assert repository.fee_override_calls == ["BTC-USD", "ETH-USD"]
