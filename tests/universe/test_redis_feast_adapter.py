from __future__ import annotations

import pytest

from services.common.adapters import RedisFeastAdapter
from services.common.security import ADMIN_ACCOUNTS


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


class EmptyUniverseRepository:
    def approved_universe(self) -> list[str]:
        return []

    def fee_override(self, instrument: str) -> dict[str, float | str] | None:
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


@pytest.mark.parametrize("account_id", sorted(ADMIN_ACCOUNTS))
def test_adapter_fallback_uses_usd_pairs_and_fee_overrides(account_id: str) -> None:
    adapter = RedisFeastAdapter(account_id=account_id, repository=EmptyUniverseRepository())

    instruments = adapter.approved_instruments()
    assert instruments, "Fallback should provide at least one instrument"
    assert all(symbol.endswith("-USD") for symbol in instruments)

    for symbol in instruments:
        override = adapter.fee_override(symbol)
        assert override is not None
        assert override.get("currency", "USD") == "USD"
