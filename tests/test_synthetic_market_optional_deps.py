import json

import pytest

import synthetic_market


def test_generate_synthetic_data_without_optional_deps(monkeypatch):
    monkeypatch.setattr(synthetic_market, "NUMPY_AVAILABLE", False)
    monkeypatch.setattr(synthetic_market, "PANDAS_AVAILABLE", False)
    monkeypatch.setattr(synthetic_market, "np", None, raising=False)
    monkeypatch.setattr(synthetic_market, "pd", None, raising=False)
    monkeypatch.setattr(synthetic_market, "_discover_dataset", lambda _: None)

    synthetic_market.random.seed(42)

    output = synthetic_market.generate_synthetic_data(hours=1)
    lines = output.splitlines()

    assert len(lines) == 60

    first_event = json.loads(lines[0])
    assert set(first_event.keys()) == {"order_book", "trade"}

    order_book = first_event["order_book"]
    trade = first_event["trade"]

    assert "levels" in order_book
    assert trade["side"] in {"buy", "sell"}
    assert trade["price"] >= 0
    assert trade["size"] >= 0


@pytest.mark.parametrize("hours", [0, -1])
def test_generate_synthetic_data_requires_positive_hours(hours):
    with pytest.raises(ValueError):
        synthetic_market.generate_synthetic_data(hours=hours)
