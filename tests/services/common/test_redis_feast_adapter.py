from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping

import pytest

from services.common.adapters import RedisFeastAdapter
from services.common.config import FeastClient, RedisClient


class StubHistoricalResult:
    def __init__(self, records: Iterable[Mapping[str, Any]]) -> None:
        self._records = [dict(record) for record in records]

    def to_dicts(self) -> List[Dict[str, Any]]:
        return [dict(record) for record in self._records]


class StubOnlineResult:
    def __init__(self, payload: Mapping[str, Any]) -> None:
        self._payload = dict(payload)

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._payload)


class StubFeastStore:
    def __init__(
        self,
        *,
        historical: Mapping[tuple[str, ...], Iterable[Mapping[str, Any]]] | None = None,
        online: Mapping[tuple[str, ...], Mapping[str, Any]] | None = None,
    ) -> None:
        self.historical_responses = {
            tuple(sorted(key)): [dict(record) for record in records]
            for key, records in (historical or {}).items()
        }
        self.online_responses = {
            tuple(sorted(key)): dict(payload)
            for key, payload in (online or {}).items()
        }
        self.historical_calls: List[Dict[str, Any]] = []
        self.online_calls: List[Dict[str, Any]] = []
        self.fail_historical = False
        self.fail_online = False

    def get_historical_features(
        self,
        *,
        entity_rows: List[Dict[str, Any]],
        feature_refs: List[str],
        start_date: datetime,
        end_date: datetime,
    ) -> StubHistoricalResult:
        if self.fail_historical:
            raise ConnectionError("historical store unavailable")
        key = tuple(sorted(feature_refs))
        self.historical_calls.append(
            {
                "entity_rows": [dict(row) for row in entity_rows],
                "feature_refs": list(feature_refs),
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        records = self.historical_responses.get(key, [])
        return StubHistoricalResult(records)

    def get_online_features(
        self,
        *,
        features: List[str],
        entity_rows: List[Dict[str, Any]],
    ) -> StubOnlineResult:
        if self.fail_online:
            raise ConnectionError("online store unavailable")
        key = tuple(sorted(features))
        self.online_calls.append(
            {
                "entity_rows": [dict(row) for row in entity_rows],
                "features": list(features),
            }
        )
        payload = self.online_responses.get(key, {})
        return StubOnlineResult(payload)


@pytest.fixture(autouse=True)
def reset_adapter_state() -> None:
    RedisFeastAdapter._features.clear()
    RedisFeastAdapter._fee_tiers.clear()
    RedisFeastAdapter._online_feature_store.clear()
    RedisFeastAdapter._feature_expirations.clear()
    RedisFeastAdapter._fee_tier_expirations.clear()
    RedisFeastAdapter._online_feature_expirations.clear()
    yield
    RedisFeastAdapter._features.clear()
    RedisFeastAdapter._fee_tiers.clear()
    RedisFeastAdapter._online_feature_store.clear()
    RedisFeastAdapter._feature_expirations.clear()
    RedisFeastAdapter._fee_tier_expirations.clear()
    RedisFeastAdapter._online_feature_expirations.clear()


def _make_adapter(store: StubFeastStore, *, cache_ttl: int = 10) -> RedisFeastAdapter:
    feast_client = FeastClient(project="test-project", account_namespace="acct")
    redis_client = RedisClient(dsn="redis://tests")

    return RedisFeastAdapter(
        account_id="company",
        feast_client_factory=lambda _: feast_client,
        redis_client_factory=lambda _: redis_client,
        feature_store_factory=lambda *_: store,
        cache_ttl=cache_ttl,
    )


def test_adapter_queries_feast_and_caches_results() -> None:
    now = datetime.now(timezone.utc)
    namespace = "acct"
    approved_key = (
        f"{namespace}__approved_instruments:approved",
        f"{namespace}__approved_instruments:instrument",
    )
    fee_override_key = (
        f"{namespace}__fee_overrides:currency",
        f"{namespace}__fee_overrides:instrument",
        f"{namespace}__fee_overrides:maker_bps",
        f"{namespace}__fee_overrides:taker_bps",
    )
    fee_tier_key = (
        f"{namespace}__fee_tiers:maker_bps",
        f"{namespace}__fee_tiers:notional_threshold",
        f"{namespace}__fee_tiers:pair",
        f"{namespace}__fee_tiers:taker_bps",
        f"{namespace}__fee_tiers:tier",
    )
    online_key = tuple(
        sorted(
            f"{namespace}__instrument_features:{field}"
            for field in RedisFeastAdapter._ONLINE_FIELDS
        )
    )

    store = StubFeastStore(
        historical={
            approved_key: [
                {"instrument": "BTC-USD", "approved": True, "event_timestamp": now},
                {"instrument": "BTC-PERP", "approved": True, "event_timestamp": now},
                {"instrument": "ETH-USD", "approved": False, "event_timestamp": now},
            ],
            fee_override_key: [
                {
                    "instrument": "BTC-USD",
                    "maker_bps": 12.0,
                    "taker_bps": 18.0,
                    "currency": "USD",
                    "event_timestamp": now,
                }
            ],
            fee_tier_key: [
                {
                    "pair": "BTC-USD",
                    "tier": "vip",
                    "maker_bps": 8.0,
                    "taker_bps": 12.0,
                    "notional_threshold": 50_000.0,
                    "event_timestamp": now,
                },
                {
                    "pair": "DEFAULT",
                    "tier": "base",
                    "maker_bps": 10.0,
                    "taker_bps": 15.0,
                    "notional_threshold": 0.0,
                    "event_timestamp": now,
                },
            ],
        },
        online={
            online_key: {
                f"{namespace}__instrument_features:features": [0.1, -0.4],
                f"{namespace}__instrument_features:expected_edge_bps": 14.0,
                f"{namespace}__instrument_features:take_profit_bps": 28.0,
                f"{namespace}__instrument_features:stop_loss_bps": 12.0,
                f"{namespace}__instrument_features:confidence": {"model": 0.9},
                f"{namespace}__instrument_features:book_snapshot": {"mid_price": 25_000.0},
                f"{namespace}__instrument_features:state": {"regime": "bull"},
            }
        },
    )

    adapter = _make_adapter(store, cache_ttl=30)

    instruments = adapter.approved_instruments()
    assert instruments == ["BTC-USD"]
    assert len(store.historical_calls) == 1
    historical_call = store.historical_calls[0]
    assert historical_call["entity_rows"][0]["account_id"] == "acct"

    override = adapter.fee_override("BTC-USD")
    assert override == {"currency": "USD", "maker": 12.0, "taker": 18.0}

    tiers = adapter.fee_tiers("BTC-USD")
    assert tiers[0]["tier"] == "base"
    assert tiers[-1]["tier"] == "vip"

    online = adapter.fetch_online_features("BTC-USD")
    assert online["expected_edge_bps"] == 14.0
    assert online["book_snapshot"]["mid_price"] == 25_000.0
    assert len(store.online_calls) == 1
    assert store.online_calls[0]["entity_rows"][0]["instrument"] == "BTC-USD"

    # Cached results should avoid additional Feast calls.
    adapter.approved_instruments()
    adapter.fee_override("BTC-USD")
    adapter.fee_tiers("BTC-USD")
    adapter.fetch_online_features("BTC-USD")
    assert len(store.historical_calls) == 3  # approved + fee_override + fee_tiers
    assert len(store.online_calls) == 1

    # Expire cache and ensure calls refresh.
    RedisFeastAdapter._feature_expirations["company"] = datetime.now(timezone.utc) - timedelta(
        seconds=1
    )
    RedisFeastAdapter._fee_tier_expirations["company"] = datetime.now(timezone.utc) - timedelta(
        seconds=1
    )
    RedisFeastAdapter._online_feature_expirations.setdefault("company", {})["BTC-USD"] = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    )

    adapter.approved_instruments()
    RedisFeastAdapter._feature_expirations["company"] = datetime.now(timezone.utc) - timedelta(
        seconds=1
    )
    adapter.fee_override("BTC-USD")
    adapter.fee_tiers("BTC-USD")
    RedisFeastAdapter._online_feature_expirations.setdefault("company", {})["BTC-USD"] = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    RedisFeastAdapter._online_feature_store.setdefault("company", {}).pop("BTC-USD", None)
    adapter.fetch_online_features("BTC-USD")
    assert len(store.historical_calls) == 6
    assert len(store.online_calls) == 2


def test_adapter_raises_when_feast_returns_no_data() -> None:
    namespace = "acct"
    approved_key = (
        f"{namespace}__approved_instruments:approved",
        f"{namespace}__approved_instruments:instrument",
    )
    store = StubFeastStore(historical={approved_key: []})
    adapter = _make_adapter(store)

    with pytest.raises(RuntimeError):
        adapter.approved_instruments()

    online_key = tuple(
        sorted(
            f"{namespace}__instrument_features:{field}"
            for field in RedisFeastAdapter._ONLINE_FIELDS
        )
    )
    store = StubFeastStore(online={online_key: {}})
    adapter = _make_adapter(store)

    with pytest.raises(RuntimeError):
        adapter.fetch_online_features("BTC-USD")


def test_adapter_propagates_feast_failures() -> None:
    namespace = "acct"
    approved_key = (
        f"{namespace}__approved_instruments:approved",
        f"{namespace}__approved_instruments:instrument",
    )
    store = StubFeastStore(historical={approved_key: []})
    store.fail_historical = True
    adapter = _make_adapter(store)

    with pytest.raises(RuntimeError):
        adapter.approved_instruments()

    online_key = tuple(
        sorted(
            f"{namespace}__instrument_features:{field}"
            for field in RedisFeastAdapter._ONLINE_FIELDS
        )
    )
    store = StubFeastStore(online={online_key: {}})
    store.fail_online = True
    adapter = _make_adapter(store)

    with pytest.raises(RuntimeError):
        adapter.fetch_online_features("BTC-USD")


def test_adapter_normalizes_inputs_and_rejects_derivatives() -> None:
    now = datetime.now(timezone.utc)
    namespace = "acct"
    approved_key = (
        f"{namespace}__approved_instruments:approved",
        f"{namespace}__approved_instruments:instrument",
    )
    fee_override_key = (
        f"{namespace}__fee_overrides:currency",
        f"{namespace}__fee_overrides:instrument",
        f"{namespace}__fee_overrides:maker_bps",
        f"{namespace}__fee_overrides:taker_bps",
    )
    fee_tier_key = (
        f"{namespace}__fee_tiers:maker_bps",
        f"{namespace}__fee_tiers:notional_threshold",
        f"{namespace}__fee_tiers:pair",
        f"{namespace}__fee_tiers:taker_bps",
        f"{namespace}__fee_tiers:tier",
    )
    online_key = tuple(
        sorted(
            f"{namespace}__instrument_features:{field}"
            for field in RedisFeastAdapter._ONLINE_FIELDS
        )
    )

    store = StubFeastStore(
        historical={
            approved_key: [
                {"instrument": "eth/usd", "approved": True, "event_timestamp": now},
                {"instrument": "ETH-PERP", "approved": True, "event_timestamp": now},
            ],
            fee_override_key: [
                {
                    "instrument": "eth/usd",
                    "maker_bps": 5.0,
                    "taker_bps": 7.0,
                    "currency": "USD",
                    "event_timestamp": now,
                }
            ],
            fee_tier_key: [
                {
                    "pair": "eth_usd",
                    "tier": "base",
                    "maker_bps": 10.0,
                    "taker_bps": 15.0,
                    "notional_threshold": 0.0,
                    "event_timestamp": now,
                }
            ],
        },
        online={
            online_key: {
                f"{namespace}__instrument_features:features": [0.2, 0.5],
                f"{namespace}__instrument_features:expected_edge_bps": 8.0,
                f"{namespace}__instrument_features:book_snapshot": {"mid_price": 1_850.0},
                f"{namespace}__instrument_features:state": {"regime": "neutral"},
            }
        },
    )

    adapter = _make_adapter(store)

    assert adapter.approved_instruments() == ["ETH-USD"]
    override = adapter.fee_override("eth/usd")
    assert override == {"currency": "USD", "maker": 5.0, "taker": 7.0}
    tiers = adapter.fee_tiers("eth_usd")
    assert tiers[0]["tier"] == "base"
    online = adapter.fetch_online_features("Eth-Usd")
    assert online["expected_edge_bps"] == 8.0

    with pytest.raises(ValueError):
        adapter.fee_override("eth-perp")
    with pytest.raises(ValueError):
        adapter.fee_tiers("eth-perp")
    with pytest.raises(ValueError):
        adapter.fetch_online_features("eth-perp")


def test_seed_fee_tiers_filters_non_spot_pairs() -> None:
    tiers = {
        "company": {
            "btc-usd": [{"tier": "vip"}],
            "btc-perp": [{"tier": "invalid"}],
            "DEFAULT": [{"tier": "fallback"}],
        }
    }

    RedisFeastAdapter.seed_fee_tiers(tiers)

    company_cache = RedisFeastAdapter._fee_tiers.get("company")
    assert company_cache is not None
    assert "BTC-USD" in company_cache
    assert "BTC-PERP" not in company_cache
    assert company_cache["DEFAULT"][0]["tier"] == "fallback"
