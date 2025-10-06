import datetime as dt
import json
from typing import Any, Dict, List

import httpx
import pytest

from alt_data import (
    AltDataMonitor,
    SecondaryDataError,
    SecondaryClientConfig,
    SecondaryMarketDataClient,
)


@pytest.mark.asyncio
async def test_secondary_client_fetches_live_price() -> None:
    observed_at = dt.datetime.now(tz=dt.timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Api-Key"] == "token"
        payload = {
            "data": {
                "price": "12345.67",
                "timestamp": observed_at.isoformat(),
            }
        }
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    config = SecondaryClientConfig(
        base_url="https://secondary.example",
        endpoint="/v1/prices?base={base}&quote={quote}",
        api_key="token",
        timeout=1.0,
        max_retries=0,
    )
    client = SecondaryMarketDataClient(config, transport=transport)

    price = await client.fetch_price("BTC/USD")

    assert price.symbol == "BTC-USD"
    assert price.price == pytest.approx(12345.67)
    assert price.observed_at == observed_at


@pytest.mark.asyncio
async def test_secondary_client_rejects_derivative_symbol() -> None:
    client = SecondaryMarketDataClient(SecondaryClientConfig(max_retries=0))

    with pytest.raises(SecondaryDataError) as excinfo:
        await client.fetch_price("BTC-PERP")

    assert "spot instruments" in str(excinfo.value)


@pytest.mark.asyncio
async def test_monitor_rejects_non_spot_symbols() -> None:
    client = SecondaryMarketDataClient(SecondaryClientConfig(max_retries=0))
    triggered: List[str] = []

    async def safe_mode_hook(reason: str) -> None:
        triggered.append(reason)

    monitor = AltDataMonitor(
        client,
        deviation_threshold_bps=10,
        max_secondary_age_seconds=30,
        safe_mode_hook=safe_mode_hook,
    )

    await monitor.process_kraken_price("BTC-PERP", 100.0)
    status = await monitor.get_status()

    assert status.secondary_feed_ok is False
    assert triggered == ["non_spot_symbol"]


@pytest.mark.asyncio
async def test_monitor_marks_secondary_feed_unhealthy_when_stale() -> None:
    stale_time = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(minutes=5)

    def handler(request: httpx.Request) -> httpx.Response:
        payload = {
            "data": {
                "price": 30000,
                "timestamp": stale_time.isoformat(),
            }
        }
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    config = SecondaryClientConfig(
        base_url="https://secondary.example",
        endpoint="/v1/prices?base={base}&quote={quote}",
        api_key="token",
        timeout=1.0,
        max_retries=0,
    )
    client = SecondaryMarketDataClient(config, transport=transport)

    triggered: List[str] = []

    async def safe_mode_hook(reason: str) -> None:
        triggered.append(reason)

    monitor = AltDataMonitor(
        client,
        deviation_threshold_bps=10,
        max_secondary_age_seconds=30,
        safe_mode_hook=safe_mode_hook,
    )

    await monitor.process_kraken_price("BTC/USD", 30100)
    status = await monitor.get_status()

    assert status.kraken_feed_ok is True
    assert status.secondary_feed_ok is False
    assert triggered == ["secondary_feed_stale"]


@pytest.mark.asyncio
async def test_monitor_handles_missing_secondary_price() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload: Dict[str, Any] = {"data": {"timestamp": dt.datetime.now(tz=dt.timezone.utc).isoformat()}}
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    config = SecondaryClientConfig(
        base_url="https://secondary.example",
        endpoint="/v1/prices?base={base}&quote={quote}",
        api_key="token",
        timeout=1.0,
        max_retries=0,
    )
    client = SecondaryMarketDataClient(config, transport=transport)

    triggered: List[str] = []

    async def safe_mode_hook(reason: str) -> None:
        triggered.append(reason)

    monitor = AltDataMonitor(
        client,
        deviation_threshold_bps=10,
        max_secondary_age_seconds=120,
        safe_mode_hook=safe_mode_hook,
    )

    await monitor.process_kraken_price("ETH/USD", 2000)
    status = await monitor.get_status()

    assert status.secondary_feed_ok is False
    assert triggered == ["secondary_fetch_failed"]


@pytest.mark.asyncio
async def test_monitor_emits_anomaly_on_large_deviation() -> None:
    observed_at = dt.datetime.now(tz=dt.timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        payload = {
            "data": {
                "price": 100.0,
                "timestamp": observed_at.isoformat(),
            }
        }
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    config = SecondaryClientConfig(
        base_url="https://secondary.example",
        endpoint="/v1/prices?base={base}&quote={quote}",
        api_key="token",
        timeout=1.0,
        max_retries=0,
    )
    client = SecondaryMarketDataClient(config, transport=transport)

    class RecordingProducer:
        def __init__(self) -> None:
            self.messages: List[bytes] = []

        def produce(self, topic: str, message: bytes) -> None:
            self.messages.append(message)

        def poll(self, _timeout: float) -> None:
            return None

    producer = RecordingProducer()
    triggered: List[str] = []

    async def safe_mode_hook(reason: str) -> None:
        triggered.append(reason)

    monitor = AltDataMonitor(
        client,
        kafka_producer=producer,  # type: ignore[arg-type]
        deviation_threshold_bps=50,
        max_secondary_age_seconds=120,
        safe_mode_hook=safe_mode_hook,
    )

    await monitor.process_kraken_price("BTC/USD", 110.0)
    status = await monitor.get_status()

    assert status.secondary_feed_ok is True
    assert status.last_diff_bps is not None
    assert not triggered
    assert producer.messages, "expected anomaly message"

    payload = json.loads(producer.messages[-1].decode("utf-8"))
    assert payload["diff_bps"] >= 50
