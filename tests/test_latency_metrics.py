import logging

from prometheus_client import CollectorRegistry

from ops.observability.latency_metrics import LatencyMetrics, LatencySnapshot


def test_observe_normalises_labels_and_records_latency() -> None:
    metrics = LatencyMetrics(registry=CollectorRegistry())

    snapshot = metrics.observe(
        "policy_latency_ms",
        symbol="BTC",
        account_id="inst-123",
        latency_ms=42.0,
    )

    assert isinstance(snapshot, LatencySnapshot)
    windows = metrics._windows["policy_latency_ms"]  # noqa: SLF001 - internal assertion
    assert ("mega_cap", "institutional") in windows

    alert_value = metrics.registry.get_sample_value(
        "latency_p95_alert",
        {"metric": "policy_latency_ms", "symbol_tier": "mega_cap", "account_segment": "institutional"},
    )
    assert alert_value == 0


def test_observe_buckets_new_symbols() -> None:
    metrics = LatencyMetrics(registry=CollectorRegistry())

    metrics.observe(
        "policy_latency_ms",
        symbol="newcoin",
        account_id="ret-001",
        latency_ms=10.0,
    )

    windows = metrics._windows["policy_latency_ms"]  # noqa: SLF001 - internal assertion
    assert ("long_tail", "retail") in windows


def test_observe_discards_disallowed_pairs(caplog) -> None:
    metrics = LatencyMetrics(
        registry=CollectorRegistry(),
        allowed_label_pairs={("mega_cap", "institutional")},
    )

    with caplog.at_level(logging.WARNING):
        snapshot = metrics.observe(
            "policy_latency_ms",
            symbol="SOL",
            account_id="ret-777",
            latency_ms=15.0,
        )

    assert snapshot == LatencySnapshot(0.0, 0.0, 0.0)
    assert ("major", "retail") not in metrics._windows["policy_latency_ms"]  # noqa: SLF001
    assert any("Discarding latency sample" in message for message in caplog.messages)

    discarded = metrics.registry.get_sample_value(
        "latency_discarded_label_pairs_total",
        {"metric": "policy_latency_ms", "symbol_tier": "major", "account_segment": "retail"},
    )
    assert discarded == 1
