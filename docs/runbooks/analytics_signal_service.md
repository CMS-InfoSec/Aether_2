# Analytics Signal & Cross-Asset Runbook

## Overview

The analytics signal and cross-asset services now read market data directly
from the authoritative TimescaleDB store that is populated by the ingestion
pipeline.  Trades are sourced from the `orders`/`fills` hypertables, order-book
snapshots come from `orderbook_snapshots`, and historical prices are loaded from
`ohlcv_bars`.  Cross-asset analytics reuse the same `ohlcv_bars` dataset to
compute lead/lag, rolling beta, and stablecoin deviations that are persisted
into `crossasset_metrics`.

## Dependencies

* **TimescaleDB** – availability of `ohlcv_bars`, `orderbook_snapshots`,
  `orders`, and `fills` is mandatory.
* **Kafka ingestion** – indirectly required to keep TimescaleDB hydrated. If
  Kafka ingestion stalls the Timescale tables will become stale.
* **Prometheus** – the services export freshness gauges which power alerts.

## Health Checks & Alerts

* `signal_service_data_age_seconds{feed="trades"|"order_book"|"prices"}`
  exposes the age of the latest market data used by the signal endpoints.
  Alerts should fire when the age exceeds the configured thresholds (10 minutes
  for books, 6 hours for prices, 2× request window for trades).
* `crossasset_data_age_seconds{symbol="..."}` tracks OHLCV staleness for the
  cross-asset service.  Alert when the value exceeds six hours for volatile
  assets or twelve hours for stablecoins.
* The services return HTTP 503 whenever staleness thresholds are exceeded so
  upstream callers can circuit-break on unhealthy feeds.

## Operational Procedures

1. **Verify ingest status** – check Kafka consumer lag for market-data topics
   (`md.trades`, `md.book`).  If lag > 0 for more than a few minutes, restart
   the ingestion deployment and replay missing data.
2. **Validate Timescale freshness** – run:
   ```sql
   SELECT market, MAX(bucket_start) AS last_bar
   FROM ohlcv_bars
   WHERE market = ANY('{BTC/USD,ETH/USD,USDT/USD}')
   GROUP BY market;
   ```
   Compare results with the alert timestamps.
3. **Rebuild metrics** – if metrics are stale after restoring data, call the
   `/signals/*` and `/crossasset/*` endpoints to repopulate `crossasset_metrics`
   and confirm persistence in TimescaleDB.
4. **Escalation** – if Timescale remains stale after ingest recovery, escalate
   to the data engineering on-call to validate upstream exchange connectivity.

## Rollback Strategy

* Restore TimescaleDB from the most recent backup if data corruption is
  detected.
* Temporarily disable trading features relying on these analytics by referencing
  the `signal_service_data_age_seconds` gauge in Alertmanager (set a silence to
  avoid alert storms while remediating).
