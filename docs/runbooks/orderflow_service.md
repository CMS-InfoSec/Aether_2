# Orderflow Analytics Runbook

## Overview

The orderflow analytics service consumes trades and order book snapshots from
TimescaleDB tables that are hydrated by the Kraken ingestion pipeline.  It
computes buy/sell imbalances, depth imbalances, liquidity holes, and impact
estimates for downstream monitoring dashboards and alerting.

## Dependencies

* **Kafka ingestion** – the `md.trades` and `md.book` topics must remain
  healthy so the ingestion service can stream data into TimescaleDB.
* **TimescaleDB** – relies on the `orders`, `fills`, and `orderbook_snapshots`
  hypertables for authoritative market data.
* **Psycopg** – optional dependency used to persist computed metrics. Without
  psycopg the service buffers snapshots in memory.

## Alerting & Staleness Monitoring

* `orderflow_data_age_seconds{feed="trades"|"order_book"}` is exported by the
  service and measures the freshness of the latest trade batch and book
  snapshot. Alert when trade freshness exceeds twice the requested window or the
  book feed is older than 10 minutes.
* HTTP 503 responses with `{"detail": "market data unavailable ..."}` indicate
  the service could not load data from TimescaleDB; alert on sustained
  occurrences (>3 in 5 minutes).
* Page the ingestion on-call if both feeds are stale and Kafka lag is non-zero.

## Operational Response

1. **Check ingestion** – inspect Kafka consumer lag for `md.trades` and
   `md.book`. Restart the ingestion deployment if lag is rising.
2. **Verify Timescale freshness** – run:
   ```sql
   SELECT market, MAX(fill_time) AS last_fill
   FROM fills
   WHERE market = 'BTC-USD'
   GROUP BY market;
   ```
   Compare timestamps with the Prometheus freshness gauges.
3. **Rehydrate snapshots** – once ingest is healthy, call the `/orderflow/*`
   endpoints to repopulate caches and confirm persistence in `orderflow_metrics`.
4. **Escalate** – if data remains stale for more than 30 minutes after ingest
   recovery, escalate to data engineering to validate upstream exchange
   connectivity.
