# Aether Data Platform

This repository provides reference infrastructure for ingesting, validating, and
serving market data for the Aether research platform. The stack centres around
TimescaleDB for historical storage, Kafka/NATS for real-time dissemination, and
Feast/Redis for feature serving.

## Components

- **Alembic migrations** (`data/alembic/`) establish TimescaleDB hypertables for
  market data, trading activity, and governance metadata.
- **Ingestion jobs** (`data/ingest/`) provide batch and streaming collectors for
  CoinGecko and Kraken data sources.
- **Feast repository** (`data/feast/`) exposes curated feature views backed by
  the offline Timescale tables with Redis configured for online serving.
- **Great Expectations** (`data/great_expectations/`) defines validation suites
  to ensure incoming datasets remain within the expected schema envelope.
- **Argo Workflows** (`ops/workflows/`) schedules nightly batch ingestion and
  manages the real-time Kraken streaming deployment.

Refer to the inline documentation within each component for detailed usage
instructions and configuration parameters.

## Operational Readiness

- [Service Level Objectives](docs/slo.md) summarise latency and response targets
  for the OMS, WebSocket gateway, kill-switch, and model canary workflows.
- Runbooks in [`docs/runbooks/`](docs/runbooks) provide incident response playbooks
  for exchange outages, WebSocket desync, model rollback, secret rotation
  failures, and kill-switch activation.
- Administrators can use the [on-call readiness and compliance checklist](docs/checklists/oncall.md)
  to attest to operational readiness and file regulatory attestations.
