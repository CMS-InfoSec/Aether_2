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

## Development Environment

The repository is packaged as an installable Python distribution using the
``src/`` layout.  During local development install dependencies in editable mode:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
```

The base install includes service runtime dependencies such as FastAPI,
SQLAlchemy, Pydantic, Kafka/NATS clients, Redis/Feast, Timescale/psycopg, MLflow,
and PyTorch.  The ``dev`` extra adds linting, formatting, typing, and testing
tooling.  Continuous integration environments can install the ``test`` extra
(``pip install .[test]``) to avoid bringing in development-only tools.

### Tooling

- **Static analysis**: ``ruff`` (linting) and ``mypy`` (type checking) use the
  configuration embedded in ``pyproject.toml``.
- **Formatting**: ``black`` and ``isort`` enforce consistent style.  Run
  ``ruff check --fix .`` or ``black . && isort .`` before submitting changes.
- **Testing**: ``pytest`` drives the unit test suite with ``hypothesis`` and
  ``pytest-asyncio`` enabled for property-based and async testing scenarios.

All tools read configuration from the checked-in ``pyproject.toml`` to provide a
single source of truth for development conventions.

## Operational Readiness

- [Service Level Objectives](docs/slo.md) summarise latency and response targets
  for the OMS, WebSocket gateway, kill-switch, and model canary workflows.
- Runbooks in [`docs/runbooks/`](docs/runbooks) provide incident response playbooks
  for exchange outages, WebSocket desync, model rollback, secret rotation
  failures, and kill-switch activation.
- Administrators can use the [on-call readiness and compliance checklist](docs/checklists/oncall.md)
  to attest to operational readiness and file regulatory attestations.
