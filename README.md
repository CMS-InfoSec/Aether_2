# Aether Data Platform

This repository packages the services, data pipelines, and operational tooling
that power Aether's USD spot-market trading platform. The stack centres around
TimescaleDB for historical storage, Kafka/NATS for live dissemination, Feast/Redis
for feature serving, and FastAPI-based microservices for policy, risk, and
reporting flows. **Only Kraken USD spot markets are supported.**

## Quickstart

1. **Create a Python environment** (the codebase targets Python 3.11+):

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   ```

2. **Install dependencies**. The project uses an editable install with optional
   extras for development and testing:

   ```bash
   pip install -e .[dev,test]
   ```

   The base install pulls runtime dependencies (FastAPI, SQLAlchemy, Pydantic,
   Timescale/psycopg, Kafka/NATS clients, Redis/Feast, MLflow, PyTorch, etc.).
   The ``dev`` and ``test`` extras add linting, typing, and pytest tooling.

3. **Seed local state directories.** Several services persist test artefacts in
   ``.aether_state/``. Create the structure once before running the suite:

   ```bash
   mkdir -p \
     .aether_state/accounts \
     .aether_state/capital_flow \
     .aether_state/hedge_service \
     .aether_state/kraken_ws \
     .aether_state/stress_engine
   ```

4. **Run the test suite** once dependencies are installed:

   ```bash
   pytest -q
   ```

   You can target subsets during development, e.g. ``pytest tests/services -q``
   or ``pytest tests/ml/test_auto_feature_discovery_insecure_defaults.py -q``.

## Local Development Guide

The repository follows the ``src/`` layout and is packaged as a single Python
distribution. Key tooling is configured through ``pyproject.toml``.

### Common tasks

- **Formatting & linting**: ``ruff check --fix .`` runs lint rules and applies
  formatting fixes. ``black .`` and ``isort .`` are also available if you prefer
  dedicated formatters.
- **Type checking**: ``mypy`` is configured via ``pyproject.toml``—invoke it with
  ``mypy`` from the repository root.
- **Regenerating API clients**: The FastAPI services expose OpenAPI schemas at
  ``/openapi.json``. Use your preferred OpenAPI tooling (for example,
  ``openapi-generator`` or ``datamodel-code-generator``) once the services are
  running locally.

### Insecure-default fallbacks

Many services provide deterministic fallbacks for environments without the full
dependency stack. Set ``AETHER_ALLOW_INSECURE_DEFAULTS=1`` (or the service-specific
flag documented in ``shared/insecure_defaults.py``) during development to enable
local JSON/SQLite storage while production deployments continue to require
managed dependencies.

## Component Overview

- **Alembic migrations** (`data/alembic/`) establish TimescaleDB hypertables for
  market data, trading activity, and governance metadata.
- **Ingestion jobs** (`data/ingest/`) provide batch and streaming collectors for
  CoinGecko and Kraken data sources.
- **Feast repository** (`data/feast/`) exposes curated feature views backed by
  Timescale tables with Redis configured for online serving.
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

## Admin Platform Persistence

The administrative FastAPI application now **requires** a shared Postgres/Timescale
database for persisting operator credentials. Deployments must provide a DSN via
one of the following environment variables (checked in order):

- `ADMIN_POSTGRES_DSN`
- `ADMIN_DATABASE_DSN`
- `ADMIN_DB_DSN`

Set the DSN in the platform's Kubernetes secret or Helm values so that the
application can connect to the shared database. A helper script is available to
migrate any historical administrator exports:

```bash
python ops/migrate_admin_repository.py --source legacy_admins.json
```

The script accepts either plaintext passwords or pre-hashed credentials in the
JSON export and will upsert the records into the configured database. See the
[admin database migration runbook](docs/runbooks/admin-database-migration.md)
for end-to-end instructions covering secret rotation and verification steps.
