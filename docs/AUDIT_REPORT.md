# Aether_2 Remediation Task Board

The repository requires coordinated fixes across persistence, services, and tests before the system can be considered production-ready. The following backlog converts the audit findings into actionable work items, grouped by subsystem. Each task includes the failure signal that exposes the bug, the component to inspect, and the suggested remediation steps.

## 1. Test Harness & Environment

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Provide deterministic Timescale substitutes for unit/integration tests | `pytest` halts with `ModuleNotFoundError: services.common.config.timescale` | `tests/` failing modules importing `services.common.config` | Re-introduce lightweight Timescale session helper or gate imports behind feature flags. Ship a sqlite-backed test helper so services can boot in CI. |
| P0 | Add Redis test double with `Redis`-compatible interface | `ImportError: cannot import name 'Redis'` from `redis` | `requirements.txt` and services using cache (`oms_service.py`, `hedging_service.py`) | Pin `redis>=4.5`, expose `Redis` stub in tests, or abstract cache layer via dependency injection. |
| P1 | Normalize env var defaults for DSNs and API keys during tests | Multiple `KeyError` / `ValueError` when config loads | `config/__init__.py`, `config_service.py` | Provide default env var values for `*_DATABASE_URL`, `KRAKEN_KEY`, etc., and load `.env.test` during pytest. |

## 2. Order Management & Simulation

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Restore `services.oms` database adapters | OMS routes crash on import | `services/oms/database.py`, `pg_session.py` | Rebuild SQLAlchemy session factory that was referenced by OMS stores; ensure account-scoped queries. |
| P0 | Rewire SimBroker to use restored session helpers | `/sim/enter` raises `AttributeError` for missing session | `services/oms/sim_broker.py` | Inject session factory via FastAPI dependency; add integration test that creates simulated fill. |
| P1 | Ensure stop-loss / take-profit enforcement | Missing coverage in tests | `risk_service.py`, `tests/integration/test_risk.py` | Write tests that create OMS orders with SL/TP and verify adjustments in `SimBroker`. |

## 3. Market Data & Training Pipeline

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Fix CoinGecko historical backfill loader | Training start fails with `ConnectionError` / missing tables | `data_loader_coingecko.py`, `training_service.py` | Mock external HTTP and ensure loader writes to Timescale (or sqlite test DB). |
| P1 | Repair Kraken WebSocket listener auto-reconnect | Listener stops without reconnection logic | `exchange_adapter.py`, `services/market_data/` | Implement exponential backoff retry and heartbeat check; add unit test mocking websocket disconnect. |
| P1 | Re-enable incremental model retraining | `ml/train/status` stuck in `pending` | `training_service.py`, `ml/pipelines/` | Audit Celery/async scheduling; ensure state persisted in DB or Redis. |

## 4. Hedging & Risk Controls

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Persist hedge override state across restarts | Manual override lost after reload | `hedging_service.py` | Store override in database/governance log with timestamps; reload on service init. |
| P1 | Calibrate volatility-based hedge sizing | Hedge percentage static regardless of volatility | `hedging_service.py`, `risk_service.py` | Use rolling volatility from market data store; add regression test verifying hedge change. |
| P1 | Add drawdown-aware kill switch | Hedge does not trigger during rapid drawdown | `kill_switch.py`, `hedging_service.py` | Integrate kill switch thresholds with hedge adjustments; ensure governance audit logs action. |

## 5. Accounts, Auth, and Governance

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Reinstate account-scoped database models with `account_id` FKs | Services share data across accounts | `accounts/`, `models/` | Add Alembic migration ensuring `account_id` on all transactional tables; update ORM relationships. |
| P0 | Audit governance logging coverage | Critical routes do not emit audit trail | `governance_simulator.py`, `app.py` | Standardize `audit_log(action, account_id, details)` decorator; require it on order, hedge, sim, and settings routes. |
| P1 | Encrypt Kraken API keys at rest | Keys stored plaintext | `secrets_service.py`, `accounts/api` endpoints | Integrate with KMS or libsodium sealed boxes; update upload/test endpoints to decrypt on use. |

## 6. Reporting & Observability

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Fix `/reports/pnl/daily_pct` aggregation | Endpoint returns 500 due to missing view | `report_service.py`, `reports/` SQL files | Create materialized view or ORM query; ensure account filter applied. |
| P1 | Wire Prometheus / OpenTelemetry exporters | Metrics endpoints empty | `metrics.py`, `deploy/` manifests | Add OTLP exporter config and include in K8s deployment env vars. |
| P1 | Ensure Timescale continuous aggregates refreshed | NAV stale in dashboards | `reports/refresh_jobs.py` | Schedule cron job / background task for refreshing aggregates. |

## 7. Deployment & Ops

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Update Helm values with per-account Kraken secrets | Deployments missing secrets | `deploy/helm/values.yaml` | Add `kraken-keys-{account}` secret mounts; document required K8s secrets. |
| P0 | Enforce HTTPS and secure headers | Ingress serves HTTP without TLS | `deploy/ingress.yaml`, `app.py` | Configure TLS certs, enable FastAPI `SecureHeadersMiddleware`. |
| P1 | Document blue/green rollout process | No ops runbook | `docs/`, `ops/` | Create runbook covering canary deployment, rollback, and health checks. |

## 8. Documentation & Tooling

| Priority | Task | Failure Signal | Investigation Starting Point | Suggested Fix |
| --- | --- | --- | --- | --- |
| P0 | Rewrite README with setup + testing workflow | Onboarding blocked | `README.md` | Document environment variables, `pytest` prerequisites, docker-compose for dependencies. |
| P1 | Generate OpenAPI spec snapshot | Lack of API reference | `services/*/router.py` | Use `fastapi-codegen` or built-in doc export; commit `openapi.json`. |
| P1 | Add CI pipeline for lint + tests | No automated validation | `.github/workflows/` | Create workflow running linting, tests, and safety checks. |

## Execution Guidance

1. **Stabilize the test environment first** so service modules import successfully (`pytest -k smoke`).
2. **Tackle P0 tasks by domain**—persistence, OMS, accounts—before moving to P1 items.
3. After each fix, **add or update tests** to cover the restored functionality.
4. Keep the task board updated as remediations land to ensure visibility across teams.

This task board should replace the previous narrative audit and serve as the single source of truth for outstanding engineering work.
