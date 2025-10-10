# Aether_2 Remediation Task Board

The repository requires coordinated fixes across persistence, services, and tests before the system can be considered production-ready. The following backlog converts the audit findings into actionable work items, grouped by subsystem. Each task includes the failure signal that exposes the bug, the component to inspect, and the suggested remediation steps.

## 1. Test Harness & Environment

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Provide deterministic Timescale substitutes for unit/integration tests | ✅ Completed | `get_timescale_session` provisions `.aether_state` SQLite fallbacks so services and tests keep running without Timescale credentials.【F:services/common/config.py†L181-L240】【F:tests/services/backtest/test_stress_engine_insecure_defaults.py†L1-L28】 |
| P0 | Add Redis test double with `Redis`-compatible interface | ✅ Completed | `common.utils.redis.create_redis_from_url` now returns an in-memory stub when the driver or server is unavailable, keeping caches operational in CI.【F:common/utils/redis.py†L29-L277】 |
| P1 | Normalize env var defaults for DSNs and API keys during tests | ✅ Completed | Configuration helpers populate deterministic Redis/Timescale DSNs and stub Kraken secrets under `.aether_state/`, preventing `KeyError`/`ValueError` during pytest bootstrap.【F:services/common/config.py†L82-L234】 |
| P0 | Prevent pytest stubs from shadowing shared adapters/security helpers | ✅ Completed | `services.common` now discards stub modules that lack `__file__` and reloads the production implementations so imports like `from services.common.adapters import TimescaleAdapter` resolve even after tests inject placeholders.【F:services/common/__init__.py†L25-L109】 |
| P0 | Run shared bootstrap during interpreter startup | ✅ Completed | `sitecustomize` invokes `ensure_common_helpers()` on import so the canonical `services.common.security` guards and `httpx` shim load before pytest collection, keeping `ADMIN_ACCOUNTS` and response helpers available even when suites register temporary stubs.【F:sitecustomize.py†L1-L66】【F:shared/common_bootstrap.py†L1-L120】 |

## 2. Order Management & Simulation

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Restore `services.oms` database adapters | ✅ Completed | `TimescaleAdapter` rebuilds the OMS persistence layer while falling back to buffered in-memory stores when Timescale is unavailable, restoring import stability.【F:services/common/adapters.py†L1507-L1719】 |
| P0 | Rewire SimBroker to use restored session helpers | ✅ Completed | `SimBroker` injects `TimescaleAdapter`/`get_timescale_session` so simulated orders persist to Timescale or durable `.aether_state` fallbacks.【F:services/oms/sim_broker.py†L90-L216】 |
| P1 | Ensure stop-loss / take-profit enforcement | ✅ Completed | The exit-rule engine now emits mandatory bracket orders and regression tests assert trailing-stop adjustments and cancellation flows.【F:services/risk/exit_rules.py†L41-L196】【F:tests/risk/test_exit_rules.py†L1-L64】 |

## 3. Market Data & Training Pipeline

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Fix CoinGecko historical backfill loader | ✅ Completed | The ingestion job guards optional dependencies, persists fallbacks to `.aether_state/coingecko/`, and normalises database DSNs so training data loads in CI environments.【F:data/ingest/coingecko_job.py†L86-L186】 |
| P1 | Repair Kraken WebSocket listener auto-reconnect | ✅ Completed | `consume` now loops with exponential backoff via `_stream_websocket`, resubscribing after disconnects and exercising the path in new regression coverage.【F:data/ingest/kraken_ws.py†L360-L518】【F:tests/data/test_kraken_ws_reconnect.py†L1-L121】 |
| P1 | Re-enable incremental model retraining | ✅ Completed | Insecure-default fallbacks unblock HPO/retraining flows by persisting study state locally and providing deterministic trainer stubs during tests.【F:ml/hpo/optuna_runner.py†L1-L286】【F:tests/ml/test_hpo_insecure_defaults.py†L1-L33】 |

## 4. Hedging & Risk Controls

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Persist hedge override state across restarts | ✅ Completed | Hedge overrides now save to `.aether_state/hedge_service/override_state.json` via `HedgeOverrideStateStore`, and regression coverage reloads overrides and history across service instances.【F:services/hedge/hedge_service.py†L1-L420】【F:tests/services/hedge/test_hedge_override_persistence.py†L1-L45】 |
| P1 | Calibrate volatility-based hedge sizing | 🚧 Pending | Hedge sizing still needs volatility-aware tuning and targeted regression coverage. |
| P1 | Add drawdown-aware kill switch | 🚧 Pending | Integration between the kill switch and hedging logic remains outstanding. |

## 5. Accounts, Auth, and Governance

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Reinstate account-scoped database models with `account_id` FKs | 🚧 Pending | Database migrations still need to enforce account isolation across transactional tables. |
| P0 | Audit governance logging coverage | 🚧 Pending | Governance actions require consistent audit decorators across order, hedge, and simulation routes. |
| P1 | Encrypt Kraken API keys at rest | 🚧 Pending | Production deployments must integrate a real secrets backend or envelope encryption beyond the insecure test stubs. |

## 6. Reporting & Observability

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Fix `/reports/pnl/daily_pct` aggregation | ✅ Completed | The daily return endpoint now falls back to a local NAV store when Timescale tables or psycopg are unavailable, keeping `/reports/pnl/daily_pct` online under insecure defaults while still preferring the database path in production.【F:services/reports/report_service.py†L60-L231】【F:services/reports/report_service.py†L666-L768】【F:tests/reports/test_daily_return_insecure_defaults.py†L1-L38】 |
| P1 | Wire Prometheus / OpenTelemetry exporters | 🚧 Pending | Exporters must be configured once production observability requirements are defined. |
| P1 | Ensure Timescale continuous aggregates refreshed | 🚧 Pending | Background refresh jobs for NAV/usage dashboards remain to be scheduled. |

## 7. Deployment & Ops

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Update Helm values with per-account Kraken secrets | 🚧 Pending | Helm manifests still require dedicated secret mounts and documentation updates. |
| P0 | Enforce HTTPS and secure headers | 🚧 Pending | TLS enforcement and secure header middleware remain to be wired through ingress manifests. |
| P1 | Document blue/green rollout process | 🚧 Pending | Deployment runbooks must outline canary, rollback, and health-check procedures. |

## 8. Documentation & Tooling

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Rewrite README with setup + testing workflow | 🚧 Pending | Contributor documentation still needs an end-to-end setup and testing guide. |
| P1 | Generate OpenAPI spec snapshot | 🚧 Pending | The consolidated API definition remains to be exported and versioned. |
| P1 | Add CI pipeline for lint + tests | 🚧 Pending | CI workflows for linting, testing, and safety checks must be introduced. |

## Execution Guidance

1. **Stabilize the test environment first** so service modules import successfully (`pytest -k smoke`).
2. **Tackle P0 tasks by domain**—persistence, OMS, accounts—before moving to P1 items.
3. After each fix, **add or update tests** to cover the restored functionality.
4. Keep the task board updated as remediations land to ensure visibility across teams.

This task board should replace the previous narrative audit and serve as the single source of truth for outstanding engineering work.
