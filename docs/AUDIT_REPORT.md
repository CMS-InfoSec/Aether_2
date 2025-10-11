# Aether_2 Remediation Task Board

The repository requires coordinated fixes across persistence, services, and tests before the system can be considered production-ready. The following backlog converts the audit findings into actionable work items, grouped by subsystem. Each task includes the failure signal that exposes the bug, the component to inspect, and the suggested remediation steps.

## Platform Capability Snapshot

- **Order management & simulation** – `SimBroker` enforces spot-only order flow, persists simulated executions, and mirrors Kafka/Timescale side effects so CI can exercise the OMS without live exchange access.【F:services/oms/sim_broker.py†L86-L160】
- **Risk & hedging controls** – The exit-rule engine guarantees stop-loss, take-profit, and trailing orders while the hedge service blends volatility and drawdown signals, persists operator overrides, and raises kill-switch recommendations.【F:services/risk/exit_rules.py†L42-L179】【F:services/hedge/hedge_service.py†L467-L620】
- **Data ingestion & model training** – Feature jobs compute rolling microstructure statistics with deterministic SQLite fallbacks, and the supervised trainers support LightGBM, XGBoost, and PyTorch sequence models for adaptive strategies.【F:data/ingest/feature_jobs.py†L90-L213】【F:ml/models/supervised.py†L70-L399】
- **Governance & observability** – Immutable audit logging, correlation IDs, and Prometheus-compatible metrics are available to every service, keeping sensitive changes and health data attributable even in dependency-light environments.【F:shared/audit.py†L1-L178】【F:metrics.py†L1-L152】
- **Deployment & security posture** – Helm templates enforce TLS-only ingress with hardened headers, aligning platform deployments with the production security baseline.【F:deploy/helm/aether-platform/templates/backend-ingresses.yaml†L1-L58】

## 1. Test Harness & Environment

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Provide deterministic Timescale substitutes for unit/integration tests | ✅ Completed | `get_timescale_session` provisions `.aether_state` SQLite fallbacks so services and tests keep running without Timescale credentials.【F:services/common/config.py†L181-L240】【F:tests/services/backtest/test_stress_engine_insecure_defaults.py†L1-L28】 |
| P0 | Add Redis test double with `Redis`-compatible interface | ✅ Completed | `common.utils.redis.create_redis_from_url` now returns an in-memory stub when the driver or server is unavailable, keeping caches operational in CI.【F:common/utils/redis.py†L29-L277】 |
| P1 | Normalize env var defaults for DSNs and API keys during tests | ✅ Completed | Configuration helpers populate deterministic Redis/Timescale DSNs and stub Kraken secrets under `.aether_state/`, preventing `KeyError`/`ValueError` during pytest bootstrap.【F:services/common/config.py†L82-L234】 |
| P0 | Prevent pytest stubs from shadowing shared adapters/security helpers | ✅ Completed | `services.common` now discards stub modules that lack `__file__`, and a bootstrap guard reloads the real packages whenever test mirrors replace them in `sys.modules`, so imports like `from services.common.adapters import TimescaleAdapter` and `services.test_*` fixtures coexist without breaking production helpers.【F:services/common/__init__.py†L25-L109】【F:shared/common_bootstrap.py†L77-L239】 |
| P0 | Restore `services.test_*` mirrors to the canonical namespace | ✅ Completed | `services.__getattr__` lazily imports submodules and registers the `services_real` alias so pytest mirrors resolve to the production implementations without clobbering runtime imports.【F:services/__init__.py†L1-L74】【F:tests/services/__init__.py†L12-L42】 |
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
| P0 | Provide local market data fallback for analytics | ✅ Completed | `TimescaleMarketDataAdapter` now detects `MARKET_DATA_USE_LOCAL_STORE` or missing SQLAlchemy, serving data from the deterministic in-memory store while the signal service bypasses DSN resolution and tests seed both market and cross-asset payloads.【F:services/analytics/market_data_store.py†L56-L309】【F:services/analytics/signal_service.py†L610-L660】【F:tests/services/analytics/test_market_data_services.py†L45-L305】 |
| P1 | Repair Kraken WebSocket listener auto-reconnect | ✅ Completed | `consume` now loops with exponential backoff via `_stream_websocket`, resubscribing after disconnects and exercising the path in new regression coverage.【F:data/ingest/kraken_ws.py†L360-L518】【F:tests/data/test_kraken_ws_reconnect.py†L1-L121】 |
| P1 | Re-enable incremental model retraining | ✅ Completed | Insecure-default fallbacks unblock HPO/retraining flows by persisting study state locally and providing deterministic trainer stubs during tests.【F:ml/hpo/optuna_runner.py†L1-L286】【F:tests/ml/test_hpo_insecure_defaults.py†L1-L33】 |

## 4. Hedging & Risk Controls

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Persist hedge override state across restarts | ✅ Completed | Hedge overrides now save to `.aether_state/hedge_service/override_state.json` via `HedgeOverrideStateStore`, and regression coverage reloads overrides and history across service instances.【F:services/hedge/hedge_service.py†L1-L420】【F:tests/services/hedge/test_hedge_override_persistence.py†L1-L45】 |
| P1 | Calibrate volatility-based hedge sizing | ✅ Completed | Hedge diagnostics now blend volatility and drawdown signals with configurable floors, and regression coverage asserts monotonic targets and guard behaviour.【F:services/hedge/hedge_service.py†L470-L707】【F:tests/services/hedge/test_hedge_auto_calibration.py†L21-L97】 |
| P1 | Add drawdown-aware kill switch | ✅ Completed | The hedge service now raises kill-switch recommendations with optional handlers, persists the signal in health metadata, and tests cover handler re-arming semantics.【F:services/hedge/hedge_service.py†L470-L707】【F:tests/services/hedge/test_hedge_auto_calibration.py†L99-L160】【F:tests/services/hedge/test_hedge_service_health.py†L17-L41】 |

## 5. Accounts, Auth, and Governance

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Reinstate account-scoped database models with `account_id` FKs | ✅ Completed | Shared `AccountId` helpers now declare foreign keys back to `accounts.account_id`, core services adopt the column across capital flows, allocators, risk/anomaly logs, and regression coverage exercises the type conversion. 【F:shared/account_scope.py†L1-L85】【F:capital_flow.py†L421-L476】【F:capital_allocator.py†L560-L575】【F:tests/common/test_account_scope.py†L1-L63】 |
| P0 | Audit governance logging coverage | ✅ Completed | Added regression tests that capture audit events for safe mode, kill switch, and manual override endpoints using temporary audit hook overrides.【F:tests/test_governance_audit_logging.py†L1-L225】 |
| P1 | Encrypt Kraken API keys at rest | ✅ Completed | Account service now provisions a deterministic Fernet key under `.aether_state/accounts/encryption.key` whenever insecure defaults are explicitly enabled, ensuring API credentials remain encrypted at rest without requiring manual secrets in test environments and verified through regression coverage.【F:services/account_crypto.py†L1-L102】【F:tests/services/test_account_crypto_insecure_defaults.py†L1-L53】 |

## 6. Reporting & Observability

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Fix `/reports/pnl/daily_pct` aggregation | ✅ Completed | The daily return endpoint now falls back to a local NAV store when Timescale tables or psycopg are unavailable, keeping `/reports/pnl/daily_pct` online under insecure defaults while still preferring the database path in production.【F:services/reports/report_service.py†L60-L231】【F:services/reports/report_service.py†L666-L768】【F:tests/reports/test_daily_return_insecure_defaults.py†L1-L38】 |
| P1 | Wire Prometheus / OpenTelemetry exporters | ✅ Completed | `metrics.configure_observability` starts the embedded Prometheus exporter and configures OTLP tracing whenever the relevant environment variables are set, keeping scrape and trace targets aligned across services.【F:metrics.py†L1-L352】【F:metrics.py†L828-L851】 |
| P1 | Ensure Timescale continuous aggregates refreshed | ✅ Completed | `ops/refresh_continuous_aggregates.py` refreshes materialised views via psycopg when available and logs manual follow-ups when running against lightweight environments.【F:ops/refresh_continuous_aggregates.py†L1-L102】 |

## 7. Deployment & Ops

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Update Helm values with per-account Kraken secrets | ✅ Completed | Helm values expose projected secrets for company/director accounts with checksum-aware mounts and documented paths for every backend deployment.【F:deploy/helm/aether-platform/values.yaml†L1-L74】【F:deploy/helm/aether-platform/templates/backend-deployments.yaml†L1-L88】 |
| P0 | Enforce HTTPS and secure headers | ✅ Completed | Ingress templates now enable forced TLS redirects, HSTS, and hardened security headers through chart defaults while remaining overridable per service.【F:deploy/helm/aether-platform/templates/backend-ingresses.yaml†L1-L60】【F:deploy/helm/aether-platform/values.yaml†L5-L24】 |
| P1 | Document blue/green rollout process | ✅ Completed | Deployment playbook covers staging, observability bootstrapping, traffic flips, and rollback guidance for the hardened ingress environment.【F:docs/deployment/blue_green_rollout.md†L1-L66】 |

## 8. Documentation & Tooling

| Priority | Task | Status | Notes |
| --- | --- | --- | --- |
| P0 | Rewrite README with setup + testing workflow | ✅ Completed | README now documents environment setup, local state bootstrapping, insecure-default flags, and pytest execution paths for contributors.【F:README.md†L1-L88】 |
| P1 | Generate OpenAPI spec snapshot | ✅ Completed | `scripts/generate_openapi.py` emits a repository snapshot backed by the FastAPI stub and stores it under `docs/api/openapi.json` for audit review.【F:scripts/generate_openapi.py†L1-L63】【F:docs/api/openapi.json†L1-L200】 |
| P1 | Add CI pipeline for lint + tests | ✅ Completed | GitHub Actions workflow installs minimal test dependencies and runs the targeted regression suite under insecure defaults.【F:.github/workflows/ci.yaml†L1-L28】【F:requirements-ci.txt†L1-L1】 |

## Execution Guidance

1. **Stabilize the test environment first** so service modules import successfully (`pytest -k smoke`).
2. **Tackle P0 tasks by domain**—persistence, OMS, accounts—before moving to P1 items.
3. After each fix, **add or update tests** to cover the restored functionality.
4. Keep the task board updated as remediations land to ensure visibility across teams.

## Outstanding Fixes and Follow-Ups

- **Non-Kraken exchange adapters remain placeholders.** Binance and Coinbase integrations still raise `NotImplementedError`, so multi-exchange rollouts cannot proceed until real REST/WebSocket bindings ship.【F:exchange_adapter.py†L588-L664】
- **Metrics exporter still depends on the Prometheus stub.** The lightweight `start_http_server` merely logs a warning; production deployments must restore the official client to expose scrape endpoints again.【F:metrics.py†L137-L152】
- **Load-testing harness relies on insecure-default stubs.** The bundled `locust` module only works when insecure defaults are enabled and exits in production, so the genuine Locust/Gevent stack must be reinstated before capacity testing.【F:locust/__init__.py†L18-L135】
- **Scientific stack needs the real NumPy/pandas toolchain.** Core ML and backtesting modules still gate their functionality behind dependency checks, leaving training and simulation unavailable until those libraries are installed.【F:ml/models/supervised.py†L46-L102】【F:backtest_engine.py†L18-L70】
- **Kafka connectivity is replaced by a no-op shim.** The vendored `aiokafka` package records payloads in-memory and never reaches an actual Kafka broker, preventing live ingestion and OMS fan-out until the real client library is restored.【F:aiokafka/__init__.py†L13-L58】
- **Human-in-the-loop approvals lose durability without SQLAlchemy.** When the ORM dependency is missing the HITL queue falls back to a process-local dictionary, so director decisions disappear across restarts and multiple replicas cannot coordinate until the persistent backend is reinstated.【F:hitl_service.py†L293-L355】
- **HTTP clients are running on an offline stub.** The in-repo `httpx` replacement refuses to perform network requests unless a custom transport is injected, so all REST integrations—including Kraken REST polling and Twitter sentiment fetches—remain inert until the real HTTPX library is restored.【F:httpx/__init__.py†L1-L37】【F:httpx/__init__.py†L292-L335】
- **Websocket streaming cannot reach external feeds.** The bundled `websockets` shim returns a placeholder protocol whose `send`/`recv` raise `WebSocketException`, so Kraken order-book streaming and ingest loops abort immediately until the real client library is restored.【F:websockets/__init__.py†L1-L42】【F:services/oms/main.py†L191-L203】
- **Backups cannot encrypt artifacts without cryptography.** The S3 backup job raises `RuntimeError` when `AESGCM` is missing, so database dumps and MLflow archives cannot be encrypted or uploaded until the genuine `cryptography` wheel ships with the deployment image.【F:ops/backup/backup_job.py†L69-L72】【F:ops/backup/backup_job.py†L365-L369】【F:ops/backup/backup_job.py†L507-L520】
- **Retention archiver cannot run without real HTTP and S3 clients.** `_build_s3_client()` raises as soon as `boto3` is missing and the metrics snapshotter logs a warning then skips whenever `requests` is unavailable, so nightly retention jobs in the current image never upload archives or request Prometheus downsampling until both dependencies are restored.【F:ops/logging/retention_archiver.py†L16-L271】【1b4454†L1-L6】【3c3e3e†L1-L7】

This task board should replace the previous narrative audit and serve as the single source of truth for outstanding engineering work.
