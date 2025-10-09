# Aether_2 System Requirements (Spot-Only Production Version)

## 1. Core Purpose

Aether_2 is an autonomous AI-driven trading platform designed to operate exclusively on Kraken's spot market. The system continuously learns from historical CoinGecko data and live Kraken market data, executes trades with embedded risk and hedge management, and can run in either live or simulation mode. Three isolated trading accounts (one company account and two director-admin accounts) are supported.

## 2. Functional Requirements

### Trading

| ID | Requirement | Description |
|----|-------------|-------------|
| T-1 | Spot Trading Only | Trade exclusively on Kraken's spot market (no futures or leverage). |
| T-2 | USD Pairs | Support only USD-based pairs (e.g., BTC/USD, ETH/USD). |
| T-3 | Secure Connection | Use each account's own Kraken API key and secret stored in Kubernetes Secrets. |
| T-4 | Stop-Loss / Take-Profit | Automatically calculate and apply stop-loss and take-profit thresholds. |
| T-5 | Fee Awareness | Include Kraken trading fees in all calculations. |
| T-6 | Trade Logging | Log all trades with timestamp, price, quantity, and PnL. |
| T-7 | API Safety | Handle API rate limits, network timeouts, and order validation errors gracefully. |

### Learning & Adaptation

| ID | Requirement | Description |
|----|-------------|-------------|
| L-1 | Historical Data | Automatically pull OHLCV data from CoinGecko for training. |
| L-2 | Live Learning | Continuously learn from Kraken real-time data. |
| L-3 | Model Training | Support retraining and auto-updating of machine learning models. |
| L-4 | Market Regime Detection | Detect market conditions (bullish, bearish, sideways). |
| L-5 | Auto Adaptation | Adjust strategy thresholds dynamically based on live data performance. |

### Simulation Mode

| ID | Requirement | Description |
|----|-------------|-------------|
| S-1 | Safe Mode | Simulation mode must prevent all live Kraken trades. |
| S-2 | SimBroker | Use internal simulated fills and virtual PnL generation. |
| S-3 | Toggle | Allow enabling/disabling simulation mode via API or UI. |
| S-4 | Account Scoped | Manage simulation state per account (not global). |

### Hedging

| ID | Requirement | Description |
|----|-------------|-------------|
| H-1 | USD Hedge | Maintain USD-based spot hedge (no derivatives). |
| H-2 | Auto Hedge | Automatically adjust hedge percentage based on volatility and drawdown. |
| H-3 | Manual Override | Allow admins to set manual hedge percentage via UI. |
| H-4 | Hedge Reporting | Display current hedge ratio and history in dashboard. |

### Risk Management

| ID | Requirement | Description |
|----|-------------|-------------|
| R-1 | Stop-Loss / Take-Profit | Enforce configured thresholds for each trade. |
| R-2 | Position Sizing | Dynamically size positions based on balance and volatility. |
| R-3 | Exposure Limits | Restrict exposure to individual pairs or correlated assets. |
| R-4 | Drawdown Protection | Disable trading after breaching maximum daily drawdown. |
| R-5 | Balance Awareness | Never exceed available Kraken balance per account. |
| R-6 | Fee Consideration | Always trade net of expected fees and slippage. |

### Accounts

| ID | Requirement | Description |
|----|-------------|-------------|
| A-1 | Multi-Account Support | Support one company account and two director accounts. |
| A-2 | Isolated Data | Maintain fully separate data and configurations per account. |
| A-3 | Unique API Keys | Each account uses its own Kraken API key and secret. |
| A-4 | Role = Admin | Directors are admin users with full management privileges. |
| A-5 | Secure Secrets | Store credentials in Kubernetes Secrets only. |
| A-6 | Account Management UI | Allow setup and configuration via Builder.io Fusion UI. |

### Governance & Audit

| ID | Requirement | Description |
|----|-------------|-------------|
| G-1 | Action Logging | Log every key action (trade, hedge change, training, API update). |
| G-2 | Audit Trail | Record timestamp, user_id, account_id, and action details. |
| G-3 | Governance Page | Display audit records in Governance UI. |
| G-4 | Immutable Logs | Prevent modification or deletion of audit data. |

### Reporting

| ID | Requirement | Description |
|----|-------------|-------------|
| RP-1 | Daily PnL | Calculate daily profit/loss percentage per account. |
| RP-2 | Trade History | Generate trade history and exportable CSV reports. |
| RP-3 | Risk Metrics | Display drawdown, exposure, and volatility metrics. |
| RP-4 | Hedge Report | Include current hedge position and performance. |
| RP-5 | Performance Overview | Show live profit trend and return curve in UI. |

### User Interface (Builder.io Fusion)

| ID | Requirement | Description |
|----|-------------|-------------|
| UI-1 | Onboarding Wizard | Guide users through initial account setup if none exist. |
| UI-2 | Account Dashboard | List all accounts, balances, and statuses. |
| UI-3 | Account Configuration | Edit trading, risk, hedge, and simulation settings. |
| UI-4 | API Key Upload | Upload and validate Kraken API keys securely. |
| UI-5 | Hedge Controls | Adjust hedge percentage and toggle auto/manual modes. |
| UI-6 | Audit & Logs | View system logs and governance actions in the application. |
| UI-7 | Role Access | Restrict configuration features to admin (director) roles. |

### Infrastructure & Deployment

| ID | Requirement | Description |
|----|-------------|-------------|
| D-1 | Kubernetes (LKE) | Deploy all components via Helm or Kustomize. |
| D-2 | HTTPS | Encrypt all API and UI traffic with TLS. |
| D-3 | Secrets Management | Use Kubernetes Secrets for API keys and credentials. |
| D-4 | Service Health | Expose `/healthz` and `/metrics` endpoints for each service. |
| D-5 | Monitoring | Integrate Prometheus and Grafana or Linode metrics. |
| D-6 | Fault Tolerance | Auto-restart failed pods; ensure stateless services recover automatically. |
| D-7 | Namespacing | Run all Aether_2 services in a dedicated Kubernetes namespace. |

## 3. Non-Functional Requirements

| Category | Requirement |
|----------|-------------|
| Security | Encrypt all secrets; disallow plaintext keys; require HTTPS. |
| Performance | Maintain trade execution latency below one second on average. |
| Scalability | Support additional accounts without redesign. |
| Reliability | Monitor all services; enable automatic failover and restarts. |
| Maintainability | Keep code modularized and documented; ensure all routes are typed. |
| Auditability | Log every trade, hedge adjustment, and setting change immutably. |
| Compliance | Enforce spot-only trading with no derivatives or leveraged trades. |

## 4. Environment & Dependencies

| Component | Technology |
|-----------|------------|
| Backend | FastAPI (Python 3.11+). |
| Database | PostgreSQL / TimescaleDB. |
| Cache | Redis (optional). |
| ML Framework | PyCaret / scikit-learn. |
| Frontend | Builder.io Fusion (React). |
| Deployment | Linode Kubernetes (Helm / Kustomize). |
| External APIs | Kraken (Spot only), CoinGecko. |
| Monitoring | Prometheus + Grafana / Linode Metrics. |

## 5. Success Criteria

| ID | Success Metric | Expected Outcome |
|----|----------------|------------------|
| SC-1 | Spot-only trading | All pairs verified as Kraken Spot USD pairs. |
| SC-2 | Risk control | Stop-loss, take-profit, and balance checks active. |
| SC-3 | Hedge stability | Auto and manual hedge adjustments function correctly. |
| SC-4 | Simulation safety | No live trades when simulation is enabled. |
| SC-5 | Data isolation | Each account's data remains completely separate. |
| SC-6 | Live readiness | All pods healthy, `/healthz` OK, TLS active. |
| SC-7 | Learning loop | AI retrains on historical and live data. |
| SC-8 | Testing | All unit and integration tests pass successfully. |

## 6. Production Readiness Assessment

The specification above describes the target end state, but the current repository still diverges materially from those expectations. Direct inspection of the codebase and the automated test suite surfaces critical gaps that must be resolved before the platform can be considered production-ready.

### 6.1 Implementation evidence snapshot

| Capability | Observation | Evidence | Status |
|------------|-------------|----------|--------|
| Spot-market scope | The simulated broker normalizes instruments to Kraken spot symbols and rejects anything that is not recognised as spot, protecting both live and simulated order flow. | `SimBroker` enforces `is_spot_symbol` when placing orders.【F:services/oms/sim_broker.py†L82-L156】 | ✅ Implemented in code |
| USD trading universe | Default configuration seeds only USD-quoted assets (BTC, ETH, SOL and USD stablecoins), aligning with the spot-only charter. | Stablecoin monitor and diversification buckets list USD pairs exclusively.【F:config/system.yaml†L1-L29】 | ✅ Implemented in config |
| Risk exits | The exit rule engine builds stop-loss, take-profit, and trailing-stop orders for every eligible entry, but the behaviour currently lacks automated regression coverage. | Exit orchestration logic registers mandatory protective orders.【F:services/risk/exit_rules.py†L82-L133】 | ⚠️ Implementation exists; validation missing |
| Account-scoped secrets | Helm values expect three dedicated Kraken secret mounts (company, director-1, director-2), ensuring credentials remain isolated per account. | Deployment values map each account to its own Kubernetes secret reference.【F:deploy/helm/aether-platform/values.yaml†L1-L53】 | ⚠️ Deployment contract defined; runtime verification pending |

### 6.2 Blocking gaps observed

* **Automated testing is failing catastrophically.** A fresh run of the test suite (`pytest -q`) aborts during collection with import errors, missing dependencies, incompatible SQLAlchemy usage, and 92 total errors, demonstrating that the repository cannot currently validate its behaviour.

  ```text
  $ pytest -q
  E   ImportError: cannot import name 'get_timescale_session' from 'services.common.config'
  E   ImportError: cannot import name 'ensure_admin_access' from 'services.common.security'
  ...
  E   AttributeError: '_SelectStatement' object has no attribute 'limit'
  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Interrupted: 92 errors during collection !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  ```

* **Administrator guard crashes when invoked.** `services/common/security.py` calls `cast(...)` without importing it; invoking `_get_session_store` therefore raises `NameError`, preventing `ensure_admin_access` and the rest of the FastAPI dependency stack from authorising administrative requests. The infrastructure scaling API inherits this dependency and now fails every request with a 500 response, leaving operators unable to audit or adjust autoscaling behaviour.【F:services/common/security.py†L118-L176】【535722†L1-L24】【7ea8cb†L1-L103】
* **Alert deduplication endpoints raise 500s at import time.** The alerts router wraps FastAPI decorators with `cast(...)`, but `services/alerts/alert_dedupe.py` never imports `typing.cast`, so the module throws a `NameError` during import and prevents `/alerts` from registering at all, breaking operational alert suppression in both simulation and production modes.【F:services/alerts/alert_dedupe.py†L1-L113】【b4acd5†L1-L17】
* **Simulation-mode persistence silently downgrades to a stub.** Importing `shared.sim_mode` currently fails with `ModuleNotFoundError: No module named 'sqlalchemy'`; the OMS catches that exception and swaps in an in-memory stub that never writes to Timescale or enforces account-scoped simulation switches, violating the safety requirements for live isolation.【7e276b†L1-L6】【F:services/oms/oms_service.py†L77-L175】
* **Timescale session helpers have no safe defaults for local testing.** Calling `get_timescale_session("company")` without hand-crafted environment variables immediately raises `RuntimeError`, so any service importing the helper during tests or local runs aborts before it can inject sqlite fallbacks or dependency overrides.【F:services/common/config.py†L145-L202】【89615a†L1-L6】
* **Existing audit findings remain unresolved.** The remediation task board documents P0 issues spanning database adapters, hedging safeguards, TLS enforcement, and observability; none of these fixes are present, leaving critical requirements unmet.【F:docs/AUDIT_REPORT.md†L1-L76】
* **Core dependencies are missing from the runtime environment.** Security tests crash immediately because `starlette.requests` cannot be imported, signalling that Starlette is absent even though FastAPI components rely on it for request objects and middleware bindings. The secrets service middleware also imports `starlette.types` at module load, so the entire secrets API fails before startup until the dependency is restored.【a87cd4†L1-L13】【F:services/secrets/middleware.py†L1-L58】【a2c64c†L1-L16】
* **Sequencer serialization is incompatible with the shipped Pydantic version.** `TradingSequencer.process_intent` still calls `BaseModel.model_dump(mode="json")`, but under Pydantic v2 that signature rejects the `mode` keyword, so every intent processing path fails before any trade orchestration can occur.【F:services/core/sequencer.py†L220-L270】【3839a3†L1-L44】
* **Signal graph analytics cannot boot without NetworkX.** The signal graph service imports `networkx` directly, yet the dependency is absent from the runtime image, causing every signal graph request (and its associated security checks) to fail with `ModuleNotFoundError` and leaving feature attribution auditing offline.【F:signal_graph.py†L22-L66】【9d7e30†L1-L13】
* **Reporting endpoints lack the mandatory PostgreSQL driver.** `services.reports.report_service` imports `psycopg2` at module load time, but the package is missing, so the system health and reconciliation APIs crash immediately, disabling daily PnL generation and observability of capital movements.【F:services/reports/report_service.py†L1-L120】【fc378c†L1-L13】
* **Transaction cost analysis API cannot import under Python 3.12.** The fallback shim in `tca_service.py` monkey-patches `sqlalchemy.select` to keep the in-memory session working when SQLAlchemy is partially available, but Python rejects the reassignment because the replacement code object has a different closure shape. As a result the module import aborts with `ValueError: _select() requires a code object with 1 free vars, not 0`, leaving the `/tca` endpoints offline and preventing TCA audits altogether.【F:tca_service.py†L279-L294】【243d8b†L1-L128】
* **SQLAlchemy 2.x APIs break the simulation-mode repository.** `shared.sim_mode` still calls `.limit(...)` on the legacy `Select` object; under SQLAlchemy 2.x the modern `Select` returned by `select()` lacks that method, so module import fails before any OMS component can read or toggle the simulation flag.【ae1b21†L1-L18】【F:shared/sim_mode.py†L228-L279】
* **Auth service initialisation crashes on new SQLAlchemy URLs.** The engine options helper expects `URL.get_backend_name()`, but SQLAlchemy 2.x replaces this accessor; constructing the ORM engine therefore raises `AttributeError`, preventing authentication services from starting at all.【d54b8d†L1-L15】【F:auth_service.py†L452-L486】
* **Configuration service falls over without SQLAlchemy installed.** In lightweight environments the module swaps to the in-memory engine, but `reset_state()` still calls `Base.metadata.drop_all`; with the fallback declarative base this resolves to a bare `SimpleNamespace` that lacks `drop_all`, so the API cannot boot or serve requests when SQLAlchemy is absent.【b7b18a†L1-L61】【F:config_service.py†L389-L395】【F:config_service.py†L43-L58】
* **Shadow OMS cannot load alongside the compiled backtest engine.** The module rewrites `_SingleOrderPolicy.__bases__` at import time to inherit from the real `Policy`, but the compiled extension rejects the reassignment with a layout mismatch, so every OMS import halts with `TypeError` before routing or execution logic can load.【2f286e†L1-L13】【F:services/oms/shadow_oms.py†L45-L109】
* **Universe service cannot even import.** The production trading universe backend leaves a bare `try:` block when SQLAlchemy is available, so Python raises a `SyntaxError` before any whitelist or audit models can load, preventing the deployment from compiling the allowed spot instruments list entirely.【3f831c†L1-L21】【F:universe_service.py†L202-L252】
* **Universe startup crashes when SQLAlchemy's URL helpers are missing.** The module imports `URL` from `sqlalchemy.engine`, but the lightweight SQLAlchemy shim bundled with the runtime omits that export. When the service boots it raises `ImportError: cannot import name 'URL'`, so even environments with valid Timescale credentials cannot start the universe API or enforce the USD spot whitelist until the dependency is restored.【F:services/universe/universe_service.py†L37-L96】【882a8a†L1-L47】
* **Strategy registry falls over when SQLAlchemy is absent.** The in-memory session shim that stands in for SQLAlchemy during lightweight deployments lacks the `.scalar()` helper that `StrategyRegistry.register()` depends on. As a result default strategies never persist, `route_trade_intent()` raises `StrategyNotFound`, and the registry cannot protect the spot-only trading flow without a full SQLAlchemy install.【F:strategy_orchestrator.py†L200-L288】【F:tests/conftest.py†L420-L501】【2670e7†L1-L78】
* **Capital flow migrations cannot run without full SQLAlchemy.** The fallback engine stub injected when SQLAlchemy is missing exposes no `dialect`, so the migration helper dereferences `engine.dialect.name` and crashes during import, leaving capital flow upgrades unusable in constrained environments.【8ff9b2†L1-L32】【F:capital_flow_migrations.py†L37-L47】
* **Capital flow API downgrades to volatile memory storage.** When SQLAlchemy is absent the service instantiates `_InMemoryStore`, resets it on every engine dispose, and services each request from process-local state. Deposits, withdrawals, and NAV baselines therefore vanish on restart and never reach Timescale, violating the capital governance requirements.【F:capital_flow.py†L232-L334】
* **Kraken order-book ingestion cannot bootstrap without SQLAlchemy tooling.** The data ingestion pipeline relies on `sqlalchemy.engine.create_mock_engine` to capture generated SQL, but the lightweight SQLAlchemy shim never exposes that helper, so the import fails immediately and spot market depth updates cannot be persisted or validated.【F:tests/data/test_kraken_ws.py†L1-L58】【6b78b5†L1-L15】
* **Risk correlation service silently accepts SQLite in production.** `_database_url()` enables SQLite any time `pytest` is present in `sys.modules`; because the test harness itself reintroduces `pytest`, the guard never fires, and the service will happily normalize a SQLite DSN even when running outside of tests, violating the Timescale dependency requirements.【730cfe†L1-L18】【F:services/risk/correlation_service.py†L72-L100】
* **Sentiment ingestion hard-crashes without SQLAlchemy.** The optional dependency shim still instantiates `BigInteger().with_variant(...)`; when SQLAlchemy is absent this resolves to the stub `_Type` object that lacks `with_variant`, so importing `sentiment_ingest` raises `AttributeError` before any service or worker can start.【a1f4a2†L1-L12】【F:sentiment_ingest.py†L379-L411】
* **Seasonality analytics service decorators are undefined.** The module applies `@_app_on_event` and `@_app_get` even though the helper functions are never imported when the FastAPI test stub is active, so every import path fails with `NameError`, preventing seasonal analytics endpoints from booting.【6d46b0†L1-L40】【F:services/analytics/seasonality_service.py†L820-L905】
* **Operational tests flag missing PyYAML dependency.** The OMS network policy test skips outright because the runtime image omits the `yaml` module, signalling that the deployment manifest tooling cannot render Kubernetes resources as required.【9c5b13†L1-L5】
* **Authentication metrics cannot be initialised.** The auth service exposes Prometheus counters but never defines the `_init_metrics` helper expected by the runtime and tests, so the module cannot reset or register metrics safely and every metrics test fails before execution, leaving login observability dark.【F:auth/service.py†L128-L161】【bba4a3†L56-L142】【1db3df†L1-L26】
* **Multi-factor authentication cannot boot without PyOTP.** The authentication service imports `pyotp` unconditionally to issue and verify TOTP secrets, but the dependency is absent from the runtime image. Any component that touches the auth service—including the sequencer performance harness—therefore aborts with `ModuleNotFoundError`, leaving MFA unusable and blocking every downstream flow that depends on an authenticated session.【F:auth/service.py†L18-L58】【091022†L1-L18】
* **Admin password hashing downgrades to PBKDF2 when argon2 is missing.** In environments without `argon2-cffi` the authentication service swaps to `_FallbackPasswordHasher`, generating `$pbkdf2-sha256$…` hashes and omitting the `_Argon2PasswordHasher` wrapper altogether. Legacy login upgrades therefore never promote credentials to Argon2, and contract tests now fail with `AssertionError` and `AttributeError` when they expect Argon2-specific features like `check_needs_rehash`.【F:auth/service.py†L24-L125】【007e83†L219-L244】【6b2569†L41-L120】
* **Prometheus shim hides MFA denials.** The bundled `prometheus_client.Counter.collect()` implementation yields bare `Sample` objects instead of metric families, so the MFA denial counter always reads as zero even after `_MFA_DENIED_COUNTER.inc()` executes. The administrative login test confirms that the metric never increments, meaning operational dashboards would miss every blocked login until the real Prometheus client is restored.【F:prometheus_client/__init__.py†L1-L114】【74c85e†L1-L41】
* **Prometheus fallback registry cannot be reset between processes.** `CollectorRegistry` in the shim omits the `_names_to_collectors` map exposed by the real client, so health checks that clear the default registry crash with `AttributeError`, leaving the admin session store unable to verify persistence across worker restarts.【F:prometheus_client/__init__.py†L23-L118】【909e34†L1-L26】
* **Load testing coverage is missing its tooling.** The Kraken throughput harness skips entirely because `locust` is not installed, so the production team has no automated way to validate latency or concurrency regressions before a release.【8bc04a†L1-L4】
* **Portfolio analytics fixtures crash before executing assertions.** The portfolio service unit suite references `sys.modules` from a fixture but the module never imports `sys`, causing every test to fail with `NameError` and leaving portfolio NAV and exposure calculations unvalidated.【F:tests/unit/test_portfolio_service.py†L47-L56】【6aad45†L1-L31】
* **OMS rate-limit telemetry breaks without Prometheus.** When `prometheus_client` is absent the metrics fallback constructs a dummy `CollectorRegistry` with no `.register` method; importing the OMS rate-limit guard then crashes because counters receive a bare object instead of a registry, preventing the OMS from starting in lightweight or misconfigured environments.【F:metrics.py†L14-L58】【6221f8†L1-L13】
* **Risk service coverage is disabled whenever SQLAlchemy is absent.** The entire `tests/test_risk_service.py` suite skips because the SQLAlchemy dependency toggle reports that the ORM is missing, meaning risk exposure, drawdown, and stop enforcement logic run completely unvalidated in the shipped environment.【177688†L1-L9】
* **Risk limits configuration omits production accounts.** `config.risk_limits._STUB_LIMITS` only defines `ACC-DEFAULT`, so `_load_account_limits("company")` raises `ConfigError`. Every `/risk/validate` call now returns 404 and the whitelist endpoint cannot load data, leaving drawdown and exposure controls unenforced for real accounts.【F:config/risk_limits.py†L1-L39】【c9e258†L1-L140】
* **Training service cannot persist job metadata without full SQLAlchemy.** `training_service.session_scope()` yields the fallback session object when the lightweight SQLAlchemy shim is active, but the stub lacks `.merge`. As soon as `/ml/train/start` or `/ml/train/promote` run, the API raises `AttributeError` and never records training runs, so the ML lifecycle is entirely untracked until the real ORM is restored.【F:training_service.py†L254-L344】【e83ce6†L1-L120】
* **Human-in-the-loop queue persistence silently downgrades.** When SQLAlchemy 2.x is installed the HITL bootstrapper still expects sessions to expose `.query()` and `.get()`; failing that check triggers the warning path that tears down the partially initialised engine and swaps in an in-memory queue store. Manual approvals therefore vanish on restart, undermining required supervisory controls for large trades.【F:hitl_service.py†L330-L377】【1cff9c†L1-L164】
* **Advisor service history evaporates without SQLAlchemy.** Lacking the ORM, `advisor_service` swaps to `_InMemoryEngine`, clears its store on `drop_all`, and satisfies session requests with per-process in-memory objects, so every Q&A transcript disappears whenever the API recycles and audit trails for human requests cannot be reconstructed.【F:advisor_service.py†L184-L348】
* **Admin session store silently reverts to an in-memory stub when Redis is missing.** `create_redis_from_url` falls back to `InMemoryRedis` whenever the `redis` package or server is unavailable, and `build_session_store_from_url` still registers it as the production session backend. In the bundled runtime the `redis` dependency is absent, so admin sessions are stored in process memory and vanish on restart, violating the platform’s security and auditing requirements.【F:common/utils/redis.py†L237-L274】【F:auth/service.py†L382-L441】【0b12fb†L1-L1】
* **Compliance whitelist migrations cannot create tables.** Importing the compliance filter executes `run_compliance_migrations`, but under the lightweight SQLAlchemy shim the declarative models never expose `__table__`, so `Base.metadata.create_all(... ComplianceAsset.__table__)` raises `AttributeError` before any restricted symbols can persist or replicate across replicas.【F:compliance_filter.py†L132-L146】【225f5b†L1-L44】
* **Sanctions ingestion fails to boot without real SQLAlchemy.** `compliance_scanner.py` imports `sqlalchemy.delete` at module load; the stub used during test and local runs omits that API, so every import aborts with `ImportError` and the automated sanctions refresh pipeline never starts.【F:compliance_scanner.py†L1-L99】【7dca9f†L1-L70】
* **Reporting stack ships without pandas.** Core reporting tests skip entirely because `pandas` is absent from the runtime environment, leaving PnL aggregation and report configuration unvalidated and unusable in production builds.【0359bc†L1-L4】【2b28fe†L1-L4】
* **Weekly explainability audit logs drop structured metadata.** `generate_weekly_xai` forwards contextual metadata to `ArtifactStorage.store_artifact`, but the resulting audit insert never exposes a `metadata` field to downstream consumers, causing the explainability pipeline to fail its own verification and preventing auditors from reconstructing report provenance.【F:reports/weekly_xai.py†L144-L184】【F:reports/storage.py†L36-L115】【4e213f†L1-L40】
* **Fee schedule service hard-depends on SQLAlchemy internals.** `services/fees/fee_service.py` imports `sqlalchemy.sql.schema.Table` and other ORM primitives directly; in the current runtime the lightweight SQLAlchemy shim omits the entire `sqlalchemy.sql` package, so every import raises `ModuleNotFoundError` and the fee endpoints, tier reports, and admin security checks all crash before startup.【F:services/fees/fee_service.py†L11-L87】【b43219†L1-L152】
* **OMS metrics clients cannot import the shared registry.** While `metrics.py` defines a module-level `_REGISTRY`, the symbol is not exported via `__all__`, so `from metrics import _REGISTRY` raises `ImportError` and the OMS rate-limit guard aborts during import. The warm-start integration test confirms the failure, blocking reconciliation and throttle enforcement workflows until the metrics module exposes the registry explicitly.【F:metrics.py†L1-L80】【F:metrics.py†L1025-L1058】【558277†L1-L21】
* **Kafka/NATS event bridge never reaches the brokers.** `KafkaNATSAdapter._attempt_publish` returns `"offline"` whenever both transports fail to bootstrap, causing publishes to buffer silently instead of raising `PublishError`. The integration harness shows that no Kafka messages are emitted and retries never fire, leaving OMS, watchdog, and sequencing flows without broker fan-out in the current build.【F:services/common/adapters.py†L1222-L1320】【2c965e†L1-L33】
* **Meta strategy falls back to heuristic weights without the scientific stack.** The meta-allocation service only trains the intended logistic-regression pipeline when NumPy, pandas, and scikit-learn are installed. All three dependencies are missing in the production image, forcing `_FallbackClassifier` to return static, frequency-based weights and undermining regime-aware allocation for director dashboards.【F:ml/policy/meta_strategy.py†L16-L146】【1e89b6†L1-L3】
* **Cross-asset analytics never initialises its database guard.** Importing `services.analytics.crossasset_service` leaves `DATABASE_URL` at `None` because `_resolve_database_url()` is only invoked from the FastAPI startup hook. When the module loads outside a live server—such as during batch jobs—the service neither raises the expected runtime error nor normalises the configured DSN, so the cross-asset API starts without any database backing.【F:services/analytics/crossasset_service.py†L209-L284】【a67fb6†L1-L34】
* **Cross-asset models lose their SQLAlchemy tables under the stubbed ORM.** With the lightweight SQLAlchemy shim active, `OhlcvBar` and `CrossAssetMetric` never receive a real `__table__`, so fixture setup crashes before creating `ohlcv_bars` and `crossasset_metrics`. The analytics service therefore cannot persist or query historical bars, leaving every `/signals/crossasset` route offline.【F:services/analytics/crossasset_service.py†L118-L169】【a1385c†L1-L44】
* **Timescale market data adapter cannot read order books without real SQLAlchemy.** The stubbed engine used in the current runtime returns a `SimpleNamespace` from `execute`, which lacks `.first()` and `.all()`. As soon as `TimescaleMarketDataAdapter.order_book_snapshot` or `.recent_trades` run, the adapter raises `AttributeError` and the order-flow analytics pipeline dies before serving any signals.【F:services/analytics/market_data_store.py†L140-L204】【4dcaea†L1-L26】【a1385c†L110-L134】
* **Capital allocator sheds every allocation snapshot without Postgres.** In environments lacking SQLAlchemy the allocator creates an in-memory engine whose `dispose()` resets `_AllocatorStore`, so NAV curves and allocation history never persist past a process restart. That breaks director-level allocation audits and violates the capital oversight requirements.【F:capital_allocator.py†L69-L309】【F:capital_allocator.py†L398-L420】
* **Secondary market data monitoring depends on an unavailable httpx feature.** The alt-data monitor, health checker, and anomaly detector all expect to inject `httpx.MockTransport` so they can test failover behaviour and validate price freshness, but the shipped httpx build omits that transport class. Every unit test exercising the Kaiko/Amberdata client now fails with `AttributeError`, demonstrating that the secondary-feed watchdog cannot be verified and would run without tested failover logic in production.【F:alt_data.py†L1-L190】【a807f3†L1-L38】
* **PostgreSQL oversight services ship without the required psycopg driver.** Mission-critical modules—including the watchdog coordinator, OMS reconciliation tasks, HITL approvals, and the audit logger—normalise DSNs to `postgresql+psycopg://…`, yet the runtime image excludes `psycopg` entirely. Every integration suite guarding those flows skips at import time, so no persistence, veto, or reconciliation pathways are exercised and the corresponding production services would fail to start.【F:watchdog.py†L1-L86】【4b478f†L1-L4】【3eda9a†L1-L4】【6b0140†L1-L4】
* **Audit and regulatory log exports fail without boto3.** The log exporter, pack exporter, and compliance evidence tooling all call `_s3_client()` which raises `MissingDependencyError` when `boto3` is absent; in the current environment the package is not installed, so required governance artifacts cannot be pushed to object storage.【F:logging_export.py†L13-L234】【b26707†L1-L1】
* **CoinGecko market data ingest cannot start without HTTP and validation libraries.** The batch job defers to `requests` and SQLAlchemy for downloading metrics and persisting whitelist rows, while the ML data loader requires pandas, requests, SQLAlchemy, and Great Expectations for validation; the runtime image ships with none of these dependencies, so both ingestion pipelines immediately raise `MissingDependencyError` and never populate the USD universe or training sets.【F:data/ingest/coingecko_job.py†L17-L152】【F:ml/training/data_loader_coingecko.py†L21-L159】【ea2e11†L1-L14】
* **Live Kraken order-book ingestion is hard-blocked by missing aiohttp.** The websocket subscriber guards the dependency with `_require_aiohttp()`, and because the runtime omits `aiohttp` altogether the import path raises `MissingDependencyError("aiohttp is required for Kraken ingest")`, leaving spot-depth persistence and market data fan-out offline.【F:data/ingest/kraken_ws.py†L17-L107】【ea2e11†L1-L9】
* **Kill-switch notifications fail whenever `requests` is absent.** Email, SMS, and webhook dispatchers call `_require_requests()` before posting alerts; in the current environment `_REQUESTS_MODULE` is `None`, so every notification path raises `MissingDependencyError("requests is required for kill switch notifications")` and directors receive no emergency escalation when trading must halt.【F:kill_alerts.py†L13-L160】【ea2e11†L1-L4】
* **Core ML workflows ship without their scientific stack.** Reinforcement-learning fallbacks require `torch`, hyper-parameter optimisation depends on NumPy, pandas, and Optuna, and even basic feature loaders expect the same stack; because all of these packages are missing the training, tuning, and inference paths raise `MissingDependencyError` before executing, leaving the adaptive trading strategy frozen to static heuristics.【F:ml/models/rl.py†L75-L194】【F:ml/hpo/optuna_runner.py†L15-L200】【F:ml/training/data_loader_coingecko.py†L21-L159】【ea2e11†L1-L8】

Collectively, these issues demonstrate that the implementation cannot be trusted in production: the code does not pass its own tests, mandatory security controls fail to import, and previously catalogued P0 defects are outstanding.

### 6.3 Recommended next steps

1. **Restore a passing automated test baseline** by reintroducing the missing dependencies (`starlette`, `redis`, `psycopg`, etc.), fixing broken imports, and addressing SQLAlchemy API changes so the suite can execute end-to-end.
2. **Close the remediation backlog** captured in the audit report, prioritising P0 defects around OMS persistence, hedging overrides, TLS enforcement, and governance logging before tackling lower-priority work.【F:docs/AUDIT_REPORT.md†L5-L76】
3. **Document and attest compliance controls**—including USD-only market access, stop-loss enforcement, and credential segregation—once automated validations and runtime checks are green. Until these artefacts exist, the success criteria in Section 5 remain unmet.

Until the items above are addressed—and verified through repeatable automation—Aether_2 should **not** be promoted to production.
