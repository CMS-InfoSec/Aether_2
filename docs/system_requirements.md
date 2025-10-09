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
* **Core dependencies are missing from the runtime environment.** Security tests crash immediately because `starlette.requests` cannot be imported, signalling that Starlette is absent even though FastAPI components rely on it for request objects and middleware bindings.【a87cd4†L1-L13】
* **Sequencer serialization is incompatible with the shipped Pydantic version.** `TradingSequencer.process_intent` still calls `BaseModel.model_dump(mode="json")`, but under Pydantic v2 that signature rejects the `mode` keyword, so every intent processing path fails before any trade orchestration can occur.【F:services/core/sequencer.py†L220-L270】【3839a3†L1-L44】
* **Signal graph analytics cannot boot without NetworkX.** The signal graph service imports `networkx` directly, yet the dependency is absent from the runtime image, causing every signal graph request (and its associated security checks) to fail with `ModuleNotFoundError` and leaving feature attribution auditing offline.【F:signal_graph.py†L22-L66】【9d7e30†L1-L13】
* **Reporting endpoints lack the mandatory PostgreSQL driver.** `services.reports.report_service` imports `psycopg2` at module load time, but the package is missing, so the system health and reconciliation APIs crash immediately, disabling daily PnL generation and observability of capital movements.【F:services/reports/report_service.py†L1-L120】【fc378c†L1-L13】
* **Transaction cost analysis API cannot import under Python 3.12.** The fallback shim in `tca_service.py` monkey-patches `sqlalchemy.select` to keep the in-memory session working when SQLAlchemy is partially available, but Python rejects the reassignment because the replacement code object has a different closure shape. As a result the module import aborts with `ValueError: _select() requires a code object with 1 free vars, not 0`, leaving the `/tca` endpoints offline and preventing TCA audits altogether.【F:tca_service.py†L279-L294】【243d8b†L1-L128】
* **SQLAlchemy 2.x APIs break the simulation-mode repository.** `shared.sim_mode` still calls `.limit(...)` on the legacy `Select` object; under SQLAlchemy 2.x the modern `Select` returned by `select()` lacks that method, so module import fails before any OMS component can read or toggle the simulation flag.【ae1b21†L1-L18】【F:shared/sim_mode.py†L228-L279】
* **Auth service initialisation crashes on new SQLAlchemy URLs.** The engine options helper expects `URL.get_backend_name()`, but SQLAlchemy 2.x replaces this accessor; constructing the ORM engine therefore raises `AttributeError`, preventing authentication services from starting at all.【d54b8d†L1-L15】【F:auth_service.py†L452-L486】
* **Configuration service falls over without SQLAlchemy installed.** In lightweight environments the module swaps to the in-memory engine, but `reset_state()` still calls `Base.metadata.drop_all`; with the fallback declarative base this resolves to a bare `SimpleNamespace` that lacks `drop_all`, so the API cannot boot or serve requests when SQLAlchemy is absent.【b7b18a†L1-L61】【F:config_service.py†L389-L395】【F:config_service.py†L43-L58】
* **Shadow OMS cannot load alongside the compiled backtest engine.** The module rewrites `_SingleOrderPolicy.__bases__` at import time to inherit from the real `Policy`, but the compiled extension rejects the reassignment with a layout mismatch, so every OMS import halts with `TypeError` before routing or execution logic can load.【2f286e†L1-L13】【F:services/oms/shadow_oms.py†L45-L109】
* **Universe service cannot even import.** The production trading universe backend leaves a bare `try:` block when SQLAlchemy is available, so Python raises a `SyntaxError` before any whitelist or audit models can load, preventing the deployment from compiling the allowed spot instruments list entirely.【3f831c†L1-L21】【F:universe_service.py†L202-L252】
* **Strategy registry falls over when SQLAlchemy is absent.** The in-memory session shim that stands in for SQLAlchemy during lightweight deployments lacks the `.scalar()` helper that `StrategyRegistry.register()` depends on. As a result default strategies never persist, `route_trade_intent()` raises `StrategyNotFound`, and the registry cannot protect the spot-only trading flow without a full SQLAlchemy install.【F:strategy_orchestrator.py†L200-L288】【F:tests/conftest.py†L420-L501】【2670e7†L1-L78】
* **Capital flow migrations cannot run without full SQLAlchemy.** The fallback engine stub injected when SQLAlchemy is missing exposes no `dialect`, so the migration helper dereferences `engine.dialect.name` and crashes during import, leaving capital flow upgrades unusable in constrained environments.【8ff9b2†L1-L32】【F:capital_flow_migrations.py†L37-L47】
* **Kraken order-book ingestion cannot bootstrap without SQLAlchemy tooling.** The data ingestion pipeline relies on `sqlalchemy.engine.create_mock_engine` to capture generated SQL, but the lightweight SQLAlchemy shim never exposes that helper, so the import fails immediately and spot market depth updates cannot be persisted or validated.【F:tests/data/test_kraken_ws.py†L1-L58】【6b78b5†L1-L15】
* **Risk correlation service silently accepts SQLite in production.** `_database_url()` enables SQLite any time `pytest` is present in `sys.modules`; because the test harness itself reintroduces `pytest`, the guard never fires, and the service will happily normalize a SQLite DSN even when running outside of tests, violating the Timescale dependency requirements.【730cfe†L1-L18】【F:services/risk/correlation_service.py†L72-L100】
* **Sentiment ingestion hard-crashes without SQLAlchemy.** The optional dependency shim still instantiates `BigInteger().with_variant(...)`; when SQLAlchemy is absent this resolves to the stub `_Type` object that lacks `with_variant`, so importing `sentiment_ingest` raises `AttributeError` before any service or worker can start.【a1f4a2†L1-L12】【F:sentiment_ingest.py†L379-L411】
* **Seasonality analytics service decorators are undefined.** The module applies `@_app_on_event` and `@_app_get` even though the helper functions are never imported when the FastAPI test stub is active, so every import path fails with `NameError`, preventing seasonal analytics endpoints from booting.【6d46b0†L1-L40】【F:services/analytics/seasonality_service.py†L820-L905】
* **Operational tests flag missing PyYAML dependency.** The OMS network policy test skips outright because the runtime image omits the `yaml` module, signalling that the deployment manifest tooling cannot render Kubernetes resources as required.【9c5b13†L1-L5】
* **Authentication metrics cannot be initialised.** The auth service exposes Prometheus counters but never defines the `_init_metrics` helper expected by the runtime and tests, so the module cannot reset or register metrics safely and every metrics test fails before execution, leaving login observability dark.【F:auth/service.py†L128-L161】【bba4a3†L56-L142】【1db3df†L1-L26】
* **Risk service coverage is disabled whenever SQLAlchemy is absent.** The entire `tests/test_risk_service.py` suite skips because the SQLAlchemy dependency toggle reports that the ORM is missing, meaning risk exposure, drawdown, and stop enforcement logic run completely unvalidated in the shipped environment.【177688†L1-L9】
* **Human-in-the-loop queue persistence silently downgrades.** When SQLAlchemy 2.x is installed the HITL bootstrapper still expects sessions to expose `.query()` and `.get()`; failing that check triggers the warning path that tears down the partially initialised engine and swaps in an in-memory queue store. Manual approvals therefore vanish on restart, undermining required supervisory controls for large trades.【F:hitl_service.py†L330-L377】【1cff9c†L1-L164】

Collectively, these issues demonstrate that the implementation cannot be trusted in production: the code does not pass its own tests, mandatory security controls fail to import, and previously catalogued P0 defects are outstanding.

### 6.3 Recommended next steps

1. **Restore a passing automated test baseline** by reintroducing the missing dependencies (`starlette`, `redis`, `psycopg`, etc.), fixing broken imports, and addressing SQLAlchemy API changes so the suite can execute end-to-end.
2. **Close the remediation backlog** captured in the audit report, prioritising P0 defects around OMS persistence, hedging overrides, TLS enforcement, and governance logging before tackling lower-priority work.【F:docs/AUDIT_REPORT.md†L5-L76】
3. **Document and attest compliance controls**—including USD-only market access, stop-loss enforcement, and credential segregation—once automated validations and runtime checks are green. Until these artefacts exist, the success criteria in Section 5 remain unmet.

Until the items above are addressed—and verified through repeatable automation—Aether_2 should **not** be promoted to production.
