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

* **Security middleware regressions prevent admin enforcement.** `services/common/security.py` references `cast` from `typing` but never imports it, causing the module import to fail and knocking out `ensure_admin_access`, MFA checks, and other administrative guards relied upon by multiple services.【F:services/common/security.py†L1-L156】
* **Simulation-mode persistence is incompatible with the current SQLAlchemy version.** The repository calls `.limit(1)` on a 2.x style `Select` object, which raises during import and prevents safe-mode toggles from loading, violating the simulation safety requirement.【F:shared/sim_mode.py†L225-L256】
* **Existing audit findings remain unresolved.** The remediation task board documents P0 issues spanning database adapters, hedging safeguards, TLS enforcement, and observability; none of these fixes are present, leaving critical requirements unmet.【F:docs/AUDIT_REPORT.md†L1-L76】

Collectively, these issues demonstrate that the implementation cannot be trusted in production: the code does not pass its own tests, mandatory security controls fail to import, and previously catalogued P0 defects are outstanding.

### 6.3 Recommended next steps

1. **Restore a passing automated test baseline** by reintroducing the missing dependencies (`starlette`, `redis`, `psycopg`, etc.), fixing broken imports, and addressing SQLAlchemy API changes so the suite can execute end-to-end.
2. **Close the remediation backlog** captured in the audit report, prioritising P0 defects around OMS persistence, hedging overrides, TLS enforcement, and governance logging before tackling lower-priority work.【F:docs/AUDIT_REPORT.md†L5-L76】
3. **Document and attest compliance controls**—including USD-only market access, stop-loss enforcement, and credential segregation—once automated validations and runtime checks are green. Until these artefacts exist, the success criteria in Section 5 remain unmet.

Until the items above are addressed—and verified through repeatable automation—Aether_2 should **not** be promoted to production.
