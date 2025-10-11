# Production Readiness Review (Validated)

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ❌ Needs Fix | FastAPI workloads reference `fastapi-secrets` keys that are not rendered and the Secrets Service encryption key is managed out-of-band, so pods fail until operators hand-create secrets. 【F:deploy/k8s/base/fastapi/deployments.yaml†L27-L114】【F:deploy/k8s/base/secrets/external-secrets.yaml†L179-L204】【F:deploy/k8s/base/aether-services/secret-secrets-service-config.yaml†L1-L6】 |
| Reliability & Observability | ❌ Needs Fix | Prometheus rules target metric names that no service exports (OMS/policy/risk histograms, order-gateway counters, model canary histogram), leaving SLOs unsatisfied despite the kill-switch exporter being wired correctly. 【F:deploy/observability/prometheus/prometheus.yaml†L55-L152】【F:metrics.py†L525-L586】【F:kill_switch.py†L26-L175】【6f6b28†L1-L2】【4ecd18†L1-L2】【a8ce36†L1-L2】 |
| Security & Compliance | ⚠️ Follow Up | Network policies and Dockerfiles are hardened, but the encryption key for the Secrets Service is applied manually outside GitOps, creating drift risk that should be automated. 【F:deploy/k8s/networkpolicy.yaml†L1-L112】【F:deploy/docker/risk-api/Dockerfile†L1-L26】【F:deploy/k8s/base/aether-services/secret-secrets-service-config.yaml†L1-L6】【F:docs/runbooks/secrets-service-key-rotation.md†L1-L102】 |
| Testing & Release Engineering | ❌ Needs Fix | Required dependencies (pytest, pytest-asyncio, aiohttp, cryptography, prometheus_client) are pinned, but the test suite aborts during import because `ADMIN_ALLOWLIST` is read before the defaults from `conftest.py` apply. 【F:requirements-ci.txt†L1-L12】【F:services/common/security.py†L71-L136】【F:conftest.py†L24-L36】【3aaabe†L1-L19】 |
| Data Integrity & Backup | ✅ Ready | TimescaleDB and Redis run as StatefulSets with PVCs, the DR playbook bootstraps its log table automatically, and the backup job ships restore routines for TimescaleDB and MLflow artifacts. 【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L112】【F:deploy/k8s/base/redis/deployment.yaml†L1-L40】【F:dr_playbook.py†L442-L520】【F:tests/ops/test_dr_playbook.py†L157-L181】【F:ops/backup/backup_job.py†L520-L620】 |
| API & Integration Consistency | ✅ Ready | Exchange adapters expose spot operations with multi-exchange gating and FastAPI services register `/metrics` endpoints via shared middleware. 【F:exchange_adapter.py†L592-L696】【F:metrics.py†L891-L920】 |
| ML & Simulation Logic | ✅ Ready | Simulation defaults remain disabled for production and the sim-mode API enforces admin allowlists while publishing events and audit logs. 【F:config/system.yaml†L19-L34】【F:sim_mode.py†L1-L120】 |
| Account Isolation & Governance | ❌ Needs Fix | Services crash without the `platform-account-allowlists` secret that carries admin/director scopes, and the repository provides no ExternalSecret or sealed secret for it. 【F:deploy/helm/aether-platform/values.yaml†L41-L66】【b22d38†L1-L4】 |
| UI Integration & Frontend Connectivity | ✅ Ready | Secrets Service routes for status and audit feeds exist, matching Builder.io Fusion UI expectations. 【F:secrets_service.py†L864-L992】 |

## Architecture & Deployment

* `fastapi-secrets` only exposes JWT/API credentials. The FastAPI deployments pull `REDIS_URL`/`NATS_URL` keys from that secret, so the pods fail configuration resolution until those keys are added. 【F:deploy/k8s/base/fastapi/deployments.yaml†L27-L114】【F:deploy/k8s/base/secrets/external-secrets.yaml†L179-L204】
* The Secrets Service requires a `secrets-service-config` secret containing `SECRET_ENCRYPTION_KEY`, but the base manifest intentionally omits it. Operators currently follow the rotation runbook to apply it manually, leaving no GitOps trace. 【F:deploy/k8s/base/aether-services/secret-secrets-service-config.yaml†L1-L6】【F:docs/runbooks/secrets-service-key-rotation.md†L27-L92】

**Remediation Tasks**

* RMT-001 — Populate `REDIS_URL` and `NATS_URL` entries in the `fastapi-secrets` ExternalSecret so API pods receive all required connection strings. Files: `deploy/k8s/base/secrets/external-secrets.yaml`. Severity: Critical. Owner: Platform. Status: Pending.
* RMT-002 — Manage the Secrets Service encryption key through an ExternalSecret or sealed secret committed to GitOps overlays instead of manual applies. Files: `deploy/k8s/base/aether-services/secret-secrets-service-config.yaml`, overlays. Severity: Critical. Owner: Security Platform. Status: Pending.

## Reliability & Observability

* The Prometheus rule set queries histograms and counters named `oms_order_latency_seconds`, `policy_latency_ms`, `risk_latency_ms`, and `order_gateway_*`. The instrumentation library exports differently named histograms (`oms_submit_latency`, `policy_inference_latency`, `risk_validation_latency`) and no order-gateway metrics, so every alert stays silent. 【F:deploy/observability/prometheus/prometheus.yaml†L55-L152】【F:metrics.py†L525-L586】【4ecd18†L1-L2】【a8ce36†L1-L2】
* The `model_canary_promotion_duration_minutes_*` histogram referenced in alerts and dashboards is never emitted anywhere in the repository. 【F:deploy/observability/prometheus/prometheus.yaml†L138-L152】【6f6b28†L1-L2】
* The kill-switch service still exports `kill_switch_response_seconds`, so that SLO path remains valid once the alert queries are corrected. 【F:kill_switch.py†L26-L175】

**Remediation Tasks**

* RMT-003 — Align OMS, policy, and risk latency instrumentation with the Prometheus rule names (rename metrics or update alerts/dashboards accordingly). Files: `metrics.py`, `deploy/observability/prometheus/prometheus.yaml`, Grafana dashboards. Severity: High. Owner: Observability. Status: Pending.
* RMT-004 — Implement `model_canary_promotion_duration_minutes` (or adjust alerts to the emitted metric) so canary SLOs observe real data. Files: ML canary pipeline, `deploy/observability/prometheus/prometheus.yaml`. Severity: High. Owner: ML Ops. Status: Pending.
* RMT-005 — Add order-gateway exported counters (`*_orders_total`, `*_fee_spend_usd`, etc.) or prune the unused Prometheus rules to eliminate blind alerts. Files: Order Gateway service, Prometheus rules. Severity: High. Owner: OMS Team. Status: Pending.

## Security & Compliance

* All runtime network egress is restricted to the Kraken and CoinGecko ranges or the egress proxy, and Docker images drop root privileges. 【F:deploy/k8s/networkpolicy.yaml†L1-L112】【F:deploy/docker/risk-api/Dockerfile†L1-L26】
* The Secrets Service encryption key still depends on a manual workflow; the runbook is current but automating it would eliminate configuration drift. 【F:docs/runbooks/secrets-service-key-rotation.md†L27-L92】

**Remediation Tasks**

* RMT-006 — Automate application of the Secrets Service encryption key (e.g., via ExternalSecrets + Vault integration) to remove the manual step highlighted in the runbook. Files: `deploy/k8s/base/aether-services/secret-secrets-service-config.yaml`, platform secrets automation. Severity: Medium. Owner: Security Platform. Status: Pending.

## Testing & Release Engineering

* `requirements-ci.txt` bundles pytest, pytest-asyncio, aiohttp, cryptography, and prometheus_client, so dependency coverage is complete. 【F:requirements-ci.txt†L1-L12】
* `pytest --maxfail=1 --disable-warnings` aborts because importing `services.common.adapters` raises a `RuntimeError` before `conftest.py` seeds the allowlist environment variables. 【F:services/common/security.py†L71-L136】【F:conftest.py†L24-L36】【3aaabe†L1-L19】

**Remediation Tasks**

* RMT-007 — Delay the ADMIN/DIRECTOR allowlist enforcement until runtime (or seed defaults earlier) so pytest can import modules without hard-failing. Files: `services/common/security.py`, test harness. Severity: High. Owner: Platform. Status: Pending.

## Data Integrity & Backup

* TimescaleDB and Redis run as StatefulSets with PVC-backed storage, and TimescaleDB includes a nightly `pg_dump` CronJob. 【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L112】【F:deploy/k8s/base/redis/deployment.yaml†L1-L40】
* `_log_dr_action` now creates the `dr_log` table on demand, and unit tests cover that bootstrap path. 【F:dr_playbook.py†L442-L479】【F:tests/ops/test_dr_playbook.py†L157-L181】
* `ops/backup/backup_job.py` supplies AES-GCM backup/restore logic for TimescaleDB dumps and MLflow artifacts. 【F:ops/backup/backup_job.py†L520-L588】

## Account Isolation & Governance

* Helm values expect a `platform-account-allowlists` secret to feed admin and director scopes, yet no ExternalSecret or manifest ships with the repository, leaving startup dependent on manual secret creation. 【F:deploy/helm/aether-platform/values.yaml†L41-L66】【b22d38†L1-L4】

**Remediation Tasks**

* RMT-008 — Define an ExternalSecret (or sealed secret) for `platform-account-allowlists` so admin/director allowlists are provisioned through GitOps. Files: `deploy/k8s/base/secrets/`, Helm values. Severity: Critical. Owner: Governance. Status: Pending.

## UI Integration & Frontend Connectivity

* The Secrets Service exposes `/secrets/status`, `/secrets/audit`, and `/secrets/kraken/status`, matching the Builder.io Fusion frontend contract. 【F:secrets_service.py†L864-L992】

## Highlights

* Kill-switch automation includes Prometheus instrumentation and audit logging, so once the alert names align with the emitted histogram the SLO path is actionable again. 【F:kill_switch.py†L26-L175】
* Disaster-recovery automation now creates its log table automatically and the backup job contains documented restore steps, improving first-run resilience. 【F:dr_playbook.py†L442-L520】【F:ops/backup/backup_job.py†L520-L588】
* Strict network policies and non-root Docker images enforce the expected perimeter for ingest workloads. 【F:deploy/k8s/networkpolicy.yaml†L1-L112】【F:deploy/docker/risk-ingestor/Dockerfile†L1-L26】

## Ongoing Monitoring

* Track completion of RMT-001/002/008 to ensure all required secrets are delivered by GitOps before the next deployment window.
* Once RMT-003 through RMT-005 are complete, re-run Prometheus rule validation to confirm the corrected metrics emit data across staging.
* Re-run `pytest --maxfail=1 --disable-warnings` after addressing RMT-007 to keep the CI gate green. 【3aaabe†L1-L19】
