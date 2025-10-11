# Production Readiness Review (Validated)

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ❌ Needs Fix | FastAPI workloads reference `fastapi-secrets` keys that are not rendered, the Secrets Service encryption key is managed out-of-band, risk-service depends on an undefined `compliance-service-database` secret, the runtime expects TLS secrets (`kafka-tls`, `timescaledb-client-cert`, `aether-risk-tls`, etc.) that have no manifests, the Feast ExternalSecret applied from the base overlay drops the required `username`, the TLS configuration hard-codes ports that do not match the plaintext services that actually ship, and the capital allocator deployment dereferences an undeclared `capital-allocator-database` secret. 【F:deploy/k8s/base/fastapi/deployments.yaml†L27-L114】【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L204】【F:deploy/k8s/base/aether-services/secret-secrets-service-config.yaml†L1-L6】【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L41-L72】【F:deploy/k8s/base/fastapi/configmap.yaml†L12-L49】【F:deploy/k8s/base/ingress/ingress.yaml†L27-L41】【F:deploy/k8s/base/feast/external-secret.yaml†L1-L17】【F:deploy/k8s/base/feast/deployment.yaml†L54-L70】【F:deploy/k8s/base/timescaledb/service.yaml†L1-L11】【F:deploy/k8s/base/redis/service.yaml†L1-L11】【F:deploy/k8s/base/feast/service.yaml†L1-L12】【F:deploy/k8s/base/aether-services/deployment-capital-allocator.yaml†L1-L85】【b792c4†L1-L1】 |
| Reliability & Observability | ❌ Needs Fix | Prometheus rules target metric names that no service exports (OMS/policy/risk/pricing histograms, order-gateway counters, model canary histogram), the scrape job only keeps pods labeled `order-gateway|pricing-service|kill-switch` so risk/policy/secrets metrics never land, the Kraken ingest Deployment never exposes `/metrics`, there is no Service to route scrapes to the pod, and the on-call runbooks reference nonexistent `kill_switch_state` and `ws_sequence_gap_ratio` metrics, leaving SLOs and responders blind despite the kill-switch exporter being wired correctly. 【F:deploy/observability/prometheus/prometheus.yaml†L33-L176】【F:metrics.py†L525-L586】【F:kill_switch.py†L26-L175】【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L1-L80】【F:deploy/k8s/base/aether-services/deployment-secrets.yaml†L1-L80】【F:deploy/k8s/base/kraken-ws-ingest/deployment.yaml†L1-L40】【F:deploy/k8s/base/kraken-ws-ingest/kustomization.yaml†L1-L4】【F:services/kraken_ws_ingest.py†L1-L160】【F:docs/runbooks/kill_switch_activation.md†L1-L34】【F:docs/runbooks/websocket_desync.md†L1-L38】【6f6b28†L1-L2】【4ecd18†L1-L2】【a8ce36†L1-L2】【b12eb7†L1-L3】 |
| Security & Compliance | ❌ Needs Fix | Network policies and Dockerfiles are hardened, but the Secrets Service key and every Kraken API credential (`kraken-keys-*`) remain manual, and the Kafka/Zookeeper stack explicitly enables plaintext and anonymous access, so critical secrets and streaming data drift outside GitOps/TLS protections. 【F:deploy/k8s/networkpolicy.yaml†L1-L112】【F:deploy/docker/risk-api/Dockerfile†L1-L26】【F:deploy/k8s/base/aether-services/secret-secrets-service-config.yaml†L1-L6】【F:deploy/k8s/base/aether-services/deployment-oms.yaml†L92-L124】【F:deploy/k8s/base/kafka-nats/stack.yaml†L24-L182】【F:docs/runbooks/secrets-service-key-rotation.md†L1-L102】 |
| Testing & Release Engineering | ❌ Needs Fix | Required dependencies (pytest, pytest-asyncio, aiohttp, cryptography, prometheus_client) are pinned, but the test suite aborts during import because `ADMIN_ALLOWLIST` is read before the defaults from `conftest.py` apply. 【F:requirements-ci.txt†L1-L12】【F:services/common/security.py†L71-L136】【F:conftest.py†L24-L36】【3aaabe†L1-L19】 |
| Data Integrity & Backup | ✅ Ready | TimescaleDB and Redis run as StatefulSets with PVCs, the DR playbook bootstraps its log table automatically, and the backup job ships restore routines for TimescaleDB and MLflow artifacts. 【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L112】【F:deploy/k8s/base/redis/deployment.yaml†L1-L40】【F:dr_playbook.py†L442-L520】【F:tests/ops/test_dr_playbook.py†L157-L181】【F:ops/backup/backup_job.py†L520-L620】 |
| API & Integration Consistency | ✅ Ready | Exchange adapters expose spot operations with multi-exchange gating and FastAPI services register `/metrics` endpoints via shared middleware. 【F:exchange_adapter.py†L592-L696】【F:metrics.py†L891-L920】 |
| ML & Simulation Logic | ✅ Ready | Simulation defaults remain disabled for production and the sim-mode API enforces admin allowlists while publishing events and audit logs. 【F:config/system.yaml†L19-L34】【F:sim_mode.py†L1-L120】 |
| Account Isolation & Governance | ❌ Needs Fix | Services crash without the `platform-account-allowlists` secret that carries admin/director scopes, and the repository provides no ExternalSecret or sealed secret for it. 【F:deploy/helm/aether-platform/values.yaml†L41-L66】【b22d38†L1-L4】 |
| UI Integration & Frontend Connectivity | ✅ Ready | Secrets Service routes for status and audit feeds exist, matching Builder.io Fusion UI expectations. 【F:secrets_service.py†L864-L992】 |

## Architecture & Deployment

* `fastapi-secrets` only exposes JWT/API credentials. The FastAPI deployments pull `REDIS_URL`/`NATS_URL` keys from that secret, so the pods fail configuration resolution until those keys are added. 【F:deploy/k8s/base/fastapi/deployments.yaml†L27-L114】【F:deploy/k8s/base/secrets/external-secrets.yaml†L179-L204】
* The Secrets Service requires a `secrets-service-config` secret containing `SECRET_ENCRYPTION_KEY`, but the base manifest intentionally omits it. Operators currently follow the rotation runbook to apply it manually, leaving no GitOps trace. 【F:deploy/k8s/base/aether-services/secret-secrets-service-config.yaml†L1-L6】【F:docs/runbooks/secrets-service-key-rotation.md†L27-L92】
* Risk Service also needs `COMPLIANCE_DATABASE_URL` and `COMPLIANCE_DB_SSLMODE`, yet no ExternalSecret or sealed secret ships those values (`compliance-service-database` is undeclared), so the deployment dereferences a secret that Git never provisions. 【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L41-L72】【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L204】
* TLS configuration in `fastapi-config` references Kubernetes secrets for Kafka, NATS, TimescaleDB, Redis, and Feast client certificates (`kafka-tls`, `timescaledb-client-cert`, etc.), but the repository does not define any manifests for them. 【F:deploy/k8s/base/fastapi/configmap.yaml†L12-L49】
* The ingress for risk-service requires the `aether-risk-tls` secret, yet no ExternalSecret or certificate definition is present. 【F:deploy/k8s/base/ingress/ingress.yaml†L27-L41】
* The base Kustomization applies two `feast-offline-store` ExternalSecrets; the one in the Feast overlay drops the `username` field the deployment expects, so pods boot without credentials. 【F:deploy/k8s/base/feast/external-secret.yaml†L1-L17】【F:deploy/k8s/base/secrets/external-secrets.yaml†L85-L103】【F:deploy/k8s/base/feast/deployment.yaml†L54-L70】
* `fastapi-config` hard-codes TLS ports (TimescaleDB 5433, Redis 6380, Feast 6567) for services that only expose plaintext listeners (5432/6379/80) and sets Kafka/NATS TLS secrets even though the streaming stack only enables unauthenticated plaintext on ports 9092/4222, so traffic cannot connect until either the manifests or config are reconciled. 【F:deploy/k8s/base/fastapi/configmap.yaml†L12-L49】【F:deploy/k8s/base/timescaledb/service.yaml†L1-L11】【F:deploy/k8s/base/redis/service.yaml†L1-L11】【F:deploy/k8s/base/feast/service.yaml†L1-L12】【F:deploy/k8s/base/kafka-nats/stack.yaml†L24-L141】
* `capital-allocator` mounts `capital-allocator-database` credentials that are never rendered by any ExternalSecret, so the pods fail to resolve their Postgres DSN. 【F:deploy/k8s/base/aether-services/deployment-capital-allocator.yaml†L1-L85】【b792c4†L1-L1】

**Remediation Tasks**

* RMT-001 — Populate `REDIS_URL` and `NATS_URL` entries in the `fastapi-secrets` ExternalSecret so API pods receive all required connection strings. Files: `deploy/k8s/base/secrets/external-secrets.yaml`. Severity: Critical. Owner: Platform. Status: Pending.
* RMT-002 — Manage the Secrets Service encryption key through an ExternalSecret or sealed secret committed to GitOps overlays instead of manual applies. Files: `deploy/k8s/base/aether-services/secret-secrets-service-config.yaml`, overlays. Severity: Critical. Owner: Security Platform. Status: Pending.
* RMT-009 — Provide a GitOps-managed secret (`compliance-service-database`) for the Risk Service to source compliance DSNs/SSL flags. Files: `deploy/k8s/base/secrets/`, Vault config. Severity: Critical. Owner: Compliance Platform. Status: Pending.
* RMT-012 — Define ExternalSecrets (or cert-manager resources) for Kafka/NATS/Timescale/Redis/Feast TLS client secrets referenced by `fastapi-config` so API pods can establish encrypted connections. Files: `deploy/k8s/base/fastapi/configmap.yaml`, `deploy/k8s/base/secrets/`. Severity: Critical. Owner: Platform. Status: Pending.
* RMT-013 — Create a GitOps-managed TLS certificate (`aether-risk-tls`) for the risk ingress instead of relying on a manually provisioned secret. Files: `deploy/k8s/base/ingress/ingress.yaml`, `deploy/k8s/base/secrets/`. Severity: High. Owner: Platform. Status: Pending.
* RMT-015 — Consolidate the Feast ExternalSecret definitions so the rendered secret includes both `username` and `password` keys required by the deployment. Files: `deploy/k8s/base/feast/external-secret.yaml`, `deploy/k8s/base/secrets/external-secrets.yaml`. Severity: High. Owner: Data Platform. Status: Pending.
* RMT-016 — Align the TLS-enabled endpoints referenced in `fastapi-config` with the actual Kafka/NATS/TimescaleDB/Redis/Feast services (either enable TLS listeners on the clusters or adjust the config to the shipped ports). Files: `deploy/k8s/base/fastapi/configmap.yaml`, `deploy/k8s/base/kafka-nats/stack.yaml`, `deploy/k8s/base/timescaledb/service.yaml`, `deploy/k8s/base/redis/service.yaml`, `deploy/k8s/base/feast/service.yaml`. Severity: Critical. Owner: Platform. Status: Pending.
* RMT-021 — Render a GitOps-managed `capital-allocator-database` secret so the capital allocator pods can resolve their Postgres DSN. Files: `deploy/k8s/base/secrets/`, Vault config. Severity: Critical. Owner: Treasury Engineering. Status: Pending.

## Reliability & Observability

* The Prometheus rule set queries histograms and counters named `oms_order_latency_seconds`, `policy_latency_ms`, `risk_latency_ms`, `pricing_service_mid_price_delta`, and `ws_delivery_latency_seconds`. The instrumentation library exports differently named histograms (`oms_submit_latency`, `policy_inference_latency`, `risk_validation_latency`) and has no pricing drift, WebSocket delivery metric, or order-gateway counters, so every alert stays silent. 【F:deploy/observability/prometheus/prometheus.yaml†L33-L150】【F:metrics.py†L525-L586】【4ecd18†L1-L2】【a8ce36†L1-L2】
* The `model_canary_promotion_duration_minutes_*` histogram referenced in alerts and dashboards is never emitted anywhere in the repository. 【F:deploy/observability/prometheus/prometheus.yaml†L138-L150】【6f6b28†L1-L2】
* The kill-switch service still exports `kill_switch_response_seconds`, so that SLO path remains valid once the alert queries are corrected. 【F:kill_switch.py†L26-L175】
* `prometheus-additional-scrape` filters for pods whose `app` label matches `order-gateway|pricing-service|kill-switch`, so the annotated risk, policy, secrets, and other API deployments never get scraped despite advertising `/metrics`. 【F:deploy/observability/prometheus/prometheus.yaml†L163-L172】【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L1-L24】【F:deploy/k8s/base/aether-services/deployment-secrets.yaml†L1-L24】
* Runbooks and the on-call checklist expect `scaling_controller_evaluations_stalled` and `scaling_gpu_pool_idle` alerts, but the live Prometheus rule definition only includes the trading, fee, drift, circuit-breaker, and SLO alerts above—no scaling controller rules are shipped. 【F:docs/checklists/oncall.md†L11-L15】【F:deploy/observability/prometheus/prometheus.yaml†L33-L150】
* `kraken-ws-ingest` is deployed without a HTTP listener or Prometheus exporter, so it never services `/metrics` scrapes. 【F:deploy/k8s/base/kraken-ws-ingest/deployment.yaml†L1-L40】【F:services/kraken_ws_ingest.py†L1-L160】
* No Kubernetes Service targets `kraken-ws-ingest`, so even after adding a `/metrics` handler the Prometheus scrape configuration would have nothing to discover. 【F:deploy/k8s/base/kraken-ws-ingest/kustomization.yaml†L1-L4】
* The kill-switch activation runbook instructs responders to watch a `kill_switch_state` metric that the FastAPI exporter never emits, so alert verification steps 3 and the recovery checklist cannot be satisfied. 【F:docs/runbooks/kill_switch_activation.md†L1-L34】【F:kill_switch.py†L1-L160】
* WebSocket desynchronisation guidance leans on a `ws_sequence_gap_ratio` metric that does not exist anywhere in the codebase, leaving responders without the documented leading indicator. 【F:docs/runbooks/websocket_desync.md†L1-L38】【b12eb7†L1-L3】

**Remediation Tasks**

* RMT-003 — Align OMS, policy, and risk latency instrumentation with the Prometheus rule names (rename metrics or update alerts/dashboards accordingly). Files: `metrics.py`, `deploy/observability/prometheus/prometheus.yaml`, Grafana dashboards. Severity: High. Owner: Observability. Status: Pending.
* RMT-004 — Implement `model_canary_promotion_duration_minutes` (or adjust alerts to the emitted metric) so canary SLOs observe real data. Files: ML canary pipeline, `deploy/observability/prometheus/prometheus.yaml`. Severity: High. Owner: ML Ops. Status: Pending.
* RMT-005 — Add order-gateway exported counters (`*_orders_total`, `*_fee_spend_usd`, etc.) or prune the unused Prometheus rules to eliminate blind alerts. Files: Order Gateway service, Prometheus rules. Severity: High. Owner: OMS Team. Status: Pending.
* RMT-010 — Emit `ws_delivery_latency_seconds` (or update alerts/dashboards to the actual ingest metric) so WebSocket latency SLOs observe live data. Files: Kraken ingest services, `metrics.py`, Prometheus rules. Severity: High. Owner: Data Platform. Status: Pending.
* RMT-014 — Expose Prometheus metrics from `kraken-ws-ingest` (e.g., via `prometheus_client` HTTP server) so observability stacks can scrape ingestion health. Files: `services/kraken_ws_ingest.py`, `deploy/k8s/base/kraken-ws-ingest/deployment.yaml`. Severity: Medium. Owner: Data Platform. Status: Pending.
* RMT-017 — Add a ClusterIP Service for `kraken-ws-ingest` so Prometheus scrape jobs and other consumers can discover the pod endpoints. Files: `deploy/k8s/base/kraken-ws-ingest/`. Severity: Medium. Owner: Data Platform. Status: Pending.
* RMT-019 — Broaden Prometheus scrape selectors (or add ServiceMonitors) so risk, policy, secrets, and other annotated APIs are collected alongside FastAPI pods. Files: `deploy/observability/prometheus/prometheus.yaml`, ServiceMonitor manifests. Severity: High. Owner: Observability. Status: Pending.
* RMT-020 — Reinstate scaling controller Prometheus alerts (or update runbooks/checklists) so operational guides match the deployed rule set. Files: `deploy/observability/prometheus/prometheus.yaml`, `docs/checklists/oncall.md`. Severity: Medium. Owner: Platform Ops. Status: Pending.
* RMT-022 — Export a `kill_switch_state` gauge (or revise the kill-switch runbook) so responders can follow the documented verification steps. Files: `kill_switch.py`, `docs/runbooks/kill_switch_activation.md`. Severity: Medium. Owner: Risk Platform. Status: Pending.
* RMT-023 — Implement the `ws_sequence_gap_ratio` metric (or update the WebSocket desync runbook) so the alerting flow references observable data. Files: WebSocket ingest/exporter, `docs/runbooks/websocket_desync.md`. Severity: Medium. Owner: Data Platform. Status: Pending.

## Security & Compliance

* All runtime network egress is restricted to the Kraken and CoinGecko ranges or the egress proxy, and Docker images drop root privileges. 【F:deploy/k8s/networkpolicy.yaml†L1-L112】【F:deploy/docker/risk-api/Dockerfile†L1-L26】
* The Secrets Service encryption key still depends on a manual workflow; the runbook is current but automating it would eliminate configuration drift. 【F:docs/runbooks/secrets-service-key-rotation.md†L27-L92】
* Exchange credentials (`kraken-keys-company`, `kraken-keys-director-1`, `kraken-keys-director-2`) are consumed by OMS, policy, universe, risk, fees, and secrets services, yet no ExternalSecret manifests exist for them, so operators must craft Kubernetes secrets manually outside GitOps. 【F:deploy/k8s/base/aether-services/deployment-oms.yaml†L92-L124】【F:deploy/k8s/base/aether-services/deployment-policy.yaml†L78-L110】【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L204】
* The Kafka and Zookeeper manifests explicitly enable plaintext listeners and anonymous logins, conflicting with the TLS-only expectations in `fastapi-config`. 【F:deploy/k8s/base/kafka-nats/stack.yaml†L24-L182】【F:deploy/k8s/base/fastapi/configmap.yaml†L12-L49】

**Remediation Tasks**

* RMT-006 — Automate application of the Secrets Service encryption key (e.g., via ExternalSecrets + Vault integration) to remove the manual step highlighted in the runbook. Files: `deploy/k8s/base/aether-services/secret-secrets-service-config.yaml`, platform secrets automation. Severity: Medium. Owner: Security Platform. Status: Pending.
* RMT-011 — Deliver Kraken credential secrets (`kraken-keys-*`) through ExternalSecrets or sealed secrets so exchange access is GitOps-managed. Files: `deploy/k8s/base/secrets/`, Vault config. Severity: Critical. Owner: Trading Platform. Status: Pending.
* RMT-018 — Harden Kafka and Zookeeper by disabling plaintext/anonymous modes and supplying TLS credentials that align with the API configuration. Files: `deploy/k8s/base/kafka-nats/stack.yaml`, `deploy/k8s/base/secrets/`. Severity: Critical. Owner: Platform. Status: Pending.

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

* Track completion of RMT-001/002/008/009/011/012/013/015/016/018/021 so all mandatory secrets and TLS artifacts (FastAPI, Secrets Service, platform allowlists, compliance DSNs, Feast credentials, capital allocator DSNs, TLS client certs, Kraken API keys, ingress certificates, hardened streaming endpoints) are delivered by GitOps before the next deployment window.
* Once RMT-003 through RMT-005, RMT-010, RMT-014, and RMT-017 are complete, re-run Prometheus rule validation to confirm the corrected metrics emit data across staging.
* After delivering RMT-019/020/022/023, validate the on-call checklist and runbooks to ensure every documented metric and alert resolves to a live Prometheus series.
* Re-run `pytest --maxfail=1 --disable-warnings` after addressing RMT-007 to keep the CI gate green. 【3aaabe†L1-L19】
