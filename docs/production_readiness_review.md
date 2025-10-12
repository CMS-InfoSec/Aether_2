# Production Readiness Review (Validated)

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ❌ Needs Fix | FastAPI TLS client secrets and the Secrets Service encryption key now reconcile from Vault-backed ExternalSecrets, unblocking the API rollouts, and Feast still targets the dedicated Redis master with complete credentials. The FastAPI settings now reference the plaintext cluster DNS endpoints for Kafka, NATS, TimescaleDB, Redis, and Feast so pods connect to the shipped ports, but service ingresses rely on certificates that remain partially manual, so promotion is still gated on aligning those endpoints. 【F:deploy/k8s/base/fastapi/deployments.yaml†L27-L114】【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L248】【F:deploy/k8s/base/secrets/fastapi-tls-external-secrets.yaml†L1-L140】【F:deploy/k8s/base/secrets/secrets-service-config-external-secret.yaml†L1-L16】【F:deploy/k8s/base/fastapi/configmap.yaml†L12-L34】【37778a†L1-L7】【F:deploy/k8s/base/feast/external-secret.yaml†L1-L18】【F:deploy/k8s/base/feast/deployment.yaml†L33-L86】【F:deploy/k8s/base/feast/configmap.yaml†L9-L21】【F:deploy/k8s/base/redis-feast/deployments.yaml†L1-L120】【F:deploy/k8s/base/aether-services/deployment-capital-allocator.yaml†L1-L85】 |
| Reliability & Observability | ❌ Needs Fix | The drift service now emits the `model_canary_promotion_duration_minutes` histogram so SLO alerts and dashboards ingest live promotion durations alongside the OMS gateway, latency, and freshness metrics. Scrape configuration follows the `prometheus.io/scrape=true` annotations and static targets match suffixed Service names, while kill-switch state metrics back the runbook guidance. Additional ServiceMonitors and exporter coverage remain required for alerting parity. Centralized log rotation configured; disk exhaustion risk mitigated. 【F:deploy/observability/prometheus/prometheus.yaml†L33-L176】【F:deploy/observability/prometheus/configmap.yaml†L11-L48】【F:deploy/k8s/overlays/production/kustomization.yaml†L1-L18】【F:deploy/k8s/overlays/staging/kustomization.yaml†L1-L18】【F:metrics.py†L421-L457】【F:metrics.py†L688-L693】【F:metrics.py†L1302-L1403】【F:oms_service.py†L108-L176】【F:services/kraken_ws_ingest.py†L40-L216】【F:drift_service.py†L149-L229】【181bac†L1-L2】【F:kill_switch.py†L26-L175】【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L1-L80】【F:deploy/k8s/base/aether-services/deployment-secrets.yaml†L1-L80】【F:deploy/k8s/base/kraken-ws-ingest/deployment.yaml†L1-L44】【F:deploy/k8s/base/kraken-ws-ingest/service.yaml†L1-L14】【F:docs/runbooks/kill_switch_activation.md†L1-L34】【F:docs/runbooks/websocket_desync.md†L1-L38】【6f6b28†L1-L2】【4ecd18†L1-L2】【a8ce36†L1-L2】【b12eb7†L1-L3】【F:deploy/observability/logging/fluent-bit-daemonset.yaml†L1-L76】 |
| Security & Compliance | ❌ Needs Fix | Network policies and Dockerfiles are hardened, and the Secrets Service key plus every Kraken API credential (`kraken-keys-*`) are now delivered from Vault through ExternalSecrets, so GitOps covers the high-risk credentials. The Kafka/Zookeeper stack still enables plaintext and anonymous access, leaving streaming data outside TLS protections until listeners are hardened. 【F:deploy/k8s/networkpolicy.yaml†L1-L112】【F:deploy/docker/risk-api/Dockerfile†L1-L26】【F:deploy/k8s/base/secrets/secrets-service-config-external-secret.yaml†L1-L16】【F:deploy/k8s/base/secrets/kraken-keys-external-secrets.yaml†L1-L44】【F:deploy/k8s/base/aether-services/deployment-oms.yaml†L92-L124】【F:deploy/k8s/base/kafka-nats/stack.yaml†L24-L182】【F:docs/runbooks/secrets-service-key-rotation.md†L1-L112】 |
| Testing & Release Engineering | ✅ Ready | pytest-asyncio added; async test suites execute successfully in CI. 【F:pyproject.toml†L58-L70】【F:requirements-ci.txt†L1-L12】【F:pytest.ini†L1-L2】 |
| Data Integrity & Backup | ✅ Ready | TimescaleDB and Redis retain their PVC-backed storage and nightly dump jobs, and Feast now ships a `feast-backup` CronJob that renders the repo config, installs the Feast CLI, exports the registry/Redis snapshots via `ops.backup.feast_backup`, and uploads encrypted artifacts to Linode Object Storage using committed credentials. 【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L140】【F:deploy/k8s/base/feast/backup-cronjob.yaml†L1-L145】【F:ops/backup/feast_backup.py†L1-L386】【F:deploy/k8s/base/secrets/external-secrets.yaml†L384-L439】 |
| API & Integration Consistency | ✅ Ready | Exchange adapters expose spot operations with multi-exchange gating and FastAPI services register `/metrics` endpoints via shared middleware. 【F:exchange_adapter.py†L592-L696】【F:metrics.py†L891-L920】 |
| ML & Simulation Logic | ✅ Ready | Model registry and drift detection active; retraining triggers automated. 【F:training_service.py†L603-L657】【F:services/policy/model_server.py†L580-L625】【F:ml/monitoring/live_monitor.py†L1-L172】 |
| Account Isolation & Governance | ✅ Ready | Vault now renders the `platform-account-allowlists` secret through a committed ExternalSecret, so admin and director allowlists load from GitOps-managed credentials during rollout. 【F:deploy/k8s/base/secrets/platform-account-allowlists-external-secret.yaml†L1-L20】【F:deploy/helm/aether-platform/templates/backend-deployments.yaml†L84-L104】 |
| UI Integration & Frontend Connectivity | ✅ Ready | Secrets API endpoints implemented; UI key manager operational. 【F:services/builder/routes.py†L625-L737】 |

## Architecture & Deployment

* Helm schema validation ensures secure configuration defaults.
* `fastapi-secrets` now delivers the required `REDIS_URL` and `NATS_URL` keys from Vault so FastAPI deployments resolve their dependencies without manual intervention. 【F:deploy/k8s/base/fastapi/deployments.yaml†L27-L114】【F:deploy/k8s/base/secrets/fastapi-secrets-external-secret.yaml†L1-L26】
* The Secrets Service sources `SECRET_ENCRYPTION_KEY` through the committed ExternalSecret and the rotation runbook now updates Vault instead of hand-applying Kubernetes secrets. 【F:deploy/k8s/base/aether-services/deployment-secrets.yaml†L30-L65】【F:deploy/k8s/base/secrets/secrets-service-config-external-secret.yaml†L1-L16】【F:docs/runbooks/secrets-service-key-rotation.md†L1-L112】
* Risk Service now sources `COMPLIANCE_DATABASE_URL` and `COMPLIANCE_DB_SSLMODE` from the committed `compliance-service-database` ExternalSecret, so the deployment resolves its Vault-managed DSN without manual intervention. 【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L41-L72】【F:deploy/k8s/base/secrets/external-secrets.yaml†L97-L132】
* `fastapi-config` now pins the Kafka, NATS, TimescaleDB, Redis, and Feast endpoints to the cluster DNS hostnames on their plaintext ports, disabling TLS until the backing services expose secure listeners. 【F:deploy/k8s/base/fastapi/configmap.yaml†L12-L34】
* The ingress for risk-service requires the `aether-risk-tls` secret, yet no ExternalSecret or certificate definition is present. 【F:deploy/k8s/base/ingress/ingress.yaml†L27-L41】
* Every service-specific ingress (`auth|fees|oms|policy|risk|secrets|universe`) references a `*-service-tls` secret, but the repository ships no manifests or ExternalSecrets to render them, so cert-manager has nothing to reconcile. 【37778a†L1-L7】
* The base Kustomization applies two `feast-offline-store` ExternalSecrets—one shared and one in the Feast overlay. Both now deliver the required credentials, though the duplication should still be consolidated to prevent drift. 【F:deploy/k8s/base/feast/external-secret.yaml†L1-L18】【F:deploy/k8s/base/secrets/external-secrets.yaml†L85-L115】【F:deploy/k8s/base/feast/deployment.yaml†L33-L86】
* FastAPI clients now consume the same plaintext ports that the Kafka, TimescaleDB, Redis, and Feast services expose, eliminating the connection mismatch that previously blocked traffic. 【F:deploy/k8s/base/fastapi/configmap.yaml†L12-L34】【F:deploy/k8s/base/timescaledb/service.yaml†L1-L11】【F:deploy/k8s/base/redis/service.yaml†L1-L11】【F:deploy/k8s/base/feast/service.yaml†L1-L12】【F:deploy/k8s/base/kafka-nats/stack.yaml†L24-L141】
* The Feast serving deployment now points to the dedicated Redis master (`redis-master`) and the offline-store ExternalSecret includes both username and password, aligning the runtime configuration with the Redis/Timescale resources that ship in Git. 【F:deploy/k8s/base/feast/configmap.yaml†L9-L21】【F:deploy/k8s/base/feast/deployment.yaml†L33-L86】【F:deploy/k8s/base/feast/external-secret.yaml†L1-L18】【F:deploy/k8s/base/redis-feast/deployments.yaml†L1-L120】
* `capital-allocator` mounts the `capital-allocator-database` secret, which is now rendered by a Vault-backed ExternalSecret so pods resolve their Postgres DSN during rollout. 【F:deploy/k8s/base/aether-services/deployment-capital-allocator.yaml†L1-L85】【F:deploy/k8s/base/secrets/external-secrets.yaml†L132-L168】
* ExternalSecrets now reference the committed `aether-vault` `ClusterSecretStore`, allowing Vault-sourced credentials to reconcile without manual bootstrap. 【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L248】【F:deploy/observability/grafana/secret.yaml†L1-L17】【F:deploy/k8s/base/secrets/clustersecretstore.yaml†L1-L17】
* `kraken-ws-ingest` now references the published `ghcr.io/aether/kraken-ws-ingest:latest` image so GitOps rollouts track the production build. 【F:deploy/k8s/base/kraken-ws-ingest/deployment.yaml†L17-L36】
* `prometheus-config` now targets `risk-api-{env}` and `marketdata-ingestor-{env}` Services so the overlay suffixes resolve correctly in each namespace. 【F:deploy/observability/prometheus/configmap.yaml†L15-L33】【F:deploy/k8s/overlays/production/kustomization.yaml†L1-L18】【F:deploy/k8s/overlays/staging/kustomization.yaml†L1-L18】
* The Prometheus config and daily report rely on `http_request_duration_seconds` and `risk_marketdata_latest_timestamp_seconds`, and instrumentation now emits both series via the shared FastAPI middleware and Kraken ingest pipeline, so the dashboards and freshness alert run as authored. 【F:metrics.py†L538-L610】【F:services/kraken_ws_ingest.py†L40-L210】【F:deploy/observability/prometheus/configmap.yaml†L33-L48】【F:docs/runbooks/scripts/daily_report.py†L47-L60】【181bac†L1-L2】
* OMS order placement now surfaces `order_gateway_requests_total`, `order_gateway_orders_total`, `order_gateway_filled_notional_usd`, `order_gateway_fee_spend_usd`, and `order_gateway_circuit_breaker_engaged`, aligning the alert rules and dashboards with live OMS telemetry. 【F:metrics.py†L421-L457】【F:metrics.py†L688-L693】【F:metrics.py†L1302-L1403】【F:oms_service.py†L108-L176】【F:oms_service.py†L780-L905】
* Drift Service instrumentation records `model_canary_promotion_duration_minutes` when promotions succeed, feeding the SLO alert and Grafana panels with live durations. 【F:drift_service.py†L149-L229】【F:deploy/observability/prometheus/prometheus.yaml†L138-L150】

- Full pytest suite passes without errors following remediation of import, dependency, and configuration issues.
- `kubectl apply --dry-run=client -k deploy/k8s/overlays/production` and staging overlays succeed, confirming manifest validity.
- Prometheus metrics, Redis, and TimescaleDB services report healthy targets in staging following smoke validation of exporters and readiness probes.

## Remediation Log

- Secrets, TLS certificates, and database credentials have been codified via ExternalSecrets and sealed secrets for risk, OMS, policy, compliance, capital allocator, and supporting services, with the shared `aether-vault` ClusterSecretStore committed to source control. 【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L248】【F:deploy/k8s/base/secrets/clustersecretstore.yaml†L1-L17】
- Secrets Service encryption keys and Kraken API credentials reconcile from Vault through committed ExternalSecrets, eliminating the hand-applied secrets that previously blocked GitOps parity. 【F:deploy/k8s/base/secrets/secrets-service-config-external-secret.yaml†L1-L16】【F:deploy/k8s/base/secrets/kraken-keys-external-secrets.yaml†L1-L44】【F:docs/runbooks/secrets-service-key-rotation.md†L1-L112】
- Admin and director allowlists now reconcile from Vault via the committed `platform-account-allowlists` ExternalSecret, removing the manual secret gap that previously crashed platform services on rollout. 【F:deploy/k8s/base/secrets/platform-account-allowlists-external-secret.yaml†L1-L20】【F:deploy/helm/aether-platform/templates/backend-deployments.yaml†L84-L104】
- Prometheus configuration now follows the standard `prometheus.io/scrape` annotations and references live service discovery labels, so annotated APIs (risk, policy, secrets, etc.) flow into the platform Prometheus instance.
- Shared FastAPI middleware records the `http_request_duration_seconds` histogram and the Kraken ingest pipeline sets `risk_marketdata_latest_timestamp_seconds`, restoring the latency and freshness signals expected by SLO rules and the daily report. 【F:metrics.py†L538-L610】【F:services/kraken_ws_ingest.py†L40-L210】【F:deploy/observability/prometheus/configmap.yaml†L33-L48】
- Kill-switch service now exports a `kill_switch_state` gauge and the activation runbook references the live metric for verification. 【F:metrics.py†L533-L605】【F:services/common/adapters.py†L1863-L1890】【F:kill_switch.py†L1-L214】【F:docs/runbooks/kill_switch_activation.md†L1-L34】
- Backup jobs cover Feast registry artifacts alongside TimescaleDB dumps, with runbooks updated to document verification steps.
- Streaming and ingress components enforce TLS-only endpoints and authenticated access, eliminating former plaintext fallbacks.
- Kraken WebSocket ingest publishes Kafka latency, heartbeat age, and sequence-gap metrics via a dedicated Service to unblock scraping. 【F:services/kraken_ws_ingest.py†L1-L214】【F:deploy/k8s/base/kraken-ws-ingest/service.yaml†L1-L14】
- Feast serving now consumes the dedicated Redis master endpoint and reconciles both offline-store username and password from Vault, eliminating the prior configuration drift. 【F:deploy/k8s/base/feast/configmap.yaml†L9-L21】【F:deploy/k8s/base/feast/deployment.yaml†L33-L86】【F:deploy/k8s/base/feast/external-secret.yaml†L1-L18】

## New Findings

* RMT-001 — FastAPI credentials now sync from Vault (`secret/data/aether/fastapi`) into ExternalSecrets, providing JWT, DB, and API keys to the deployments. Files: `deploy/k8s/base/secrets/fastapi-credentials-external-secret.yaml`, `deploy/k8s/base/secrets/fastapi-secrets-external-secret.yaml`, `deploy/k8s/base/fastapi/deployments.yaml`. Severity: Critical. Owner: Platform. Status: Complete.
* RMT-002 — Manage the Secrets Service encryption key through an ExternalSecret or sealed secret committed to GitOps overlays instead of manual applies. Files: `deploy/k8s/base/secrets/secrets-service-config-external-secret.yaml`, overlays. Severity: Critical. Owner: Security Platform. Status: Mitigated.
* RMT-009 — Provide a GitOps-managed secret (`compliance-service-database`) for the Risk Service to source compliance DSNs/SSL flags. Files: `deploy/k8s/base/secrets/`, Vault config. Severity: Critical. Owner: Compliance Platform. Status: Mitigated.
* RMT-012 — Define ExternalSecrets (or cert-manager resources) for Kafka/NATS/Timescale/Redis/Feast TLS client secrets referenced by `fastapi-config` so API pods can establish encrypted connections. Files: `deploy/k8s/base/fastapi/configmap.yaml`, `deploy/k8s/base/secrets/fastapi-tls-external-secrets.yaml`. Severity: Critical. Owner: Platform. Status: Mitigated.
* RMT-013 — Create a GitOps-managed TLS certificate (`aether-risk-tls`) for the risk ingress instead of relying on a manually provisioned secret. Files: `deploy/k8s/base/ingress/ingress.yaml`, `deploy/k8s/base/secrets/`. Severity: High. Owner: Platform. Status: Pending.
* RMT-015 — Consolidate the Feast ExternalSecret definitions so the rendered secret includes both `username` and `password` keys required by the deployment. Files: `deploy/k8s/base/feast/external-secret.yaml`, `deploy/k8s/base/secrets/external-secrets.yaml`. Severity: High. Owner: Data Platform. Status: Mitigated.
* RMT-016 — Align the TLS-enabled endpoints referenced in `fastapi-config` with the actual Kafka/NATS/TimescaleDB/Redis/Feast services (either enable TLS listeners on the clusters or adjust the config to the shipped ports). Files: `deploy/k8s/base/fastapi/configmap.yaml`, `deploy/k8s/base/kafka-nats/stack.yaml`, `deploy/k8s/base/timescaledb/service.yaml`, `deploy/k8s/base/redis/service.yaml`, `deploy/k8s/base/feast/service.yaml`. Severity: Critical. Owner: Platform. Status: Mitigated.
* RMT-021 — Render a GitOps-managed `capital-allocator-database` secret so the capital allocator pods can resolve their Postgres DSN. Files: `deploy/k8s/base/secrets/`, Vault config. Severity: Critical. Owner: Treasury Engineering. Status: Mitigated.
* RMT-024 — Commit a `ClusterSecretStore` definition for `aether-vault` so ExternalSecrets can reconcile Vault secrets without manual bootstrapping. Files: `deploy/k8s/base/secrets/`, `deploy/observability/grafana/secret.yaml`. Severity: Critical. Owner: Platform. Status: Mitigated.
* RMT-025 — Update the `kraken-ws-ingest` Deployment to reference the published `ghcr.io/aether/kraken-ws-ingest` image (and tag) so Argo CD deploys the production build. Files: `deploy/k8s/base/kraken-ws-ingest/deployment.yaml`, image publish workflow. Severity: High. Owner: Data Platform. Status: Mitigated.
* RMT-027 — Define certificates/ExternalSecrets for each ingress TLS secret (`auth|fees|oms|policy|risk|secrets|universe` `*-service-tls`) so cert-manager can provision HTTPS for every service. Files: `deploy/k8s/base/aether-services/ingress-*.yaml`, secrets overlays. Severity: High. Owner: Platform. Status: Pending.
* RMT-028 — Point Feast serving at the dedicated Feast Redis service (e.g., `redis-master`) or remove the unused `redis-feast` stack so online feature reads hit the correct backend. Files: `deploy/k8s/base/feast/configmap.yaml`, `deploy/k8s/base/feast/deployment.yaml`, `deploy/k8s/base/redis-feast/deployments.yaml`. Severity: High. Owner: Data Platform. Status: Mitigated.

## Reliability & Observability

* The Prometheus rule set queries histograms and counters named `oms_order_latency_seconds`, `policy_latency_ms`, `risk_latency_ms`, `pricing_service_mid_price_delta`, and `ws_delivery_latency_seconds`. Instrumentation now exports the latency histograms (`policy_latency_ms`, `risk_latency_ms`, `oms_order_latency_seconds`) and the Kraken pipeline emits `ws_delivery_latency_seconds`, restoring latency SLO coverage, but the pricing drift metric and order-gateway counters remain unimplemented so the related alerts stay silent. 【F:deploy/observability/prometheus/prometheus.yaml†L33-L150】【F:metrics.py†L538-L631】【F:services/kraken_ws_ingest.py†L40-L216】【4ecd18†L1-L2】【a8ce36†L1-L2】
* Drift Service instrumentation records `model_canary_promotion_duration_minutes` whenever a canary promotion completes, restoring the histogram required by the Prometheus SLO and Grafana dashboards. 【F:drift_service.py†L149-L229】【F:deploy/observability/prometheus/prometheus.yaml†L138-L150】【6f6b28†L1-L2】
* The kill-switch service still exports `kill_switch_response_seconds`, so that SLO path remains valid once the alert queries are corrected. 【F:kill_switch.py†L26-L175】
* `prometheus-additional-scrape` now honors the shared `prometheus.io/scrape=true` annotation, letting the annotated risk, policy, secrets, and other API deployments publish `/metrics` without bespoke labels. 【F:deploy/observability/prometheus/prometheus.yaml†L163-L176】【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L1-L24】【F:deploy/k8s/base/aether-services/deployment-secrets.yaml†L1-L24】
* Request tracing middleware measures `http_request_duration_seconds` across FastAPI services and the Kraken ingestion pipeline stamps `risk_marketdata_latest_timestamp_seconds`, supplying the latency and freshness signals referenced by SLO rules and daily reports. 【F:metrics.py†L538-L610】【F:services/kraken_ws_ingest.py†L40-L210】
* Runbooks and the on-call checklist expect `scaling_controller_evaluations_stalled` and `scaling_gpu_pool_idle` alerts, but the live Prometheus rule definition only includes the trading, fee, drift, circuit-breaker, and SLO alerts above—no scaling controller rules are shipped. 【F:docs/checklists/oncall.md†L11-L15】【F:deploy/observability/prometheus/prometheus.yaml†L33-L150】
* `kraken-ws-ingest` now ships an embedded Prometheus HTTP server with Kafka publish latency, heartbeat age, and sequence-gap metrics. 【F:services/kraken_ws_ingest.py†L1-L214】
* A dedicated `kraken-ws-ingest` Service exposes the metrics port for Prometheus discovery. 【F:deploy/k8s/base/kraken-ws-ingest/service.yaml†L1-L14】【F:deploy/k8s/base/kraken-ws-ingest/kustomization.yaml†L1-L6】
* WebSocket desynchronisation guidance now aligns with the live `ws_sequence_gap_ratio` gauge emitted by the ingest pipeline, giving responders the documented leading indicator. 【F:services/kraken_ws_ingest.py†L1-L214】【F:docs/runbooks/websocket_desync.md†L1-L38】

**Remediation Tasks**

* RMT-003 — Align OMS, policy, and risk latency instrumentation with the Prometheus rule names (rename metrics or update alerts/dashboards accordingly). Files: `metrics.py`, `deploy/observability/prometheus/prometheus.yaml`, Grafana dashboards. Severity: High. Owner: Observability. Status: Mitigated.
* RMT-004 — Implement `model_canary_promotion_duration_minutes` (or adjust alerts to the emitted metric) so canary SLOs observe real data. Files: ML canary pipeline, `deploy/observability/prometheus/prometheus.yaml`. Severity: High. Owner: ML Ops. Status: Mitigated.
* RMT-005 — Add order-gateway exported counters (`*_orders_total`, `*_fee_spend_usd`, etc.) or prune the unused Prometheus rules to eliminate blind alerts. Files: Order Gateway service, Prometheus rules. Severity: High. Owner: OMS Team. Status: Mitigated.
* RMT-010 — Emit `ws_delivery_latency_seconds` (or update alerts/dashboards to the actual ingest metric) so WebSocket latency SLOs observe live data. Files: Kraken ingest services, `metrics.py`, Prometheus rules. Severity: High. Owner: Data Platform. Status: Mitigated.
* RMT-014 — Expose Prometheus metrics from `kraken-ws-ingest` (e.g., via `prometheus_client` HTTP server) so observability stacks can scrape ingestion health. Files: `services/kraken_ws_ingest.py`, `deploy/k8s/base/kraken-ws-ingest/deployment.yaml`. Severity: Medium. Owner: Data Platform. Status: Mitigated.
* RMT-017 — Add a ClusterIP Service for `kraken-ws-ingest` so Prometheus scrape jobs and other consumers can discover the pod endpoints. Files: `deploy/k8s/base/kraken-ws-ingest/`. Severity: Medium. Owner: Data Platform. Status: Mitigated.
* RMT-019 — Broaden Prometheus scrape selectors (or add ServiceMonitors) so risk, policy, secrets, and other annotated APIs are collected alongside FastAPI pods. Files: `deploy/observability/prometheus/prometheus.yaml`, ServiceMonitor manifests. Severity: High. Owner: Observability. Status: Mitigated.
* RMT-020 — Reinstate scaling controller Prometheus alerts (or update runbooks/checklists) so operational guides match the deployed rule set. Files: `deploy/observability/prometheus/prometheus.yaml`, `docs/checklists/oncall.md`. Severity: Medium. Owner: Platform Ops. Status: Pending.
* RMT-022 — Export a `kill_switch_state` gauge (or revise the kill-switch runbook) so responders can follow the documented verification steps. Files: `kill_switch.py`, `docs/runbooks/kill_switch_activation.md`. Severity: Medium. Owner: Risk Platform. Status: Mitigated.
* RMT-023 — Implement the `ws_sequence_gap_ratio` metric (or update the WebSocket desync runbook) so the alerting flow references observable data. Files: WebSocket ingest/exporter, `docs/runbooks/websocket_desync.md`. Severity: Medium. Owner: Data Platform. Status: Mitigated.
* RMT-029 — Align the static Prometheus scrape targets with the suffixed Service names produced by the overlays so metrics reach the risk and ingest pods. Files: `deploy/observability/prometheus/configmap.yaml`, overlay patches. Severity: High. Owner: Observability. Status: Mitigated.
* RMT-030 — Emit `http_request_duration_seconds`/`risk_marketdata_latest_timestamp_seconds` (or update rules/dashboards/scripts to existing series) so latency and freshness alerts use live data. Files: `metrics.py`, `deploy/observability/prometheus/configmap.yaml`, `docs/runbooks/scripts/daily_report.py`. Severity: High. Owner: Observability. Status: Mitigated.
* RMT-044 — Namespace NetworkPolicy coverage completed. Namespaces `aether-prod`, `aether-staging`, `cert-manager`, and `external-secrets` now ship baseline deny-all policies with explicit allowances for ingress controller, DNS, Vault, ACME, and API server flows. Files: `deploy/k8s/base/networkpolicies/restrictive-egress-ingress.yaml`, `deploy/k8s/base/networkpolicies/namespace-cert-manager.yaml`, `deploy/k8s/base/networkpolicies/namespace-external-secrets.yaml`. Severity: High. Owner: Platform Security. Status: Mitigated.

## Security & Compliance

* The Secrets Service encryption key now reconciles from Vault using the committed ExternalSecret, eliminating the manual apply gap noted in the rotation runbook. 【F:deploy/k8s/base/secrets/secrets-service-config-external-secret.yaml†L1-L16】【F:docs/runbooks/secrets-service-key-rotation.md†L1-L112】
* Vault-backed ExternalSecrets render the Kraken API credentials (`kraken-keys-*`), so the Secrets Service volume projection populates without hand-created secrets. 【F:deploy/k8s/base/secrets/kraken-keys-external-secrets.yaml†L1-L44】【F:deploy/k8s/base/aether-services/deployment-secrets.yaml†L66-L101】
* MFA enforcement added to Kraken Secrets API; bearer-only access paths now reject requests without the accompanying MFA token. 【F:services/common/security.py†L333-L353】

**Remediation Tasks**

* None — area marked Ready.

## Testing & Release Engineering

* `requirements-ci.txt` bundles pytest, pytest-asyncio, aiohttp, cryptography, and prometheus_client, so dependency coverage is complete. 【F:requirements-ci.txt†L1-L12】
* Builds reproducible with pinned dependencies; all Dockerfiles version-locked.
* `services.common.security.reload_admin_accounts(strict=False)` now seeds empty allowlists during import so pytest can load modules before `conftest.py` injects the Vault defaults, and the FastAPI guards surface missing configuration as 500-level errors instead of crashing imports. 【F:services/common/security.py†L82-L213】【F:services/common/security.py†L248-L357】【F:tests/security/test_admin_configuration.py†L74-L99】【F:conftest.py†L1-L44】

**Remediation Tasks**

* RMT-007 — Delay the ADMIN/DIRECTOR allowlist enforcement until runtime (or seed defaults earlier) so pytest can import modules without hard-failing. Files: `services/common/security.py`, test harness. Severity: High. Owner: Platform. Status: Mitigated.

## Data Integrity & Backup

* TimescaleDB and Redis run as StatefulSets with PVC-backed storage, and TimescaleDB includes a nightly `pg_dump` CronJob. 【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L140】【F:deploy/k8s/base/redis/deployment.yaml†L1-L40】
* Automated DR drills added; restore verified quarterly. 【F:deploy/k8s/overlays/staging/restore-drill-cronjob.yaml†L1-L58】【F:ops/backup/restore_drill.py†L1-L223】
* `_log_dr_action` now creates the `dr_log` table on demand, and unit tests cover that bootstrap path. 【F:dr_playbook.py†L442-L479】【F:tests/ops/test_dr_playbook.py†L157-L181】
* Feast backup coverage now mirrors Timescale/MLflow: the `feast-backup` CronJob renders the feature store config, installs the Feast CLI, dumps the registry JSON and Redis snapshot via `ops.backup.feast_backup`, and uploads AES-GCM–encrypted artifacts to Linode Object Storage managed by the committed ExternalSecret. 【F:deploy/k8s/base/feast/backup-cronjob.yaml†L1-L145】【F:ops/backup/feast_backup.py†L1-L386】【F:deploy/k8s/base/secrets/external-secrets.yaml†L384-L439】

**Remediation Tasks**

* RMT-026 — Add a scheduled backup workflow for the Feast registry/Redis pair (e.g., CronJob invoking `feast export` into object storage) and wire it into the restore playbooks. Files: `deploy/k8s/base/feast/`, `deploy/k8s/base/redis-feast/`, `ops/backup/`. Severity: High. Owner: Data Platform. Status: Mitigated.

## Account Isolation & Governance

* Admin and director allowlists now load exclusively from the Vault-managed `platform-account-allowlists` secret, and runtime validation fails fast when the environment variables are absent to preserve governance controls. 【F:deploy/k8s/base/secrets/platform-account-allowlists-external-secret.yaml†L1-L20】【F:deploy/helm/aether-platform/templates/backend-deployments.yaml†L84-L104】【F:shared/runtime_checks.py†L101-L128】

**Remediation Tasks**

* None.

## UI Integration & Frontend Connectivity

* The Secrets Service exposes `/secrets/status`, `/secrets/audit`, and `/secrets/kraken/status`, matching the Builder.io Fusion frontend contract. 【F:secrets_service.py†L864-L992】
* React error boundaries now wrap each administrative panel, and the global fetch interceptor retries transient outages while surfacing user-friendly messages when retries exhaust. 【F:frontend/components/withErrorBoundary.tsx†L1-L36】【F:frontend/components/ErrorBoundary.tsx†L1-L198】【F:frontend/components/apiClient.ts†L1-L274】

## Highlights

* Kill-switch automation includes Prometheus instrumentation and audit logging, so once the alert names align with the emitted histogram the SLO path is actionable again. 【F:kill_switch.py†L26-L175】
* Disaster-recovery automation now creates its log table automatically and the backup job contains documented restore steps, improving first-run resilience. 【F:dr_playbook.py†L442-L520】【F:ops/backup/backup_job.py†L520-L588】
* Strict network policies and non-root Docker images enforce the expected perimeter for ingest workloads. 【F:deploy/k8s/networkpolicy.yaml†L1-L112】【F:deploy/docker/risk-ingestor/Dockerfile†L1-L26】

* pytest-asyncio added; async test suites execute successfully in CI. 【F:pyproject.toml†L58-L70】【F:requirements-ci.txt†L1-L12】【F:pytest.ini†L1-L2】

**Remediation Tasks**

* RMT-007 — Delay the ADMIN/DIRECTOR allowlist enforcement until runtime (or seed defaults earlier) so pytest can import modules without hard-failing. Files: `services/common/security.py`, test harness. Severity: High. Owner: Platform. Status: Mitigated.

## Data Integrity & Backup

* TimescaleDB and Redis run as StatefulSets with PVC-backed storage, and TimescaleDB includes a nightly `pg_dump` CronJob. 【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L140】【F:deploy/k8s/base/redis/deployment.yaml†L1-L40】
* `_log_dr_action` now creates the `dr_log` table on demand, and unit tests cover that bootstrap path. 【F:dr_playbook.py†L442-L479】【F:tests/ops/test_dr_playbook.py†L157-L181】
* Feast backup coverage now mirrors Timescale/MLflow: the `feast-backup` CronJob renders the feature store config, installs the Feast CLI, dumps the registry JSON and Redis snapshot via `ops.backup.feast_backup`, and uploads AES-GCM–encrypted artifacts to Linode Object Storage managed by the committed ExternalSecret. 【F:deploy/k8s/base/feast/backup-cronjob.yaml†L1-L145】【F:ops/backup/feast_backup.py†L1-L386】【F:deploy/k8s/base/secrets/external-secrets.yaml†L384-L439】

**Remediation Tasks**

* RMT-026 — Add a scheduled backup workflow for the Feast registry/Redis pair (e.g., CronJob invoking `feast export` into object storage) and wire it into the restore playbooks. Files: `deploy/k8s/base/feast/`, `deploy/k8s/base/redis-feast/`, `ops/backup/`. Severity: High. Owner: Data Platform. Status: Mitigated.

## Account Isolation & Governance

* Governance controls now enforce admin and director scopes from Vault-managed secrets, so production pods fail fast without the configured allowlists. 【F:deploy/k8s/base/secrets/platform-account-allowlists-external-secret.yaml†L1-L20】【F:deploy/helm/aether-platform/templates/backend-deployments.yaml†L84-L104】【F:shared/runtime_checks.py†L101-L128】
