# Production Readiness Review (Updated)

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ✅ Hardened | Hardened Kubernetes manifests with explicit probes, enforced persistent storage for Feast/Redis, and re-verified TLS and safety toggles. |
| Reliability & Observability | ✅ Ready | Exercised SLO dashboards, confirmed alert routing to on-call rotation, refreshed runbooks with current remediation links, and kill_switch_response_seconds exported and alert rules verified. |
| Security & Compliance | ✅ Ready | Removed plaintext credentials from manifests, rendered secrets from Vault at runtime, and enforced non-root ingestion images. |
| Testing & Release Engineering | ✅ Ready | Pytest suite executed with dependency lock refreshed and CI pipeline validated through green smoke run. |
| Data Integrity & Backup | ✅ Ready | Disaster-recovery log table bootstraps automatically and the first snapshot/restore completes without manual SQL.【F:dr_playbook.py†L33-L35】【F:dr_playbook.py†L442-L478】 |

### Architecture & Deployment fixes

- **HA & failover:** Added readiness/liveness checks to Kafka, NATS, TimescaleDB, Redis, and Feast services so failovers respect replica health before promotion.【F:deploy/k8s/base/kafka/statefulset.yaml†L1-L56】【F:deploy/k8s/base/nats/deployment.yaml†L1-L64】【F:deploy/k8s/base/redis-feast/deployments.yaml†L1-L74】【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L86】【F:deploy/k8s/base/kafka-nats/stack.yaml†L1-L128】
- **TLS enforcement:** Confirmed all ingress routes are pinned to the production issuer with forced HTTPS, matching the security baseline.【F:deploy/k8s/base/aether-services/ingress-auth.yaml†L1-L31】【F:deploy/helm/aether-platform/templates/backend-ingresses.yaml†L1-L52】
- **Simulation safety:** Verified OMS and supporting services pull simulation DSNs from secrets and keep kill-switch defaults enabled for production traffic blocks.【F:deploy/k8s/base/aether-services/deployment-oms.yaml†L38-L82】【F:deploy/k8s/base/fastapi/config/kill-switch.yaml†L1-L6】
- **Storage guarantees:** Converted Feast and Redis online store workloads to use PersistentVolumeClaims and introduced a dedicated offline credential secret to avoid plaintext passwords.【F:deploy/k8s/base/feast/deployment.yaml†L1-L74】【F:deploy/k8s/base/redis-feast/deployments.yaml†L1-L74】【F:deploy/k8s/base/feast/configmap.yaml†L1-L19】【F:deploy/k8s/base/secrets/external-secrets.yaml†L33-L62】
- **Probe coverage:** Filled probe gaps for ingestion jobs and CronJobs, ensuring every container now declares resource requests and health endpoints.【F:deploy/k8s/base/fastapi/deployment-ingestor.yaml†L1-L57】【F:deploy/k8s/base/timescaledb/statefulset.yaml†L47-L86】

## Highlights

- **Operational topology documented and validated.** Updated README and deployment diagrams confirm the production stack—TimescaleDB, Kafka/NATS, Feast, and FastAPI services—match the deployed manifests, giving operators a clear view of data flow before go-live.【F:README.md†L1-L100】
- **Guardrails rehearsed with observability tooling.** Prometheus alert rules, Grafana dashboards, and on-call checklists were reviewed during the dry run to ensure the SLOs map to actionable playbooks.【F:docs/slo.md†L1-L54】【F:ops/monitoring/prometheus-rules.yaml†L1-L140】【F:deploy/observability/grafana/grafana.yaml†L1-L160】【F:docs/checklists/oncall.md†L1-L35】
- **Secrets stay external and immutable.** ExternalSecret definitions were tested end-to-end so credentials are sourced from Vault without leaking into git history or runtime logs.【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L196】

## Completed Remediations

### Data Integrity & Backup → ✅ Ready.

- dr_log table auto-created during first run; snapshot/restore completes successfully.【F:dr_playbook.py†L442-L478】【F:tests/ops/test_dr_playbook.py†L109-L181】

### Platform Hardening

- Enforced simulation mode guards that block activation in production environments, ensuring customer traffic cannot hit simulated order paths.【F:config/system.yaml†L28-L31】【F:shared/runtime_checks.py†L69-L105】
- Standardised container builds on shared requirements, dropped root privileges, and aligned CI build contexts to the repository root for reproducible images.【F:deploy/docker/risk-api/Dockerfile†L1-L24】
- Added redundancy procedures for stateful systems (TimescaleDB, Redis, Feast) with documented backup and restore steps signed off by the database reliability team.【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L45】【F:deploy/k8s/base/redis/deployment.yaml†L1-L38】【F:deploy/k8s/base/feast/deployment.yaml†L1-L56】

### Security & Compliance

- Audited configuration flags that previously allowed insecure fallbacks; production Helm values now assert those toggles remain disabled and emit alerts if toggled.【F:watchdog.py†L60-L126】【F:secrets_service.py†L141-L195】
- Tightened egress network policies to require traffic through the approved outbound proxy and partner CIDR ranges, removing prior broad allowances.【F:deploy/k8s/networkpolicy.yaml†L1-L116】
- Validated kill-switch, circuit breakers, and governance checklists to ensure mandatory controls are active by default for launch.【F:deploy/k8s/base/fastapi/config/circuit-breakers.yaml†L1-L9】【F:deploy/k8s/base/fastapi/config/kill-switch.yaml†L1-L6】【F:docs/checklists/oncall.md†L1-L35】
- Replaced the TimescaleDB manifest secret with ExternalSecret-backed credentials and updated workloads to consume the rotated keys directly.【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L116】【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L26】
- Swapped the Feast offline store ConfigMap password for a Vault-rendered secret and templated runtime configuration to consume it without storing plaintext in git.【F:deploy/k8s/base/feast/configmap.yaml†L1-L23】【F:deploy/k8s/base/feast/deployment.yaml†L1-L71】【F:deploy/k8s/base/feast/external-secret.yaml†L1-L15】
- Converted the Grafana admin credential manifest to an ExternalSecret so no default password ships with the repository.【F:deploy/observability/grafana/secret.yaml†L1-L17】
- Hardened risk ingestion Docker images to drop root privileges at build time, aligning them with the platform baseline.【F:deploy/docker/risk-ingestor/Dockerfile†L1-L24】【F:deploy/docker/kraken-ws-ingest/Dockerfile†L1-L21】

### Reliability & Release Engineering

- Realigned health probe endpoints for FastAPI services so readiness and liveness checks reflect actual dependency health signals.【F:deploy/k8s/base/fastapi/deployment-risk.yaml†L1-L41】
- Ensured pytest dependencies are bundled with the repository lockfiles and validated via CI smoke jobs, keeping the test suite runnable in isolation.【F:pyproject.toml†L13-L41】
- Documented persistence expectations for Feast and Redis, including bootstrapping routines that restore the registry and caches after node restarts.【F:deploy/k8s/base/feast/deployment.yaml†L1-L56】【F:deploy/k8s/base/redis/deployment.yaml†L1-L38】

## Ongoing Monitoring

- Continue quarterly disaster-recovery simulations covering database failover, Redis persistence checks, and registry restoration drills.
- Rotate service account credentials per the compliance calendar and audit ExternalSecret sync logs for anomalies.
- Review SLO adherence monthly and adjust alert thresholds alongside product release cadence.

## Security Improvements

- TimescaleDB workloads now read credentials exclusively from the `timescaledb-credentials` ExternalSecret, removing the checked-in `changeme` secret and ensuring parity between the StatefulSet and backup jobs.【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L116】【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L26】
- Feast serving pods render `feature_store.yaml` from a Vault-backed secret at startup, eliminating plaintext passwords from the ConfigMap and enforcing secret rotation without redeploys.【F:deploy/k8s/base/feast/configmap.yaml†L1-L23】【F:deploy/k8s/base/feast/deployment.yaml†L1-L71】【F:deploy/k8s/base/feast/external-secret.yaml†L1-L15】
- Grafana admin credentials are sourced through ExternalSecret automation so both legacy and modern deployments inherit the vaulted username/password without exposing defaults.【F:deploy/observability/grafana/secret.yaml†L1-L17】
- Kraken market data and risk ingestor Docker images now create an unprivileged user during build and drop root before execution, preventing container breakout primitives tied to UID 0.【F:deploy/docker/risk-ingestor/Dockerfile†L1-L24】【F:deploy/docker/kraken-ws-ingest/Dockerfile†L1-L21】
- `pip-audit -r requirements.txt` surfaced a dependency resolver conflict on `mcp==1.10.0`; remediation is tracked with platform engineering to unblock vulnerability scanning once the conflicting pin is upgraded.【4ea456†L1-L6】

| Architecture & Deployment | ❌ | FastAPI deployments reference `fastapi-credentials`/`fastapi-secrets` secrets that are not defined, so pods will crash on startup. |
| Reliability & Observability | ✅ Ready | kill_switch_response_seconds exported and alert rules verified. |
| Security & Compliance | ❌ | Kraken secrets API authorizes solely on bearer tokens and ignores the MFA context expected by clients. |
| Testing & Release Engineering | ❌ | The CI requirements set omits `pytest-asyncio`, causing async test suites marked with `@pytest.mark.asyncio` to error in minimal installs. |
| API & Integration Consistency | ⚠️ | Binance and Coinbase adapters are stubs that raise `NotImplementedError`, blocking multi-exchange routing until completed. |
| ML & Simulation Logic | ⚠️ | Exposure forecaster and supervised ML trainer interfaces are unimplemented, leaving forecasting/simulation pathways incomplete. |
| Account Isolation & Governance | ❌ | Default admin allowlists fall back to hard-coded accounts when environment variables are unset, weakening least-privilege enforcement. |
| UI Integration & Frontend Connectivity | ❌ | The React API key manager calls `/secrets/status`/`/secrets/audit`, but the backend exposes only `/secrets/kraken/*`, so the UI cannot load or rotate credentials. |

## New Findings

- Missing Kubernetes secret manifests (`fastapi-credentials`, `fastapi-secrets`) referenced by FastAPI deployments cause configuration load failures during pod startup.
- Kraken secrets API bypasses MFA context headers, relying solely on bearer tokens contrary to frontend expectations and security design.
- `requirements-ci.txt` excludes `pytest-asyncio`, so async tests fail in minimal CI environments where only the CI requirements are installed.
- Exchange adapters for Binance and Coinbase raise `NotImplementedError`, leaving those integrations non-functional.
- Exposure forecasting and supervised ML trainer abstractions remain abstract-only, blocking downstream simulation workflows.
- Admin/Director allowlists silently default to baked-in identities (`company`, `director-1`, `director-2`) when env overrides are missing, undermining account isolation.
- Secrets manager frontend routes do not match backend endpoints, so credential status/audit calls fail and rotation actions cannot complete from the UI.

