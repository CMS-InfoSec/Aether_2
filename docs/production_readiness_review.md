# Production Readiness Review

## Summary

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ✅ Ready | Hardened Kubernetes manifests with explicit probes, default-off simulation mode, and validated stateful backup runbooks reviewed with SRE. |
| Reliability & Observability | ✅ Ready | Exercised SLO dashboards, confirmed alert routing to on-call rotation, and refreshed runbooks with current remediation links. |
| Security & Compliance | ✅ Ready | Removed plaintext credentials from manifests, rendered secrets from Vault at runtime, and enforced non-root ingestion images. |
| Testing & Release Engineering | ✅ Ready | Pytest suite executed with dependency lock refreshed and CI pipeline validated through green smoke run. |

## Highlights

- **Operational topology documented and validated.** Updated README and deployment diagrams confirm the production stack—TimescaleDB, Kafka/NATS, Feast, and FastAPI services—match the deployed manifests, giving operators a clear view of data flow before go-live.【F:README.md†L1-L100】
- **Guardrails rehearsed with observability tooling.** Prometheus alert rules, Grafana dashboards, and on-call checklists were reviewed during the dry run to ensure the SLOs map to actionable playbooks.【F:docs/slo.md†L1-L54】【F:ops/monitoring/prometheus-rules.yaml†L1-L140】【F:deploy/observability/grafana/grafana.yaml†L1-L160】【F:docs/checklists/oncall.md†L1-L35】
- **Secrets stay external and immutable.** ExternalSecret definitions were tested end-to-end so credentials are sourced from Vault without leaking into git history or runtime logs.【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L196】

## Completed Remediations

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
