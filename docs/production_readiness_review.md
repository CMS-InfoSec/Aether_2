# Production Readiness Review

## Summary

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ✅ Ready | TimescaleDB now runs with replicas and scheduled backups; production data redundancy achieved. |
| Reliability & Observability | ✅ Ready | Documented SLOs, Prometheus alert rules, and Grafana dashboards provide solid monitoring coverage tied to runbooks. |
| Security & Compliance | ✅ Ready | Egress policy restricted to verified Kraken and CoinGecko endpoints only. |
| Testing & Release Engineering | ❌ Blocker | End-to-end pytest invocation currently aborts because dependencies are missing, and image builds depend on absent requirements files. |

## Strengths

- **Documented platform topology.** The README explains the core data and services stack—TimescaleDB, Kafka/NATS, Feast/Redis, and FastAPI microservices—giving operators a clear view of the moving pieces before deployment.【F:README.md†L1-L100】
- **Operational guardrails are codified.** Latency and recovery SLOs are defined with matching Prometheus rules and Grafana dashboards, and on-call checklists capture weekly readiness tasks and compliance attestations.【F:docs/slo.md†L1-L54】【F:ops/monitoring/prometheus-rules.yaml†L1-L140】【F:deploy/observability/grafana/grafana.yaml†L1-L160】【F:docs/checklists/oncall.md†L1-L35】
- **Secrets management is externalised.** Kubernetes manifests rely on ExternalSecret objects that pull credentials from Vault-backed stores, reducing secret sprawl in git.【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L196】

## Gaps & Recommendations

### Critical

1. **Test suite is not runnable as-is.** `pytest -q` aborts before collecting tests because `prometheus_client` is missing, which makes CI/CD verification impossible. Ensure runtime dependencies are installed (for example via the `test` extra) or stub the optional import in tests so the suite can execute in isolated environments.【5e8c9b†L1-L74】
2. **Risk API Docker image build will fail.** The Dockerfile expects a `requirements.txt` in its build context, but that file is absent under `deploy/docker/risk-api/`, so `COPY requirements.txt ./` will error. Either add the requirements file alongside the Dockerfile or adjust the build context/paths to reference the repository root.【F:deploy/docker/risk-api/Dockerfile†L1-L22】
3. **Primary database resilience tracked.** The updated TimescaleDB StatefulSet ships with three replicas, persistent volumes, and a nightly `pg_dumpall` CronJob stored on dedicated backup storage. Continue monitoring restore drills to ensure the process stays rehearsed.【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L84】

### High

1. **Simulation mode is enabled by default.** ✅ Addressed: `simulation.enabled` now defaults to `false` so production rollouts do not need to override the flag to avoid simulated order paths.【F:config/system.yaml†L1-L28】
2. **Docker images run as root.** ✅ Addressed: The risk API Dockerfile provisions an `app` user, adjusts ownership, and runs the service as non-root for defence in depth.【F:deploy/docker/risk-api/Dockerfile†L6-L25】
3. **Dual PostgreSQL drivers inflate attack surface.** ✅ Addressed: The dependency set now standardises on `psycopg[binary]` and drops the duplicate `psycopg2-binary` package.【F:pyproject.toml†L12-L46】

### Medium

1. **Insecure fallbacks require explicit suppression.** Multiple services (e.g., watchdog, secrets service) silently generate SQLite stores or default secrets when their `_ALLOW_INSECURE_DEFAULTS` flags are toggled. Confirm production deployments never set these flags and add runtime assertions or configuration validation in Helm values to prevent accidental enablement.【F:watchdog.py†L60-L126】【F:secrets_service.py†L141-L195】
2. **Network policy egress tightened.** Updated egress restrictions now enforce traversal through the outbound proxy and Cloudflare’s verified ranges dedicated to Kraken and CoinGecko APIs, removing the previous blanket access.【F:deploy/k8s/networkpolicy.yaml†L1-L116】
3. **Config map embeds connection targets without TLS hints.** The shared FastAPI configmap encodes TimescaleDB, Redis, and Feast endpoints but omits TLS/port annotations or secrets references, which could lead to plain-text connections unless overridden. Document TLS expectations or move these values to secrets to avoid drift.【F:deploy/k8s/base/fastapi/configmap.yaml†L1-L34】
4. **Risk API probes inverted.** The dedicated risk API deployment wires readiness to `/healthz` and liveness to `/ready`, flipping the intended semantics and risking delayed restarts during dependency failures. Swap the endpoints or expose matching health handlers before shipping.【F:deploy/k8s/base/fastapi/deployment-risk.yaml†L1-L41】
5. **Feast registry is transient.** Feast mounts its registry database from an `emptyDir`, so any pod restart or rescheduling erases feature definitions until a manual bootstrap occurs. Persist the registry on durable storage or sync it from artifact storage during startup.【F:deploy/k8s/base/feast/deployment.yaml†L1-L56】

### Low / Observations

- **Circuit breakers and kill-switch configuration exist** and the kill-switch now ships enabled for production exercises, with rollout drills documented in the runbook.【F:deploy/k8s/base/fastapi/config/circuit-breakers.yaml†L1-L9】【F:deploy/k8s/base/fastapi/config/kill-switch.yaml†L1-L6】【F:docs/runbooks/kill-switch.md†L1-L74】
- **On-call checklist is comprehensive**; integrate completion tracking with governance tooling to retain audit trails automatically.【F:docs/checklists/oncall.md†L1-L35】

## Next Steps

1. Restore CI health by satisfying pytest dependencies and adding a minimal smoke-test target to prevent regressions.
2. Harden container images: provide deterministic dependency manifests, drop root privileges, and ensure vulnerability scans cover the combined dependency tree.
3. Ship configuration validation that asserts simulation mode and insecure fallbacks are disabled before the services start.
4. Revisit network egress policy with security to ensure only mandated third-party ranges are reachable, potentially by routing via a controlled egress gateway.
