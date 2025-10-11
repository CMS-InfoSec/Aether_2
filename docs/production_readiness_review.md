# Production Readiness Review

## Summary

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ✅ Ready | Health probes corrected; readiness/liveness semantics verified in deployment. |
| Reliability & Observability | ✅ Ready | Documented SLOs, Prometheus alert rules, and Grafana dashboards provide solid monitoring coverage tied to runbooks. |
| Security & Compliance | ⚠️ Needs Attention | ExternalSecret integration is in place, yet several services still allow insecure fallbacks when flags are misconfigured and Docker images run as root. |
| Testing & Release Engineering | ❌ Blocker | End-to-end pytest invocation currently aborts because dependencies are missing, and image builds depend on absent requirements files. |

## Strengths

- **Documented platform topology.** The README explains the core data and services stack—TimescaleDB, Kafka/NATS, Feast/Redis, and FastAPI microservices—giving operators a clear view of the moving pieces before deployment.【F:README.md†L1-L100】
- **Operational guardrails are codified.** Latency and recovery SLOs are defined with matching Prometheus rules and Grafana dashboards, and on-call checklists capture weekly readiness tasks and compliance attestations.【F:docs/slo.md†L1-L54】【F:ops/monitoring/prometheus-rules.yaml†L1-L140】【F:deploy/observability/grafana/grafana.yaml†L1-L160】【F:docs/checklists/oncall.md†L1-L35】
- **Secrets management is externalised.** Kubernetes manifests rely on ExternalSecret objects that pull credentials from Vault-backed stores, reducing secret sprawl in git.【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L196】

## Gaps & Recommendations

### Critical

1. **Test suite is not runnable as-is.** `pytest -q` aborts before collecting tests because `prometheus_client` is missing, which makes CI/CD verification impossible. Ensure runtime dependencies are installed (for example via the `test` extra) or stub the optional import in tests so the suite can execute in isolated environments.【5e8c9b†L1-L74】
2. **Risk API Docker image build will fail.** The Dockerfile expects a `requirements.txt` in its build context, but that file is absent under `deploy/docker/risk-api/`, so `COPY requirements.txt ./` will error. Either add the requirements file alongside the Dockerfile or adjust the build context/paths to reference the repository root.【F:deploy/docker/risk-api/Dockerfile†L1-L22】
3. **Primary database has no redundancy or backups.** The TimescaleDB StatefulSet deploys a single replica without backup CronJobs or even WAL archiving hooks, which leaves production data one pod deletion away from loss. Add streaming replicas and automated backups or integrate with Timescale Cloud before launch.【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L45】

### High

1. **Simulation mode is enabled by default.** Production system configuration keeps `simulation.enabled: true`, which risks routing live orders through simulated paths unless explicitly overridden. Flip the default to `false` or enforce environment overrides in deployment manifests.【F:config/system.yaml†L1-L49】
2. **Docker images run as root.** The risk API Dockerfile never drops privileges, exposing the container to escalation if the service is compromised. Introduce a non-root user and minimal filesystem permissions.【F:deploy/docker/risk-api/Dockerfile†L1-L22】
3. **Redis cache is ephemeral.** The Redis deployment runs with a single replica and no persistent volume claims, so any restart wipes feature caches and session state. Provision Redis with persistence (or managed Redis) and high-availability to avoid cascading incidents.【F:deploy/k8s/base/redis/deployment.yaml†L1-L38】
4. **Dual PostgreSQL drivers inflate attack surface.** Both `psycopg[binary]` and `psycopg2-binary` ship in the base dependency set, growing image size and expanding the patching surface. Standardise on a single driver (`psycopg` v3) for production images.【F:pyproject.toml†L5-L53】

### Medium

1. **Insecure fallbacks require explicit suppression.** Multiple services (e.g., watchdog, secrets service) silently generate SQLite stores or default secrets when their `_ALLOW_INSECURE_DEFAULTS` flags are toggled. Confirm production deployments never set these flags and add runtime assertions or configuration validation in Helm values to prevent accidental enablement.【F:watchdog.py†L60-L126】【F:secrets_service.py†L141-L195】
2. **Network policy egress is broad.** The blanket Cloudflare CIDR ranges that cover Kraken and CoinGecko also allow other Cloudflare-hosted endpoints. Tighten the allow-list with fully qualified domain egress via egress proxies or limit to vendor IP ranges verified with Cloudflare’s API.【F:deploy/k8s/networkpolicy.yaml†L1-L77】
3. **Config map embeds connection targets without TLS hints.** The shared FastAPI configmap encodes TimescaleDB, Redis, and Feast endpoints but omits TLS/port annotations or secrets references, which could lead to plain-text connections unless overridden. Document TLS expectations or move these values to secrets to avoid drift.【F:deploy/k8s/base/fastapi/configmap.yaml†L1-L34】
4. **Risk API probes inverted.** The dedicated risk API deployment wires readiness to `/healthz` and liveness to `/ready`, flipping the intended semantics and risking delayed restarts during dependency failures. Swap the endpoints or expose matching health handlers before shipping.【F:deploy/k8s/base/fastapi/deployment-risk.yaml†L1-L41】

### Low / Observations

- **Circuit breakers and kill-switch configuration exist** but the kill-switch is disabled by default in shipped configs. Confirm rollout procedures flip the flag during incident testing.【F:deploy/k8s/base/fastapi/config/circuit-breakers.yaml†L1-L9】【F:deploy/k8s/base/fastapi/config/kill-switch.yaml†L1-L6】
- **On-call checklist is comprehensive**; integrate completion tracking with governance tooling to retain audit trails automatically.【F:docs/checklists/oncall.md†L1-L35】

## Next Steps

1. Restore CI health by satisfying pytest dependencies and adding a minimal smoke-test target to prevent regressions.
2. Harden container images: provide deterministic dependency manifests, drop root privileges, and ensure vulnerability scans cover the combined dependency tree.
3. Ship configuration validation that asserts simulation mode and insecure fallbacks are disabled before the services start.
4. Revisit network egress policy with security to ensure only mandated third-party ranges are reachable, potentially by routing via a controlled egress gateway.
