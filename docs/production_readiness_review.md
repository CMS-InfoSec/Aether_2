# Production Readiness Review

## Summary

| Area | Status | Notes |
| --- | --- | --- |
| Architecture & Deployment | ⚠️ Needs Attention | Kubernetes manifests cover multi-service deployment with probes and configmaps, and simulation defaults are now production-safe, yet some container hardening gaps remain. |
| Reliability & Observability | ✅ Ready | Documented SLOs, Prometheus alert rules, and Grafana dashboards provide solid monitoring coverage tied to runbooks. |
| Security & Compliance | ⚠️ Needs Attention | ExternalSecret integration is in place, the risk API image now drops root privileges, and a runtime guard blocks insecure fallbacks, while policy hardening items remain outstanding. |
| Testing & Release Engineering | ⚠️ Needs Attention | Pytest now exercises the risk circuit breaker flow end-to-end because the monitor binds dynamically to patched Timescale adapters and records safe-mode activation deterministically, though broader data-store scenarios still need hardening. |

## Strengths

- **Documented platform topology.** The README explains the core data and services stack—TimescaleDB, Kafka/NATS, Feast/Redis, and FastAPI microservices—giving operators a clear view of the moving pieces before deployment.【F:README.md†L1-L100】
- **Operational guardrails are codified.** Latency and recovery SLOs are defined with matching Prometheus rules and Grafana dashboards, and on-call checklists capture weekly readiness tasks and compliance attestations.【F:docs/slo.md†L1-L54】【F:ops/monitoring/prometheus-rules.yaml†L1-L140】【F:deploy/observability/grafana/grafana.yaml†L1-L160】【F:docs/checklists/oncall.md†L1-L35】
- **Secrets management is externalised.** Kubernetes manifests rely on ExternalSecret objects that pull credentials from Vault-backed stores, reducing secret sprawl in git.【F:deploy/k8s/base/secrets/external-secrets.yaml†L1-L196】

## Gaps & Recommendations

### Critical

1. **Test suite is not runnable as-is.** ✅ Addressed: CI now installs FastAPI, Prometheus client, httpx, cryptography, and other test-time dependencies via `requirements-ci.txt`, allowing pytest to progress past collection. Runtime helpers now project the `accounts` table definition into SQLite-backed metadata so services such as the ESG filter and diversification allocator can create their tables without `NoReferencedTableError`, unlocking deeper functional assertions in the suite. The circuit breaker monitor also resolves the Timescale adapter from the shared module at runtime and persists safe-mode engagement so the risk thresholds trip reliably under pytest.【F:requirements-ci.txt†L1-L9】【F:shared/account_scope.py†L1-L124】【F:esg_filter.py†L1-L210】【F:services/risk/diversification_allocator.py†L240-L310】【F:services/anomaly/execution_anomaly.py†L1-L260】【F:services/risk/circuit_breakers.py†L1-L550】【3b74e9†L1-L31】【1be583†L1-L80】
2. **Risk API Docker image build will fail.** ✅ Addressed: The Dockerfile now ships with a colocated `requirements.txt`, installs from it, and cleans up build artefacts to keep layers slim.【F:deploy/docker/risk-api/Dockerfile†L1-L25】【F:deploy/docker/risk-api/requirements.txt†L1-L15】

### High

1. **Simulation mode is enabled by default.** ✅ Addressed: `simulation.enabled` now defaults to `false` so production rollouts do not need to override the flag to avoid simulated order paths.【F:config/system.yaml†L1-L28】
2. **Docker images run as root.** ✅ Addressed: The risk API Dockerfile provisions an `app` user, adjusts ownership, and runs the service as non-root for defence in depth.【F:deploy/docker/risk-api/Dockerfile†L6-L25】
3. **Dual PostgreSQL drivers inflate attack surface.** ✅ Addressed: The dependency set now standardises on `psycopg[binary]` and drops the duplicate `psycopg2-binary` package.【F:pyproject.toml†L12-L46】

### Medium

1. **Insecure fallbacks require explicit suppression.** ✅ Addressed: A global runtime guard now raises if any `_ALLOW_INSECURE_DEFAULTS` toggle is set when the common service bootstrap executes, preventing production pods from silently downgrading to local stores while keeping pytest overrides functional.【F:shared/runtime_checks.py†L1-L63】【F:shared/common_bootstrap.py†L1-L120】
2. **Network policy egress is broad.** The blanket Cloudflare CIDR ranges that cover Kraken and CoinGecko also allow other Cloudflare-hosted endpoints. Tighten the allow-list with fully qualified domain egress via egress proxies or limit to vendor IP ranges verified with Cloudflare’s API.【F:deploy/k8s/networkpolicy.yaml†L1-L77】
3. **Config map embeds connection targets without TLS hints.** The shared FastAPI configmap encodes TimescaleDB, Redis, and Feast endpoints but omits TLS/port annotations or secrets references, which could lead to plain-text connections unless overridden. Document TLS expectations or move these values to secrets to avoid drift.【F:deploy/k8s/base/fastapi/configmap.yaml†L1-L34】

### Low / Observations

- **Circuit breakers and kill-switch configuration exist** but the kill-switch is disabled by default in shipped configs. Confirm rollout procedures flip the flag during incident testing.【F:deploy/k8s/base/fastapi/config/circuit-breakers.yaml†L1-L9】【F:deploy/k8s/base/fastapi/config/kill-switch.yaml†L1-L6】
- **On-call checklist is comprehensive**; integrate completion tracking with governance tooling to retain audit trails automatically.【F:docs/checklists/oncall.md†L1-L35】

## Next Steps

1. Restore CI health by satisfying pytest dependencies and adding a minimal smoke-test target to prevent regressions.
2. Harden container images: provide deterministic dependency manifests, drop root privileges, and ensure vulnerability scans cover the combined dependency tree.
3. Ship configuration validation that asserts simulation mode and insecure fallbacks are disabled before the services start.
4. Revisit network egress policy with security to ensure only mandated third-party ranges are reachable, potentially by routing via a controlled egress gateway.
