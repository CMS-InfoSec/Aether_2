# Backend Production Readiness Review

## Scope & Methodology
- Reviewed the FastAPI admin platform application factory and shared platform utilities to understand default wiring (authentication, scaling controller, metrics, health checks, audit logging).
- Analysed Kubernetes base manifests, ExternalSecret definitions, and observability stack overlays to evaluate deployment, configuration, and monitoring posture.
- Sampled critical documentation (README, SLOs, runbooks) to assess operational maturity and supporting materials.
- Focused on controls that impact production resilience, security, observability, and day-two operations for the backend services.

## Executive Summary
### Readiness Decision
- **Status:** Not production ready. Blockers include the lack of a dependency-aware `/readyz` endpoint, permissive fallbacks that can bypass Postgres and Argon2 safeguards, and insufficient automated static analysis coverage to detect regressions before release.【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L93-L108】【F:app.py†L120-L128】【F:auth/service.py†L455-L469】【F:pyproject.toml†L121-L153】

### Strengths
- The application factory enforces strong startup contracts by verifying Postgres connectivity, Redis-backed sessions, scaling controller lifecycle, metrics, correlation IDs, and audit integrations before serving traffic.【F:app.py†L206-L299】
- Runtime safety checks block insecure fallbacks and misconfiguration of account allowlists or simulation mode in production, reducing the chance that dev-mode toggles leak into live environments.【F:shared/runtime_checks.py†L49-L163】
- Deployment assets ship with hardened pod security contexts, ExternalSecret-driven DSNs, and comprehensive Prometheus/Grafana alerting rules that map directly to documented SLOs and runbooks.【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L21-L155】【F:deploy/k8s/base/secrets/external-secrets.yaml†L206-L254】【F:deploy/observability/prometheus/prometheus.yaml†L45-L200】【F:docs/slo.md†L1-L53】
- Operator documentation includes detailed kill-switch, incident, and SLO runbooks, supporting consistent response and auditability during crises.【F:docs/runbooks/kill-switch.md†L1-L52】

### Key Risks & Gaps
- Several deployments use the Prometheus `/metrics` endpoint for readiness, which can mask dependency failures or cause noisy restarts when metrics scraping stalls; a dedicated `/readyz` endpoint with explicit dependency checks is recommended.【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L93-L108】【F:deploy/k8s/base/aether-services/deployment-auth.yaml†L49-L60】
- The admin service silently falls back to in-memory repositories when psycopg is missing or insecure overrides are toggled, risking credential loss or bypassing Postgres durability in production if images are misbuilt or env flags leak; build-time validation and environment guardrails should make this failure hard.【F:app.py†L92-L151】【F:auth/service.py†L452-L470】
- Type-checking and static analysis coverage is narrow (focused on two files), limiting automated detection of regressions across the broader backend footprint; expanding mypy/ruff scopes and enforcing CI gates would improve confidence.【F:pyproject.toml†L108-L153】
- Dependency pins are fully locked to specific patch versions with large surface area (ML, data, infra libraries), increasing toil for security updates; adopting automated vulnerability scanning and scheduled dependency refreshes is necessary for ongoing readiness.【F:pyproject.toml†L11-L64】

## Detailed Findings & Recommendations
### Architecture & Deployment
- Kubernetes manifests define replicas, security contexts, PodDisruptionBudgets, and NetworkPolicies for core services, supporting baseline high availability and isolation.【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L10-L155】
- Readiness probes currently target `/metrics`, which only reflects exporter liveness. Introduce application-level `/readyz` handlers that exercise downstream dependencies (database, Redis, Kafka) and update probes accordingly.【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L93-L108】【F:deploy/k8s/base/aether-services/deployment-auth.yaml†L49-L60】
- ExternalSecret wiring ensures DSNs and sensitive tokens are sourced from Vault, but the configuration relies on manual key provisioning. Add automated reconciliation tests or admission policies validating required keys exist before deployment promotions.【F:deploy/k8s/base/secrets/external-secrets.yaml†L206-L254】

### Configuration & Secrets Management
- System-wide toggles (audit, simulation, stablecoin monitoring) live in versioned YAML, aligning change management with GitOps.【F:config/system.yaml†L1-L48】
- Runtime safety utilities assert allowlists and disable insecure defaults in production, yet they rely on environment detection; ensure cluster manifests set `ENV=production` and avoid exporting `AETHER_ALLOW_INSECURE_DEFAULTS` except in sandbox namespaces.【F:shared/runtime_checks.py†L19-L163】

### Reliability & Resilience
- The application factory validates Postgres writes via sentinel admin accounts and asserts Redis-backed session connectivity on startup, ensuring critical dependencies are healthy before the app is ready.【F:app.py†L206-L257】
- Scaling controller lifecycle management is embedded in the FastAPI lifespan hook, which is positive, but there is no explicit crash-loop protection if the scaling controller fails to start; propagate exceptions to fail startup rather than continuing with degraded automation.【F:app.py†L216-L225】
- Consider adding chaos and disaster recovery validations to CI/CD (e.g., running `chaos_tests.py` or scenario simulators) to ensure resilience mechanisms stay effective between releases.【F:chaos_tests.py†L1-L120】

### Security & Compliance
- Password hashing defaults to Argon2 with deterministic fallbacks only when dependencies are missing; however, enabling the fallback in production due to missing psycopg or argon2 wheels would downgrade security. Enforce dependency availability in container build pipelines and surface alerts if fallbacks activate at runtime.【F:auth/service.py†L83-L200】【F:auth/service.py†L452-L470】
- Session stores mandate Redis DSNs and refuse in-memory operation outside pytest, which protects multi-instance deployments.【F:app.py†L162-L188】
- Audit logging is wired via `SensitiveActionRecorder` and Timescale-backed audit store, satisfying traceability requirements; verify retention policies on the Timescale cluster align with regulatory obligations.【F:app.py†L206-L299】

### Observability & Alerting
- Prometheus rules capture trading KPIs, latency SLOs, circuit breakers, and scaling signals with runbook annotations, providing actionable alerts. Maintain drift detection thresholds with periodic reviews to avoid alert fatigue as volumes change.【F:deploy/observability/prometheus/prometheus.yaml†L45-L200】
- Metrics middleware exposes histograms/counters and optional OpenTelemetry tracing, but exporter activation depends on `PROMETHEUS_EXPORTER_PORT`. Ensure Kubernetes services scrape the in-process `/metrics` endpoint and disable the standalone exporter to avoid duplication.【F:metrics.py†L183-L209】
- Extend tracing initialization to emit warnings if OTEL exporters are misconfigured, preventing silent no-op tracing in production.【F:metrics.py†L165-L209】

### Data & Persistence
- Postgres admin repository bootstraps schema automatically, yet lacks migrations for future schema evolution; introduce Alembic migrations dedicated to admin tables to handle controlled changes and auditing.【F:auth/service.py†L452-L507】
- Timescale, Kafka, and Redis dependencies are assumed to be provisioned by base manifests; document recovery procedures for these stateful systems alongside existing runbooks to complete the DR plan.【F:deploy/k8s/base/kustomization.yaml†L1-L20】【F:docs/runbooks/disaster-recovery.md†L1-L120】

### Testing & Quality
- Test suite guidance exists (`pytest -q`), but CI enforcement of lint/type coverage is unclear. Expand mypy targets beyond `training_service.py`/`pg_session.py`, and require `ruff`/`pytest` in pipelines to catch regressions sooner.【F:README.md†L21-L40】【F:pyproject.toml†L108-L153】
- Consider adding smoke tests that hit the `/healthz` and future `/readyz` endpoints, validating dependency connectivity after deployment rollouts.【F:shared/health.py†L22-L56】

### Operations & Runbooks
- Comprehensive runbooks (kill-switch, latency investigations, model rollback) exist and reference concrete commands, supporting on-call response. Continue keeping runbooks in sync with manifest updates to avoid drift.【F:docs/runbooks/kill-switch.md†L1-L52】【F:docs/slo.md†L1-L53】
- README and SLO documents provide new operator onboarding context, but consolidating production-specific prerequisites (required secrets, environment flags, database migrations) into a single "production checklist" would streamline go-live reviews.【F:README.md†L1-L64】【F:docs/checklists/oncall.md†L1-L160】

## Recommended Next Steps
1. **Ship dependency-aware readiness endpoints.** Add a `/readyz` router to every FastAPI service that exercises Postgres (read/write sentinel), Redis (ping), Kafka (metadata), and any optional providers. Update the Kubernetes readiness probes in `deploy/k8s/base/aether-services/*` to target the new endpoint with appropriate initial delays/timeouts, and add smoke tests that assert a `503` response when dependencies fail.
2. **Harden container builds around critical wheels.** Extend the Docker build and deploy workflows to verify psycopg, argon2, and other security-sensitive wheels are installed (e.g., `python -c "import psycopg,argon2"`). Make the build fail on missing modules, and emit structured alerts if runtime fallback code paths activate so incidents are raised before production impact.
3. **Expand and gate static analysis.** Broaden `mypy` modules to include `app.py`, `auth/`, `shared/`, and `metrics.py`, enable `ruff` across the backend package, and wire both tools into CI required checks alongside `pytest`. Block merges when lint/type regressions occur and publish coverage summaries in build artifacts.
4. **Institute dependency refresh & scanning.** Schedule a monthly pip-compile refresh for `pyproject.lock`, execute vulnerability scans (e.g., Trivy, Dependabot, or Snyk) as part of the cadence, and track remediation SLAs in the operations checklist to keep the pinned set current and secure.
5. **Validate ExternalSecret integrity pre-promotion.** Automate a CI/CD job that queries Vault/ExternalSecrets to confirm all keys referenced in `deploy/k8s/base/secrets/external-secrets.yaml` exist before promoting manifests, preventing drift-induced runtime failures.
6. **Practice stateful service recovery.** Produce detailed recovery drills for Timescale, Kafka, and Redis—covering backup validation, restore runbooks, failover testing, and RTO/RPO verification—and automate at least quarterly tabletop or simulated exercises aligned with the disaster recovery guidance.
