# `/readyz` Readiness Contract and Rollout Plan

## Overview

This plan defines the production readiness contract for services exposing the shared `/readyz` endpoint, how the signal is wired into alerting, the standard response expected from the SRE rotation, and the communication and tracking approach for finishing the rollout across all FastAPI services.

## Readiness contract

Every service that mounts the shared router from `shared.readyz_router` must satisfy the following contract:

- **Dependency coverage:** The endpoint executes Postgres read and write sentinels, Redis ping, and Kafka metadata probes when the respective providers are configured. Optional providers must register a probe that asserts their availability (for example, feature stores or third-party APIs).
- **Timeout discipline:** Individual probes inherit the default deadline from `shared.readiness.PROBE_TIMEOUT`, and services must not override it with values greater than 5 seconds to keep Kubernetes eviction responsive.
- **Failure semantics:** Any probe failure returns HTTP `503` with a JSON body identifying the dependency (`{"dependency": "Redis", "error": "PING timeout"}`) so SREs can rapidly triage.
- **Test coverage:** Smoke tests in `tests/smoke/test_readyz_dependency_failures.py` must include the service to assert that dependency failures surface as `503` responses.
- **Kubernetes integration:** Deployments in `deploy/k8s/**` must target `/readyz` for readiness probes with an initial delay aligned to bootstrapping requirements (database migrations, cache warming, etc.).

## Alerting integration

1. **Metric export:** The FastAPI middleware emits `aether_readyz_success{service="<name>"}` and `aether_readyz_latency_seconds` histograms. The success gauge flips to `0` for failures and remains `1` when all probes pass.
2. **Prometheus rule:** The Observability team will deploy an alert rule:
   ```yaml
   - alert: ReadyzDependencyFailure
     expr: min_over_time(aether_readyz_success{service!="self-healer"}[5m]) == 0
     for: 5m
     labels:
       severity: page
       team: sre
     annotations:
       summary: "{{ $labels.service }} readiness probe failing"
       description: "Dependency readiness for {{ $labels.service }} has been failing for 5 minutes. Investigate the failing probe and escalate if dependency-wide."
   ```
3. **PagerDuty routing:** Alerts trigger the "Platform SRE" escalation policy. Services with dedicated on-call rotations can add a `team` label override in their deployment manifests to route to the owning team.
4. **Dashboarding:** Grafana dashboards include a `/readyz` panel with per-service spark lines, latency percentiles, and an annotation stream for maintenance windows.

## SRE response to `/readyz` alerts

When PagerDuty fires a `ReadyzDependencyFailure` alert, on-call SREs should:

1. **Acknowledge within 5 minutes** and join the `#alerts-readyz` Slack channel thread automatically created by the alert webhook.
2. **Inspect the failing dependency** via the JSON payload in the alert and confirm by curling the `/readyz` endpoint directly (for example, `kubectl exec` into a pod and run `curl -sSf localhost:8000/readyz`).
3. **Differentiate scopes:**
   - If only a single pod fails readiness, check the pod logs for migration or configuration errors and coordinate a restart if safe.
   - If multiple pods or services fail on the same dependency, escalate to the owning infrastructure team (e.g., `#db-oncall` for Postgres) and update the incident status page.
4. **Mitigation:** Apply temporary overrides documented in existing runbooks (cache flush, feature flag disable) to restore readiness while the root cause is addressed.
5. **Resolution:** Once the probe returns HTTP `200`, document the remediation in the incident ticket and resolve the PagerDuty alert. File a follow-up issue if code or configuration changes are required to avoid recurrence.

## Rollout communication plan

- **Kickoff announcement:** Send an RFC and summary to `#eng-platform` and the release managers mailing list outlining the contract, timeline, and service coverage matrix.
- **Weekly updates:** Publish progress every Tuesday in the platform status update, highlighting services that adopted the router and outstanding blockers.
- **Change management:** Major migrations (switching readiness probes in production) require a CAB ticket with at least 24 hours' notice. Include a rollback plan referencing the prior `/metrics` probe to speed reversal if needed.
- **Stakeholder demos:** Host an enablement session with service owners to demonstrate probe registration, local testing, and alert workflows.

## Completion tracking

- **Source of truth:** Track adoption in the `/readyz` rollout table within `docs/fastapi_entrypoints.md`; each row must move from ⏳ to ✅ once the shared router and smoke tests are in place.
- **Issue board:** Create a GitHub project board "Readyz Rollout" with one card per service. Cards move through `Backlog → In progress → In review → Done`.
- **Alert verification:** After each deployment flips the readiness probe, trigger a synthetic readiness failure (for example, pausing the Redis sidecar or toggling a feature flag that forces the probe to fail) to validate alert delivery end-to-end. Record the PagerDuty incident ID in the project card.
- **Sign-off:** The platform lead signs off when all services show ✅ in the rollout table, all project board cards are in `Done`, and PagerDuty analytics confirm no open readiness incidents from the rollout period.

