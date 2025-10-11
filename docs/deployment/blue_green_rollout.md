# Blue/Green Rollout Playbook

This playbook describes how to perform a blue/green deployment of the Aether platform on Linode Kubernetes. The procedure assumes the hardened ingress defaults and account-specific secret projections from the Helm chart are in place.

## 1. Preparation

1. **Tag the release.** Ensure the container image for the new version is pushed and tagged. Record the Helm chart version that encapsulates the ingress and secret changes.
2. **Verify migrations.** Run `ops/migrate_admin_repository.py` and account-scoped Timescale migrations. Use the new `ops/refresh_continuous_aggregates.py` helper to refresh analytics materialised views ahead of the cutover.
3. **Export a health snapshot.** Capture current `/healthz` and `/metrics` output for every service and store them in the governance audit trail.
4. **Stage config.** Commit any configuration overrides to the blue environment values file and verify secrets are present for company and director accounts.

## 2. Deploy the Green Stack

1. **Create the green namespace.** Use `kubectl create namespace aether-green`.
2. **Install the chart.** Deploy the Helm chart with `--namespace aether-green` and override the ingress hosts to use the staging subdomain.
3. **Bootstrap observability.** Set `PROMETHEUS_EXPORTER_PORT`/`OTEL_EXPORTER_OTLP_ENDPOINT` for each service so the new `metrics.configure_observability` hook binds scrape and trace exporters automatically.
4. **Seed state.**
   - Run database migrations.
   - Synchronise `.aether_state` artifacts (safe mode, capital allocator snapshots, account crypto keys) required for insecure-default fallbacks.
   - Execute `scripts/generate_openapi.py` against the green stack to archive the API contract.
5. **Smoke test.** Run the targeted CI workflow (`.github/workflows/ci.yaml`) and spot-check the governance audit tests against the green namespace credentials.

## 3. Traffic Switch

1. **Freeze writes.** Enable safe mode on the blue environment to halt order submission while keeping read APIs available.
2. **Flip DNS.** Update the ingress host records to point clients to the green load balancer. Confirm the hardened headers remain present.
3. **Monitor.** Watch Prometheus dashboards and OpenTelemetry traces for latency or error spikes for at least two evaluation cycles of the scaling controller.
4. **Re-enable trading.** Disable safe mode on the green stack once metrics remain steady.

## 4. Post-Deployment Verification

1. **Refresh aggregates.** Re-run `ops/refresh_continuous_aggregates.py` to ensure dashboards reflect the new traffic.
2. **Audit governance events.** Confirm kill-switch, safe mode, and manual override events appear in the governance log via `tests/test_governance_audit_logging.py`.
3. **Archive artefacts.** Store the generated OpenAPI snapshot, CI logs, and Prometheus scrape artifacts in the compliance bucket.

## 5. Rollback Procedure

1. **Re-enable safe mode** on the green stack.
2. **Restore DNS** to the blue ingress addresses.
3. **Validate** that the blue stack resumes processing intents.
4. **Investigate** the failing change using the preserved OpenTelemetry traces and Prometheus snapshots before attempting another rollout.

Document the full timeline in `docs/AUDIT_REPORT.md` under the deployment section after each blue/green event.
