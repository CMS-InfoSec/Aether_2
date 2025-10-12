# Vault Health Monitoring Runbook

## Overview

The `vault-health-monitor` CronJob polls the Vault `/v1/sys/health` endpoint
every five minutes and posts a Slack alert to the `#ops` channel when the
cluster is sealed, uninitialised, or unreachable. Alerts are delivered via the
`ops/slack/ops` webhook stored in Vault and rendered into the
`vault-health-monitor` Kubernetes secret.

## Scheduled job details

* **Schedule:** `*/5 * * * *`
* **Namespace:** `aether-{env}`
* **Image:** `ghcr.io/aether/ops-runtime:latest`
* **Command:** `python -m ops.monitoring.vault_health`
* **Secrets:** `vault-health-monitor`
  * `vault_token` — optional token used to authenticate the health request.
  * `slack_webhook` — Slack incoming webhook for the `#ops` channel.
* **Environment:**
  * `VAULT_ADDR=https://vault.platform.svc:8200`
  * `VAULT_STATUS_TIMEOUT_SECONDS=5`

The ExternalSecret definition lives at
`deploy/k8s/base/secrets/vault-health-monitor-external-secret.yaml` and pulls
both the Slack webhook and optional Vault token from Vault paths. The CronJob
definition is in
`deploy/k8s/base/secrets/vault-health-monitor-cronjob.yaml`.

## Alert handling

When the job detects a sealed or unreachable Vault, it exits with a non-zero
status and posts a `:rotating_light:` notification summarising the failure.

1. **Acknowledge in Slack:** Confirm the alert in `#ops` and ensure on-call is
   aware.
2. **Validate Vault status:**
   ```bash
   kubectl -n platform exec deploy/vault -- vault status
   ```
3. **If sealed:** Unseal using the documented operator procedure. Verify the
   CronJob succeeds on the next run and the Slack channel receives the recovery
   log line.
4. **If unreachable:** Inspect the Vault pods and the platform network policies.
   Restart pods if required and validate that the health endpoint returns `200`
   with `sealed=false` before closing the incident.

## Manual execution

To run the health check manually from a toolbox pod:

```bash
python -m ops.monitoring.vault_health
```

Ensure the `VAULT_ADDR`, `VAULT_TOKEN`, and `SLACK_WEBHOOK_URL` environment
variables are exported in the shell before invoking the module. The command
exits with status `1` when Vault is unhealthy.
