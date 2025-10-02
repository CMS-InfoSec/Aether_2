# Reporting Automation

Automated reporting ensures the risk committee receives daily exposure summaries, limit breaches, and market data quality metrics.

## Daily report workflow
1. The GitHub Actions workflow `Build OCI Images` uploads SBOMs and artifacts.
2. A scheduled cron job (see script below) pulls Prometheus metrics and aggregates them into a CSV summary.
3. The script emails the summary to `risk-committee@aether.dev` and uploads it to the GRC drive.

## Execution schedule
- Runs at 06:00 UTC every trading day (Monday–Friday) using Kubernetes CronJobs.
- CronJob definition references the container image built from `scripts/daily_report.py` packaged into the risk-ops toolbox image.

## Validation checks
- Confirms Prometheus SLO queries return data within 30 seconds.
- Ensures exported CSV contains at least the BTC, ETH, and SOL universes defined in `docs/config/universe-controls.yaml`.
- Verifies Loki log exports succeed for the prior 24-hour window to detect ingest gaps.

## Escalation
If the script exits with a non-zero status:
1. PagerDuty notifies the reporting on-call rotation.
2. Run `kubectl logs job/daily-risk-report` to identify the failing step.
3. Re-run manually:
   ```bash
   python docs/runbooks/scripts/daily_report.py --date $(date -u +%Y-%m-%d)
   ```

## API authentication requirements

Operations frequently uses the reporting and compliance endpoints while audit partners
review daily exports. The following HTTP headers must be provided with each request to
pass the new security dependencies:

| Endpoint | Audience | Required headers |
| --- | --- | --- |
| `GET /reports/xai` | Admin only | `X-Account-ID` set to an account in `ADMIN_ACCOUNTS` |
| `GET /logs/export` | Auditors | `X-Role: auditor` and `X-Account-ID` matching the configured auditor allow-list |
| `GET /compliance/export` | Auditors | `X-Role: auditor` and `X-Account-ID` matching the configured auditor allow-list |
| `GET /alerts/prioritized` | Admin only | `X-Account-ID` set to an account in `ADMIN_ACCOUNTS` |
| `GET /alerts/active`, `GET /alerts/policies` | Admin only | `X-Account-ID` set to an account in `ADMIN_ACCOUNTS` |

Auditor accounts are defined in the audit mode configuration (see `audit_mode.AuditModeConfig`).
Administrative calls may additionally require MFA headers where noted by each service.
