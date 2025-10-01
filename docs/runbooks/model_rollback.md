# Model Rollback Runbook

## Purpose
Ensure rapid rollback to the last known good production model when a regression or anomaly is detected during canary or full rollout.

## Preconditions
- PagerDuty incident triggered by `model_latency_p99` or `model_error_rate` exceeding thresholds defined in `../slo.md`.
- Access to MLflow registry credentials and Argo Rollouts/Workflows permissions.
- Awareness of the current deployment wave (canary, partial, or full).

## Detection
1. Inspect the Argo Workflows canary report artifact for drift or quality regressions.
2. Review ML monitoring dashboards (AUC, calibration, latency) for deviations beyond SLO error budgets.
3. Confirm customer-facing impact via error logs or support tickets.

## Containment and Response Steps
1. **Freeze promotion**: Halt any active promotions with `argo terminate ml-canary-deployment`.
2. **Promote last stable model**: Run `python -m ml.registry.promote --stage Production --model-version $(cat CURRENT_STABLE)` from the ops bastion.
3. **Redeploy services**: Trigger redeploy using `kubectl rollout restart deploy/model-serving` and verify pods converge on the stable image digest.
4. **Invalidate features if necessary**: If root cause is feature drift, pause feature ingestion jobs via `argo suspend feature-refresh`.

## Recovery Validation
- Model serving pods report the expected stable model version hash.
- Inference latency and error rate return within their SLO targets for at least two evaluation windows.
- Canary workflow no longer queues promotion steps.

## Communication
- Update the incident channel with rollback completion time and current metrics.
- Notify product and customer success leads about potential client-facing effects and mitigation timeline.

## Postmortem Checklist
- Export comparison metrics between failed and stable models.
- Capture RCA for regression and update testing coverage or guardrails.
- Review canary criteria to ensure earlier detection next deployment.
