# Kill Switch Activation Runbook

## Purpose
Describe the controlled process for activating and deactivating the trading kill switch to mitigate catastrophic risk while meeting the 60-second response SLO.

## Related SLOs
- [Kill-Switch Response SLO](../slo.md#kill-switch-response) — activation to full halt must complete within 60 seconds.
- [OMS Latency SLO](../slo.md#oms-latency) — confirm post-activation recovery keeps latency within the 150 ms budget.

## Preconditions
- Trigger condition: kill-switch Prometheus alert `kill_switch_slo_warning` (derived from `histogram_quantile(0.99, kill_switch_response_seconds_bucket)`) predicting an SLO breach or manual directive from Head of Trading/Security.
- Access to OMS admin CLI and ArgoCD application controls.
- Communication channels with trading desks and compliance.

## Detection
1. Confirm alert details, including triggering metric values and impacted services.
2. Review OMS order rejection rates and open exposure to validate the need for shutdown.
3. Coordinate with trading lead to confirm that halting trading is acceptable.

## Activation Steps
1. **Announce intent**: Inform #ops-trading and #risk immediately, noting timestamp and reason for kill-switch request.
2. **Activate switch**: Run `python -m services.oms.tools kill-switch --activate` from the secure bastion. Ensure confirmation message indicates success.
3. **Verify halt**: Check `kill_switch_state` metric equals `1` and OMS rejects new orders within 30 seconds.
4. **Suspend automation**: Pause any workflows submitting orders, e.g., `argo suspend market-making`.

## Deactivation Steps
1. **Risk sign-off**: Obtain written approval from risk/compliance before re-enabling trading.
2. **Deactivate switch**: Execute `python -m services.oms.tools kill-switch --deactivate`.
3. **Gradual restore**: Re-enable automation in stages, monitoring order acceptance and latency metrics.
4. **Post-restore validation**: Ensure OMS latency meets the 150 ms SLO and kill-switch response metric resets below the 60-second target documented in `../slo.md`.

## Recovery Validation
- `kill_switch_state` returns to `0` once trading resumes.
- No unexpected order submissions occurred during the halt window.
- Prometheus alert resets and acknowledges the response time within budget.

## Communication
- Provide updates every 5 minutes in incident channel and log all commands issued.
- After deactivation, postmortem summary must be distributed to leadership and archived in the incident tracker.

## Postmortem Checklist
- Record timeline of decision, activation, and release.
- Review command audit logs for discrepancies.
- Evaluate automation improvements to reach the 60-second activation SLO consistently.
