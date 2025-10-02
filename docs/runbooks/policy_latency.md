# Policy Latency Investigation

This runbook outlines the steps to follow when the policy evaluation latency SLO is at risk.

## Quick Checklist
- [ ] Confirm the `policy_latency_p95_slo_breach` alert in Prometheus.
- [ ] Review the `Trading Latency Percentiles` Grafana dashboard for the affected symbol/account pair.
- [ ] Check the policy service autoscaling status and recent deploys.

## Diagnostic Steps
1. **Validate Input Load**
   - Inspect the policy service request rate in Grafana and compare it with historical baselines.
   - Verify upstream sequencer message backlog using `kubectl logs deploy/sequencer`.
2. **Service Health**
   - Ensure pods are not CPU throttled: `kubectl top pods -n trading | grep policy`.
   - Look for recent restarts: `kubectl get pods -n trading -l app=policy-service`.
3. **Model Performance**
   - Review model inference logs for anomalies or slow responses.
   - If a new model was deployed, consider rolling back using the change management procedure.

## Mitigation
- Scale the policy service horizontally by increasing the replica count.
- If external dependencies are slow, enable the policy cache feature flag.
- Escalate to the on-call ML engineer if latency is caused by model regression.

## Post-Incident
- Record findings in the incident tracker.
- Update the SLO dashboard annotations with remediation details.
