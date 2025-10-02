# Risk Latency Investigation

Follow this procedure when the risk validation latency SLO is threatened.

## Quick Checklist
- [ ] Verify `risk_latency_p95_slo_breach` alert details, focusing on symbol/account labels.
- [ ] Inspect upstream policy responses for anomalies that could cascade.
- [ ] Confirm risk engine pods are healthy and autoscaling rules are active.

## Diagnostic Steps
1. **Queue Depth**
   - Use `kubectl exec` to inspect Redis or Kafka queue depth powering the risk engine.
   - Ensure there is no backlog by checking the `risk_queue_depth` metric.
2. **Dependency Latency**
   - Review database slow query logs for the `risk_decisions` table.
   - Check external credit services status pages for incidents.
3. **Resource Saturation**
   - Run `kubectl top pods -n trading | grep risk-engine` to validate CPU and memory headroom.
   - Inspect pod events for OOM kills or restarts.

## Mitigation
- Temporarily raise the risk service concurrency limit using feature flags.
- Engage the database team if queries are throttled.
- Fail over to the backup region if dependency degradation is regional.

## Post-Incident
- File an incident review summarising root cause and remediation.
- Update runbook steps if new mitigations were required.
