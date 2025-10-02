# OMS Latency Investigation

This runbook covers mitigation for OMS submission latency SLO alerts.

## Quick Checklist
- [ ] Confirm `oms_latency_p95_slo_breach` alert context for impacted symbols/accounts.
- [ ] Review OMS infrastructure dashboards for connection pool saturation.
- [ ] Check with exchange status pages for upstream incidents.

## Diagnostic Steps
1. **Exchange Connectivity**
   - Inspect FIX/REST gateway logs for throttling or rate-limit errors.
   - Run synthetic order submissions against the sandbox endpoint to gauge response times.
2. **Infrastructure Health**
   - Validate pod resource usage with `kubectl top pods -n trading | grep oms`.
   - Check Nginx ingress latency and error rates.
3. **Downstream Dependencies**
   - Ensure clearing service acknowledgements are not delayed.
   - Review message broker lag metrics for the OMS topic.

## Mitigation
- Increase OMS worker replicas during sustained load.
- Switch affected symbols to backup gateways when available.
- Coordinate with exchanges to raise rate limits if necessary.

## Post-Incident
- Capture remediation steps in the incident tracker.
- Schedule load testing if capacity limits were reached.
