# Exchange Outage Runbook

## Purpose
Guide on-call responders through diagnosing and mitigating upstream exchange outages that impact ingestion pipelines and OMS order routing.

## Preconditions
- Pager alert from `exchange_connectivity_total` Prometheus rule or direct notification from exchange status page.
- Access to Kubernetes cluster with permissions to restart ingestion/OMS deployments.
- Slack access to #ops-trading and #exchange-relations channels.

## Related SLOs
- [OMS Latency SLO](../slo.md#oms-latency) — 99th percentile acknowledgement latency must remain ≤ 150 ms.
- [Kill-Switch Response SLO](../slo.md#kill-switch-response) — ensure circuit breakers remain responsive during failovers.

## Detection
1. Confirm alert context: `exchange_connectivity_total` dropping below 80% success for 5 minutes or OMS fill latency breaching the 150 ms SLO budget.
2. Check the exchange public status page and incident communications for acknowledged outages.
3. Inspect ingestion pod logs (`kubectl logs deploy/kraken-ingest -n data`) for repeated socket errors or HTTP 5xx responses.
4. Review OMS error dashboards for increases in order rejects or fill timeouts.

## Containment and Response Steps
1. **Throttle new orders**: Enable the OMS circuit breaker to cap order submission rates to 10% of normal volumes using `python -m services.oms.tools throttle --ratio 0.1`.
2. **Fail over data feeds**: Switch ingestion to backup exchanges by applying `kubectl apply -f ops/workflows/kraken_stream_workflow.yaml --prune -l app=kraken-backup`.
3. **Coordinate with trading**: Notify #ops-trading with the status, expected impact, and failover ETA.
4. **Escalate**: If outage exceeds 15 minutes, escalate to exchange account manager via runbook contacts and open a Sev-1 ticket.

## Recovery Validation
- Exchange status page indicates resolution and connectivity metric returns above 98% for 10 continuous minutes.
- OMS latency metrics fall back under the 150 ms SLO threshold as documented in [`docs/slo.md`](../slo.md).
- Ingestion pods show sustained successful heartbeats with no socket errors for 5 minutes.

## Communication
- Post updates every 15 minutes in #ops-trading and attach Prometheus graphs.
- File an incident report in the internal tracker referencing outage window and remediation steps.

## Postmortem Checklist
- Capture timeline of alerts and actions.
- Validate that failover automation executed successfully or document gaps.
- Create follow-up tasks for resilience improvements discovered during response.
