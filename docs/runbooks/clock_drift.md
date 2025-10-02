# Clock Drift Remediation Playbook

Clock synchronisation is mandatory for regulatory audit trails and cross-venue
latency comparisons. The `clock_drift_exceeds_200ms` Prometheus alert fires when
Chrony reports more than 200 milliseconds of drift for at least 10 minutes.

## Detection

- Alert: `clock_drift_exceeds_200ms`
- Severity: `critical`
- Source metric: `chrony_tracking_last_offset_seconds` scraped from the
  `chrony-monitor` DaemonSet.
- Trigger condition: `abs(offset) > 0.2` for 10 minutes.

## Immediate Actions

1. **Acknowledge the alert** in PagerDuty and join the `#ops` Slack channel.
2. **Identify affected nodes** by inspecting the alert labels (`instance` and
   `node`) and querying Prometheus:
   ```promql
   chrony_tracking_last_offset_seconds{node="$NODE"}
   ```
3. **Check Chrony health** on the node:
   ```bash
   kubectl -n observability exec -it ds/chrony-monitor -c chronyd -- chronyc tracking
   kubectl -n observability exec -it ds/chrony-monitor -c chronyd -- chronyc sources -v
   ```
4. **Verify host time** against a reference:
   ```bash
   kubectl -n observability exec -it ds/chrony-monitor -c chronyd -- date -u
   ```
5. **Inspect node conditions** for NTP/clock issues (kernel logs, `dmesg`,
   virtualization host warnings).

## Remediation Steps

1. **Resync the node clock**:
   ```bash
   kubectl -n observability exec -it ds/chrony-monitor -c chronyd -- chronyc makestep
   ```
2. **Restart Chrony sidecar** if the offset persists:
   ```bash
   kubectl -n observability rollout restart ds/chrony-monitor
   ```
3. **Cordoning and draining**
   - If drift remains above 200 ms after resync, cordon the node to prevent
     trading workloads from scheduling:
     ```bash
     kubectl cordon $NODE
     kubectl drain $NODE --ignore-daemonsets --delete-emptydir-data
     ```
   - Engage the platform team to investigate hypervisor clock sources.
4. **Swap out hardware** when drift is correlated with specific bare-metal
   hosts. Document the serial number and open a vendor ticket.

## Post Incident

1. Capture a snapshot of Chrony stats for the incident ticket:
   ```bash
   kubectl -n observability exec -it ds/chrony-monitor -c chronyd -- chronyc sourcestats
   ```
2. Submit an incident timeline including:
   - When the alert fired and was acknowledged.
   - Nodes impacted and remedial actions taken.
   - Any secondary impacts (missed fills, reconciliation delays).
3. File a retrospective if drift exceeded 500 ms or impacted market access.
4. Update this runbook with contributing factors and mitigation learnings.

## Related Resources

- [Ops On-call Handbook](./oncall.md)
