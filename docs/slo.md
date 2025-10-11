# Service Level Objectives

This document defines the quantitative availability and responsiveness expectations for mission-critical workflows. Each SLO is paired with Prometheus alerting thresholds to ensure we react before error budgets are exhausted.

## Summary

| Capability | SLI Definition | SLO Target | Prometheus Alert |
| --- | --- | --- | --- |
| Policy Latency | 95th percentile policy evaluation latency (`histogram_quantile(0.95, policy_latency_ms_bucket)`) | ≤ 200 ms over rolling 5 minutes | `policy_latency_p95_slo_breach` fires when > 200 ms for 2/3 evaluations |
| Risk Latency | 95th percentile risk validation latency (`histogram_quantile(0.95, risk_latency_ms_bucket)`) | ≤ 200 ms over rolling 5 minutes | `risk_latency_p95_slo_breach` fires when > 200 ms for 2/3 evaluations |
| OMS Latency | 95th percentile OMS submission latency (`histogram_quantile(0.95, oms_latency_ms_bucket)`) with a p99 early-warning rule | ≤ 500 ms p95 over rolling 5 minutes (`p99` early warning at 150 ms) | `oms_latency_p95_slo_breach` fires when > 500 ms for 2/3 evaluations; `oms_latency_slo_breach` warns when p99 > 150 ms for 10 minutes |
| WebSocket Ingest Latency | 99th percentile delta propagation latency (`histogram_quantile(0.99, ws_delivery_latency_seconds_bucket)`) | ≤ 0.300 s over rolling 5 minutes | `ws_latency_slo_breach` fires when > 0.250 s for 3/5 evaluations |
| Kill-Switch Response | Time from activation command to OMS halt (`kill_switch_response_seconds`) | ≤ 60 s per activation | `kill_switch_slo_warning` fires when > 45 s for a single evaluation |
| Model Canary Promotion | Completion time of canary to production promotion (`model_canary_promotion_duration_minutes`) | ≤ 45 minutes for 95% of promotions in 30-day window | `model_canary_promotion_slow` fires when 3 consecutive promotions exceed 30 minutes |

## Policy Latency
- **Measurement**: Prometheus histogram `policy_latency_ms` tagged by `symbol_tier` and `account_segment` tracks end-to-end policy evaluation time without exploding series cardinality.
- **Target**: Keep the rolling 5-minute p95 ≤ 200 ms to prevent backlog in the decisioning pipeline.
- **Alerting**: `policy_latency_p95_slo_breach` triggers when p95 latency breaches 200 ms for two of the last three evaluations.
- **Runbooks**: Use [`docs/runbooks/policy_latency.md`](runbooks/policy_latency.md) for triage steps.

## Risk Latency
- **Measurement**: Histogram `risk_latency_ms` captures validation latency per order candidate, aggregated by `symbol_tier` and `account_segment`.
- **Target**: Maintain rolling 5-minute p95 ≤ 200 ms to ensure OMS submissions are not blocked.
- **Alerting**: `risk_latency_p95_slo_breach` fires when p95 latency is above 200 ms for two of the last three intervals.
- **Runbooks**: Follow [`docs/runbooks/risk_latency.md`](runbooks/risk_latency.md).

## OMS Latency
- **Measurement**: Histogram `oms_latency_ms` records submission latency to external venues, keyed by `symbol_tier` and `account_segment`. A companion histogram `oms_order_latency_seconds` tracks p99 latency for an early-warning circuit.
- **Target**: Hold the rolling 5-minute p95 ≤ 500 ms to absorb market spikes without missing fills. The p99 alert raises awareness when latency exceeds 150 ms so responders can intervene before the SLO is breached.
- **Alerting**: `oms_latency_p95_slo_breach` fires when the rolling p95 is above 500 ms for two of the last three evaluations. `oms_latency_slo_breach` fires when p99 latency stays above 150 ms for 10 minutes.
- **Runbooks**: See [`docs/runbooks/oms_latency.md`](runbooks/oms_latency.md) and [`docs/runbooks/kill_switch_activation.md`](runbooks/kill_switch_activation.md) for remediation guidance.

## WebSocket Ingest Latency
- **Measurement**: Calculated from Kafka offset commit to WebSocket emit delta, exported via `ws_delivery_latency_seconds` histogram.
- **Target**: Maintain 99th percentile ≤ 300 ms to keep derived trading signals aligned.
- **Alerting**: Prometheus alert `ws_latency_slo_breach` warns when latency exceeds 250 ms for three out of five evaluations.
- **Runbooks**: Use [`docs/runbooks/websocket_desync.md`](runbooks/websocket_desync.md) for mitigation steps.

## Kill-Switch Response
- **Measurement**: `kill_switch_response_seconds` gauges elapsed time between command invocation and confirmation that OMS rejects new orders.
- **Target**: Achieve ≤ 60 s response to contain catastrophic exposure.
- **Alerting**: Prometheus alert `kill_switch_slo_warning` fires if any activation exceeds 45 s so responders can investigate before the SLO budget is consumed.
- **Runbooks**: Refer to [`docs/runbooks/kill_switch_activation.md`](runbooks/kill_switch_activation.md).

## Model Canary Promotion
- **Measurement**: `model_canary_promotion_duration_minutes` summarises time from canary deployment start to final promotion in Argo workflows.
- **Target**: 95% of promotions should complete within 45 minutes over a 30-day window, ensuring experiments are actionable.
- **Alerting**: Prometheus alert `model_canary_promotion_slow` triggers when three consecutive promotions exceed 30 minutes, indicating a trend toward violating the SLO.
- **Runbooks**: Use [`docs/runbooks/model_rollback.md`](runbooks/model_rollback.md) when canaries regress or stall.

## Implementation Notes
- Prometheus alerting rules in [`ops/monitoring/prometheus-rules.yaml`](../ops/monitoring/prometheus-rules.yaml) and [`deploy/observability/prometheus/prometheus.yaml`](../deploy/observability/prometheus/prometheus.yaml) annotate each alert with the relevant SLO target.
- Grafana dashboards provisioned in [`deploy/observability/grafana/dashboards/slos.json`](../deploy/observability/grafana/dashboards/slos.json) surface policy, risk, OMS (p95 and p99), WebSocket, kill-switch, and canary metrics with colour-coded thresholds that mirror this document.【F:deploy/observability/grafana/dashboards/slos.json†L1-L172】
