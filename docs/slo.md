# Service Level Objectives

This document defines the quantitative availability and responsiveness expectations for mission-critical workflows. Each SLO is paired with Prometheus alerting thresholds to ensure we react before error budgets are exhausted.

## Summary

| Capability | SLI Definition | SLO Target | Prometheus Alert |
| --- | --- | --- | --- |
| OMS Latency | 99th percentile order acknowledgement latency (`oms_order_latency_seconds{quantile="0.99"}`) | ≤ 0.150 s over rolling 5 minutes | `oms_latency_slo_breach` fires when > 0.120 s for 2/3 evaluations |
| WebSocket Ingest Latency | 99th percentile delta propagation latency (`ws_delivery_latency_seconds{quantile="0.99"}`) | ≤ 0.300 s over rolling 5 minutes | `ws_latency_slo_breach` fires when > 0.250 s for 3/5 evaluations |
| Kill-Switch Response | Time from activation command to OMS halt (`kill_switch_response_seconds`) | ≤ 60 s per activation | `kill_switch_slo_warning` fires when > 45 s for a single evaluation |
| Model Canary Promotion | Completion time of canary to production promotion (`model_canary_promotion_duration_minutes`) | ≤ 45 minutes for 95% of promotions in 30-day window | `model_canary_promotion_slow` fires when 3 consecutive promotions exceed 30 minutes |

## OMS Latency
- **Measurement**: Derived from OMS acknowledgement telemetry emitted per order, aggregated into Prometheus histogram `oms_order_latency_seconds`.
- **Target**: Keep the 99th percentile ≤ 150 ms to satisfy trading partner SLAs.
- **Alerting**: Prometheus rule `oms_latency_slo_breach` fires when the rolling 5-minute p99 exceeds 120 ms for two out of the last three intervals, giving buffer before the SLO is violated.
- **Runbooks**: See [`docs/runbooks/exchange_outage.md`](runbooks/exchange_outage.md) and [`docs/runbooks/kill_switch_activation.md`](runbooks/kill_switch_activation.md) for remediation guidance.

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
- Grafana dashboards provisioned in [`deploy/observability/grafana/grafana.yaml`](../deploy/observability/grafana/grafana.yaml) include dedicated panels with threshold markers that align to these targets.
