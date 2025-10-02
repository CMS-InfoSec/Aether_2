# OMS Resilience Chaos Suite

Validates OMS behavior across cascading failures by progressing through
connectivity loss, service restarts, and downstream back-pressure.

**Environment:** staging **Owner:** Trading Reliability Guild **Schedule:** warmup=PT5M, ramp_interval=PT3M

## SLO Summary

| SLO | Outcome | Details |
| --- | --- | --- |
| Max OMS p95 latency | ✅ Pass | max observed=260; targets=<= 250, <= 275 |
| No orphaned orders | ✅ Pass | max observed=0; targets=== 0 |
| Safe mode triggers when expected | ✅ Pass | max observed=True; targets=== False, == True |

## Stage Results

### ✅ WebSocket Drop to REST Fallback (ws_drop)

Blackhole outbound WebSocket connectivity from the client gateway and
validate REST order entry fallback capacity.

*Fault:* type=network.blackhole, target=edge-gateway-ws, duration=PT2M
*Actions:* Divert order entry to REST API clients; Scale REST rate-limiters by 2x
*Monitors:* metrics: edge.websocket.disconnections, rest.order_entry.qps, oms.latency.p95_ms; logs: edge-gateway, oms

| SLO | Observed | Target | Window | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| Max OMS p95 latency | 198 | <= 250 | PT5M | ✅ | - |
| No orphaned orders | 0 | == 0 | - | ✅ | - |
| Safe mode triggers when expected | false | == False | - | ✅ | Safe mode should remain inactive while REST absorbs traffic. |

### ✅ REST Fallback Saturation (rest_fallback)

Apply rate shaping to REST ingress to emulate degraded partner APIs and
confirm queuing controls keep latency and retries within expectations.

*Fault:* type=traffic.shaping, target=rest-order-entry, params={'drop_rate': 0.35, 'latency_injection_ms': 120}, duration=PT4M
*Actions:* Throttle non-critical order amendments; Enable adaptive retry with exponential backoff
*Monitors:* metrics: rest.order_entry.retry_rate, oms.latency.p95_ms, retry.queue.depth; logs: rest-order-entry, retry-controller

| SLO | Observed | Target | Window | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| Max OMS p95 latency | 228 | <= 250 | PT5M | ✅ | - |
| No orphaned orders | 0 | == 0 | - | ✅ | - |
| Safe mode triggers when expected | false | == False | - | ✅ | Automation handles backpressure without escalating to safe mode. |

### ✅ OMS Rolling Restart (oms_restart)

Recycle primary OMS instances to validate connection draining, warm
caches, and watchdog takeover procedures.

*Fault:* type=service.restart, target=oms, params={'batch_size': 2, 'wait_between_batches': 'PT1M'}
*Actions:* Drain in-flight sessions before recycle; Verify state sync via leader election metrics
*Monitors:* metrics: oms.instance.health, oms.latency.p95_ms, order.book.sync_lag; logs: oms, watchdog

| SLO | Observed | Target | Window | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| Max OMS p95 latency | 242 | <= 250 | PT10M | ✅ | - |
| No orphaned orders | 0 | == 0 | - | ✅ | - |
| Safe mode triggers when expected | true | == True | - | ✅ | Safe mode should briefly engage to gate manual supervision. |

### ✅ Partial Fill Storm (partial_fill_storm)

Replay market data bursts that drive partial fills and reconciliation
load to validate downstream clearing and alerting.

*Fault:* type=marketdata.replay, target=oms-reconciler, params={'scenario': 'partial-fill-burst', 'speedup': 3}, duration=PT6M
*Actions:* Enable enhanced reconciliation telemetry; Trigger clearing house sandbox responses
*Monitors:* metrics: reconciler.partial_fill_rate, oms.latency.p95_ms, alerting.safe_mode_signal; logs: reconciler, clearing-adapter

| SLO | Observed | Target | Window | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| Max OMS p95 latency | 260 | <= 275 | PT10M | ✅ | Relax latency budget due to expected reconciliation overhead. |
| No orphaned orders | 0 | == 0 | - | ✅ | - |
| Safe mode triggers when expected | true | == True | - | ✅ | Safe mode must remain active while reconciliation catches up. |

### ✅ Kafka Consumer Lag (kafka_lag)

Introduce broker throttling to create consumer lag and validate that
downstream services degrade gracefully while preserving order integrity.

*Fault:* type=messaging.throttle, target=kafka-broker, params={'throttle_kb_per_s': 256}, duration=PT8M
*Actions:* Pause non-critical analytics consumers; Promote standby broker to leader if lag exceeds SLA
*Monitors:* metrics: kafka.consumer.lag, oms.latency.p95_ms, safe_mode.active; logs: kafka, oms

| SLO | Observed | Target | Window | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| Max OMS p95 latency | 245 | <= 250 | PT10M | ✅ | - |
| No orphaned orders | 0 | == 0 | - | ✅ | - |
| Safe mode triggers when expected | true | == True | - | ✅ | Safe mode continues until lag clears to protect clients. |
