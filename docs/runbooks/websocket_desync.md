# WebSocket Desynchronization Runbook

## Purpose
Provide a deterministic response plan when downstream consumers observe desynchronization between WebSocket streams and canonical order book snapshots.

## Preconditions
- Alert from `ws_sequence_gap_ratio` exceeding 2% for 3 consecutive evaluation periods.
- Access to Redis cache, Kafka topics, and WebSocket gateway pods.
- Feature flag permissions to pause broadcast fan-out.

## Detection
1. Verify the alert in Prometheus and Grafana dashboards; confirm that gaps are from a specific venue or across all venues.
2. Inspect the WebSocket gateway logs for `sequence_gap_detected` warnings.
3. Validate the latest order book snapshot in Redis against Kafka topic offsets to confirm divergence direction.
4. Poll a reference client to confirm client-facing desync symptoms.

## Containment and Response Steps
1. **Pause broadcasts**: Toggle the `ws.broadcast.enabled` flag via `python -m services.universe.flags disable ws.broadcast` to stop propagating corrupted deltas.
2. **Rebuild cache**: Trigger a cache warmup from canonical snapshots by running `python -m services.universe.backfill --scope orderbooks --force`.
3. **Resync offsets**: Align the WebSocket gateway consumer offsets with Kafka by executing `kubectl exec deploy/ws-gateway -- python -m services.universe.ws sync-offsets`.
4. **Resume broadcasts**: Re-enable the broadcast flag once cache consistency is validated.

## Recovery Validation
- `ws_sequence_gap_ratio` metric returns below 0.5% for 10 minutes.
- Spot-check 3 symbol feeds comparing WebSocket deltas with REST snapshots and ensure parity.
- Redis cache reports matching sequence IDs with Kafka offsets.

## Communication
- Announce desync mitigation progress in #ops-realtime and include expected restore time.
- Notify integration partners subscribed to dedicated feeds via status page update.

## Postmortem Checklist
- Document the root cause and contributing factors.
- Capture metrics before/after resync and attach to the incident ticket.
- Evaluate automation opportunities for offset realignment.
