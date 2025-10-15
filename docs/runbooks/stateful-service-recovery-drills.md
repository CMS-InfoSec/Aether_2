# Stateful Service Recovery Drills

This playbook expands the core [disaster recovery automation](./disaster-recovery.md)
into detailed drills for the TimescaleDB, Kafka, and Redis platforms. Each drill
covers backup validation, full restore steps, failover testing, and objective
verification so responders can prove the recovery posture on a recurring basis.

## Recovery objectives

| Service     | Recovery Time Objective (RTO) | Recovery Point Objective (RPO) |
| ----------- | ----------------------------- | ------------------------------ |
| TimescaleDB | ≤ 15 minutes for automated restore drills | ≤ 24 hours based on nightly logical dumps |
| Kafka       | ≤ 10 minutes to re-establish leader throughput after broker failure | ≤ 15 minutes thanks to dual-stream replication checkpoints |
| Redis (Feast online store) | ≤ 10 minutes to restore feature serving | ≤ 60 minutes by replaying encrypted snapshot manifests |

The RTO targets align with the automated workflows in `ops/backup` and the
chaos exercises in `ops/chaos`. RPO targets reflect the cadence of backup jobs
and snapshot manifests.

## TimescaleDB drill

1. **Backup validation**  
   * Nightly logical dumps are created by the `timescaledb-pgdump` CronJob; the
     monthly staging drill invokes `python -m ops.backup.restore_drill` to
     decompress the latest archive, replay it into a temporary database, and
     verify schema counts and optional validation queries before cleaning up the
     environment.【F:docs/runbooks/disaster-recovery.md†L8-L59】【F:ops/backup/restore_drill.py†L1-L199】
   * Capture the runtime of the drill by reading the Slack notification emitted
     from `ops.backup.restore_drill.main`; the duration forms the baseline for
     RTO verification.【F:ops/backup/restore_drill.py†L200-L287】
2. **Restore runbook**  
   * For production incidents, use `python dr_playbook.py snapshot` to produce a
     crash-consistent bundle spanning TimescaleDB, Redis, and MLflow artifacts,
     then `python dr_playbook.py restore <snapshot_uri>` to rehydrate the
     cluster. Run with `--no-clean` inside a scratch namespace when validating a
     snapshot ahead of a switchover.【F:dr_playbook.py†L1-L168】【F:docs/runbooks/disaster-recovery.md†L61-L82】
3. **Failover testing**  
   * Exercise the TimescaleDB high-availability plan quarterly by cordoning the
     primary statefulset pod and ensuring the replica becomes leader within the
     15-minute RTO window. Track query availability through the staging
     application’s health checks instrumented via `metrics.py` to confirm end
     users experience minimal disruption.【F:metrics.py†L1-L104】
4. **RTO/RPO verification**  
   * Confirm the restore completes within 15 minutes by reviewing the duration
     logged by `ops.backup.restore_drill`.  
   * Validate the most recent dump timestamp and manifest ID from the backup log
     produced by `ops.backup.backup_job.BackupJob` to ensure no more than 24
     hours of data exposure.【F:ops/backup/backup_job.py†L200-L278】

## Kafka drill

1. **Backup validation**  
   * Export topic configurations and consumer offsets using the broker
     automation that accompanies the Kafka consumer lag chaos stage. Review the
     exported offsets alongside replication checkpoints recorded in the signal
     bus to ensure dual-stream parity.【F:ops/chaos/chaos_suite.yml†L140-L199】【F:strategy_bus.py†L1-L243】
2. **Restore runbook**  
   * Use the strategy bus tooling to bootstrap topics in a recovery cluster.
     Instantiate the `KafkaNATSAdapter` to republish control-plane events, then
     replay retained payloads using the warm start coordinator to realign order
     books before resuming live intake.【F:services/common/adapters.py†L1308-L1452】【F:services/oms/warm_start.py†L21-L388】
3. **Failover testing**  
   * Run the `kafka_lag` stage from `ops/chaos/chaos_suite.yml` or invoke the
     `KafkaProxyController` drop routine to simulate a broker outage. Validate
     that non-critical consumers pause, safe-mode remains engaged, and the
     promoted broker clears lag within ten minutes.【F:ops/chaos/chaos_suite.yml†L160-L199】【F:ops/chaos/chaos_monkey.py†L175-L310】
4. **RTO/RPO verification**  
   * Record the time from broker isolation to restored throughput when the lag
     stage resolves; this verifies the ten-minute RTO.  
   * Confirm that replicated offsets differ by less than fifteen minutes by
     inspecting the consumer history maintained by the in-memory adapter and the
     replay checkpoints used by the warm start coordinator.【F:services/common/adapters.py†L1308-L1452】【F:services/oms/warm_start.py†L21-L388】

## Redis (Feast) drill

1. **Backup validation**  
   * Execute `python -m ops.backup.feast_backup backup` to encrypt the Redis
     snapshot and Feast registry into Linode Object Storage. Ensure the manifest
     includes both artifacts and retention metadata before uploading.【F:ops/backup/feast_backup.py†L1-L204】
2. **Restore runbook**  
   * Decrypt the stored artifacts with the recorded nonce and restore the Redis
     snapshot. Reapply the Feast registry YAML captured in the backup to make
     feature serving available again.【F:ops/backup/feast_backup.py†L205-L380】
3. **Failover testing**  
   * Simulate Redis loss by switching the Feast online store URL in staging to a
     standby instance, then replay the latest encrypted snapshot. Monitor the
     `RedisRestartLogStore` metrics in the self-healer to confirm automated
     restart attempts remain bounded.【F:self_healer.py†L59-L155】【F:ops/backup/feast_backup.py†L1-L204】
4. **RTO/RPO verification**  
   * Measure the elapsed time to restore features via the warm start replay to
     satisfy the ten-minute RTO.  
   * Use the manifest timestamp embedded by `FeastBackupJob` to verify the
     snapshot age does not exceed sixty minutes and adjust backup frequency if
     required.【F:ops/backup/feast_backup.py†L1-L204】

## Quarterly tabletop automation

Run the quarterly tabletop scheduler to rotate through the drills automatically
and deliver scenario plans to the incident response channel:

```bash
python -m docs.runbooks.scripts.stateful_service_exercise --date 2024-07-01 --output ./artifacts/stateful-drill-plan.md
```

The planner selects a scenario for each service based on the quarter, writes a
Markdown briefing, and optionally posts to Slack when the webhook environment
variable is configured. See `ops/workflows/stateful-service-recovery-drills.yaml`
for the Argo CronWorkflow that executes this automation on the first day of each
quarter.【F:docs/runbooks/scripts/stateful_service_exercise.py†L1-L214】【F:ops/workflows/stateful-service-recovery-drills.yaml†L1-L52】
