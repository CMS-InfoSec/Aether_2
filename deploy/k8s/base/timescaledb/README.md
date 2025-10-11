# TimescaleDB Base Manifests

This directory contains the base Kubernetes resources used to operate TimescaleDB in the platform cluster. The StatefulSet now deploys three replicas with persistent storage and scheduled logical backups so that the database can survive node loss and support point-in-time recovery from the latest dump.

## Backup Strategy

- **Streaming replicas:** The StatefulSet runs three pods (`timescaledb-0..2`) backed by a shared headless service. TimescaleDB-HA manages replication between the pods to provide failover while keeping reads and writes available.
- **Persistent volumes:** Each replica provisions a `ReadWriteOnce` persistent volume claim (`data`) sized at 50&nbsp;GiB using the cluster's `standard` storage class.
- **Automated dumps:** A `timescaledb-pgdump` CronJob executes daily at 02:00 UTC. It authenticates with the same credentials as the StatefulSet and stores a gzipped `pg_dumpall` output on the `timescaledb-backup` persistent volume claim.

## Recovery Procedure

1. **Identify the desired snapshot.** List the backup files inside the `timescaledb-backup` PVC (`kubectl exec` into the CronJob pod or mount the PVC in a toolbox pod) and pick the timestamped archive you need.
2. **Scale down writers if needed.** If performing a full cluster restore, temporarily cordon application workloads or set them to maintenance mode to prevent new writes.
3. **Restore into a new replica (preferred).**
   - Scale the StatefulSet to provision a fresh replica (`kubectl scale statefulset timescaledb --replicas=4`).
   - Mount the backup PVC into the new pod (e.g., using an init container or manual `kubectl cp`).
   - Run `pg_restore` or `psql < backup.sql` inside the replica to seed it from the chosen dump.
   - Promote the newly restored replica and gracefully redirect traffic.
4. **In-place recovery (emergency).** If you must recover in-place, cordon the existing pods, delete the damaged PVC, recreate it from the backup archive, and restart the StatefulSet so TimescaleDB replays WAL and data from the restored files.
5. **Validate and resume traffic.** Run health checks (`SELECT now();`, replication status queries) before allowing writers back online.

Document the backup file used, commands executed, and validation evidence in the incident log for auditability.
