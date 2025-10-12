# Disaster Recovery Automation Playbook

Disaster-recovery (DR) coverage is implemented through a combination of
Kubernetes CronJobs and operator-driven scripts. This playbook documents the
automation and provides manual execution guidance. All commands below are
validated automatically against the deployed jobs and scripts to prevent drift.

## Nightly TimescaleDB Backup (`timescaledb-pgdump`)

The production TimescaleDB instance is snapshotted nightly by the
`timescaledb-pgdump` CronJob defined in
`deploy/k8s/base/timescaledb/statefulset.yaml`. The job executes the exact shell
script shown below to push compressed logical dumps onto the
`timescaledb-backup` persistent volume.

<!-- DR-CHECK: cronjob=timescaledb-pgdump manifest=deploy/k8s/base/timescaledb/statefulset.yaml -->
```bash
set -euo pipefail
BACKUP_FILE="/backups/timescaledb-$(date +%Y%m%d%H%M%S).sql.gz"
export PGPASSWORD
pg_dumpall -h "$PGHOST" -p "$PGPORT" -U "$POSTGRES_USER" | gzip > "$BACKUP_FILE"
```

To trigger an ad-hoc backup, run:

```bash
kubectl create job --from=cronjob/timescaledb-pgdump timescaledb-pgdump-manual
```

## Monthly Restore Drill (`timescaledb-restore-drill`)

The staging cluster runs a monthly restore drill using
`deploy/k8s/overlays/staging/restore-drill-cronjob.yaml`. The CronJob mounts the
same backup volume as production and executes the restore verification module.

<!-- DR-CHECK: cronjob=timescaledb-restore-drill manifest=deploy/k8s/overlays/staging/restore-drill-cronjob.yaml -->
```bash
python -m ops.backup.restore_drill
```

Manual drill executions use the same container image and command:

```bash
kubectl create job --from=cronjob/timescaledb-restore-drill manual-restore-drill
```

The drill expects access to the TimescaleDB credentials secret and will report
results to the on-call Slack webhook when configured. Set
`RESTORE_KEEP_DATABASE_ON_FAILURE=true` for post-mortem inspection.

## Operator-Driven Restore Playbook (`dr_playbook.py`)

For full cluster restores outside of the automated drill, operators use the
Python playbook. It relies on the `DR_*` environment variables described in the
module docstring to connect to TimescaleDB, Redis, MLflow, and object storage.

### Create a snapshot bundle

<!-- DR-CHECK: cli=dr_playbook.py command=snapshot -->
```bash
python dr_playbook.py snapshot
```

The command builds a crash-consistent bundle and uploads it to the configured
object store prefix. The CLI prints the resulting S3 key.

### Restore from a stored snapshot

<!-- DR-CHECK: cli=dr_playbook.py command=restore -->
```bash
python dr_playbook.py restore s3://aether-dr/disaster-recovery/<snapshot_id>.tar.gz
```

During restore the playbook drops existing objects by default and rehydrates the
cluster. Pass `--no-clean` to preserve existing schemas when validating the
bundle in a scratch environment.

### Direct module invocation (automation reuse)

Automation workflows (for example, the CronJob above) invoke the restore drill
module directly. Operators can reuse the same entry point locally.

<!-- DR-CHECK: module=ops.backup.restore_drill callable=main -->
```bash
python -m ops.backup.restore_drill
```

The module locates the newest backup archive, restores it into a temporary
database, validates table counts and backup metadata, and finally posts a status
message to Slack when the webhook is configured.
