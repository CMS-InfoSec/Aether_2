# Admin Database Migration

This runbook describes how to move administrator credentials from the legacy
in-memory store into the shared Postgres/Timescale repository now required by the
admin FastAPI application.

## Prerequisites

1. Provision a Postgres/Timescale database accessible from the cluster.
2. Collect the connection string (DSN) with an account that has privileges to
   create tables and upsert administrator rows.
3. Export existing administrator accounts to a JSON file. Each entry must include
   `email`, `admin_id`, `mfa_secret`, and either `password_hash` or `password`.
   Optionally, include `allowed_ips` as an array of CIDR strings.

## Update Secrets and Deployments

1. Create or update the Kubernetes secret that stores the admin database DSN:

   ```bash
   kubectl create secret generic admin-platform-database \
     --from-literal=dsn="postgresql://admin:password@timescale.example.com/admin" \
     --dry-run=client -o yaml | kubectl apply -f -
   ```

2. Configure the deployment (Helm values or manifest) to expose the DSN via one
   of the recognised environment variables:

   - `ADMIN_POSTGRES_DSN`
   - `ADMIN_DATABASE_DSN`
   - `ADMIN_DB_DSN`

   Example Helm values snippet:

   ```yaml
   backendServices:
     adminPlatform:
       enabled: true
       env:
         - name: ADMIN_POSTGRES_DSN
           valueFrom:
             secretKeyRef:
               name: admin-platform-database
               key: dsn
         - name: ADMIN_DB_SSLMODE
           value: require
   ```

3. Redeploy the admin platform workload to pick up the new environment variable.

## Migrate Administrator Accounts

1. Run the migration helper from your workstation or an administrative pod:

   ```bash
   ADMIN_POSTGRES_DSN="postgresql://admin:password@timescale.example.com/admin" \
   python ops/migrate_admin_repository.py --source legacy_admins.json
   ```

   The script will upsert each entry into the shared repository. Plaintext
   passwords are automatically re-hashed before insertion.

2. Confirm the migration succeeded by checking the script output and querying the
   database for the expected email addresses:

   ```sql
   SELECT email, admin_id FROM admin_accounts ORDER BY email;
   ```

## Verification

1. Restart the admin platform pod (or roll the deployment) to trigger the startup
   health check introduced in this release. The pod will fail fast if it cannot
   connect to the database or persist the sentinel account.
2. Inspect the logs for `Admin repository is not writable` errors. Absence of the
   error indicates the repository accepted writes.
3. Perform a login using an administrator account to ensure credentials and MFA
   secrets survived the migration.

## Rollback

If migration fails or the deployment cannot reach the database:

1. Scale the admin platform deployment to zero to prevent repeated crash loops.
2. Restore access to the previous environment variable or configuration.
3. Re-run the migration after correcting the DSN or database availability.
