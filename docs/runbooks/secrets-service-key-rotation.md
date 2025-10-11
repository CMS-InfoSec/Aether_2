# Secrets Service Encryption Key Rotation

## Purpose
Provide a standardized procedure for rotating the AES encryption key that protects payloads managed by the Secrets Service.

## Preconditions
- Access to the Aether Kubernetes clusters (staging and production) with permissions to update Secrets and restart Deployments.
- Access to the secret management tooling for the cluster (Vault access to `secret/aether/secrets-service` and permissions to restart deployments).
- A secure workstation with OpenSSL (or equivalent tooling) for generating random keys.
- Coordinated maintenance window approved by Security and Platform teams if production traffic will be impacted.

## Generate a Replacement Key
1. On a secure host, define a temporary location for the key and generate a 32-byte
   AES value encoded with base64:
   ```bash
   export KEY_FILE=/tmp/secrets-service.aes
   umask 077
   openssl rand -base64 32 > "$KEY_FILE"
   ```

   > **Note:** Setting a restrictive `umask` ensures the file is readable only by the
   > current operator.
2. Copy the value stored in `$KEY_FILE` into a password manager entry for audit
   purposes. The exact same key must be reused for every environment updated during
   this rotation.

## Update the Managed Secret

1. Export the previously generated key into an environment variable without exposing it in shell history. If you stored it in
   `$KEY_FILE`, run:
   ```bash
   export SECRET_ENCRYPTION_KEY="$(tr -d '\n' < "$KEY_FILE")"

   ```
   If the key was copied into a password manager and the temporary file was removed, set
   `SECRET_ENCRYPTION_KEY` by pasting the stored base64 value instead of reading
   from `$KEY_FILE`.
2. Write the value into Vault so the `secrets-service-config` ExternalSecret reconciles the update:
   ```bash
   vault kv put secret/aether/secrets-service \
     SECRET_ENCRYPTION_KEY="$SECRET_ENCRYPTION_KEY"
   ```
   > **Note:** The GitOps base renders the Kubernetes secret via
   > `deploy/k8s/base/secrets/secrets-service-config-external-secret.yaml`, so no
   > manual `kubectl apply` steps are required.
3. Confirm the ExternalSecret synced the new value in staging:
   ```bash
   kubectl --context aether-staging -n aether-services get externalsecret secrets-service-config
   kubectl --context aether-staging -n aether-services get secret secrets-service-config
   ```
   Once the secret shows a recent refresh time, restart the deployment to pick up the
   key:
   ```bash
   kubectl --context aether-staging rollout restart deployment/secrets-service
   kubectl --context aether-staging rollout status deployment/secrets-service
   ```
4. Verify the service logs show successful startup and encryption operations, then clear the environment variable:
   ```bash
   unset SECRET_ENCRYPTION_KEY
   ```

## Promote to Production
1. Once staging validation passes, write the exact same key into the production Vault path and restart the deployment:
   ```bash
   export SECRET_ENCRYPTION_KEY="$(tr -d '\n' < "$KEY_FILE")"
   vault kv put secret/aether/secrets-service \
     SECRET_ENCRYPTION_KEY="$SECRET_ENCRYPTION_KEY"

   kubectl --context aether-production -n aether-services get externalsecret secrets-service-config
   kubectl --context aether-production -n aether-services get secret secrets-service-config
   kubectl --context aether-production rollout restart deployment/secrets-service
   kubectl --context aether-production rollout status deployment/secrets-service
   ```
   Remove any local copies once validation succeeds:
   ```bash
   unset SECRET_ENCRYPTION_KEY
   rm -f "$KEY_FILE"
   unset KEY_FILE
   ```
2. Confirm production logs and metrics (error rate, latency) remain healthy for 30 minutes.

## Post-Rotation Validation
- Run the integration suite focused on encryption/decryption (`pytest tests/integration/test_audit_chain.py -k encryption`).
- Confirm dependent services can retrieve and decrypt secrets without errors.
- Update the rotation record in the secrets ledger with timestamp, operator, and key fingerprint (hash the base64 value using SHA-256).

## Rollback Plan
- If issues occur, retrieve the previous key from the password manager entry and write it back to Vault with `vault kv put secret/aether/secrets-service SECRET_ENCRYPTION_KEY=<old value>`, then restart the deployment.
- Notify stakeholders via #security-ops and #platform-alerts channels.
- Conduct a post-incident review to determine root cause before attempting rotation again.

## Automation Opportunities
- Integrate this runbook into the CI pipeline so that a merged PR automatically updates the clusters via Argo CD sync.
- Schedule quarterly reminders in the Security team calendar to ensure the key rotation cadence is maintained.
