# Secrets Service Encryption Key Rotation

## Purpose
Provide a standardized procedure for rotating the AES encryption key that protects payloads managed by the Secrets Service.

## Preconditions
- Access to the Aether Kubernetes clusters (staging and production) with permissions to update Secrets and restart Deployments.
- Access to the Git repository to update the declarative manifest `deploy/k8s/base/aether-services/secret-secrets-service-config.yaml`.
- A secure workstation with OpenSSL (or equivalent tooling) for generating random keys.
- Coordinated maintenance window approved by Security and Platform teams if production traffic will be impacted.

## Generate a Replacement Key
1. On a secure host, generate a 32-byte AES key and encode it with base64:
   ```bash
   openssl rand -base64 32 > /tmp/secrets-service.aes
   ```
2. Store the value in the "Secrets Service Encryption Key" item in the team password manager so the previous key remains recoverable.

## Update the Kubernetes Secret
> ⚠️ **Do not commit encryption keys to Git.** The key must only live in secure secret storage and the Kubernetes Secret resource.

1. Export the key for the current shell session:
   ```bash
   export SECRET_ENCRYPTION_KEY="$(cat /tmp/secrets-service.aes)"
   ```
2. Apply the updated Secret to staging without writing the value to disk or Git:
   ```bash
   kubectl --context aether-staging create secret generic secrets-service-config \
     --from-literal=SECRET_ENCRYPTION_KEY="$SECRET_ENCRYPTION_KEY" \
     --dry-run=client -o yaml | \
     kubectl --context aether-staging apply -f -
   kubectl --context aether-staging rollout restart deployment/secrets-service
   kubectl --context aether-staging rollout status deployment/secrets-service
   ```
3. Verify the service logs show successful startup and encryption operations.
4. Update the password manager entry with the rotation timestamp and operator name.

## Promote to Production
1. Once staging validation passes, repeat the Secret update against production:
   ```bash
   kubectl --context aether-production create secret generic secrets-service-config \
     --from-literal=SECRET_ENCRYPTION_KEY="$SECRET_ENCRYPTION_KEY" \
     --dry-run=client -o yaml | \
     kubectl --context aether-production apply -f -
   kubectl --context aether-production rollout restart deployment/secrets-service
   kubectl --context aether-production rollout status deployment/secrets-service
   ```
2. Confirm production logs and metrics (error rate, latency) remain healthy for 30 minutes.
3. Unset the exported variable from your shell:
   ```bash
   unset SECRET_ENCRYPTION_KEY
   ```

## Post-Rotation Validation
- Run the integration suite focused on encryption/decryption (`pytest tests/integration/test_audit_chain.py -k encryption`).
- Confirm dependent services can retrieve and decrypt secrets without errors.
- Update the rotation record in the secrets ledger with timestamp, operator, and key fingerprint (hash the base64 value using SHA-256).

## Rollback Plan
- If issues occur, retrieve the previous key from the password manager entry and reapply it using the `kubectl create secret … | kubectl apply -f -` workflow above.
- Notify stakeholders via #security-ops and #platform-alerts channels.
- Conduct a post-incident review to determine root cause before attempting rotation again.

## Automation Opportunities
- Integrate this runbook into the CI pipeline so that a merged PR automatically updates the clusters via Argo CD sync.
- Schedule quarterly reminders in the Security team calendar to ensure the key rotation cadence is maintained.
