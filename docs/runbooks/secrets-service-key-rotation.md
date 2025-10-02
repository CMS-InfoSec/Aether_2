# Secrets Service Encryption Key Rotation

## Purpose
Provide a standardized procedure for rotating the AES encryption key that protects payloads managed by the Secrets Service.

## Preconditions
- Access to the Aether Kubernetes clusters (staging and production) with permissions to update Secrets and restart Deployments.
- Access to the secret management tooling for the cluster (Sealed Secrets controller or direct `kubectl` access to the namespace).
- A secure workstation with OpenSSL (or equivalent tooling) for generating random keys.
- Coordinated maintenance window approved by Security and Platform teams if production traffic will be impacted.

## Generate a Replacement Key
1. On a secure host, generate a 32-byte AES key and encode it with base64:
   ```bash
   openssl rand -base64 32 > /tmp/secrets-service.aes
   ```
2. Copy the value into a password manager entry for audit purposes.

## Update the Kubernetes Secret

1. Export the previously generated key into an environment variable without exposing it in shell history. If you stored it in
   `/tmp/secrets-service.aes`, run:
   ```bash
   export SECRET_ENCRYPTION_KEY="$(tr -d '\n' < /tmp/secrets-service.aes)"

   ```
   If the key was copied into a password manager, retrieve it from there instead of regenerating a new value.
2. Create an updated secret manifest locally. If the cluster uses Sealed Secrets, run:
   ```bash
   kubectl --context aether-staging \
     create secret generic secrets-service-config \
     --namespace aether-services \
     --from-literal=SECRET_ENCRYPTION_KEY="$SECRET_ENCRYPTION_KEY" \
     --dry-run=client -o yaml \
   | kubeseal --format yaml --scope cluster-wide > /tmp/secrets-service-config.sealed.yaml
   ```
   Apply the sealed secret:
   ```bash
   kubectl --context aether-staging apply -f /tmp/secrets-service-config.sealed.yaml
   ```
   For clusters without Sealed Secrets, apply the secret directly without persisting it:
   ```bash
   kubectl --context aether-staging \
     create secret generic secrets-service-config \
     --namespace aether-services \
     --from-literal=SECRET_ENCRYPTION_KEY="$SECRET_ENCRYPTION_KEY" \
     --dry-run=client -o yaml \
   | kubectl --context aether-staging apply -f -
   ```
   > **Note:** The GitOps base no longer includes a placeholder `secrets-service-config` Secret. This manual apply (or a sealed
   > secret committed to a secure repo) is the source of truth for the rotated key.
3. Restart the deployment to pick up the new key:
   ```bash
   kubectl --context aether-staging rollout restart deployment/secrets-service
   kubectl --context aether-staging rollout status deployment/secrets-service
   ```
4. Verify the service logs show successful startup and encryption operations, then clear the environment variable:
   ```bash
   unset SECRET_ENCRYPTION_KEY
   ```

## Promote to Production
1. Once staging validation passes, repeat the secret creation process against production using the exact same key that was
   recorded earlier. Retrieve it securely (for example, `cat /tmp/secrets-service.aes` or copy it from the password manager)
   and apply it using the appropriate secure method (Sealed Secrets or direct `kubectl apply -f -`).
   ```bash
   export SECRET_ENCRYPTION_KEY="$(tr -d '\n' < /tmp/secrets-service.aes)"
   kubectl --context aether-production \
     create secret generic secrets-service-config \
     --namespace aether-services \
     --from-literal=SECRET_ENCRYPTION_KEY="$SECRET_ENCRYPTION_KEY" \
     --dry-run=client -o yaml \
   | kubectl --context aether-production apply -f -
   kubectl --context aether-production rollout restart deployment/secrets-service
   kubectl --context aether-production rollout status deployment/secrets-service
   unset SECRET_ENCRYPTION_KEY
   rm -f /tmp/secrets-service.aes
   ```
2. Confirm production logs and metrics (error rate, latency) remain healthy for 30 minutes.

## Post-Rotation Validation
- Run the integration suite focused on encryption/decryption (`pytest tests/integration/test_audit_chain.py -k encryption`).
- Confirm dependent services can retrieve and decrypt secrets without errors.
- Update the rotation record in the secrets ledger with timestamp, operator, and key fingerprint (hash the base64 value using SHA-256).

## Rollback Plan
- If issues occur, reapply the previous manifest from Git history and restart the deployment.
- Notify stakeholders via #security-ops and #platform-alerts channels.
- Conduct a post-incident review to determine root cause before attempting rotation again.

## Automation Opportunities
- Integrate this runbook into the CI pipeline so that a merged PR automatically updates the clusters via Argo CD sync.
- Schedule quarterly reminders in the Security team calendar to ensure the key rotation cadence is maintained.
