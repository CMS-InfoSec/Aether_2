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
2. Copy the value into a password manager entry for audit purposes.

## Update the Kubernetes Secret
1. Edit `deploy/k8s/base/aether-services/secret-secrets-service-config.yaml` and replace the placeholder with the new base64 value.
2. Commit the change and open a pull request for review.
3. After approval, deploy to staging:
   ```bash
   kubectl --context aether-staging apply -f deploy/k8s/base/aether-services/secret-secrets-service-config.yaml
   kubectl --context aether-staging rollout restart deployment/secrets-service
   kubectl --context aether-staging rollout status deployment/secrets-service
   ```
4. Verify the service logs show successful startup and encryption operations.

## Promote to Production
1. Once staging validation passes, apply the manifest to production:
   ```bash
   kubectl --context aether-production apply -f deploy/k8s/base/aether-services/secret-secrets-service-config.yaml
   kubectl --context aether-production rollout restart deployment/secrets-service
   kubectl --context aether-production rollout status deployment/secrets-service
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
