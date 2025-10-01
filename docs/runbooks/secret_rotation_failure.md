# Secret Rotation Failure Runbook

## Purpose
Outline remediation actions when automated secret rotations fail or leave services in an inconsistent state.

## Preconditions
- Alert from `secret_rotation_last_success_seconds` exceeding 43,200 seconds (12 hours) or failed job notification from Vault/SealedSecrets pipeline.
- Access to secret management tooling (Vault CLI), Kubernetes cluster, and CI/CD logs.
- Knowledge of impacted services and their credential scopes.

## Detection
1. Review the failing rotation pipeline logs to identify error messages (permission denied, policy conflict, etc.).
2. Inspect Vault audit logs for denied access or version conflicts.
3. Confirm whether downstream services are failing auth using `kubectl describe pod` and checking crash loops.
4. Verify the last successful secret version in the registry to determine rollback candidates.

## Containment and Response Steps
1. **Disable automatic retries**: Pause the rotation workflow by setting `argo suspend secret-rotation` to avoid thrashing.
2. **Rollback to last good secret**: Retrieve previous secret version via `vault kv get -version=<N-1> secret/data/<path>` and reapply using `kubectl apply -f <secret-file>`.
3. **Regenerate credentials if compromised**: Coordinate with the provider (exchange, database) to issue new credentials and update Vault entries.
4. **Re-enable rotation**: Once stable, resume automation with `argo resume secret-rotation` and monitor the next run manually.

## Recovery Validation
- `secret_rotation_last_success_seconds` metric drops below 3,600 seconds following a successful run.
- All affected pods restart with updated secrets and no authentication failures.
- Vault shows the expected new secret version and policy attachments.

## Communication
- Notify #security-ops and #ops-platform channels with scope of impact and mitigation plan.
- Update the audit log with the incident reference and credential lifecycle details.

## Postmortem Checklist
- Document root cause and add regression tests or policy checks where applicable.
- Review rotation frequency and adjust SLO targets if the workload changed.
- Ensure all credentials rotated during the incident are flagged for follow-up verification.
- Coordinate with compliance to document credential handling per SOC 2 requirements and attach the signed attestation to the incident record.
