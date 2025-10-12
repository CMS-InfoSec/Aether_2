# ExternalSecret Validation Runbook

## Purpose

Ensure every Kubernetes `ExternalSecret` reconciles successfully before deployments and during incident response. ExternalSecrets deliver credentials from Vault and other secret stores; missing reconciliation blocks workloads that depend on them.

## Automated CI/CD Check

The CI pipeline runs `scripts/verify_externalsecrets_ready.sh`, which issues:

```bash
kubectl get externalsecrets -A -o json | jq '.items[] | select(( [ .status.conditions[]? | select(.type=="Ready") ] | length == 0 ) or any(.status.conditions[]?; .type=="Ready" and .status!="True"))'
```

The script fails the build if any ExternalSecret reports a `Ready` condition with a status other than `True`. A passing run prints `All ExternalSecrets report Ready=True.`

## Manual Verification Steps

1. Ensure your kubeconfig targets the cluster under investigation.
2. Run the validation script locally or execute the command directly:
   ```bash
   scripts/verify_externalsecrets_ready.sh
   ```
   or
   ```bash
   kubectl get externalsecrets -A -o json | jq '.items[] | select(( [ .status.conditions[]? | select(.type=="Ready") ] | length == 0 ) or any(.status.conditions[]?; .type=="Ready" and .status!="True"))'
   ```
3. If the command outputs nothing, every ExternalSecret is ready. Any JSON objects returned identify the namespace/name of secrets that require investigation.

## Troubleshooting Not Ready ExternalSecrets

- Inspect the ExternalSecret status for detailed error messages:
  ```bash
  kubectl describe externalsecret <name> -n <namespace>
  ```
- Validate the referenced `SecretStore`/`ClusterSecretStore` credentials and access policies.
- Confirm that the target secret engine contains the expected keys and versions.
- Re-run the validation after remediation to ensure the `Ready` condition transitions to `True`.
