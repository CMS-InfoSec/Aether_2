#!/usr/bin/env bash
set -euo pipefail

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but was not found in PATH" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required but was not found in PATH" >&2
  exit 1
fi

if ! kubectl config current-context >/dev/null 2>&1; then
  echo "No Kubernetes context configured; skipping ExternalSecret readiness verification."
  exit 0
fi

# Capture ExternalSecret objects and select any that are missing a Ready=True condition.
not_ready_json=$(kubectl get externalsecrets -A -o json |
  jq -c '.items[] | select(( [ .status.conditions[]? | select(.type == "Ready") ] | length == 0 ) or any(.status.conditions[]?; .type == "Ready" and .status != "True"))')

if [[ -z "${not_ready_json}" ]]; then
  echo "All ExternalSecrets report Ready=True."
  exit 0
fi

echo "The following ExternalSecrets are not Ready:"
while IFS= read -r secret; do
  name=$(echo "$secret" | jq -r '.metadata.name')
  namespace=$(echo "$secret" | jq -r '.metadata.namespace')
  echo "- ${namespace}/${name}"
done <<< "${not_ready_json}"

exit 1
