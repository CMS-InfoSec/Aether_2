# OMS NetworkPolicy Allowlist

The `allow-egress-kraken-coingecko-oms` NetworkPolicy confines inbound traffic to the OMS API to a small
set of controller and operator namespaces. This prevents lateral movement from arbitrary workloads in
our clusters while still allowing ingress traffic that is intentionally proxied.

## Approved ingress sources

| Source | Selector | Notes |
| ------ | -------- | ----- |
| NGINX ingress controller | `namespaceSelector.matchLabels.kubernetes.io/metadata.name=ingress-nginx`<br>`podSelector.matchLabels.app.kubernetes.io/name=ingress-nginx` | Required for public traffic that terminates at the managed ingress controller. |
| Trusted operator namespaces | `namespaceSelector.matchLabels.networking.aether.io/trusted="true"` | Apply this label to namespaces that host incident response or smoke-test jobs which must reach the OMS API. Keep this list short and review quarterly. |

Namespaces that legitimately need direct OMS access (for example `trading-ops` or temporary red-team
validations) **must** be labelled with `networking.aether.io/trusted: "true"`. Remove the label as soon as
access is no longer required.

## Verification checklist

Run the following smoke tests after deploying policy updates:

1. Confirm ingress controller connectivity:
   ```bash
   kubectl -n ingress-nginx exec deploy/ingress-nginx-controller -- \
     wget -qO- --timeout=5 http://oms-service.aether-services.svc.cluster.local:8000/healthz
   ```
   The command should return the OMS health payload with a `200 OK` status.
2. Validate trusted namespace access (replace `<namespace>` with a labelled namespace):
   ```bash
   kubectl -n <namespace> run oms-smoke --rm -i --image=busybox:1.36 --restart=Never --labels="networking.aether.io/trusted=true" -- \
     wget -qO- --timeout=5 http://oms-service.aether-services.svc.cluster.local:8000/healthz
   ```
3. Ensure untrusted namespaces are blocked:
   ```bash
   kubectl -n default run oms-block-test --rm -i --image=busybox:1.36 --restart=Never -- \
     wget -qO- --timeout=5 http://oms-service.aether-services.svc.cluster.local:8000/healthz
   ```
   This command must fail with a timeout or `wget: server returned error` response.

Record the outcomes in the change ticket and attach the CLI output for auditability.
