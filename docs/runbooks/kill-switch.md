# Kill-Switch Procedure

The kill-switch disables external trading integration and isolates the risk platform from upstream market data (Kraken/CoinGecko) to prevent erroneous portfolio actions.

## Preconditions
- Incident commander authorizes the kill-switch activation.
- Approval captured in the incident channel (#risk-ops) with a change ticket reference.

## Activation steps
1. Scale down ingestion and trading surface area:
   ```bash
   kubectl scale deploy/marketdata-ingestor-prod --replicas=0 -n aether-prod
   kubectl patch networkpolicy allow-kraken-coingecko-egress-prod -n aether-prod \
     --type='merge' -p '{"spec":{"egress":[]}}'
   ```
2. Freeze outbound order flow:
   ```bash
   argocd app sync aether-risk-prod --grpc-web --revision kill-switch
   ```
   The `kill-switch` branch removes trading webhooks and sets `RISK_ENFORCEMENT_MODE=observe` via Kustomize patches.
3. Confirm data isolation:
   ```bash
   kubectl exec -n aether-prod deploy/risk-api-prod -- curl -Is https://api.kraken.com | head -n 1
   ```
   Expect a timeout due to the tightened NetworkPolicy.

## Rollback steps
1. Restore NetworkPolicy egress targets from the `deploy/k8s/base/networkpolicies/egress-marketdata.yaml` manifest and sync via ArgoCD.
2. Scale ingestion back to desired state defined in the production overlay:
   ```bash
   kubectl scale deploy/marketdata-ingestor-prod --replicas=3 -n aether-prod
   ```
3. Validate Prometheus `MarketDataStale` alert clears within 10 minutes.

## Audit requirements
- Record activation and rollback timestamps in the incident tracker.
- Attach Loki logs proving outbound traffic was halted.
- File a post-mortem referencing the kill-switch change set.
