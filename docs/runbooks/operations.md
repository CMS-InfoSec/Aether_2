# Operations Runbook

## Overview
This runbook describes the day-2 operational workflows for the Aether risk platform. It covers environment validation, deployment health checks, data pipeline verification, and communication steps when service-level objectives (SLOs) degrade.

## 1. Environment validation
1. Confirm cluster access:
   ```bash
   kubectl config use-context aether-prod
   kubectl get nodes
   ```
2. Validate ArgoCD application status:
   ```bash
   argocd app list --project aether-risk
   argocd app get aether-risk-prod
   ```
3. Ensure policy engines are running:
   ```bash
   kubectl get deployment -n kyverno kyverno
   kubectl get deployment -n gatekeeper-system gatekeeper-controller-manager
   ```

## 2. Deployment health checks
1. Check FastAPI services:
   ```bash
   kubectl get deploy -n aether-prod | grep risk
   kubectl describe deploy risk-api-prod -n aether-prod
   ```
2. Inspect ingress routing via Grafana or curl:
   ```bash
   kubectl run tmp --rm -i --tty --image=curlimages/curl -- bash
   curl https://risk.aether.local/api/healthz -H "Host: risk.aether.local"
   ```
3. Validate Kafka/NATS connectivity from the ingestor pod:
   ```bash
   kubectl exec -n aether-prod deploy/marketdata-ingestor-prod -- nc -z kafka-prod 9092
   kubectl exec -n aether-prod deploy/marketdata-ingestor-prod -- nats-server --help
   ```

## 3. Data freshness verification
1. Query TimescaleDB for the latest market tick:
   ```bash
   kubectl exec -n aether-prod statefulset/timescaledb-prod -- psql -d marketdata -c "select max(event_time) from market_ticks;"
   ```
2. Inspect Feast feature latency via Feast CLI:
   ```bash
   kubectl exec -n aether-prod deploy/feast-online-prod -- feast materialize-incremental 15m
   ```

## 4. Incident response and escalation
* Thresholds:
  * Risk API P99 latency > 500ms for 10 minutes triggers PagerDuty SEV-2.
  * Market data gap > 2 minutes triggers PagerDuty SEV-3.
* Escalation path:
  1. Page on-call quant engineer.
  2. Notify #risk-ops Slack channel with alert context and current mitigation steps.
  3. File an incident in the incident tracker referencing the Prometheus alert ID.

## 5. Post-incident review checklist
- [ ] Capture Grafana dashboard snapshots.
- [ ] Export Loki query for `service=risk-api` covering the incident window.
- [ ] Attach SBOM artifacts from the latest pipeline run to the incident ticket.
- [ ] Summarize remediations and create follow-up tasks in Jira.
