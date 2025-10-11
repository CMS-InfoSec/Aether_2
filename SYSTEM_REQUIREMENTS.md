# System Requirements

This document summarises the compute footprint and critical dependencies for the
Aether trading platform as implemented in the repository manifests. Use it to
validate cluster capacity planning and confirm that the running services match
the documented architecture.

## Shared Infrastructure

- **TimescaleDB** – Three-node StatefulSet with 1 vCPU / 2 GiB memory limits per
  pod and 50 GiB of storage, plus a nightly backup CronJob writing to an
  auxiliary 100 GiB volume.【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L114】
- **Kafka** – Single-node KRaft broker by default (scaled to three replicas in
  the production overlay) requesting 0.5 vCPU and 1 GiB memory with 20 GiB of
  persistent storage.【F:deploy/k8s/base/kafka/statefulset.yaml†L1-L52】【F:deploy/k8s/overlays/production/kustomization.yaml†L24-L29】
- **NATS JetStream** – Deployment with 0.1 vCPU / 128 MiB requests and 0.5 vCPU /
  512 MiB limits storing state on an ephemeral volume.【F:deploy/k8s/base/nats/deployment.yaml†L1-L47】
- **Redis (session/cache)** – Two-node StatefulSet with 0.1 vCPU / 256 MiB
  requests and 5 GiB persistent volumes for append-only storage.【F:deploy/k8s/base/redis/deployment.yaml†L1-L53】
- **Feast Online Store** – Two replicas of the gRPC/HTTP serving binary backed by
  Redis and an in-cluster registry volume.【F:deploy/k8s/base/feast/deployment.yaml†L1-L56】
- **Kraken WebSocket Ingest** – Single replica streaming Kraken market data into
  Kafka with 0.1 vCPU / 128 MiB requests and readiness/liveness exec probes.【F:deploy/k8s/base/kraken-ws-ingest/deployment.yaml†L1-L55】

## Core Services

All application services expose FastAPI endpoints on port 8000 with liveness and
readiness probes at ``/healthz``. Resource requests are sized for the base
manifests; production overlays can scale replicas as needed.

| Service | Replicas | Requests (CPU / Memory) | Key dependencies |
| --- | --- | --- | --- |
| **Auth Service** (`auth-service`) | 3 | 100m / 256Mi | Postgres credentials via ``auth-service-config`` secret and Redis session store at ``redis.aether.svc``.【F:deploy/k8s/base/aether-services/deployment-auth.yaml†L1-L68】 |
| **Config Service** (`config-service`) | 2 | 100m / 256Mi | Postgres DSN from ``config-service-database`` and read-only tmp volume for caching responses.【F:deploy/k8s/base/aether-services/deployment-config.yaml†L1-L79】 |
| **Capital Allocator** (`capital-allocator`) | 2 | 200m / 512Mi | Postgres DSN ``CAPITAL_ALLOCATOR_DB_URL`` and optional SSL settings supplied by the ExternalSecret.【F:deploy/k8s/base/aether-services/deployment-capital-allocator.yaml†L1-L85】 |
| **Policy Service** (`policy-service`) | 2 | 200m / 512Mi | Kraken API credentials projected at ``/var/run/secrets/kraken`` for venue policy checks.【F:deploy/k8s/base/aether-services/deployment-policy.yaml†L1-L108】 |
| **Risk Service** (`risk-service`) | 2 | 200m / 512Mi | Postgres DSNs for compliance, diversification, and ESG datasets plus Kraken credential projections.【F:deploy/k8s/base/aether-services/deployment-risk.yaml†L1-L152】 |
| **OMS Service** (`oms-service`) | 2 | 200m / 512Mi | Kraken credentials, Redis session URL, and simulation-mode Postgres DSN for reconciliation.【F:deploy/k8s/base/aether-services/deployment-oms.yaml†L1-L120】 |
| **Fees Service** (`fees-service`) | 2 | 200m / 512Mi | Kraken credentials and Timescale/Fees database DSN provided by ``fees-service-database`` secret.【F:deploy/k8s/base/aether-services/deployment-fees.yaml†L1-L117】 |
| **Secrets Service** (`secrets-service`) | 2 | 200m / 512Mi | Kraken credentials for key management and a dedicated encryption key sourced from ``secrets-service-config``.【F:deploy/k8s/base/aether-services/deployment-secrets.yaml†L1-L111】 |
| **Universe Service** (`universe-service`) | 2 | 200m / 512Mi | Postgres DSN ``UNIVERSE_DATABASE_URL`` and Kraken credential projections for venue sync validation.【F:deploy/k8s/base/aether-services/deployment-universe.yaml†L1-L117】 |
| **Strategy Orchestrator** (`strategy-orchestrator`) | 2 | 200m / 512Mi (600m limit) | Strategy Postgres DSN from ``strategy-orchestrator-database`` for workflow state.【F:deploy/k8s/base/aether-services/deployment-strategy-orchestrator.yaml†L1-L84】 |

## Data & Workflow Automation

Nightly and intra-day workflows in ``ops/workflows/`` schedule Kraken stream
mirrors, CoinGecko batch imports, and report generation, relying on the Kafka and
Timescale infrastructure described above.【F:ops/workflows/kraken_stream_workflow.yaml†L1-L48】【F:ops/workflows/coingecko_ingest_workflow.yaml†L1-L60】

## Observability

Prometheus, Grafana, Alertmanager, Loki, and Tempo are deployed via the
``deploy/observability`` manifests to provide metrics collection, alerting, and
trace/log aggregation. Dashboards and alert rules referenced by the SLO
documentation are committed in this repository to keep the deployed stack in
lockstep with expectations.【F:deploy/observability/prometheus/prometheus.yaml†L1-L120】【F:deploy/observability/grafana/grafana.yaml†L1-L120】
