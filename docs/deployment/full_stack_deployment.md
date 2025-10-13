# Full Platform Deployment Guide

This runbook describes how to deploy the complete Aether platform — data stores,
backend microservices, scheduled pipelines, and the Next.js UI — onto a
Kubernetes cluster using Helm. The process assumes you will operate the cluster
through Lens and rely on Helm releases for both the shared dependencies and the
application stack so that a single workflow can bootstrap and reconfigure the
platform end to end.

## 1. Understand the Platform Components

* **Data plane dependencies** – TimescaleDB (PostgreSQL), Redis/Feast, Kafka,
  and NATS provide storage, feature serving, and streaming backbones referenced
  by every service deployment.【F:deploy/k8s/base/kustomization.yaml†L5-L27】
* **Backend services** – The Helm chart exposes configuration for each FastAPI
  service (risk, policy, OMS, secrets, reports, etc.) including images, resource
  profiles, ingress, HPAs, and PodDisruptionBudgets.【F:deploy/helm/aether-platform/values.yaml†L1-L1049】
* **Frontend** – The `aether-2-ui` deployment serves the web client and relies
  on environment variables that point at the public risk API ingress.
  【F:deploy/helm/aether-platform/values.yaml†L1052-L1080】
* **Security & runtime guardrails** – Global settings enforce TLS issuers,
  seccomp profiles, Kraken credential projections, and secure network policies
  so the release is production ready by default.
  【F:deploy/helm/aether-platform/values.yaml†L1-L103】【F:deploy/helm/aether-platform/templates/backend-deployments.yaml†L1-L152】

## 2. Prerequisites

1. **Cluster & access**
   * Kubernetes 1.24+ with dynamic storage classes that satisfy the TimescaleDB
     and Kafka StatefulSets.【F:deploy/k8s/base/timescaledb/statefulset.yaml†L1-L140】
   * Lens connected to the cluster with credentials that permit Helm releases,
     secret creation, and namespace administration.
2. **Tooling**
   * `helm` 3.8+, `kubectl`, `docker` (or `nerdctl`), and `python3` installed on
     your workstation running Lens.
3. **Container registry**
   * Push access to a registry for the images you build locally. Update
     `image.repository`/`tag` entries in your Helm values to point at that
     registry.【F:deploy/helm/aether-platform/values.yaml†L200-L1049】
4. **Secrets & PKI**
   * Vault/secret manager populated with database DSNs, TLS bundles, Kraken API
     keys, and account allow-lists that the chart consumes through Kubernetes
     Secrets.【F:deploy/helm/aether-platform/values.yaml†L19-L1049】
5. **DNS & certificates**
   * DNS records for each ingress hostname and a cluster issuer that matches
     `global.tls.issuer` (default `letsencrypt-production`).
     【F:deploy/helm/aether-platform/values.yaml†L3-L30】【F:deploy/helm/aether-platform/templates/ui.yaml†L83-L118】

## 3. Build and Publish Container Images

The repository ships Dockerfiles for the key workloads. Build them and push to
your registry before you run Helm so the chart references published tags.

```bash
# Risk API
export REGISTRY=registry.example.com/aether
export TAG=$(git rev-parse --short HEAD)

docker build -f deploy/docker/risk-api/Dockerfile \
  -t "$REGISTRY/risk-service:$TAG" .
docker push "$REGISTRY/risk-service:$TAG"

# Real-time market data ingestion
docker build -f deploy/docker/kraken-ws-ingest/Dockerfile \
  -t "$REGISTRY/kraken-ws-ingest:$TAG" .
docker push "$REGISTRY/kraken-ws-ingest:$TAG"

# Batch risk feature ingestion
docker build -f deploy/docker/risk-ingestor/Dockerfile \
  -t "$REGISTRY/risk-ingestor:$TAG" .
docker push "$REGISTRY/risk-ingestor:$TAG"
```

Override the corresponding `image.repository` and `image.tag` fields for each
service you own in your Helm values file so the deployments reference your
artifacts.【F:deploy/helm/aether-platform/values.yaml†L200-L1049】 If you rely on
pre-built upstream images for ancillary services, keep the defaults.

Build the web client using your CI pipeline or a local Node build, then publish a
container image (or use the default `ghcr.io/aether/aether-2-ui:latest`). Update
`ui.image` if you host a private build.【F:deploy/helm/aether-platform/values.yaml†L1052-L1071】

## 4. Prepare Kubernetes Secrets

Every backend deployment reads credentials and connection strings from secrets
referenced in the chart. Either sync them from Vault or create them manually
before running Helm. The table below lists the critical secrets; adjust the list
if you disable optional services.

| Secret name | Required keys | Purpose |
|-------------|---------------|---------|
| `account-service-database` | `dsn` | SQLAlchemy DSN for account storage.【F:deploy/helm/aether-platform/values.yaml†L188-L215】 |
| `account-service-secrets` | `encryptionKey` | Fernet key for customer data encryption.【F:deploy/helm/aether-platform/values.yaml†L180-L209】 |
| `auth-service-config` | `AUTH_DATABASE_URL`, `AUTH_JWT_SECRET` | Login API DSN plus JWT signing secret.【F:deploy/helm/aether-platform/values.yaml†L204-L221】 |
| `behavior-service-database` | `dsn` | Behavior analytics database DSN.【F:deploy/helm/aether-platform/values.yaml†L223-L268】 |
| `compliance-service-database` | `dsn` | Compliance checks datastore consumed by the risk API.【F:deploy/helm/aether-platform/values.yaml†L228-L236】【F:deploy/helm/aether-platform/values.yaml†L1196-L1203】 |
| `capital-allocator-database` | `dsn` | Capital allocation database used by the allocator service.【F:deploy/helm/aether-platform/values.yaml†L240-L252】 |
| `config-service-database` | `dsn` | Configuration service DSN used by migrations and runtime APIs.【F:deploy/helm/aether-platform/values.yaml†L253-L265】 |
| `kraken-keys-*` | `credentials.json` | Kraken trading credentials for company/directors when Kraken access is enabled.【F:deploy/helm/aether-platform/values.yaml†L19-L60】 |
| `platform-account-allowlists` | `admins`, `directors` | Account-level access control lists consumed by the services.【F:deploy/helm/aether-platform/values.yaml†L61-L78】 |
| `release-manifest-database` | `dsn` | TimescaleDB DSN for release manifest auditing.【F:deploy/helm/aether-platform/values.yaml†L333-L341】 |
| `strategy-orchestrator-database` | `dsn` | Strategy orchestration datastore DSN.【F:deploy/helm/aether-platform/values.yaml†L307-L319】 |

Create the secrets with `kubectl create secret` or configure External Secrets so
Lens shows them as ready before you install the chart. Make sure TLS secrets for
Ingress (`*-tls`) exist or that cert-manager can mint them automatically.

## 5. Install Data Plane Dependencies with Helm

Install supporting databases and brokers via Helm so the application chart can
bind to them automatically. The service expectations are encoded in the default
config maps and values, so keep hostnames consistent.

1. **Add upstream Helm repositories** for the dependencies you plan to use (for
   example Bitnami for PostgreSQL/Redis and the Apache charts for Kafka/NATS).
2. **Create a dependencies values file** (`deps-values.yaml`) that aligns service
   names with the DNS the platform expects, such as
   `timescaledb.aether.svc.cluster.local`, `redis.aether.svc.cluster.local`, and
   `kafka.aether.svc.cluster.local`.
   【F:deploy/k8s/base/fastapi/configmap.yaml†L25-L54】【F:deploy/helm/aether-platform/values.yaml†L31-L112】
3. **Install the releases** either from the Lens Helm UI or the CLI:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm repo add kafka https://charts.bitnami.com/bitnami

# PostgreSQL with TimescaleDB extensions
helm upgrade --install timescaledb bitnami/postgresql \
  --namespace aether --create-namespace \
  --values deps-values.yaml

# Redis for session storage / Feast online store
helm upgrade --install redis bitnami/redis \
  --namespace aether --values deps-values.yaml

# Kafka and NATS for streaming integrations
helm upgrade --install kafka bitnami/kafka \
  --namespace aether --values deps-values.yaml
helm upgrade --install nats nats/nats \
  --namespace aether --values deps-values.yaml
```

When the dependency pods reach `Ready`, record their connection strings and load
those values into the Kubernetes secrets referenced in Section 4. If you enforce
TLS, match the secret names projected into the pods in the chart values so the
FastAPI services mount the correct certificates.

## 6. Configure Helm Values for the Platform

Create a `platform-values.yaml` that sets global options, image overrides, and
per-service environment variables. Start from the published defaults and adjust
for your domains, secrets, and scaling targets.【F:deploy/helm/aether-platform/values.yaml†L1-L1179】

Key items to review:

* `global.ingressClassName` and `global.tls.issuer` to match your ingress
  controller and cert-manager issuer.【F:deploy/helm/aether-platform/values.yaml†L3-L30】
* `global.krakenSecrets` if you trade on Kraken. Set the secret checksum so Helm
  rolls pods when credentials rotate.【F:deploy/helm/aether-platform/values.yaml†L19-L60】
* `runtimeSafety._ALLOW_INSECURE_DEFAULTS` must remain `false` for production so
  services refuse to use on-disk fallbacks.【F:deploy/helm/aether-platform/values.yaml†L116-L118】
* Backend service `env` sections for DSNs, Redis URLs, and API keys. Reference
  the secrets you created earlier.【F:deploy/helm/aether-platform/values.yaml†L180-L1049】
* UI environment variables (`NEXT_PUBLIC_API_BASE_URL`, `VITE_API_BASE_URL`) so
  compiled assets call the deployed risk API ingress.
  【F:deploy/helm/aether-platform/values.yaml†L1052-L1071】
* Ingress hosts for every service and the UI to align with your DNS entries.
  【F:deploy/helm/aether-platform/values.yaml†L180-L1179】

## 7. Deploy the Platform Chart

With dependencies online and values prepared, install the chart. You can do this
through Lens (Helm ➜ Charts ➜ Install from path) or via CLI:

```bash
helm upgrade --install aether-platform deploy/helm/aether-platform \
  --namespace aether \
  --values platform-values.yaml
```

Helm renders Deployments, Services, Ingresses, HPAs, PodDisruptionBudgets, and
NetworkPolicies for every enabled backend plus the UI, applying the global
security context and Prometheus scraping annotations automatically.
【F:deploy/helm/aether-platform/templates/backend-deployments.yaml†L1-L188】【F:deploy/helm/aether-platform/templates/ui.yaml†L1-L118】【F:deploy/helm/aether-platform/templates/networkpolicies.yaml†L1-L120】

Use Lens to watch the release: open the **Workloads ➜ Deployments** view for the
`aether` namespace and verify all pods become Ready, then inspect the Helm tab to
see the rendered values and manifest history.

## 8. Run Database Migrations and Bootstraps

After the services are online, execute the configuration and account migrations
exactly once so schema and seed data are present. Because the risk-service
deployment already has the environment variables your Helm release injected,
re-use that runtime to run the bootstrap commands:

```bash
kubectl exec deploy/risk-service -n aether -- \
  python migrate.py apply config/bootstrap.yaml

kubectl exec deploy/risk-service -n aether -- \
  python -c "import account_migrations as m;\nfrom sqlalchemy import create_engine;\nengine = create_engine(\"$ADMIN_POSTGRES_DSN\");\nm.upgrade(engine)"
```

The commands match the bootstrap scripts used in non-Kubernetes environments and
seed the shared configuration plus admin tables the APIs rely on.
【F:migrate.py†L1-L117】【F:account_migrations.py†L1-L117】 Ensure the DSNs in the pod
environment point at the TimescaleDB release you installed in Step 5.

## 9. Validate in Lens

1. **Health checks** – Use Lens to port-forward the risk, policy, and OMS
   services and confirm `/healthz` and `/readyz` return HTTP 200.
   【F:deploy/helm/aether-platform/values.yaml†L90-L118】
2. **Ingress** – Browse to each ingress hostname to validate TLS and the UI.
3. **Metrics** – Confirm Prometheus is scraping pods via the annotations Helm
   applied, or add a Lens metrics source pointed at your Prometheus endpoint.
4. **Background jobs** – Check Kafka and NATS consumer groups to ensure the
   ingestion deployments are connected.

## 10. Post-Deployment Operations

* **Scaling & resiliency** – Tune replicas, HPAs, and PodDisruptionBudgets in
  your values file and run `helm upgrade` when you need to scale out.
  【F:deploy/helm/aether-platform/values.yaml†L188-L1049】
* **Observability** – Integrate Prometheus/Grafana using the scrape annotations
  and metrics ports defined in the chart, or point to a managed monitoring
  solution.【F:deploy/helm/aether-platform/values.yaml†L90-L112】
* **Future upgrades** – Edit `platform-values.yaml` and re-run `helm upgrade`
  from Lens or the CLI; Helm tracks revisions so you can roll back if needed.

Following these steps ensures a fully automated Helm-driven rollout of the Aether
platform — dependencies, services, and UI — with Lens providing cluster
visualization and lifecycle management throughout the process.
