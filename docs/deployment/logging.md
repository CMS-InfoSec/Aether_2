# Logging Architecture

Aether services emit structured JSON logs to standard output. The platform Helm chart
publishes a shared manifest for each workload under `backendServices`, so every API and
worker pod runs with the same logging behaviour and label selectors. 【F:deploy/helm/aether-platform/values.yaml†L136-L220】

## Centralised Collection

Cluster log collection is handled by a dedicated Fluent Bit DaemonSet that tails the CRI
log files for every container in the cluster. The collector enriches each record with
pod metadata and forwards the stream to Loki using a JSON payload, while bounding the
local buffer to prevent node disk exhaustion. 【F:deploy/observability/logging/fluent-bit-configmap.yaml†L1-L75】【F:deploy/observability/logging/fluent-bit-daemonset.yaml†L1-L76】

| Component | Purpose |
| --- | --- |
| ServiceAccount / RBAC | Grants Fluent Bit read-only access to pod and namespace metadata required for Kubernetes enrichment. 【F:deploy/observability/logging/fluent-bit-serviceaccount.yaml†L1-L11】【F:deploy/observability/logging/fluent-bit-clusterrole.yaml†L1-L18】【F:deploy/observability/logging/fluent-bit-clusterrolebinding.yaml†L1-L18】 |
| ConfigMap (`fluent-bit.conf`) | Enables CRI parsing, attaches Kubernetes labels, and routes output to Loki with disk buffers capped at 50 MiB. 【F:deploy/observability/logging/fluent-bit-configmap.yaml†L12-L75】 |
| DaemonSet | Mounts `/var/log` and `/var/lib/docker/containers` on every node so all service pods are ingested, exposing `/api/v1/health` for readiness checks. 【F:deploy/observability/logging/fluent-bit-daemonset.yaml†L1-L73】 |

## Retention

Loki retention is fixed at 21 days (within the 14–30 day production guideline). Samples
older than seven days are rejected at ingest to catch clock skew, preventing runaway
storage. 【F:deploy/observability/loki-tempo/loki-config.yaml†L1-L39】

## Operational Notes

* Fluent Bit exposes metrics and health probes on port `2020`, allowing Prometheus to
  alert on backpressure or buffer exhaustion before disk fills. 【F:deploy/observability/logging/fluent-bit-configmap.yaml†L18-L28】【F:deploy/observability/logging/fluent-bit-daemonset.yaml†L26-L54】
* Buffers persist to `/var/lib/fluent-bit` and `/var/log/fluent-bit-buffers`, giving the
  agent a bounded, restart-tolerant backlog without consuming container writable layers.
  【F:deploy/observability/logging/fluent-bit-daemonset.yaml†L54-L73】
* Because the DaemonSet reads from `/var/log/containers/*.log`, no per-service changes
  are required when onboarding new deployments—the collector automatically captures all
  workloads that follow Kubernetes logging conventions. 【F:deploy/observability/logging/fluent-bit-configmap.yaml†L22-L37】
