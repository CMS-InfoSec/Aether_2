# Scaling Controller Runbook

The scaling controller adjusts OMS replicas and GPU training capacity based on
real-time load. The controller now exports dedicated Prometheus series and
OpenTelemetry spans so responders can confirm decisions quickly.

## Key Metrics

| Metric | Description |
| --- | --- |
| `scaling_oms_replicas` | Current OMS replica count recorded by the controller. |
| `scaling_gpu_nodes` | GPU nodes currently provisioned for model training. |
| `scaling_pending_training_jobs` | Number of pending training jobs observed during the last evaluation. |
| `scaling_evaluation_duration_seconds` | Histogram of the evaluation loop duration. |
| `scaling_evaluations_total` | Counter of completed evaluations. |

Metrics are exposed on the platform's `/metrics` endpoint (served by the admin
FastAPI app). They are also charted on the **Infra Scaling State** and **Scaling
Evaluation Duration** panels in the "Trading Latency Percentiles" Grafana
dashboard.

## Alert Responses

### `scaling_controller_evaluations_stalled`

1. Open the **Scaling Evaluation Duration** Grafana panel and verify the
   histogram shows recent activity. If the counter is flat, the controller task
   may be unhealthy.
2. Check the service logs for `scaling-controller-loop` errors or crashes.
3. Attempt a manual restart of the admin platform pod. Escalate to the platform
   team if restarts fail.

### `scaling_gpu_pool_idle`

1. Review `scaling_gpu_nodes` and `scaling_pending_training_jobs` to confirm the
   controller still believes GPUs are provisioned without work.
2. Inspect the Kubernetes cluster to ensure the GPU pool scaled down properly.
3. If Linode automation is stuck, trigger `deprovision_gpu_pool` manually via
   the controller admin API.
4. File an incident if idle GPU capacity persists beyond an hour.

## Manual Evaluation

To force a re-evaluation without waiting for the scheduler, call the
`/infra/scaling/status` endpoint to confirm the controller is responsive, then
invoke `evaluate_once` via the Python shell attached to the service pod. Verify
that the metrics update immediately afterwards.
