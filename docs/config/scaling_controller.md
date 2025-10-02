# Scaling controller configuration

The scaling controller manages OMS pod replica counts and GPU worker pools based
on telemetry such as OMS throughput and training job backlogs. The following
configuration knobs control when the controller scales **down** the OMS
deployment:

- `OMS_MIN_REPLICAS` – the floor that the controller will enforce for the OMS
  deployment. Any downscale decision respects this value. Defaults to the
  fallback replica count configured for the deployment (or 1 if unspecified).
- `OMS_DOWNSCALE_THRESHOLD` – throughput (orders per minute) that constitutes a
  "low load" condition. Throughput below this value starts the stabilization
  timer. Defaults to 50% of `OMS_THROUGHPUT_THRESHOLD` when unset.
- `OMS_DOWNSCALE_STABILIZATION_SECONDS` – duration that throughput must remain
  below the downscale threshold before the controller decrements replicas.
  Defaults to 15 minutes.

Throughput must remain below the downscale threshold for the entire stabilization
window before the controller reduces replicas. Each reduction is by one replica
at a time, and subsequent reductions require another full stabilization period.

These settings complement the existing `OMS_THROUGHPUT_THRESHOLD`, which is used
for scale-up decisions, and can be mixed with the GPU idle timeout and scaling
interval settings already supported by the controller.
