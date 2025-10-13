# FastAPI Entry Points and Infrastructure Dependencies

This document enumerates every FastAPI application defined in the repository, highlights the infrastructure clients each service uses (Postgres/SQLAlchemy, Redis, Kafka/NATS, and notable optional providers), and links services to their Kubernetes deployment manifests when applicable. Line numbers reference the first `FastAPI` instantiation in each module. Dependency detections are derived from the module imports and in-code connection setup (e.g., SQLAlchemy engines, Redis URL builders, or Kafka adapters).

| Module | FastAPI line | Dependencies | Optional providers | Deployment manifest |
| --- | --- | --- | --- | --- |
| `advisor_service.py` | 741 | Postgres/SQLAlchemy, Redis | HTTPX | — |
| `alt_data.py` | 531 | Kafka/NATS | HTTPX | — |
| `anomaly_service.py` | 599 | Postgres/SQLAlchemy | None | — |
| `app.py` | 227 | Redis | None | — |
| `auth_service.py` | 1010 | Postgres/SQLAlchemy, Redis | HTTPX | — |
| `behavior_service.py` | 536 | Postgres/SQLAlchemy | None | — |
| `benchmark_service.py` | 415 | Postgres/SQLAlchemy | None | — |
| `capital_allocator.py` | 902 | Postgres/SQLAlchemy | None | deploy/k8s/base/aether-services/deployment-capital-allocator.yaml |
| `capital_flow.py` | 723 | Postgres/SQLAlchemy | None | — |
| `capital_optimizer.py` | 270 | Postgres/SQLAlchemy | None | — |
| `collab_service.py` | 189 | Postgres/SQLAlchemy | None | — |
| `compliance_filter.py` | 492 | Postgres/SQLAlchemy | None | — |
| `compliance_scanner.py` | 103 | Postgres/SQLAlchemy | HTTPX | — |
| `config_sandbox.py` | 503 | Postgres/SQLAlchemy | None | — |
| `config_service.py` | 555 | Postgres/SQLAlchemy | None | deploy/k8s/base/aether-services/deployment-config.yaml |
| `drift_service.py` | 185 | None | Requests | — |
| `esg_filter.py` | 523 | Postgres/SQLAlchemy | None | — |
| `governance_simulator.py` | 108 | Postgres/SQLAlchemy | None | — |
| `hitl_service.py` | 710 | Postgres/SQLAlchemy | None | — |
| `kill_switch.py` | 21 | Kafka/NATS | None | — |
| `ml/features/feature_versioning.py` | 358 | None | None | — |
| `ml/policy/meta_strategy.py` | 440 | None | None | — |
| `ml/training/training_service.py` | 234 | None | None | — |
| `model_server.py` | 270 | None | None | — |
| `oms_service.py` | 65 | Redis, Kafka/NATS | None | — |
| `ops/chaos/chaos_monkey.py` | 277 | Kafka/NATS | None | — |
| `ops/metrics/cost_monitor.py` | 617 | None | HTTPX | — |
| `ops/observability/latency_metrics.py` | 287 | None | None | — |
| `ops/replay/snapshot_replay.py` | 745 | Redis | None | — |
| `override_service.py` | 201 | Postgres/SQLAlchemy | None | — |
| `policy_service.py` | 165 | Redis | HTTPX | — |
| `portfolio_service.py` | 267 | Postgres/SQLAlchemy | None | — |
| `prompt_refiner.py` | 611 | None | None | — |
| `report_service.py` | 841 | Postgres/SQLAlchemy | None | — |
| `risk_service.py` | 854 | Postgres/SQLAlchemy, Redis | HTTPX | — |
| `safe_mode.py` | 38 | Redis, Kafka/NATS | None | — |
| `scenario_simulator.py` | 129 | Postgres/SQLAlchemy | None | — |
| `secrets_service.py` | 563 | None | HTTPX | — |
| `self_healer.py` | 479 | Redis | HTTPX | — |
| `sentiment_ingest.py` | 919 | Postgres/SQLAlchemy, Redis | HTTPX | — |
| `sequencer.py` | 1172 | Kafka/NATS | HTTPX | — |
| `services/account_service.py` | 461 | Postgres/SQLAlchemy | None | — |
| `services/analytics/crossasset_service.py` | 456 | Postgres/SQLAlchemy | None | — |
| `services/analytics/orderflow_service.py` | 617 | Redis | None | — |
| `services/analytics/seasonality_service.py` | 799 | Postgres/SQLAlchemy, Redis | None | — |
| `services/analytics/signal_service.py` | 605 | Redis | None | — |
| `services/analytics/volatility_service.py` | 67 | Postgres/SQLAlchemy | None | — |
| `services/analytics/whale_detector.py` | 294 | None | None | — |
| `services/anomaly/execution_anomaly.py` | 504 | Postgres/SQLAlchemy | HTTPX | — |
| `services/auth/auth_service.py` | 434 | Redis | HTTPX | deploy/k8s/base/aether-services/deployment-auth.yaml |
| `services/core/backpressure.py` | 403 | Kafka/NATS | None | — |
| `services/fees/fee_service.py` | 244 | Postgres/SQLAlchemy, Redis | None | deploy/k8s/base/aether-services/deployment-fees.yaml |
| `services/oms/main.py` | 153 | Redis, Kafka/NATS | WebSockets | deploy/k8s/base/aether-services/deployment-oms.yaml |
| `services/oms/oms_service.py` | 230 | None | WebSockets | — |
| `services/policy/main.py` | 36 | Redis, Kafka/NATS | None | deploy/k8s/base/aether-services/deployment-policy.yaml |
| `services/policy/policy_service.py` | 663 | Redis | None | — |
| `services/risk/correlation_service.py` | 588 | Postgres/SQLAlchemy | None | — |
| `services/risk/main.py` | 41 | None | None | deploy/k8s/base/aether-services/deployment-risk.yaml |
| `services/risk/portfolio_risk.py` | 492 | None | None | — |
| `services/secrets/main.py` | 20 | None | None | deploy/k8s/base/aether-services/deployment-secrets.yaml |
| `services/secrets/secrets_service.py` | 217 | None | None | — |
| `services/ui/explain_service.py` | 258 | None | None | — |
| `services/universe/main.py` | 20 | Redis | None | deploy/k8s/base/aether-services/deployment-universe.yaml |
| `services/universe/universe_service.py` | 424 | Postgres/SQLAlchemy | None | — |
| `signal_graph.py` | 688 | None | AWS SDK | — |
| `sim_mode.py` | 24 | Kafka/NATS | None | — |
| `strategy_orchestrator.py` | 654 | Postgres/SQLAlchemy, Redis | HTTPX | deploy/k8s/base/aether-services/deployment-strategy-orchestrator.yaml |
| `taxlots.py` | 220 | None | None | — |
| `tca_service.py` | 672 | Postgres/SQLAlchemy | None | — |
| `training_service.py` | 890 | Postgres/SQLAlchemy | None | — |
| `watchdog.py` | 925 | Postgres/SQLAlchemy, Kafka/NATS | None | — |
