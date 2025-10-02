# Model Training Pipeline Runbook

## Overview
This runbook explains how to trigger model training, manage data preparation, build features, train and evaluate models, and operate the deployment lifecycle. It covers both the programmatic APIs and UI workflows and highlights the mechanisms that ensure reproducibility.

---

## Triggering a Training Run

### API Workflow
1. Ensure you have an authentication token with `ml:train` scope.
2. Issue a `POST /api/trainings` request with the payload:
   ```json
   {
     "model_key": "<strategy-model>",
     "dataset_version": "<data-tag>",
     "feature_repo_ref": "<git-sha>",
     "hyperparams": { ... },
     "schedule": "adhoc" | "backfill"
   }
   ```
3. Optional flags:
   * `run_coingecko_fetch`: default `true`. Set `false` to skip data backfill.
   * `auto_promote`: set `true` for automated model promotion on success.
4. The API responds with a `training_id`. Track status via `GET /api/trainings/{training_id}` or subscribe to the `training.status` event bus topic.

### UI Workflow
1. Navigate to **ML Ops → Training Runs**.
2. Click **New Training Run** and select the target strategy model.
3. Choose the data cut (`dataset_version`) and feature repo reference (branch, tag, or SHA).
4. Provide hyper-parameters or select a saved tuning profile.
5. Toggle **Run CoinGecko Backfill** and **Auto Promote on Success** as needed.
6. Submit. The UI presents live logs from the orchestration pipeline and links to the MLflow experiment run.

---

## Market Data Backfill and Validation

1. **Automatic CoinGecko History Fetch**
   * The `coingecko_sync` task pulls OHLCV history for all assets linked to the training dataset.
   * The task respects rate limits using the shared `MarketDataLimiter` token bucket and retries with exponential backoff.
2. **Validation**
   * Every payload is validated against schema `schemas/market_data.json` for completeness and data types.
   * Gaps larger than 5 minutes trigger the `data-gap` alert and mark the training run as `blocked` until manual override.
   * Price jumps >8σ produce an `anomaly` flag and route to the HITL queue.
3. **Timescale Upsert**
   * Validated rows are written through the `timescale_upsert` stored procedure.
   * Existing rows are updated only when the incoming timestamp is newer or the checksum differs, ensuring idempotency.
   * Batch size defaults to 5,000 rows with COPY fallback if latency exceeds 2 s per batch.

---

## Feature Engineering with Feast

1. **Feature Build**
   * The training orchestration pins the Feast repository to the `feature_repo_ref` provided in the request.
   * The `feature_build` step materializes feature views to the offline store (`s3://aether-feast-offline/<dataset_version>`).
   * Validation includes schema drift checks against the `feature_contract.yaml` manifest.
2. **Versioning**
   * Each materialization emits a `feature_build.json` manifest capturing Feast repo commit SHA, dataset version, and materialization timestamp.
   * Feature references are written to MLflow tags `feast_repo_sha` and `feast_dataset_version` for traceability.

---

## Model Training Lifecycle

1. **Training**
   * Jobs run on the `training` queue in the Ray cluster using the `trainer.py` entrypoint.
   * Inputs: materialized features path, hyper-parameter set, and run metadata.
   * Checkpoints are stored in `s3://aether-mlflow-artifacts/<training_id>/checkpoints/`.
2. **Evaluation**
   * The `evaluate.py` step computes offline metrics (Sharpe, Sortino, max drawdown, PnL) and stores artefacts in MLflow.
   * Backtest simulations are triggered if `schedule == "backfill"`.
3. **MLflow Registration**
   * Successful runs log to the MLflow experiment named after `model_key`.
   * The best-performing checkpoint is registered to the MLflow Model Registry under `Models/<model_key>` with stage `Staging` unless auto-promotion is enabled.
   * Tags applied: `training_id`, `feature_manifest`, `dataset_version`, `git_sha`, and `coingecko_snapshot`.

---

## Deployment Controls

1. **Auto-Promotion** (optional)
   * If `auto_promote` is `true`, the deployment controller evaluates guardrails:
     - Offline Sharpe ≥ target.
     - No critical alerts during data validation.
     - Backtest drawdown within policy thresholds.
   * Passing all checks promotes the model from `Staging` to `Production` in MLflow and triggers a rollout through the strategy orchestrator.
2. **Rollback**
   * Use `POST /api/models/{model_key}/rollback` with the desired `model_version`.
   * The rollback pipeline re-hydrates the previous feature manifests and replays the latest production batch for verification before confirmation.
   * UI: **ML Ops → Model Registry → Version History → Roll Back**.

---

## Reproducibility Guarantees

1. **Tags and Metadata**
   * Every MLflow run logs tags: `training_id`, `dataset_version`, `feature_repo_sha`, `training_manifest_sha`, `coingecko_snapshot`, and `hyperparam_profile`.
   * Artifacts include `training_manifest.yaml` (orchestration DAG, task image digests) and `feature_build.json`.
2. **Manifests**
   * `training_manifest.yaml` records:
     - Container image digests for each step.
     - Git commit SHAs for code and configs.
     - Parameter hashes for hyper-parameter search.
   * Stored both in MLflow artifacts and under `artifacts/trainings/<training_id>/`.
3. **Rerunning Identically**
   * API: `POST /api/trainings/{training_id}/rerun`. The orchestrator reads the original manifest, locks all referenced SHAs, and clones checkpoints if required.
   * CLI: `bin/trainings rerun --training-id <id>`.
   * Ensure the same data snapshot exists; if purged, restore from Timescale backups using the recorded `coingecko_snapshot` timestamp.

---

## Troubleshooting

* **Training stuck in `waiting-data`**: Check CoinGecko rate limits; consider toggling `run_coingecko_fetch=false` and providing manual data.
* **Feature drift alerts**: Inspect `feature_contract.yaml` diff. Update Feast repo or adjust schemas.
* **MLflow registration failure**: Verify artifact store connectivity and ensure the model registry permissions grant `ml:write`.

