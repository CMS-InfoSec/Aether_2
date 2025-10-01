These directories are placeholders for knowledge pack assets.

- `model_weights/` should contain the serialized model parameters exported from the
  training pipeline.
- `feature_importance/` should hold feature attribution dumps that accompany the
  models.
- `anomaly_tags/` should contain the curated anomaly tagging datasets that inform
  the detection heuristics.

The exporter consumes these paths when assembling knowledge packs.  Replace the
placeholder files with the real artefacts in production environments.
