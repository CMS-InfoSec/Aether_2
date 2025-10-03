"""Training package for orchestrating ML workflows."""

from .workflow import TrainingResult, run_training_job

__all__ = ["run_training_job", "TrainingResult"]
