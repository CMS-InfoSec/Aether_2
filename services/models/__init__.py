"""Model service exports used by API layers."""

from .model_server import Intent, predict_intent  # noqa: F401

__all__ = ["Intent", "predict_intent"]

