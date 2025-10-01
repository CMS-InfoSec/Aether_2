
"""Compatibility wrapper exposing the policy intent model server."""

from __future__ import annotations

from services.policy.model_server import Intent, get_active_model, predict_intent

__all__ = ["Intent", "predict_intent", "get_active_model"]


