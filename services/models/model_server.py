"""Compatibility wrapper exposing the policy intent model server."""

from __future__ import annotations

from services.policy.model_server import Intent, predict_intent

__all__ = ["Intent", "predict_intent"]

