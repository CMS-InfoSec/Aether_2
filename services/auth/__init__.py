"""Authentication service package exports with lazy loading."""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any

__all__ = ["jwt_tokens", "create_admin_session", "load_settings"]


def _load_auth_service() -> ModuleType:
    module = importlib.import_module(".auth_service", __name__)
    globals().update(
        {
            "create_admin_session": module.create_admin_session,
            "load_settings": module.load_settings,
        }
    )
    return module


def __getattr__(name: str) -> Any:
    if name == "jwt_tokens":
        module = importlib.import_module(".jwt_tokens", __name__)
        globals()["jwt_tokens"] = module
        return module
    if name in {"create_admin_session", "load_settings"}:
        module = _load_auth_service()
        return getattr(module, name)
    raise AttributeError(name)
