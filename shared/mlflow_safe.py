"""Utilities that mitigate unsafe MLflow deserialization behaviour.

The upstream MLflow project currently deserializes PyTorch models using
``torch.load`` with pickle enabled.  The behaviour is exploitable because a
crafted model artifact can execute arbitrary code during deserialization.  A
patch has not been released upstream yet, therefore we defensively disable the
dangerous code paths unless the operator explicitly opts in.

The helpers in this module install runtime guards that prevent MLflow from
loading PyTorch models by default.  The behaviour can be re-enabled by setting
the ``MLFLOW_ALLOW_PYTORCH_DESERIALIZATION`` environment variable to ``"1"``
in trusted environments.
"""

from __future__ import annotations

import importlib
import logging
import os
import threading
from types import ModuleType
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)


class MlflowSecurityError(RuntimeError):
    """Raised when MLflow attempts to perform a blocked unsafe operation."""


_PATCH_LOCK = threading.Lock()
_PATCHED_MODULES: set[int] = set()


def harden_mlflow(module: ModuleType | None = None) -> ModuleType | None:
    """Install defensive guards that block unsafe MLflow deserialization.

    Parameters
    ----------
    module:
        The MLflow module instance.  When ``None`` we attempt to import
        :mod:`mlflow` lazily.  If MLflow cannot be imported the function is a
        no-op and returns ``None``.
    """

    if module is None:
        try:
            module = importlib.import_module("mlflow")
        except Exception:  # pragma: no cover - mlflow is truly optional.
            return None

    with _PATCH_LOCK:
        module_id = id(module)
        if module_id in _PATCHED_MODULES:
            return module

        _disable_pytorch_deserialization(module)
        _PATCHED_MODULES.add(module_id)

    return module


def _disable_pytorch_deserialization(module: ModuleType) -> None:
    """Replace MLflow's PyTorch loaders with secure guard rails."""

    allow_unsafe = os.getenv("MLFLOW_ALLOW_PYTORCH_DESERIALIZATION") == "1"
    if allow_unsafe:
        LOGGER.warning(
            "MLflow PyTorch deserialization hardening disabled via environment"
            " override"
        )
        return

    try:
        pytorch_module = importlib.import_module("mlflow.pytorch")
    except Exception:
        return

    for attribute in ("load_model", "_load_model", "_load_model_impl"):
        if hasattr(pytorch_module, attribute):
            original = getattr(pytorch_module, attribute)
            guarded = _guarded_loader(original)
            setattr(pytorch_module, attribute, guarded)


def _guarded_loader(loader: Callable[..., Any]) -> Callable[..., Any]:
    """Return a wrapper that blocks execution with a descriptive exception."""

    def _blocked_loader(*args: Any, **kwargs: Any) -> Any:
        raise MlflowSecurityError(
            "MLflow PyTorch model deserialization is disabled because it uses "
            "pickle under the hood and is vulnerable to arbitrary code "
            "execution (GHSA-cjg2-g96f-x6q6). Set the environment variable "
            "MLFLOW_ALLOW_PYTORCH_DESERIALIZATION=1 to explicitly opt in."
        )

    _blocked_loader.__name__ = getattr(loader, "__name__", "blocked_loader")
    original_doc = getattr(loader, "__doc__", "") or ""
    _blocked_loader.__doc__ = original_doc + "\n\n    (hardened by Aether runtime)"
    return _blocked_loader

