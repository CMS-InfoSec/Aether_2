from __future__ import annotations

import importlib
import sys
from types import ModuleType, SimpleNamespace

import pytest


def _install_dummy_mlflow(monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, SimpleNamespace]:
    """Register a lightweight MLflow stub for exercising the hardening layer."""

    for name in [key for key in list(sys.modules) if key.startswith("mlflow")]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    mlflow_module = ModuleType("mlflow")
    pytorch_module = SimpleNamespace()

    def _loader(*args, **kwargs):
        return {"args": args, "kwargs": kwargs}

    for attribute in ("load_model", "_load_model", "_load_model_impl"):
        setattr(pytorch_module, attribute, _loader)

    mlflow_module.pytorch = pytorch_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "mlflow", mlflow_module)
    monkeypatch.setitem(sys.modules, "mlflow.pytorch", pytorch_module)

    return mlflow_module, pytorch_module


def _reload_module(_monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    module = sys.modules.get("shared.mlflow_safe")
    if module is None:
        import shared.mlflow_safe as module  # type: ignore[no-redef]
    else:
        module = importlib.reload(module)
    return module


def test_harden_mlflow_blocks_pytorch_deserialization(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MLFLOW_ALLOW_PYTORCH_DESERIALIZATION", raising=False)
    module = _reload_module(monkeypatch)
    mlflow_module, pytorch_module = _install_dummy_mlflow(monkeypatch)

    module.harden_mlflow(mlflow_module)

    for attribute in ("load_model", "_load_model", "_load_model_impl"):
        loader = getattr(pytorch_module, attribute, None)
        if loader is None:
            continue
        with pytest.raises(module.MlflowSecurityError):
            loader("runs:/malicious")


def test_harden_mlflow_respects_explicit_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MLFLOW_ALLOW_PYTORCH_DESERIALIZATION", "1")
    module = _reload_module(monkeypatch)
    mlflow_module, pytorch_module = _install_dummy_mlflow(monkeypatch)

    module.harden_mlflow(mlflow_module)

    for attribute in ("load_model", "_load_model", "_load_model_impl"):
        loader = getattr(pytorch_module, attribute, None)
        if loader is None:
            continue
        result = loader("runs:/trusted")
        assert result["args"][0] == "runs:/trusted"
