"""Runtime patches ensuring unit tests import the project secrets package safely."""
from __future__ import annotations

import importlib
import importlib.util
import sys
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType

_PROJECT_ROOT = Path(__file__).resolve().parent
_SECRETS_SHIM = _PROJECT_ROOT / "secrets" / "__init__.py"
_ML_PACKAGE = _PROJECT_ROOT / "ml" / "__init__.py"


def _ensure_project_root_on_path() -> None:
    """Guarantee the repository root is importable before pytest stubs load."""

    root = str(_PROJECT_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


def _ensure_services_namespace() -> None:
    """Attach the project services package to pytest stubs missing search paths."""

    services_module = sys.modules.get("services")
    if services_module is None:
        return

    services_path = str(_PROJECT_ROOT / "services")
    locations = list(getattr(services_module, "__path__", []) or [])
    if services_path not in locations:
        locations.append(services_path)
        services_module.__path__ = locations  # type: ignore[attr-defined]

    spec: ModuleSpec | None = getattr(services_module, "__spec__", None)
    if spec is not None:
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]


def _ensure_common_namespace() -> None:
    """Attach the project common package to pytest stubs missing search paths."""

    common_module = sys.modules.get("common")
    if common_module is None:
        return

    common_path = str(_PROJECT_ROOT / "common")
    locations = list(getattr(common_module, "__path__", []) or [])
    if common_path not in locations:
        locations.append(common_path)
        common_module.__path__ = locations  # type: ignore[attr-defined]

    spec: ModuleSpec | None = getattr(common_module, "__spec__", None)
    if spec is not None:
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]


def _ensure_ml_namespace() -> None:
    """Attach the project ML package to pytest stubs missing search paths."""

    ml_module = sys.modules.get("ml")
    if ml_module is None:
        return

    ml_path = str(_PROJECT_ROOT / "ml")
    locations = list(getattr(ml_module, "__path__", []) or [])
    if ml_path not in locations:
        locations.append(ml_path)
        ml_module.__path__ = locations  # type: ignore[attr-defined]

    spec: ModuleSpec | None = getattr(ml_module, "__spec__", None)
    if spec is not None:
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]


def _ensure_auth_namespace() -> None:
    """Attach the project auth package to pytest stubs missing search paths."""

    auth_module = sys.modules.get("auth")
    if auth_module is None:
        return

    auth_path = str(_PROJECT_ROOT / "auth")
    locations = list(getattr(auth_module, "__path__", []) or [])
    if auth_path not in locations:
        locations.append(auth_path)
        auth_module.__path__ = locations  # type: ignore[attr-defined]

    spec: ModuleSpec | None = getattr(auth_module, "__spec__", None)
    if spec is None:
        spec = ModuleSpec("auth", loader=None)
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]
        auth_module.__spec__ = spec
    else:
        spec.submodule_search_locations = locations  # type: ignore[attr-defined]


def _install_secrets_shim() -> None:
    """Ensure ``import secrets`` resolves to the in-repo compatibility shim."""

    if not _SECRETS_SHIM.exists():
        return

    _ensure_project_root_on_path()

    current = sys.modules.get("secrets")
    if isinstance(current, ModuleType) and getattr(current, "__file__", None) == str(
        _SECRETS_SHIM
    ):
        return

    spec = importlib.util.spec_from_file_location("secrets", _SECRETS_SHIM)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        return

    module = importlib.util.module_from_spec(spec)
    sys.modules["secrets"] = module
    spec.loader.exec_module(module)


def _preload_services_package() -> None:
    """Import the project ``services`` package so tests mirror the real module."""

    try:
        importlib.import_module("services")
    except Exception:  # pragma: no cover - avoid hard failures if import breaks early
        pass


def _preload_core_package() -> None:
    """Ensure the lightweight ``services.core`` loader is registered early."""

    try:
        importlib.import_module("services.core")
    except Exception:  # pragma: no cover - skip when dependencies are unavailable
        pass


def _preload_shared_package() -> None:
    """Import the ``shared`` package before pytest can register lightweight stubs."""

    try:
        importlib.import_module("shared")
    except Exception:  # pragma: no cover - keep bootstrap resilient
        pass


def _preload_ml_package() -> None:
    """Import the project ``ml`` package before pytest inserts stubs."""

    if not _ML_PACKAGE.exists():
        return

    try:
        importlib.import_module("ml")
    except Exception:  # pragma: no cover - keep bootstrap resilient
        pass


def _ensure_fastapi_testclient() -> None:
    """Pre-import the FastAPI test client shim when available."""

    try:
        import fastapi.testclient as _testclient
    except Exception:  # pragma: no cover - optional dependency may be absent
        pass
    else:
        if not hasattr(_testclient, "TestClient"):
            try:
                import importlib

                importlib.reload(_testclient)
            except Exception:  # pragma: no cover - keep import resilient
                pass


_ensure_project_root_on_path()
_install_secrets_shim()
_preload_services_package()
_preload_core_package()
_preload_shared_package()
_preload_ml_package()
_ensure_services_namespace()
_ensure_common_namespace()
_ensure_ml_namespace()
_ensure_auth_namespace()
_ensure_fastapi_testclient()

try:  # pragma: no cover - shared bootstrap may be unavailable in some contexts
    from shared.common_bootstrap import ensure_common_helpers, preload_core_modules
except Exception:  # pragma: no cover - avoid hard failures during early startup
    ensure_common_helpers = None  # type: ignore[assignment]
    preload_core_modules = None  # type: ignore[assignment]
else:
    try:
        ensure_common_helpers()
    except Exception:  # pragma: no cover - defensive guard to keep import resilient
        pass

    if preload_core_modules is not None:
        try:
            preload_core_modules()
        except Exception:  # pragma: no cover - defensive guard
            pass
