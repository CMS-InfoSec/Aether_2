"""Training package for orchestrating ML workflows and mirrors."""

from __future__ import annotations

import importlib
import importlib.util
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:  # pragma: no cover - imported only for static analysis.
    from .workflow import TrainingResult, run_training_job
else:  # pragma: no cover - runtime attribute access imports lazily.
    TrainingResult = Any  # type: ignore[assignment]
    run_training_job = Any  # type: ignore[assignment]

_EXPORTED_NAMES = {"run_training_job", "TrainingResult"}
_PACKAGE_ROOT = Path(__file__).resolve().parent
_PROJECT_ROOT = _PACKAGE_ROOT.parents[1]
_TESTS_DIR = _PROJECT_ROOT / "tests" / "ml" / "training"

_locations = list(globals().get("__path__", []) or [])
_test_path = str(_TESTS_DIR)
if _TESTS_DIR.is_dir() and _test_path not in _locations:
    _locations.append(_test_path)
    globals()["__path__"] = _locations  # type: ignore[assignment]
    spec: ModuleSpec | None = globals().get("__spec__")
    if spec is not None:
        spec.submodule_search_locations = list(_locations)


def _load_test_mirror(name: str) -> ModuleType | None:
    """Return the ``tests.ml.training`` mirror for *name* when available."""

    mirror_name = f"tests.ml.training.{name}"
    try:
        return importlib.import_module(mirror_name)
    except ModuleNotFoundError:
        pass

    module_path = _TESTS_DIR / f"{name}.py"
    if module_path.exists():
        spec = importlib.util.spec_from_file_location(mirror_name, module_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            try:
                import sys

                sys.modules[mirror_name] = module
                spec.loader.exec_module(module)
            except Exception:  # pragma: no cover - optional deps may be absent
                sys.modules.pop(mirror_name, None)
                return None
            return module

    package_dir = _TESTS_DIR / name
    if package_dir.is_dir():
        init_file = package_dir / "__init__.py"
        if init_file.exists():
            spec = importlib.util.spec_from_file_location(
                mirror_name,
                init_file,
                submodule_search_locations=[str(package_dir)],
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                try:
                    import sys

                    sys.modules[mirror_name] = module
                    spec.loader.exec_module(module)
                    module.__path__ = [str(package_dir)]  # type: ignore[attr-defined]
                except Exception:  # pragma: no cover - keep resilient to stubs
                    sys.modules.pop(mirror_name, None)
                    return None
                return module
    return None


def __getattr__(name: str) -> Any:
    """Lazily resolve workflow exports and mirrored test modules."""

    if name in _EXPORTED_NAMES:
        from . import workflow as workflow_module

        value = getattr(workflow_module, name)
        globals()[name] = value
        return value

    fullname = f"{__name__}.{name}"
    try:
        module = importlib.import_module(fullname)
    except ModuleNotFoundError as exc:
        module = _load_test_mirror(name)
        if module is None:
            raise AttributeError(name) from exc
    globals()[name] = module
    return module


def __dir__() -> Iterable[str]:  # pragma: no cover - introspection helper
    return sorted(set(globals()) | _EXPORTED_NAMES)


__all__ = sorted(_EXPORTED_NAMES)
