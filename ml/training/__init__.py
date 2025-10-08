"""Training package for orchestrating ML workflows."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - imported only for static analysis.
    from .workflow import TrainingResult, run_training_job
else:  # pragma: no cover - runtime attribute access imports lazily.
    TrainingResult = Any  # type: ignore[assignment]
    run_training_job = Any  # type: ignore[assignment]

_EXPORTED_NAMES = {"run_training_job", "TrainingResult"}


def __getattr__(name: str) -> Any:
    """Lazily resolve workflow exports to avoid importing heavy dependencies."""

    if name in _EXPORTED_NAMES:
        from . import workflow as workflow_module

        return getattr(workflow_module, name)
    raise AttributeError(name)


__all__ = sorted(_EXPORTED_NAMES)
