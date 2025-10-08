"""Typed compatibility helpers for optional Pydantic dependency."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Protocol


class _BaseModelProtocol(Protocol):
    """Protocol capturing the minimal BaseModel surface we rely on."""

    model_config: Dict[str, Any]

    def __init__(self, **data: Any) -> None: ...

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]: ...

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]: ...


if TYPE_CHECKING:  # pragma: no cover - used for static analysis only
    class BaseModel(_BaseModelProtocol):  # type: ignore[too-many-ancestors]
        ...
else:  # pragma: no cover - runtime fallback when Pydantic is optional
    try:
        from pydantic import BaseModel as BaseModel  # type: ignore[assignment]
    except Exception:
        class BaseModel(_BaseModelProtocol):  # type: ignore[too-many-ancestors]
            """Minimal runtime stand-in replicating attribute assignment."""

            model_config: Dict[str, Any] = {}

            def __init__(self, **data: Any) -> None:
                for key, value in data.items():
                    setattr(self, key, value)

            def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
                del args, kwargs
                return dict(self.__dict__)

            def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
                return self.dict(*args, **kwargs)


if TYPE_CHECKING:  # pragma: no cover - static signature for field helper
    def Field(default: Any = None, **kwargs: Any) -> Any: ...
else:  # pragma: no cover - runtime import with fallback
    try:
        from pydantic import Field  # type: ignore[assignment]
    except Exception:
        def Field(default: Any = None, **kwargs: Any) -> Any:
            default_factory = kwargs.get("default_factory")
            if callable(default_factory):
                return default_factory()
            return default


try:  # pragma: no cover - available on Pydantic v2
    from pydantic import ConfigDict as ConfigDict  # type: ignore[assignment]
except Exception:  # pragma: no cover - compatibility with v1 or missing dependency
    ConfigDict = Dict[str, Any]


__all__ = ["BaseModel", "Field", "ConfigDict"]
