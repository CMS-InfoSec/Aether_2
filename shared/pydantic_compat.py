"""Typed compatibility helpers for optional Pydantic dependency."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Protocol, TypeVar


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
        from pydantic import BaseModel as _PydanticBaseModel  # type: ignore[assignment]
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

    else:

        import inspect

        class BaseModel(_PydanticBaseModel):  # type: ignore[too-many-ancestors]
            """Compatibility wrapper adding Pydantic v1 ``model_dump`` semantics."""

            def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
                mode = kwargs.pop("mode", None)
                dump_attr = getattr(_PydanticBaseModel, "model_dump", None)
                if dump_attr is not None:
                    accepts_mode = "mode" in inspect.signature(dump_attr).parameters
                    call_kwargs = dict(kwargs)
                    if accepts_mode and mode is not None:
                        call_kwargs["mode"] = mode
                    result = super().model_dump(*args, **call_kwargs)  # type: ignore[misc]
                    if not accepts_mode and mode not in (None, "json"):
                        raise TypeError(
                            "model_dump mode must be 'json' when using Pydantic v1 compatibility layer"
                        )
                    return result

                if mode not in (None, "json"):
                    raise TypeError(
                        "model_dump mode must be 'json' when using Pydantic v1 compatibility layer"
                    )
                return super().dict(*args, **kwargs)


_FnT = TypeVar("_FnT", bound=Callable[..., Any])


if TYPE_CHECKING:  # pragma: no cover - static signature for field helper
    def Field(default: Any = None, **kwargs: Any) -> Any: ...


    def field_validator(
        *fields: str, **kwargs: Any
    ) -> Callable[[_FnT], _FnT]: ...


    def model_validator(
        *, mode: str | None = None
    ) -> Callable[[_FnT], _FnT]: ...


    def model_serializer(
        *, mode: str | None = None
    ) -> Callable[[_FnT], _FnT]: ...
else:  # pragma: no cover - runtime import with fallback
    try:
        from pydantic import Field  # type: ignore[assignment]
    except Exception:

        def Field(default: Any = None, **kwargs: Any) -> Any:
            default_factory = kwargs.get("default_factory")
            if callable(default_factory):
                return default_factory()
            return default

    try:
        from pydantic import field_validator as field_validator  # type: ignore[assignment]
    except Exception:

        def field_validator(*fields: str, **kwargs: Any) -> Callable[[_FnT], _FnT]:
            def decorator(func: _FnT) -> _FnT:
                return func

            return decorator

    try:
        from pydantic import model_validator as model_validator  # type: ignore[assignment]
    except Exception:

        def model_validator(*, mode: str | None = None) -> Callable[[_FnT], _FnT]:
            del mode

            def decorator(func: _FnT) -> _FnT:
                return func

            return decorator

    try:
        from pydantic import model_serializer as model_serializer  # type: ignore[assignment]
    except Exception:

        def model_serializer(*, mode: str | None = None) -> Callable[[_FnT], _FnT]:
            del mode

            def decorator(func: _FnT) -> _FnT:
                return func

            return decorator


try:  # pragma: no cover - available on Pydantic v2
    from pydantic import ConfigDict as ConfigDict  # type: ignore[assignment]
except Exception:  # pragma: no cover - compatibility with v1 or missing dependency
    ConfigDict = Dict[str, Any]


__all__ = [
    "BaseModel",
    "Field",
    "ConfigDict",
    "field_validator",
    "model_validator",
    "model_serializer",
    "model_dump",
]


def model_dump(instance: _BaseModelProtocol, **kwargs: Any) -> Dict[str, Any]:
    """Return a mapping representation for *instance* using modern semantics."""

    dump = getattr(instance, "model_dump", None)
    if callable(dump):
        try:
            return dump(**kwargs)
        except TypeError:
            clean_kwargs = {
                key: value
                for key, value in kwargs.items()
                if key not in {"by_alias", "mode"}
            }
            return dump(**clean_kwargs)
    clean_kwargs = {key: value for key, value in kwargs.items() if key != "by_alias"}
    return instance.dict(**clean_kwargs)


__all__.append("model_dump")
