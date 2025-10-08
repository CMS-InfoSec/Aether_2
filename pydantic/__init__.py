"""Lightweight runtime stubs for the :mod:`pydantic` package.

This repository's test-suite exercises a large portion of the business logic
without installing third-party optional dependencies.  Many of the services
depend on :mod:`pydantic` models for configuration and request validation,
which means importing them would normally raise :class:`ImportError` when the
real library is unavailable.

To keep those modules importable we provide a deliberately tiny subset of the
Pydantic API that the tests rely on.  The goal of the stub is not to offer
full validation – it merely mimics the shape of the interfaces (``BaseModel``,
``Field`` helpers, and the decorator utilities) closely enough for the
application code to run in a dependency-light environment.

The implementation focuses on predictable behaviour rather than strict type
enforcement:

* ``BaseModel`` stores declared attributes and exposes ``model_dump``/``dict``
  helpers returning shallow copies of the instance data.
* ``Field`` preserves metadata so default values and default factories work as
  expected when constructing models.
* Decorators such as ``field_validator`` simply return the wrapped function so
  they can be used freely in production code while remaining no-ops here.

The module is intentionally small but we document and cover the helpers in the
unit tests to guard against regressions as additional services are made
compatible with the stubbed environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping as ABCMapping, Sequence as ABCSequence, Set as ABCSet
import types as _types
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

try:  # pragma: no cover - typing helpers may be unavailable on very old Python versions
    from typing import get_args, get_origin, get_type_hints
except Exception:  # pragma: no cover - compatibility fallback
    def get_origin(annotation: Any) -> Any:
        return None

    def get_args(annotation: Any) -> Tuple[Any, ...]:
        return ()

    def get_type_hints(obj: Any) -> Dict[str, Any]:
        return {}

_UNION_TYPES = {Union}
UnionType = getattr(_types, "UnionType", None)
if UnionType is not None:  # pragma: no cover - Python <3.10 compatibility
    _UNION_TYPES.add(UnionType)

__all__ = [
    "BaseModel",
    "ConfigDict",
    "Field",
    "EmailStr",
    "HttpUrl",
    "PositiveFloat",
    "SecretStr",
    "ValidationError",
    "constr",
    "field_serializer",
    "field_validator",
    "model_serializer",
    "model_validator",
    "validator",
]


_T = TypeVar("_T")


class ValidationError(ValueError):
    """Fallback validation error raised by ``BaseModel`` helpers."""


class SecretStr:
    """Very small replacement for :class:`pydantic.SecretStr`."""

    def __init__(self, value: str) -> None:
        self._value = value

    def get_secret_value(self) -> str:
        return self._value

    def __repr__(self) -> str:  # pragma: no cover - defensive helper
        return "SecretStr('***')"

    def __str__(self) -> str:  # pragma: no cover - defensive helper
        return "***"


EmailStr = str
HttpUrl = str
PositiveFloat = float


def constr(*_: Any, **__: Any) -> Type[str]:
    """Return ``str`` as a lightweight stand-in for :func:`pydantic.constr`."""

    return str


def ConfigDict(**kwargs: Any) -> Dict[str, Any]:
    """Mirror Pydantic's ``ConfigDict`` helper by returning a ``dict``."""

    return dict(kwargs)


@dataclass
class _FieldInfo:
    default: Any = None
    default_factory: Optional[Callable[[], Any]] = None
    metadata: Dict[str, Any] | None = None

    def __post_init__(self) -> None:  # pragma: no cover - defensive guard
        if self.metadata is None:
            self.metadata = {}

    @property
    def alias(self) -> Optional[str]:
        return self.metadata.get("alias") if self.metadata else None

    def get_default(self) -> Any:
        if self.default_factory is not None:
            return self.default_factory()
        return self.default


def Field(
    default: Any = None,
    *,
    default_factory: Optional[Callable[[], Any]] = None,
    **metadata: Any,
) -> _FieldInfo:
    """Capture default metadata for ``BaseModel`` attributes."""

    return _FieldInfo(default=default, default_factory=default_factory, metadata=metadata)


def _register_field_validator(
    func: Callable[..., Any],
    fields: Tuple[str, ...],
    *,
    mode: str = "after",
) -> Callable[..., Any]:
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)
    setattr(func, "__pydantic_validator__", True)
    setattr(func, "__pydantic_fields__", fields)
    setattr(func, "__pydantic_mode__", mode)
    return func


def field_validator(*fields: str, mode: str = "after", **_: Any) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    def decorator(func: Callable[..., _T]) -> Callable[..., _T]:
        return _register_field_validator(func, tuple(fields), mode="before" if mode == "before" else "after")

    return decorator


def validator(*fields: str, pre: bool = False, **_: Any) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    mode = "before" if pre else "after"

    def decorator(func: Callable[..., _T]) -> Callable[..., _T]:
        return _register_field_validator(func, tuple(fields), mode=mode)

    return decorator


def model_validator(*_: Any, **__: Any) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    def decorator(func: Callable[..., _T]) -> Callable[..., _T]:
        setattr(func, "__pydantic_model_validator__", True)
        setattr(func, "__pydantic_model_mode__", __.get("mode", "after"))
        return func

    return decorator


def field_serializer(*_: Any, **__: Any) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    def decorator(func: Callable[..., _T]) -> Callable[..., _T]:
        return func

    return decorator


def model_serializer(*_: Any, **__: Any) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    def decorator(func: Callable[..., _T]) -> Callable[..., _T]:
        return func

    return decorator


class BaseModel:
    """Extremely small subset of :class:`pydantic.BaseModel`."""

    model_config: Mapping[str, Any] = {}

    def __init__(self, **data: Any) -> None:
        annotations = dict(getattr(self, "__annotations__", {}))
        provided_fields: set[str] = set()
        try:
            resolved_annotations = get_type_hints(self.__class__)
        except Exception:  # pragma: no cover - typing helper best-effort resolution
            resolved_annotations = {}
        else:
            if resolved_annotations:
                for key, value in resolved_annotations.items():
                    if key in annotations:
                        annotations[key] = value
        values: Dict[str, Any] = {}
        validators = self._field_validators()

        for name in annotations:
            default = getattr(self.__class__, name, None)
            field_info = default if isinstance(default, _FieldInfo) else None

            lookup_keys: Iterable[str] = (name,)
            if field_info and field_info.alias:
                lookup_keys = (name, field_info.alias)

            found = False
            for key in lookup_keys:
                if key in data:
                    values[name] = data.pop(key)
                    provided_fields.add(name)
                    found = True
                    break

            if found:
                values[name] = self._apply_field_validators(validators, name, "before", values[name])
                continue

            if field_info is not None:
                candidate = field_info.get_default()
            else:
                candidate = default
            values[name] = self._apply_field_validators(validators, name, "before", candidate)

        extra_behaviour = getattr(self, "model_config", {}).get("extra")

        if extra_behaviour == "forbid" and data:
            raise ValidationError(f"Unexpected fields: {sorted(data.keys())}")

        values.update(data)

        for key, value in list(values.items()):
            annotation = annotations.get(key)
            coerced = self._coerce_value(annotation, value)
            values[key] = self._apply_field_validators(validators, key, "after", coerced)

        for key, value in values.items():
            setattr(self, key, value)
        self.model_fields_set = provided_fields

    def dict(self, *, exclude_none: bool = False) -> Dict[str, Any]:
        return self.model_dump(exclude_none=exclude_none)

    def model_dump(self, *, exclude_none: bool = False) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        annotations = getattr(self.__class__, "__annotations__", {})
        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            if key.startswith("model_") and key not in annotations:
                continue
            if exclude_none and value is None:
                continue
            if isinstance(value, BaseModel):
                result[key] = value.model_dump(exclude_none=exclude_none)
            elif isinstance(value, list):
                result[key] = [
                    item.model_dump(exclude_none=exclude_none)
                    if isinstance(item, BaseModel)
                    else item
                    for item in value
                ]
            elif isinstance(value, dict):
                result[key] = {
                    item_key: (
                        item_value.model_dump(exclude_none=exclude_none)
                        if isinstance(item_value, BaseModel)
                        else item_value
                    )
                    for item_key, item_value in value.items()
                }
            else:
                result[key] = value
        return dict(result)

    def model_copy(self, *, update: Optional[MutableMapping[str, Any]] = None) -> "BaseModel":
        data = self.model_dump()
        if update:
            data.update(update)
        return self.__class__(**data)

    @classmethod
    def model_validate(cls: Type["BaseModel"], data: Mapping[str, Any]) -> "BaseModel":
        if isinstance(data, cls):
            return data
        if not isinstance(data, Mapping):
            raise ValidationError("model_validate expects a mapping")
        return cls(**dict(data))

    @classmethod
    def model_construct(cls: Type["BaseModel"], **data: Any) -> "BaseModel":  # pragma: no cover - helper parity
        instance = cls.__new__(cls)
        for key, value in data.items():
            setattr(instance, key, value)
        return instance

    def copy(self, *, update: Optional[Mapping[str, Any]] = None) -> "BaseModel":
        return self.model_copy(update=dict(update) if update else None)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        data = ", ".join(f"{key}={value!r}" for key, value in self.__dict__.items())
        return f"{self.__class__.__name__}({data})"

    @classmethod
    def _field_validators(cls) -> Dict[str, Dict[str, list[Callable[[Any], Any]]]]:
        cached = getattr(cls, "__field_validators__", None)
        if cached is not None:
            return cached

        mapping: Dict[str, Dict[str, list[Callable[[Any], Any]]]] = {}
        for name, attribute in cls.__dict__.items():
            if not getattr(attribute, "__pydantic_validator__", False):
                continue
            bound = getattr(cls, name)
            fields: Tuple[str, ...] = getattr(attribute, "__pydantic_fields__", ())
            mode: str = getattr(attribute, "__pydantic_mode__", "after")
            for field in fields:
                slots = mapping.setdefault(field, {"before": [], "after": []})
                slots.setdefault(mode, [])
                slots[mode].append(bound)

        setattr(cls, "__field_validators__", mapping)
        return mapping

    @staticmethod
    def _apply_field_validators(
        validators: Dict[str, Dict[str, Iterable[Callable[[Any], Any]]]],
        field: str,
        mode: str,
        value: Any,
    ) -> Any:
        field_validators = validators.get(field)
        if not field_validators:
            return value
        funcs = field_validators.get(mode, [])
        for func in funcs:
            try:
                value = func(value)
            except Exception as exc:  # pragma: no cover - error propagation
                raise ValidationError(str(exc)) from exc
        return value

    @staticmethod
    def _coerce_value(annotation: Any, value: Any) -> Any:
        if annotation is None:
            return value

        origin = get_origin(annotation)
        if origin is None:
            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                if value is None or isinstance(value, annotation):
                    return value
                if isinstance(value, Mapping):
                    return annotation(**value)
                return annotation(value)
            return value

        if origin in (list, List, ABCSequence):
            args = get_args(annotation)
            inner = args[0] if args else Any
            if isinstance(value, (list, tuple, set, frozenset)):
                coerced = [BaseModel._coerce_value(inner, item) for item in value]
                return coerced
            return value

        if origin is tuple:
            args = get_args(annotation)
            inner = args[0] if args else Any
            if isinstance(value, tuple):
                return tuple(BaseModel._coerce_value(inner, item) for item in value)
            if isinstance(value, list):
                return tuple(BaseModel._coerce_value(inner, item) for item in value)
            return value

        if origin in (set, frozenset, ABCSet):
            args = get_args(annotation)
            inner = args[0] if args else Any
            if isinstance(value, (set, frozenset, list, tuple)):
                coerced = {BaseModel._coerce_value(inner, item) for item in value}
                if origin is frozenset:
                    return frozenset(coerced)
                return set(coerced)
            return value

        if origin in (dict, Dict, ABCMapping):
            key_type, value_type = (get_args(annotation) + (Any, Any))[:2]
            if isinstance(value, Mapping):
                return {
                    BaseModel._coerce_value(key_type, key): BaseModel._coerce_value(value_type, item)
                    for key, item in value.items()
                }
            return value

        if origin in _UNION_TYPES:
            for option in get_args(annotation):
                if option is type(None):  # noqa: E721 - intentional identity check
                    if value is None:
                        return None
                    continue
                try:
                    return BaseModel._coerce_value(option, value)
                except Exception:
                    continue
        return value


__version__ = "0.0"

