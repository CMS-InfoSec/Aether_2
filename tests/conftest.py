
"""Shared pytest configuration for the Aether test suite."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


pytest_plugins = ["tests.fixtures.backends"]


def _install_sqlalchemy_stub() -> None:
    if "sqlalchemy" in sys.modules:
        return

    sa = ModuleType("sqlalchemy")

    class _Column:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    class _Type:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    def _select(*args: object, **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(order_by=lambda *a, **k: SimpleNamespace(
            where=lambda *a, **k: SimpleNamespace(
                group_by=lambda *a, **k: SimpleNamespace(subquery=lambda *a, **k: SimpleNamespace())
            )
        ))

    sa.Column = _Column
    sa.Float = _Type
    sa.Integer = _Type
    sa.String = _Type
    sa.Boolean = _Type
    sa.DateTime = _Type
    sa.Numeric = _Type
    sa.func = SimpleNamespace(count=lambda *a, **k: 0)
    sa.select = _select
    sa.create_engine = lambda *a, **k: SimpleNamespace()

    sys.modules["sqlalchemy"] = sa

    orm = ModuleType("sqlalchemy.orm")

    class Session(SimpleNamespace):
        def execute(self, *args: object, **kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: []))

        def get(self, *args: object, **kwargs: object) -> None:
            return None

        def close(self) -> None:
            return None

    orm.Session = Session
    orm.sessionmaker = lambda *a, **k: lambda: Session()

    class _BaseMeta(type):
        def __new__(mcls, name, bases, attrs):
            cls = super().__new__(mcls, name, bases, attrs)

            @classmethod
            def __get_pydantic_core_schema__(cls, source_type, handler):  # type: ignore[override]
                return handler.generate_schema(dict)

            cls.__get_pydantic_core_schema__ = __get_pydantic_core_schema__  # type: ignore[attr-defined]
            return cls

    class DeclarativeBase(metaclass=_BaseMeta):
        def __init__(self, **kwargs: object) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    def declarative_base(**kwargs: object):
        return DeclarativeBase

    orm.declarative_base = declarative_base
    sys.modules["sqlalchemy.orm"] = orm

    engine = ModuleType("sqlalchemy.engine")
    engine.Engine = SimpleNamespace
    sys.modules["sqlalchemy.engine"] = engine

    dialects = ModuleType("sqlalchemy.dialects")
    postgresql = ModuleType("sqlalchemy.dialects.postgresql")

    class JSONB:  # pragma: no cover - simple placeholder
        ...

    class UUID:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    postgresql.JSONB = JSONB
    postgresql.UUID = UUID
    sys.modules["sqlalchemy.dialects"] = dialects
    sys.modules["sqlalchemy.dialects.postgresql"] = postgresql


def _install_prometheus_stub() -> None:
    if "prometheus_client" in sys.modules:
        return

    prom = ModuleType("prometheus_client")

    class _Metric:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

        def labels(self, **kwargs: object) -> "_Metric":
            return self

        def set(self, value: float) -> None:
            self._value = value

        def inc(self, value: float = 1.0) -> None:
            self._value = getattr(self, "_value", 0.0) + value

    prom.Counter = _Metric
    prom.Gauge = _Metric
    prom.Summary = _Metric
    prom.Histogram = _Metric
    prom.CollectorRegistry = SimpleNamespace
    prom.CONTENT_TYPE_LATEST = "text/plain"
    prom.generate_latest = lambda registry: b""
    sys.modules["prometheus_client"] = prom


_install_sqlalchemy_stub()
_install_prometheus_stub()

try:  # pragma: no cover - defensive in case pydantic is absent
    import pydantic.fields as _pydantic_fields
except Exception:  # pragma: no cover
    _pydantic_fields = None

if _pydantic_fields is not None:
    _original_field = _pydantic_fields.Field

    def _compat_field(*args: object, **kwargs: object):
        if "regex" in kwargs and "pattern" not in kwargs:
            kwargs["pattern"] = kwargs.pop("regex")
        return _original_field(*args, **kwargs)

    _pydantic_fields.Field = _compat_field  # type: ignore[assignment]

try:  # pragma: no cover - optional import guard
    from pydantic.deprecated import class_validators as _class_validators
except Exception:  # pragma: no cover
    _class_validators = None

if _class_validators is not None:
    _original_root_validator = _class_validators.root_validator

    def _compat_root_validator(*args: object, **kwargs: object):
        kwargs.setdefault("skip_on_failure", True)
        return _original_root_validator(*args, **kwargs)

    _class_validators.root_validator = _compat_root_validator  # type: ignore[assignment]


@pytest.fixture
def fixtures_path() -> Path:
    """Return the base path for test fixture assets."""

    return Path(__file__).parent / "fixtures"

