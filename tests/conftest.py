
"""Shared pytest configuration for the Aether test suite."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


pytest_plugins = [
    "tests.fixtures.backends",
    "tests.fixtures.mock_kraken",
]


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

    class MetaData:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.tables: dict[str, "Table"] = {}

        def create_all(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - noop
            return None

    class Table:
        def __init__(self, name: str, metadata: MetaData, *columns: object, **kwargs: object) -> None:
            self.name = name
            self.columns = columns
            self.kwargs = kwargs
            metadata.tables[name] = self

    class _Insert:
        def __init__(self, table: Table) -> None:
            self.table = table
            self._values: dict[str, object] | None = None

        def values(self, *args: object, **kwargs: object) -> "_Insert":
            self._values = kwargs if kwargs else (args[0] if args else None)
            return self

        def on_conflict_do_update(self, *args: object, **kwargs: object) -> "_Insert":
            return self

        def returning(self, *args: object, **kwargs: object) -> "_Insert":
            return self

    def _select(*args: object, **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(order_by=lambda *a, **k: SimpleNamespace(
            where=lambda *a, **k: SimpleNamespace(
                group_by=lambda *a, **k: SimpleNamespace(subquery=lambda *a, **k: SimpleNamespace())
            )
        ))

    def _insert(table: Table) -> _Insert:
        return _Insert(table)

    def _create_engine(*args: object, **kwargs: object) -> SimpleNamespace:
        class _Connection(SimpleNamespace):
            def execute(self, *c_args: object, **c_kwargs: object) -> SimpleNamespace:
                return SimpleNamespace(
                    fetchall=lambda: [],
                    scalar=lambda: None,
                    scalars=lambda: SimpleNamespace(all=lambda: []),
                )

        return SimpleNamespace(connect=lambda: _Connection(), dispose=lambda: None)

    def _engine_from_config(*args: object, **kwargs: object) -> SimpleNamespace:
        return _create_engine()

    sa.Column = _Column
    sa.Float = _Type
    sa.Integer = _Type
    sa.String = _Type
    sa.Boolean = _Type
    sa.BigInteger = _Type
    sa.DateTime = _Type
    sa.Numeric = _Type
    sa.JSON = _Type
    sa.JSONB = _Type
    sa.MetaData = MetaData
    sa.Table = Table
    sa.func = SimpleNamespace(count=lambda *a, **k: 0)
    sa.select = _select
    sa.insert = _insert
    sa.create_engine = _create_engine
    sa.engine_from_config = _engine_from_config

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
        metadata = SimpleNamespace(create_all=lambda *a, **k: None)

        def __init__(self, **kwargs: object) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    def declarative_base(**kwargs: object):
        return DeclarativeBase

    def _registry(*args: object, **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(mapper=lambda *a, **k: None)

    orm.declarative_base = declarative_base
    orm.registry = _registry
    sys.modules["sqlalchemy.orm"] = orm

    engine = ModuleType("sqlalchemy.engine")
    engine.Engine = SimpleNamespace
    sys.modules["sqlalchemy.engine"] = engine

    exc = ModuleType("sqlalchemy.exc")
    exc.SQLAlchemyError = Exception
    sys.modules["sqlalchemy.exc"] = exc

    pool = ModuleType("sqlalchemy.pool")
    pool.NullPool = object
    pool.StaticPool = object
    sa.pool = pool
    sys.modules["sqlalchemy.pool"] = pool

    dialects = ModuleType("sqlalchemy.dialects")
    postgresql = ModuleType("sqlalchemy.dialects.postgresql")

    class JSONB:  # pragma: no cover - simple placeholder
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    class UUID:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    postgresql.JSONB = JSONB
    postgresql.UUID = UUID
    postgresql.insert = _insert
    sys.modules["sqlalchemy.dialects"] = dialects
    sys.modules["sqlalchemy.dialects.postgresql"] = postgresql

    ext = ModuleType("sqlalchemy.ext")
    sys.modules["sqlalchemy.ext"] = ext

    ext_asyncio = ModuleType("sqlalchemy.ext.asyncio")

    class _AsyncConnection(SimpleNamespace):
        async def execute(self, *args: object, **kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: []))

    class AsyncEngine(SimpleNamespace):
        def begin(self) -> "_AsyncBegin":
            return _AsyncBegin()

        async def dispose(self) -> None:  # pragma: no cover - noop
            return None

    class _AsyncBegin:
        async def __aenter__(self) -> _AsyncConnection:
            return _AsyncConnection()

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    def create_async_engine(*args: object, **kwargs: object) -> AsyncEngine:
        return AsyncEngine()

    ext_asyncio.AsyncEngine = AsyncEngine
    ext_asyncio.create_async_engine = create_async_engine
    sys.modules["sqlalchemy.ext.asyncio"] = ext_asyncio

    ext_compiler = ModuleType("sqlalchemy.ext.compiler")

    def compiles(*args: object, **kwargs: object):
        def decorator(func):
            return func

        return decorator

    ext_compiler.compiles = compiles
    sys.modules["sqlalchemy.ext.compiler"] = ext_compiler


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

