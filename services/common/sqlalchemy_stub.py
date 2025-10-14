"""Lightweight SQLAlchemy compatibility layer for dependency-light environments."""

from __future__ import annotations

import importlib.util
import sys
from importlib.machinery import ModuleSpec
from types import ModuleType, SimpleNamespace
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlparse, urlunparse

__all__ = ["install", "is_installed"]


def is_installed() -> bool:
    """Return whether the stub has already been inserted into ``sys.modules``."""

    module = sys.modules.get("sqlalchemy")
    return bool(module) and getattr(module, "__spec__", None) is not None


def install() -> ModuleType:
    """Install an in-memory SQLAlchemy stub if the real dependency is missing."""

    if is_installed():
        return sys.modules["sqlalchemy"]

    sa = ModuleType("sqlalchemy")
    sa.__spec__ = ModuleSpec("sqlalchemy", loader=None)
    sa.__path__ = []  # type: ignore[attr-defined]

    class _Column:
        def __init__(self, name: object | None, *args: object, **kwargs: object) -> None:
            self.name = name
            self.args = args
            self.kwargs = kwargs

    class _Type:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

        def with_variant(self, variant: object, *_dialects: object) -> "_Type":
            del variant, _dialects
            return self

    class _Constraint:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    class MetaData:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.tables: dict[str, "Table"] = {}

        def create_all(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
            return None

        def drop_all(self, *args: object, **kwargs: object) -> None:  # pragma: no cover
            return None

    class _ExcludedAccessor:
        def __getattr__(self, name: str) -> str:
            return f"excluded.{name}"

    class Table:
        def __init__(
            self,
            name: str,
            metadata: MetaData,
            *columns: object,
            **kwargs: object,
        ) -> None:
            self.name = name
            self.columns = columns
            self.kwargs = kwargs
            self._records: list[dict[str, object]] = []
            metadata.tables[name] = self
            namespace = SimpleNamespace()
            for column in columns:
                column_name = getattr(column, "name", None)
                if isinstance(column_name, str):
                    setattr(namespace, column_name, column)
            self._column_namespace = namespace

        def insert(self, *args: object, **kwargs: object) -> "_Insert":
            return _Insert(self)

        @property
        def c(self) -> SimpleNamespace:
            return self._column_namespace

    class _Insert:
        def __init__(self, table: Table) -> None:
            self.table = table
            self._values: dict[str, object] = {}
            self._index_elements: tuple[object, ...] = ()
            self._update_values: dict[str, object] = {}
            self.excluded = _ExcludedAccessor()

        def values(self, *args: object, **kwargs: object) -> "_Insert":
            payload: dict[str, object]
            if args and isinstance(args[0], dict):
                payload = dict(args[0])
            else:
                payload = dict(kwargs)
            self._values = payload
            return self

        def on_conflict_do_update(
            self,
            *,
            index_elements: Iterable[object] = (),
            set_: Mapping[str, object] | None = None,
            **_: object,
        ) -> "_Insert":
            self._index_elements = tuple(index_elements)
            if set_ is not None:
                self._update_values = dict(set_)
            return self

        def __str__(self) -> str:  # pragma: no cover - debugging helper
            columns = ", ".join(f'"{name}"' for name in self._values.keys())
            conflict = ", ".join(
                f'"{getattr(element, "name", element)}"' for element in self._index_elements
            )
            return (
                f"INSERT INTO {self.table.name} ({columns}) VALUES (...)"
                + (f" ON CONFLICT ({conflict}) DO UPDATE" if conflict else "")
            )

    class _FuncCallable(SimpleNamespace):
        def __call__(self, *args: object, **kwargs: object) -> SimpleNamespace:  # pragma: no cover
            return SimpleNamespace(name=self.name, args=args, kwargs=kwargs)

    class _FuncProxy(SimpleNamespace):
        def __getattr__(self, name: str) -> _FuncCallable:  # pragma: no cover - simple proxy
            return _FuncCallable(name=name, args=(), kwargs={})

    func_proxy = _FuncProxy()

    def _select(*entities: object, **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(_entities=entities, kwargs=kwargs)

    def _insert(table: Table) -> _Insert:
        return table.insert()

    class _BeginContext:
        def __init__(self, connection: "_Connection") -> None:
            self._connection = connection

        def __enter__(self) -> "_Connection":  # type: ignore[override]
            return self._connection

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

    class _Connection:
        def execute(self, statement: object, *multiparams: object, **params: object) -> SimpleNamespace:
            del multiparams, params
            values = getattr(statement, "_values", {})
            table = getattr(statement, "table", None)
            if table is not None and isinstance(table, Table):
                table._records.append(dict(values))
            return SimpleNamespace(rowcount=1, inserted_primary_key=None)

        def close(self) -> None:  # pragma: no cover - compatibility shim
            return None

    class _AsyncConnection:
        def __init__(self) -> None:
            self._sync = _Connection()

        async def execute(self, statement: object, *multiparams: object, **params: object) -> SimpleNamespace:
            return self._sync.execute(statement, *multiparams, **params)

        async def close(self) -> None:  # pragma: no cover - compatibility shim
            return None

    class _AsyncBeginContext:
        def __init__(self, connection: _AsyncConnection) -> None:
            self._connection = connection

        async def __aenter__(self) -> _AsyncConnection:  # type: ignore[override]
            return self._connection

        async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

    def _create_engine(url: str, **kwargs: object) -> SimpleNamespace:
        del kwargs
        engine = SimpleNamespace(
            connect=lambda: _Connection(),
            dispose=lambda: None,
            begin=lambda: _BeginContext(_Connection()),
            url=url,
        )
        engine._aether_url = url
        return engine

    class _AsyncEngine:
        def __init__(self, url: str) -> None:
            self.url = url

        def begin(self) -> _AsyncBeginContext:
            return _AsyncBeginContext(_AsyncConnection())

        async def dispose(self) -> None:
            return None

        async def connect(self) -> _AsyncConnection:  # pragma: no cover - compatibility shim
            return _AsyncConnection()

        @property
        def sync_engine(self) -> SimpleNamespace:
            return _create_engine(self.url)

    def _engine_from_config(*args: object, **kwargs: object) -> SimpleNamespace:
        return _create_engine(str(args[0]) if args else "sqlite://", **kwargs)

    def _text(statement: str, **params: object) -> SimpleNamespace:
        return SimpleNamespace(text=statement, params=params)

    def _inspect(target: object) -> SimpleNamespace:
        tables = getattr(target, "tables", {})
        return SimpleNamespace(
            has_table=lambda name: name in tables,
            get_table_names=lambda: list(tables),
        )

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
    sa.Text = _Type
    sa.UniqueConstraint = _Constraint
    sa.ForeignKey = _Constraint
    sa.PrimaryKeyConstraint = _Constraint
    sa.Index = _Constraint
    sa.MetaData = MetaData
    sa.Table = Table
    sa.func = func_proxy
    sa.select = _select
    sa.insert = _insert
    sa.text = _text
    sa.create_engine = _create_engine
    sa.engine_from_config = _engine_from_config
    sa.inspect = _inspect

    sys.modules["sqlalchemy"] = sa

    original_find_spec = getattr(importlib.util, "_sqlalchemy_original_find_spec", None)
    if original_find_spec is None:
        original_find_spec = importlib.util.find_spec

        def _find_spec(name: str, package: str | None = None):  # type: ignore[override]
            if name == "sqlalchemy":
                return None
            return original_find_spec(name, package)

        importlib.util._sqlalchemy_original_find_spec = original_find_spec  # type: ignore[attr-defined]
        importlib.util.find_spec = _find_spec  # type: ignore[assignment]

    class Session:
        def __init__(self, bind: object | None = None) -> None:
            self.bind = bind

        def __enter__(self) -> "Session":  # pragma: no cover - compatibility shim
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            self.close()

        def execute(self, statement: object, *args: object, **kwargs: object) -> SimpleNamespace:
            del args, kwargs
            connection = _Connection()
            connection.execute(statement)

            def _scalar_container(values: list[object]) -> SimpleNamespace:
                return SimpleNamespace(
                    all=lambda: list(values),
                    first=lambda: values[0] if values else None,
                    one=lambda: values[0] if values else None,
                    one_or_none=lambda: values[0] if values else None,
                )

            scalars = _scalar_container([])
            return SimpleNamespace(
                scalars=lambda: scalars,
                scalar_one=lambda: None,
                scalar_one_or_none=lambda: None,
            )

        def commit(self) -> None:  # pragma: no cover - compatibility shim
            return None

        def rollback(self) -> None:  # pragma: no cover - compatibility shim
            return None

        def close(self) -> None:  # pragma: no cover - compatibility shim
            return None

        def merge(self, instance: object, *args: object, **kwargs: object) -> object:
            store = getattr(self, "_merged_instances", None)
            if store is None:
                store = []
                setattr(self, "_merged_instances", store)
            store.append(instance)
            return instance

        def get(self, model: object, identity: object) -> object | None:
            del model, identity
            return None

    class _TransactionContext:
        def __init__(self, session: Session) -> None:
            self._session = session

        def __enter__(self) -> Session:  # type: ignore[override]
            return self._session

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

    def _sessionmaker(*args: object, **kwargs: object):
        bind = kwargs.get("bind")
        if bind is None and args:
            bind = args[0]

        def _factory() -> Session:
            return Session(bind=bind)

        _factory.bind = bind  # type: ignore[attr-defined]
        _factory.close_all = lambda: None  # type: ignore[attr-defined]
        return _factory

    orm = ModuleType("sqlalchemy.orm")
    orm.__spec__ = ModuleSpec("sqlalchemy.orm", loader=None)
    orm.Session = Session
    orm.sessionmaker = _sessionmaker

    class _BaseMeta(type):
        def __new__(mcls, name, bases, attrs):
            cls = super().__new__(mcls, name, bases, attrs)

            @classmethod
            def __get_pydantic_core_schema__(cls, source_type, handler):  # type: ignore[override]
                return handler.generate_schema(dict)

            cls.__get_pydantic_core_schema__ = __get_pydantic_core_schema__  # type: ignore[attr-defined]
            return cls

    class DeclarativeBase(metaclass=_BaseMeta):
        metadata = MetaData()

        def __init__(self, **kwargs: object) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    def declarative_base(**kwargs: object):
        del kwargs
        return DeclarativeBase

    orm.DeclarativeBase = DeclarativeBase
    orm.declarative_base = declarative_base
    orm.registry = lambda *a, **k: SimpleNamespace(mapper=lambda *args, **kwargs: None)
    orm.relationship = lambda *a, **k: None  # type: ignore[attr-defined]
    orm.Mapped = Any  # type: ignore[attr-defined]

    def _mapped_column(*args: object, **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(args=args, kwargs=kwargs)

    orm.mapped_column = _mapped_column  # type: ignore[attr-defined]

    sys.modules["sqlalchemy.orm"] = orm
    sa.orm = orm  # type: ignore[attr-defined]

    engine = ModuleType("sqlalchemy.engine")
    engine.__spec__ = ModuleSpec("sqlalchemy.engine", loader=None)
    engine.Engine = SimpleNamespace
    engine.create_engine = _create_engine
    engine.URL = SimpleNamespace  # type: ignore[attr-defined]

    class _URL(SimpleNamespace):
        def __init__(self, raw_url: str) -> None:
            parsed = urlparse(raw_url)
            super().__init__(
                drivername=parsed.scheme,
                host=parsed.hostname,
                query={key: value for key, value in parse_qsl(parsed.query)},
            )
            self._raw = raw_url
            self._parsed = parsed

        def set(self, drivername: str) -> "_URL":
            parts = list(self._parsed)
            parts[0] = drivername
            return _URL(urlunparse(parts))

        def render_as_string(self, hide_password: bool = False) -> str:
            del hide_password
            return self._raw

    def _make_url(raw_url: str) -> _URL:
        return _URL(raw_url)

    def _create_mock_engine(url: str, executor):
        url_obj = _make_url(url)

        class _MockConnection(SimpleNamespace):
            def execute(self, statement, *multiparams, **params):  # type: ignore[override]
                del multiparams, params
                executor(statement)
                return SimpleNamespace(rowcount=1)

        class _MockTransaction:
            def __enter__(self):  # type: ignore[override]
                return _MockConnection()

            def __exit__(self, exc_type, exc, tb):  # type: ignore[override]
                return None

        return SimpleNamespace(
            url=url_obj,
            begin=lambda: _MockTransaction(),
            connect=lambda: _MockConnection(),
            dispose=lambda: None,
        )

    engine.create_mock_engine = _create_mock_engine
    engine.URL = _URL  # type: ignore[attr-defined]
    engine.make_url = _make_url  # type: ignore[attr-defined]

    engine_url = ModuleType("sqlalchemy.engine.url")
    engine_url.__spec__ = ModuleSpec("sqlalchemy.engine.url", loader=None)
    engine_url.URL = _URL
    engine_url.make_url = _make_url

    engine.url = engine_url  # type: ignore[attr-defined]

    sys.modules["sqlalchemy.engine"] = engine
    sys.modules["sqlalchemy.engine.url"] = engine_url

    exc = ModuleType("sqlalchemy.exc")
    exc.__spec__ = ModuleSpec("sqlalchemy.exc", loader=None)

    class SQLAlchemyError(Exception):
        pass

    class OperationalError(SQLAlchemyError):
        pass

    class IntegrityError(SQLAlchemyError):
        pass

    class ArgumentError(SQLAlchemyError):
        pass

    class NoSuchTableError(SQLAlchemyError):
        pass

    exc.SQLAlchemyError = SQLAlchemyError
    exc.OperationalError = OperationalError
    exc.IntegrityError = IntegrityError
    exc.ArgumentError = ArgumentError
    exc.NoSuchTableError = NoSuchTableError

    sys.modules["sqlalchemy.exc"] = exc
    sa.exc = exc  # type: ignore[attr-defined]

    pool = ModuleType("sqlalchemy.pool")
    pool.__spec__ = ModuleSpec("sqlalchemy.pool", loader=None)
    pool.NullPool = object
    pool.StaticPool = object
    sys.modules["sqlalchemy.pool"] = pool
    sa.pool = pool  # type: ignore[attr-defined]

    dialects = ModuleType("sqlalchemy.dialects")
    dialects.__spec__ = ModuleSpec("sqlalchemy.dialects", loader=None)
    postgresql = ModuleType("sqlalchemy.dialects.postgresql")
    postgresql.__spec__ = ModuleSpec("sqlalchemy.dialects.postgresql", loader=None)

    class JSONB(_Type):
        pass

    class UUID(_Type):
        pass

    postgresql.JSONB = JSONB
    postgresql.UUID = UUID
    postgresql.insert = _insert

    sys.modules["sqlalchemy.dialects"] = dialects
    sys.modules["sqlalchemy.dialects.postgresql"] = postgresql
    sa.dialects = dialects  # type: ignore[attr-defined]

    ext = ModuleType("sqlalchemy.ext")
    ext.__spec__ = ModuleSpec("sqlalchemy.ext", loader=None)
    sys.modules["sqlalchemy.ext"] = ext
    sa.ext = ext  # type: ignore[attr-defined]

    ext_declarative = ModuleType("sqlalchemy.ext.declarative")
    ext_declarative.__spec__ = ModuleSpec("sqlalchemy.ext.declarative", loader=None)
    ext_declarative.declarative_base = declarative_base  # type: ignore[attr-defined]
    sys.modules["sqlalchemy.ext.declarative"] = ext_declarative
    ext.declarative = ext_declarative  # type: ignore[attr-defined]

    ext_asyncio = ModuleType("sqlalchemy.ext.asyncio")
    ext_asyncio.__spec__ = ModuleSpec("sqlalchemy.ext.asyncio", loader=None)

    class AsyncSession(Session):
        async def execute(self, *args: object, **kwargs: object) -> SimpleNamespace:
            return super().execute(*args, **kwargs)

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

    ext_asyncio.AsyncSession = AsyncSession
    ext_asyncio.AsyncEngine = _AsyncEngine  # type: ignore[attr-defined]

    def _create_async_engine(url: str, **kwargs: object) -> _AsyncEngine:
        del kwargs
        return _AsyncEngine(url)

    ext_asyncio.create_async_engine = _create_async_engine  # type: ignore[attr-defined]

    sys.modules["sqlalchemy.ext.asyncio"] = ext_asyncio
    ext.asyncio = ext_asyncio  # type: ignore[attr-defined]

    sql = ModuleType("sqlalchemy.sql")
    sql.__spec__ = ModuleSpec("sqlalchemy.sql", loader=None)
    sql.func = func_proxy
    sql.select = _select  # type: ignore[attr-defined]
    sql.text = _text  # type: ignore[attr-defined]

    sql_schema = ModuleType("sqlalchemy.sql.schema")
    sql_schema.__spec__ = ModuleSpec("sqlalchemy.sql.schema", loader=None)
    sql_schema.Table = Table
    sql_schema.Column = _Column  # type: ignore[attr-defined]

    sys.modules["sqlalchemy.sql"] = sql
    sys.modules["sqlalchemy.sql.schema"] = sql_schema
    sql.schema = sql_schema  # type: ignore[attr-defined]

    sa.sql = sql  # type: ignore[attr-defined]

    return sa
