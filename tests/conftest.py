
"""Shared pytest configuration for the Aether test suite."""

from __future__ import annotations

import importlib
import asyncio
import base64
import inspect
import os
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping
from urllib.parse import parse_qsl, urlparse, urlunparse

import pytest


class _InMemoryDatabase:
    """Lightweight stand-in for services expecting a SQLAlchemy-style engine."""

    def __init__(self, url: str) -> None:
        self.url = url

    def connect(self) -> "_InMemoryDatabase":  # pragma: no cover - compatibility shim
        return self

    # The real SQLAlchemy engine offers context-manager semantics; expose minimal
    # implementations so call sites relying on ``with engine.connect():`` remain
    # functional during unit tests.
    def __enter__(self) -> "_InMemoryDatabase":  # pragma: no cover - compatibility shim
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - compatibility shim
        return None


class _InMemoryRedis:
    """Simple in-memory Redis replacement used across the suite."""

    def __init__(self) -> None:
        self._values: Dict[str, bytes] = {}

    def set(self, key: str, value: str | bytes) -> None:
        if isinstance(value, str):
            value = value.encode("utf-8")
        self._values[key] = value

    def setex(self, key: str, ttl_seconds: int, value: str | bytes) -> None:
        del ttl_seconds
        self.set(key, value)

    def get(self, key: str) -> bytes | None:
        return self._values.get(key)

    def delete(self, key: str) -> None:
        self._values.pop(key, None)

    def flushall(self) -> None:
        self._values.clear()


class _RecordingKrakenClient:
    """Capture Kraken WebSocket interactions for assertions."""

    def __init__(self, account_id: str = "test-account", **kwargs: Any) -> None:
        del kwargs
        self.account_id = account_id
        self.requests: list[dict[str, Any]] = []

    def add_order(self, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        self.requests.append({"payload": payload, "timeout": timeout})
        return {"status": "ok", "txid": "TESTTX", "transport": "websocket"}

    def open_orders(self) -> dict[str, Any]:
        return {"open": []}

    def own_trades(self, txid: str | None = None) -> dict[str, Any]:
        return {"trades": []}

    def close(self) -> None:  # pragma: no cover - compatibility shim
        return None


_DEFAULT_MASTER_KEY = base64.b64encode(b"\x00" * 32).decode("ascii")
os.environ.setdefault("SECRET_ENCRYPTION_KEY", _DEFAULT_MASTER_KEY)
os.environ.setdefault("LOCAL_KMS_MASTER_KEY", _DEFAULT_MASTER_KEY)

os.environ.setdefault("AUTH_JWT_SECRET", "unit-test-secret")
os.environ.setdefault("AUTH_DATABASE_URL", "sqlite:////tmp/aether-auth-test.db")
os.environ.setdefault(
    "DATABASE_URL",
    "timescale://test:test@localhost:5432/aether_test",
)

os.environ.setdefault("AZURE_AD_CLIENT_ID", "unit-test-client")
os.environ.setdefault("AZURE_AD_CLIENT_SECRET", "unit-test-client-secret")

os.environ.setdefault("SESSION_REDIS_URL", "memory://oms-test")

os.environ.setdefault("TIMESCALE_DSN", "postgresql://localhost:5432/aether_test")


try:
    from shared.common_bootstrap import ensure_common_helpers
except Exception:  # pragma: no cover - shared helpers may be unavailable in some suites
    ensure_common_helpers = None  # type: ignore[assignment]
else:
    ensure_common_helpers()

try:
    from services.oms import order_ack_cache as _order_ack_cache
except Exception:  # pragma: no cover - services module may be unavailable in some suites
    _order_ack_cache = None  # type: ignore[assignment]

try:
    from services.common.adapters import KafkaNATSAdapter
except Exception:  # pragma: no cover - adapters may be unavailable in lightweight suites
    KafkaNATSAdapter = None  # type: ignore[assignment]
else:
    if not hasattr(KafkaNATSAdapter, "reset"):

        @classmethod
        def _reset_adapter(cls, account_id: str | None = None) -> None:
            buffer = getattr(cls, "_fallback_buffer", {})
            published = getattr(cls, "_published_events", {})
            if account_id is None:
                buffer.clear()
                published.clear()
            else:
                normalized = str(account_id)
                buffer.pop(normalized, None)
                published.pop(normalized, None)

        setattr(KafkaNATSAdapter, "reset", _reset_adapter)


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


pytest_plugins = [
    "tests.fixtures.backends",
    "tests.fixtures.mock_kraken",
]


def pytest_configure(config: Any) -> None:
    config.addinivalue_line("markers", "asyncio: execute test inside an asyncio event loop")
    config.addinivalue_line("markers", "anyio: execute test inside an asyncio event loop")

    original_run = getattr(asyncio, "run", None)

    if original_run is not None and not getattr(asyncio, "_nest_asyncio_patched", False):

        def _nested_run(coro: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return original_run(coro, *args, **kwargs)

            result: Dict[str, Any] = {}

            def _runner() -> None:
                try:
                    result["value"] = original_run(coro, *args, **kwargs)
                except Exception as exc:  # pragma: no cover - defensive guard
                    result["error"] = exc

            thread = threading.Thread(target=_runner, daemon=True)
            thread.start()
            thread.join()
            if "error" in result:
                raise result["error"]
            return result.get("value")

        asyncio.run = _nested_run  # type: ignore[assignment]
        asyncio._nest_asyncio_patched = True  # type: ignore[attr-defined]


def pytest_pyfunc_call(pyfuncitem: Any) -> bool | None:
    testfunc = pyfuncitem.obj
    if inspect.iscoroutinefunction(testfunc):
        argnames = getattr(pyfuncitem, "_fixtureinfo", None)
        if argnames is not None:
            argnames = argnames.argnames
        else:  # pragma: no cover - fallback for unexpected pytest internals
            argnames = tuple(pyfuncitem.funcargs.keys())
        testargs = {name: pyfuncitem.funcargs[name] for name in argnames}

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(testfunc(**testargs))
        finally:
            loop.close()
        return True
    return None


@pytest.fixture(autouse=True)
def _clear_ack_cache() -> None:
    if _order_ack_cache is None:
        yield
        return
    _order_ack_cache.get_order_ack_cache.cache_clear()
    try:
        yield
    finally:
        _order_ack_cache.get_order_ack_cache.cache_clear()

if "pyotp" not in sys.modules:
    pyotp_stub = ModuleType("pyotp")

    class _TOTP:
        def __init__(self, secret: str) -> None:
            self._secret = secret

        def verify(self, code: str, valid_window: int = 1) -> bool:  # pragma: no cover - simple stub
            return bool(code)

        def now(self) -> str:  # pragma: no cover - deterministic value
            return "000000"

    pyotp_stub.TOTP = _TOTP
    pyotp_stub.random_base32 = lambda: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
    sys.modules["pyotp"] = pyotp_stub


try:
    from auth.service import InMemorySessionStore
except Exception:  # pragma: no cover - optional dependency not available
    InMemorySessionStore = None  # type: ignore[assignment]

try:
    from services.common import security as security_module
except Exception:  # pragma: no cover - optional dependency not available
    security_module = None


class _RedisSessionBackend:
    def __init__(self) -> None:
        self._values: Dict[str, tuple[bytes, datetime]] = {}

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        expires = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        self._values[key] = (value.encode("utf-8"), expires)

    def get(self, key: str) -> bytes | None:
        entry = self._values.get(key)
        if not entry:
            return None
        value, expires = entry
        if datetime.now(timezone.utc) >= expires:
            self._values.pop(key, None)
            return None
        return value

    def delete(self, key: str) -> None:
        self._values.pop(key, None)

    def flushall(self) -> None:
        self._values.clear()


@pytest.fixture(autouse=True)
def _configure_session_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    try:  # pragma: no cover - prefer the real redis client when available
        import redis  # type: ignore[import-untyped]
    except Exception:  # pragma: no cover - fallback stub when redis not installed
        redis = ModuleType("redis")

        class _RedisClient:  # pragma: no cover - minimal stub for session store
            pass

        redis.Redis = _RedisClient  # type: ignore[attr-defined]
        sys.modules["redis"] = redis
    else:
        if not hasattr(redis, "Redis"):
            redis.Redis = type("Redis", (), {})  # type: ignore[attr-defined]

    backend = _RedisSessionBackend()

    def _from_url(url: str, *args, **kwargs):  # type: ignore[override]
        del url, args, kwargs
        return backend

    monkeypatch.setenv("SESSION_REDIS_URL", "redis://session-backend:6379/0")
    monkeypatch.setattr("redis.Redis.from_url", _from_url, raising=False)

    try:
        yield
    finally:
        backend.flushall()


@pytest.fixture()
def database(tmp_path: Path) -> _InMemoryDatabase:
    """Return an isolated in-memory database engine for tests."""

    db_path = tmp_path / "aether-tests.db"
    return _InMemoryDatabase(f"sqlite:///{db_path}")


@pytest.fixture()
def redis_client() -> Iterable[_InMemoryRedis]:
    """Provide a shared Redis stub with automatic cleanup."""

    backend = _InMemoryRedis()
    try:
        yield backend
    finally:
        backend.flushall()


@pytest.fixture()
def kraken_client() -> _RecordingKrakenClient:
    """Return a recording Kraken client stub for behavioural tests."""

    return _RecordingKrakenClient()


from importlib.machinery import ModuleSpec


def _install_sqlalchemy_stub() -> None:
    if "sqlalchemy" in sys.modules:
        return

    sa = ModuleType("sqlalchemy")
    sa.__spec__ = ModuleSpec("sqlalchemy", loader=None)
    if sa.__spec__ is not None:
        sa.__spec__.submodule_search_locations = []
    sa.__path__ = []  # type: ignore[attr-defined]

    class _Column:
        def __init__(self, name: object, *args: object, **kwargs: object) -> None:
            self.name = str(name) if isinstance(name, str) else name
            self.args = args
            self.kwargs = kwargs

    class _Type:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    class _Constraint:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.args = args
            self.kwargs = kwargs

    class MetaData:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.tables: dict[str, "Table"] = {}

        def create_all(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - noop
            return None

        def drop_all(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - noop
            return None

    class _ExcludedAccessor:
        def __getattr__(self, name: str) -> str:
            return f"excluded.{name}"

    class Table:
        def __init__(self, name: str, metadata: MetaData, *columns: object, **kwargs: object) -> None:
            self.name = name
            self.columns = columns
            self.kwargs = kwargs
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

        def __str__(self) -> str:  # pragma: no cover - exercised indirectly in SQL capture tests
            columns = ", ".join(f'"{name}"' for name in self._values.keys())
            conflict = ", ".join(
                f'"{getattr(element, "name", element)}"' for element in self._index_elements
            )
            return (
                f"INSERT INTO {self.table.name} ({columns}) VALUES (...)"
                + (
                    f" ON CONFLICT ({conflict}) DO UPDATE"
                    if conflict
                    else ""
                )
            )

        def returning(self, *args: object, **kwargs: object) -> "_Insert":
            return self

    class _FunctionCall:
        def __init__(self, name: str, *args: object, **kwargs: object) -> None:
            self.name = name
            self.args = args
            self.kwargs = kwargs

    class _FuncProxy:
        def __getattr__(self, name: str) -> Callable[..., _FunctionCall]:
            return lambda *args, **kwargs: _FunctionCall(name, *args, **kwargs)

    class _SelectStatement:
        def __init__(self, *entities: object) -> None:
            self._entities = entities

        def order_by(self, *args: object, **kwargs: object) -> "_SelectStatement":
            return self

        def where(self, *args: object, **kwargs: object) -> "_SelectStatement":
            return self

        def select_from(self, *args: object, **kwargs: object) -> "_SelectStatement":
            return self

        def group_by(self, *args: object, **kwargs: object) -> "_SelectStatement":
            return self

        def subquery(self, *args: object, **kwargs: object) -> "_SelectStatement":
            return self

        def scalars(self) -> SimpleNamespace:
            return SimpleNamespace(all=lambda: [])

        def scalar(self) -> None:
            return None

    def _select(*args: object, **kwargs: object) -> _SelectStatement:
        return _SelectStatement(*args)

    def _insert(table: Table) -> _Insert:
        return _Insert(table)

    def _create_engine(*args: object, **kwargs: object) -> SimpleNamespace:
        url = args[0] if args else kwargs.get("url", "sqlite://")

        class _Connection(SimpleNamespace):
            def execute(self, *c_args: object, **c_kwargs: object) -> SimpleNamespace:
                return SimpleNamespace(
                    fetchall=lambda: [],
                    all=lambda: [],
                    scalar=lambda: None,
                    scalars=lambda: SimpleNamespace(all=lambda: []),
                )

            def __enter__(self) -> "_Connection":
                return self

            def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
                return None

        class _BeginContext:
            def __enter__(self) -> _Connection:
                return _Connection()

            def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
                return None

        engine = SimpleNamespace(
            connect=lambda: _Connection(),
            dispose=lambda: None,
            begin=lambda: _BeginContext(),
            url=url,
        )
        engine._aether_url = url
        return engine

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
    sa.Text = _Type
    sa.JSON = _Type
    sa.JSONB = _Type
    sa.Text = _Type
    sa.UniqueConstraint = _Constraint
    sa.ForeignKey = _Constraint
    sa.PrimaryKeyConstraint = _Constraint
    sa.Index = _Constraint
    sa.MetaData = MetaData
    sa.Table = Table
    sa.func = _FuncProxy()
    sa.select = _select
    sa.insert = _insert
    sa.create_engine = _create_engine
    sa.text = lambda sql, **kwargs: sql
    sa.engine_from_config = _engine_from_config

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

    orm = ModuleType("sqlalchemy.orm")
    orm.__spec__ = ModuleSpec("sqlalchemy.orm", loader=None)
    orm.Mapped = Any  # type: ignore[attr-defined]

    def _mapped_column(*args: object, **kwargs: object) -> _Column:
        return _Column(*(args or (None,)), **kwargs)

    orm.mapped_column = _mapped_column  # type: ignore[attr-defined]

    class Session:
        def __init__(self, bind: object | None = None) -> None:
            self.bind = bind
            self._bind_identifier = getattr(bind, "_aether_url", None)

        def __enter__(self) -> "Session":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            self.close()

        def begin(self) -> "_TransactionContext":
            return _TransactionContext(self)

        def execute(self, *args: object, **kwargs: object) -> SimpleNamespace:
            statement = args[0] if args else None
            memory_runs = getattr(self, "_memory_runs", None)
            if memory_runs is not None and getattr(statement, "_entities", None):
                entities = list(getattr(statement, "_entities", ()))
                entity = entities[0] if entities else None

                def _scalar_result(values: Iterable[object]) -> SimpleNamespace:
                    materialised = list(values)
                    return SimpleNamespace(
                        all=lambda: list(materialised),
                        first=lambda: materialised[0] if materialised else None,
                        __iter__=lambda self=materialised: iter(self),
                    )

                if isinstance(entity, str):
                    if entity == "run_id":
                        values = [record.run_id for record in memory_runs]
                    else:
                        values = []
                    return SimpleNamespace(
                        scalars=lambda: _scalar_result(values),
                        scalar_one=lambda: values[0] if values else None,
                        scalar_one_or_none=lambda: values[0] if values else None,
                    )

                if entity is not None:
                    values = list(memory_runs)
                    return SimpleNamespace(
                        scalars=lambda: _scalar_result(values),
                        scalar_one=lambda: values[0] if values else None,
                        scalar_one_or_none=lambda: values[0] if values else None,
                    )

            return SimpleNamespace(
                scalars=lambda: SimpleNamespace(all=lambda: []),
                scalar_one=lambda: None,
                scalar_one_or_none=lambda: None,
            )

        def get(self, *args: object, **kwargs: object) -> None:
            return None

        def merge(self, instance: object, *args: object, **kwargs: object) -> object:
            """Record merged instances for inspection in lightweight fallbacks."""

            merged = getattr(self, "_merged_instances", None)
            if merged is None:
                merged = []
                setattr(self, "_merged_instances", merged)
            merged.append(instance)
            return instance

        def add(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - noop stub
            return None

        def flush(self) -> None:  # pragma: no cover - noop stub
            return None

        def commit(self) -> None:  # pragma: no cover - noop stub
            return None

        def rollback(self) -> None:  # pragma: no cover - noop stub
            return None

        def close(self) -> None:
            return None

    class _TransactionContext:
        def __init__(self, session: Session) -> None:
            self._session = session

        def __enter__(self) -> Session:
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
        return DeclarativeBase

    def _registry(*args: object, **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(mapper=lambda *a, **k: None)

    orm.declarative_base = declarative_base
    orm.DeclarativeBase = DeclarativeBase
    orm.registry = _registry
    orm.relationship = lambda *a, **k: None  # type: ignore[attr-defined]
    sys.modules["sqlalchemy.orm"] = orm

    engine = ModuleType("sqlalchemy.engine")
    engine.__spec__ = ModuleSpec("sqlalchemy.engine", loader=None)
    engine.Engine = SimpleNamespace
    engine.create_engine = _create_engine

    class _URL(SimpleNamespace):
        def __init__(self, raw_url: str) -> None:
            parsed = urlparse(raw_url)
            if not parsed.scheme:
                raise ValueError(f"Could not parse URL from string '{raw_url}'")
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
                if not multiparams:
                    payload = getattr(statement, "_values", {})
                    multiparams = (payload,) if payload else ()
                executor(statement, *multiparams, **params)
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

    engine_url = ModuleType("sqlalchemy.engine.url")
    engine_url.__spec__ = ModuleSpec("sqlalchemy.engine.url", loader=None)
    engine_url.URL = _URL
    engine_url.make_url = _make_url
    engine.url = engine_url
    sys.modules["sqlalchemy.engine"] = engine
    sys.modules["sqlalchemy.engine.url"] = engine_url

    exc = ModuleType("sqlalchemy.exc")
    exc.__spec__ = ModuleSpec("sqlalchemy.exc", loader=None)

    class SQLAlchemyError(Exception):
        """Base error type exposed by the lightweight SQLAlchemy stub."""

    class OperationalError(SQLAlchemyError):
        """Raised when the database connection or statement execution fails."""

    class IntegrityError(SQLAlchemyError):
        """Raised when inserts or updates violate a constraint."""

    class ArgumentError(SQLAlchemyError):
        """Raised when configuration or invocation arguments are invalid."""

    class NoSuchTableError(SQLAlchemyError):
        """Raised when metadata lookups reference an unknown table."""

    exc.SQLAlchemyError = SQLAlchemyError
    exc.OperationalError = OperationalError
    exc.IntegrityError = IntegrityError
    exc.ArgumentError = ArgumentError
    exc.NoSuchTableError = NoSuchTableError
    sys.modules["sqlalchemy.exc"] = exc

    pool = ModuleType("sqlalchemy.pool")
    pool.__spec__ = ModuleSpec("sqlalchemy.pool", loader=None)
    pool.NullPool = object
    pool.StaticPool = object
    sa.pool = pool
    sys.modules["sqlalchemy.pool"] = pool

    dialects = ModuleType("sqlalchemy.dialects")
    dialects.__spec__ = ModuleSpec("sqlalchemy.dialects", loader=None)
    postgresql = ModuleType("sqlalchemy.dialects.postgresql")
    postgresql.__spec__ = ModuleSpec("sqlalchemy.dialects.postgresql", loader=None)

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
    ext.__spec__ = ModuleSpec("sqlalchemy.ext", loader=None)
    sys.modules["sqlalchemy.ext"] = ext

    ext_asyncio = ModuleType("sqlalchemy.ext.asyncio")
    ext_asyncio.__spec__ = ModuleSpec("sqlalchemy.ext.asyncio", loader=None)

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
    ext_compiler.__spec__ = ModuleSpec("sqlalchemy.ext.compiler", loader=None)

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
        def __init__(
            self,
            *args: object,
            labelnames: tuple[str, ...] | list[str] | None = None,
            registry: "_CollectorRegistry" | None = None,
            **kwargs: object,
        ) -> None:
            del args, kwargs
            self._labelnames = tuple(labelnames or ())
            self._values: dict[tuple[str, ...], float] = {}
            if registry is not None:
                registry.register(self)

        def labels(self, *args: object, **kwargs: object) -> "_Metric":
            if args and kwargs:
                raise ValueError("Use positional or keyword labels, not both")
            if args:
                key = tuple(str(value) for value in args)
            else:
                key = tuple(str(kwargs.get(name, "")) for name in self._labelnames)
            self._current_key = key
            self._values.setdefault(key, 0.0)
            return self

        def set(self, value: float) -> None:
            key = getattr(self, "_current_key", ())
            self._values[key] = value

        def inc(self, value: float = 1.0) -> None:
            key = getattr(self, "_current_key", ())
            self._values[key] = self._values.get(key, 0.0) + value

        def observe(self, value: float) -> None:
            self.set(value)

        def collect(self) -> list[SimpleNamespace]:
            samples = []
            for key, value in self._values.items():
                labels = {name: label for name, label in zip(self._labelnames, key)}
                samples.append(SimpleNamespace(name=getattr(self, "_name", ""), labels=labels, value=value))
            return [SimpleNamespace(samples=samples)]

    class _CollectorRegistry:
        def __init__(self) -> None:
            self._collectors: set[_Metric] = set()

        def register(self, collector: _Metric) -> None:
            self._collectors.add(collector)

        def unregister(self, collector: _Metric) -> None:
            self._collectors.discard(collector)

        def collect(self) -> list[SimpleNamespace]:
            families: list[SimpleNamespace] = []
            for collector in list(self._collectors):
                families.extend(collector.collect())
            return families

    prom.Counter = _Metric
    prom.Gauge = _Metric
    prom.Summary = _Metric
    prom.Histogram = _Metric
    prom.CollectorRegistry = _CollectorRegistry
    prom.REGISTRY = _CollectorRegistry()
    prom.CONTENT_TYPE_LATEST = "text/plain"
    prom.generate_latest = lambda registry: b""
    sys.modules["prometheus_client"] = prom


try:  # prefer real SQLAlchemy when available for integration-style tests
    import sqlalchemy as _sa  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - fallback to stub when import fails
    _install_sqlalchemy_stub()
else:
    if not hasattr(_sa, "select") or not hasattr(_sa, "text"):
        _install_sqlalchemy_stub()
    del _sa
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


def _precision_fixture_payload() -> Dict[str, Dict[str, str]]:

    return {
        "XXBTZUSD": {
            "wsname": "XBT/USD",
            "base": "XXBT",
            "quote": "ZUSD",
            "tick_size": "0.1",
            "lot_step": "0.0001",
        },
        "XXBTZEUR": {
            "wsname": "XBT/EUR",
            "base": "XXBT",
            "quote": "ZEUR",
            "tick_size": "0.5",
            "lot_step": "0.0005",
        },
        "XETHZUSD": {
            "wsname": "ETH/USD",
            "base": "XETH",
            "quote": "ZUSD",
            "tick_size": "0.05",
            "lot_step": "0.001",
        },
        "SOLUSD": {
            "wsname": "SOL/USD",
            "base": "SOL",
            "quote": "ZUSD",
            "tick_size": "0.01",
            "lot_step": "0.1",
        },
        "USDTZUSD": {
            "wsname": "USDT/USD",
            "base": "USDT",
            "quote": "ZUSD",
            "tick_size": "0.001",
            "lot_step": "1.0",
        },
        "ADAUSD": {
            "wsname": "ADA/USD",
            "base": "ADA",
            "quote": "ZUSD",
            "tick_size": "0.0001",
            "lot_step": "0.1",
        },
        "TSTUSD": {
            "wsname": "TST/USD",
            "base": "TST",
            "quote": "USD",
            "tick_size": str(1e-08),
            "lot_step": str(1e-08),
        },

    }


@pytest.fixture(autouse=True)
def _seed_precision_cache(monkeypatch: pytest.MonkeyPatch):
    try:
        from services.common import precision as precision_module
    except Exception:  # pragma: no cover - precision module not available
        yield
        return

    monkeypatch.setenv("MODEL_HEALTH_URL", "")
    payload = _precision_fixture_payload()
    provider = precision_module.PrecisionMetadataProvider(
        fetcher=lambda: payload,
        refresh_interval=0.0,
    )

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(provider.refresh(force=True))
    finally:
        loop.close()

    monkeypatch.setattr(precision_module, "precision_provider", provider)

    policy_module = sys.modules.get("policy_service")
    if policy_module is not None:
        monkeypatch.setattr(policy_module, "precision_provider", provider, raising=False)
        async def _always_healthy() -> bool:
            return True
        monkeypatch.setattr(policy_module, "_model_health_ok", _always_healthy, raising=False)

    position_sizer_module = sys.modules.get("services.risk.position_sizer")
    if position_sizer_module is not None:
        monkeypatch.setattr(position_sizer_module, "precision_provider", provider, raising=False)

    yield provider


@pytest.fixture(autouse=True)
def _configure_security_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    try:  # FastAPI is optional in some test environments
        from fastapi.testclient import TestClient
    except Exception:  # pragma: no cover - FastAPI not installed
        if security_module is not None:
            security_module.set_default_session_store(None)
        yield
        return

    if InMemorySessionStore is None or security_module is None:
        yield
        return

    store = InMemorySessionStore(ttl_minutes=240)
    security_module.set_default_session_store(store)
    issued_tokens: Dict[str, str] = {}

    def _issue_token(account_id: str) -> str:
        token = issued_tokens.get(account_id)
        if token:
            return token
        session = store.create(account_id)
        issued_tokens[account_id] = session.token
        return session.token

    original_request = TestClient.request

    def _request_with_session(self, method: str, url: str, **kwargs):  # type: ignore[override]
        headers = kwargs.get("headers")
        if headers is None:
            headers = {}
            kwargs["headers"] = headers
        elif not isinstance(headers, dict):
            headers = dict(headers)
            kwargs["headers"] = headers
        account = headers.get("X-Account-ID")
        auth_header = headers.get("Authorization") or headers.get("authorization")
        if account and not auth_header:
            headers["Authorization"] = f"Bearer {_issue_token(str(account))}"

        approvals = headers.get("X-Director-Approvals")
        if approvals:
            approval_tokens = []
            for raw_part in approvals.split(","):
                candidate = raw_part.strip()
                if not candidate:
                    continue
                if candidate in issued_tokens.values():
                    approval_tokens.append(candidate)
                else:
                    approval_tokens.append(_issue_token(candidate))
            if approval_tokens:
                headers["X-Director-Approvals"] = ",".join(approval_tokens)

        return original_request(self, method, url, **kwargs)

    monkeypatch.setattr(TestClient, "request", _request_with_session)
    try:
        yield
    finally:
        security_module.set_default_session_store(None)
        monkeypatch.setattr(TestClient, "request", original_request, raising=False)


@pytest.fixture
def fixtures_path() -> Path:
    """Return the base path for test fixture assets."""

    return Path(__file__).parent / "fixtures"

