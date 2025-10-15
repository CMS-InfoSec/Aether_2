"""Regression tests for the lightweight SQLAlchemy shim."""

import importlib
import sys
from types import SimpleNamespace


def _clear_sqlalchemy(monkeypatch) -> None:
    for name in list(sys.modules):
        if name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, name, raising=False)


def test_sqlalchemy_stub_exposes_submodules(monkeypatch):
    """The shim should provide commonly imported SQLAlchemy namespaces."""

    _clear_sqlalchemy(monkeypatch)

    stub = importlib.import_module("services.common.sqlalchemy_stub")
    module = stub.install()

    assert module is sys.modules["sqlalchemy"]

    declarative = importlib.import_module("sqlalchemy.ext.declarative")
    base_cls = declarative.declarative_base()
    assert hasattr(base_cls, "metadata")

    orm = importlib.import_module("sqlalchemy.orm")
    mapped_column = getattr(orm, "mapped_column")
    mapped = getattr(orm, "Mapped")
    assert mapped is not None
    column = mapped_column("example", default=SimpleNamespace())
    assert isinstance(column, SimpleNamespace)

    sql_module = importlib.import_module("sqlalchemy.sql")
    func = getattr(sql_module, "func")
    assert hasattr(func, "now")
    assert func.now() is not None

    schema = importlib.import_module("sqlalchemy.sql.schema")
    table = schema.Table("example", base_cls.metadata)
    assert table.name == "example"

    engine_module = importlib.import_module("sqlalchemy.engine")
    assert hasattr(engine_module, "URL")

    types_module = importlib.import_module("sqlalchemy.types")
    decorator = getattr(types_module, "TypeDecorator")
    char_type = getattr(types_module, "CHAR")

    class _ExampleDecorator(decorator):
        impl = char_type

    instance = _ExampleDecorator()
    assert instance.process_bind_param("value", object()) == "value"
