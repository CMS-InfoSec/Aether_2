"""Regression tests for the fee service module import."""

from __future__ import annotations

import importlib
import sys
import types
from contextlib import contextmanager

import pytest


@contextmanager
def sqlalchemy_stub() -> None:
    """Provide lightweight SQLAlchemy stubs so the module can import."""

    original_modules = {}

    def install(name: str, module: types.ModuleType) -> None:
        original_modules[name] = sys.modules.get(name)
        sys.modules[name] = module

    try:
        root = types.ModuleType("sqlalchemy")
        root.create_engine = lambda *args, **kwargs: object()
        root.func = types.SimpleNamespace(count=lambda: None)

        class _DummySelect:
            def order_by(self, *args, **kwargs):
                return self

            def scalars(self):
                return types.SimpleNamespace(all=lambda: [])

        root.select = lambda *args, **kwargs: _DummySelect()
        root.Column = lambda *args, **kwargs: object()
        root.DateTime = lambda *args, **kwargs: object()
        root.Numeric = lambda *args, **kwargs: object()
        root.String = lambda *args, **kwargs: object()
        install("sqlalchemy", root)

        engine_module = types.ModuleType("sqlalchemy.engine")
        engine_module.Engine = type("Engine", (), {})
        install("sqlalchemy.engine", engine_module)

        orm_module = types.ModuleType("sqlalchemy.orm")
        orm_module.Session = type("Session", (), {})

        def _sessionmaker(*args, **kwargs):
            class _SessionFactory:
                def __call__(self, *args, **kwargs):
                    return orm_module.Session()

            return _SessionFactory()

        def _declarative_base():
            class _Base:
                metadata = types.SimpleNamespace(create_all=lambda bind=None: None)

            return _Base

        orm_module.sessionmaker = _sessionmaker
        orm_module.declarative_base = _declarative_base
        install("sqlalchemy.orm", orm_module)

        yield
    finally:
        for name, module in original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_fee_service_import_does_not_raise() -> None:
    with sqlalchemy_stub():
        sys.modules.pop("services.fees.models", None)
        sys.modules.pop("services.fees.fee_service", None)
        module = importlib.import_module("services.fees.fee_service")
    assert hasattr(module, "app")


