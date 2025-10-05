"""Test configuration for the fees service suite."""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from types import ModuleType

_ORIGINAL_FEES_DSN = os.environ.get("FEES_DATABASE_URL")
_ORIGINAL_TIMESCALE_DSN = os.environ.get("TIMESCALE_DSN")

os.environ["FEES_DATABASE_URL"] = os.environ.get(
    "FEES_DATABASE_URL", "postgresql://test:test@localhost:5432/fees_test"
)
os.environ.pop("TIMESCALE_DSN", None)

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_module(name: str, path: Path, package: bool = False) -> ModuleType:
    search_locations = [str(path.parent)] if package else None
    spec = importlib.util.spec_from_file_location(name, path, submodule_search_locations=search_locations)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError(f"Failed to load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    if package:
        module.__path__ = [str(path.parent)]
    module.__spec__ = spec
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SERVICES_ROOT = ROOT / "services"
_load_module("services", SERVICES_ROOT / "__init__.py", package=True)
_load_module("services.common", SERVICES_ROOT / "common" / "__init__.py", package=True)
_load_module("services.common.security", SERVICES_ROOT / "common" / "security.py")
_load_module("services.common.adapters", SERVICES_ROOT / "common" / "adapters.py")

@pytest.fixture(scope="session", autouse=True)
def fees_test_database(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Provide an isolated SQLite database for the fees test suite."""

    import services.fees.fee_service as fee_service

    db_path = tmp_path_factory.mktemp("fees_service") / "fees.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(
        url,
        future=True,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    session_factory = sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
        future=True,
    )

    fee_service.ENGINE.dispose()
    fee_service.ENGINE = engine
    fee_service.SessionLocal = session_factory
    fee_service.DATABASE_URL = url
    fee_service.Base.metadata.drop_all(bind=engine)
    fee_service.Base.metadata.create_all(bind=engine)

    try:
        yield
    finally:
        session_factory.close_all()
        engine.dispose()

        if _ORIGINAL_FEES_DSN is None:
            os.environ.pop("FEES_DATABASE_URL", None)
        else:
            os.environ["FEES_DATABASE_URL"] = _ORIGINAL_FEES_DSN

        if _ORIGINAL_TIMESCALE_DSN is None:
            os.environ.pop("TIMESCALE_DSN", None)
        else:
            os.environ["TIMESCALE_DSN"] = _ORIGINAL_TIMESCALE_DSN


@pytest.fixture
def fees_main_module() -> ModuleType:
    """Return the lazily imported fees FastAPI module."""

    return importlib.import_module("services.fees.main")


@pytest.fixture
def fees_app(fees_main_module: ModuleType):
    return fees_main_module.app


@pytest.fixture
def fee_service_module() -> ModuleType:
    return importlib.import_module("services.fees.fee_service")
