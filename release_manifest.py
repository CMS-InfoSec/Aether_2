"""CLI utilities for creating and managing release manifests.

This module collects the versions of deployed services, machine learning models,
and configuration records so that we can pin an entire release to a reproducible
state.  The data is stored in the ``release_manifests`` table which persists a
JSON payload for each manifest, a cryptographic hash for integrity, and the
timestamp when it was recorded.  The CLI can export both JSON and Markdown
summaries for downstream consumers.

Usage
-----
    python release_manifest.py create [--id <manifest-id>]
    python release_manifest.py list [--limit 10]
    python release_manifest.py rollback --id <manifest-id>
    python release_manifest.py verify --id <manifest-id>

The ``create`` command writes the manifest both to the database and, by default,
updates ``release_manifest_current.json`` and ``release_manifest_current.md``
with the latest payload.  The ``rollback`` command fetches a previous manifest
and re-promotes it as the current manifest files so that other systems can pick
up the rollback.  ``verify`` compares the active deployment against a stored
manifest and reports discrepancies.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, cast
from urllib.parse import parse_qsl, unquote, urlparse, urlunparse

try:  # pragma: no cover - alembic is optional in lightweight environments
    from alembic import command
    from alembic.config import Config
except Exception:  # pragma: no cover - allow module import without alembic
    command = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]

try:  # pragma: no cover - SQLAlchemy is optional for lightweight deployments
    from sqlalchemy import (
        JSON,
        Column,
        DateTime,
        MetaData,
        String,
        Table,
        create_engine,
        func,
        select,
    )
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import make_url as _sa_make_url
    from sqlalchemy.exc import ArgumentError, NoSuchTableError, SQLAlchemyError
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
    _SQLALCHEMY_AVAILABLE = True
except Exception:  # pragma: no cover - provide lightweight stand-ins
    JSON = Column = DateTime = MetaData = String = Table = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]
    _sa_make_url = None  # type: ignore[assignment]
    _SQLALCHEMY_AVAILABLE = False

    class SQLAlchemyError(Exception):
        pass


    class ArgumentError(SQLAlchemyError):
        pass


    class NoSuchTableError(SQLAlchemyError):
        pass


    Session = Any  # type: ignore[assignment]

    def declarative_base():  # type: ignore[override]
        class _Base:
            metadata = None

        return _Base


    def sessionmaker(**_: object):  # type: ignore[override]
        raise RuntimeError("SQLAlchemy sessionmaker is unavailable in this environment")


    class StaticPool:  # type: ignore[override]
        pass

yaml = load_yaml_module()


LOGGER = logging.getLogger("release_manifest")
_RELEASE_MANIFEST_ALLOW_SQLITE_FLAG = "RELEASE_MANIFEST_ALLOW_SQLITE_FOR_TESTS"


if _sa_make_url is not None:
    make_url = _sa_make_url
else:

    class _ParsedURL:
        """Lightweight substitute for SQLAlchemy's URL parsing."""

        def __init__(self, raw_url: str):
            parsed = urlparse(raw_url)
            if not parsed.scheme:
                raise ArgumentError(f"Could not parse URL from string '{raw_url}'")
            self._parsed = parsed
            self.drivername = parsed.scheme
            self.query = {key: value for key, value in parse_qsl(parsed.query)}
            self.host = parsed.hostname
            path = parsed.path[1:] if parsed.path.startswith("/") else parsed.path
            self.database = path or None

        def set(self, drivername: str) -> "_ParsedURL":
            parts = list(self._parsed)
            parts[0] = drivername
            return _ParsedURL(urlunparse(parts))

        def render_as_string(self, hide_password: bool = False) -> str:
            return urlunparse(self._parsed)


    def make_url(raw_url: str) -> "_ParsedURL":
        return _ParsedURL(raw_url)


def _normalize_release_db_url(raw_url: str) -> str:
    """Normalise *raw_url* so SQLAlchemy uses the psycopg2 driver."""

    if raw_url.startswith("postgres://"):
        raw_url = "postgresql://" + raw_url.split("://", 1)[1]
    if raw_url.startswith("timescale://"):
        raw_url = "postgresql+psycopg2://" + raw_url.split("://", 1)[1]

    url = make_url(raw_url)
    driver = url.drivername.lower()

    if driver in {"postgres", "postgresql"}:
        url = url.set(drivername="postgresql+psycopg2")
    elif driver == "timescale":
        url = url.set(drivername="postgresql+psycopg2")
    elif driver.startswith("postgresql+psycopg"):
        url = url.set(drivername="postgresql+psycopg2")

    return url.render_as_string(hide_password=False)


def _require_release_db_url() -> str:
    """Return the managed Postgres/Timescale DSN for release manifests."""

    raw_url = (
        os.getenv("RELEASE_MANIFEST_DATABASE_URL")
        or os.getenv("RELEASE_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
    )

    if not raw_url:
        raise RuntimeError(
            "RELEASE_MANIFEST_DATABASE_URL (or legacy RELEASE_DATABASE_URL/TIMESCALE_DSN/DATABASE_URL) must be set "
            "to a managed Postgres/Timescale DSN."
        )

    try:
        normalised = _normalize_release_db_url(raw_url)
        parsed = make_url(normalised)
    except ArgumentError as exc:
        raise RuntimeError(f"Invalid release manifest database URL '{raw_url}': {exc}") from exc

    driver = parsed.drivername.lower()
    if driver.startswith("sqlite"):
        if os.getenv(_RELEASE_MANIFEST_ALLOW_SQLITE_FLAG) == "1":
            LOGGER.warning(
                "Allowing SQLite release manifest database URL '%s' because %s=1.",
                raw_url,
                _RELEASE_MANIFEST_ALLOW_SQLITE_FLAG,
            )
            return normalised
        raise RuntimeError(
            "Release manifests require a PostgreSQL/Timescale database; "
            f"received driver '{parsed.drivername}'."
        )

    if not driver.startswith("postgresql"):
        raise RuntimeError(
            "Release manifests require a PostgreSQL/Timescale database; "
            f"received driver '{parsed.drivername}'."
        )

    return normalised


DEFAULT_RELEASE_DB_URL = _require_release_db_url()

_SQLITE_FALLBACK_FLAG = "CONFIG_ALLOW_SQLITE_FOR_TESTS"


def _require_config_db_url() -> str:
    url = os.getenv("CONFIG_DATABASE_URL")
    if not url:
        raise RuntimeError(
            "CONFIG_DATABASE_URL must be set to read configs for the release manifest."
        )

    normalized = url.lower()
    allowed_prefixes = ("postgresql://", "postgresql+psycopg://", "postgresql+psycopg2://")
    if normalized.startswith("postgres://"):
        url = "postgresql://" + url.split("://", 1)[1]
        normalized = url.lower()

    if normalized.startswith(allowed_prefixes):
        return url

    if os.getenv(_SQLITE_FALLBACK_FLAG) == "1":
        LOGGER.warning(
            "Allowing non-Postgres CONFIG_DATABASE_URL '%s' because %s=1.",
            url,
            _SQLITE_FALLBACK_FLAG,
        )
        return url

    raise RuntimeError(
        "CONFIG_DATABASE_URL must point to a PostgreSQL/TimescaleDB instance; "
        f"received '{url}'."
    )


DEFAULT_CONFIG_DB_URL = _require_config_db_url()
DEFAULT_SERVICES_DIR = Path("deploy/k8s/base/aether-services")
DEFAULT_MODELS_DIR = Path("ml/models")
DEFAULT_MANIFEST_FILE = Path("release_manifest_current.json")
DEFAULT_MANIFEST_MARKDOWN = Path("release_manifest_current.md")


if _SQLALCHEMY_AVAILABLE:
    Base = declarative_base()

    class ReleaseManifest(Base):
        """ORM model for persisted release manifests."""

        __tablename__ = "release_manifests"

        manifest_id = Column(String, primary_key=True)
        manifest_json = Column(JSON, nullable=False)
        manifest_hash = Column(String, nullable=True)
        ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
else:
    Base = None

    class ReleaseManifest:  # pragma: no cover - placeholder for type checkers
        pass


@dataclass
class Manifest:
    """In-memory representation of a release manifest."""

    manifest_id: str
    payload: Dict[str, Dict[str, str]]
    ts: datetime
    manifest_hash: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "manifest_id": self.manifest_id,
            "ts": self.ts.isoformat(),
            "payload": self.payload,
            "hash": self.manifest_hash,
        }


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        LOGGER.warning("Invalid integer value for %s: %s", key, raw)
        return default


_SQLITE_MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS release_manifests (
    manifest_id TEXT PRIMARY KEY,
    manifest_json TEXT NOT NULL,
    manifest_hash TEXT,
    ts TEXT NOT NULL
)
"""


def _sqlite_path_from_url(url: str) -> str:
    parsed = urlparse(url)
    driver = parsed.scheme.lower()
    if driver not in {"sqlite", "sqlite+pysqlite"}:
        raise RuntimeError(
            "The lightweight release_manifest fallback only supports sqlite URLs; "
            f"received '{url}'."
        )

    netloc = parsed.netloc or ""
    path = parsed.path or ""
    if netloc and not path.startswith("/"):
        path = f"/{path}"
    full_path = f"/{netloc}{path}" if netloc else path
    if full_path in {"", "/"}:
        return ":memory:"
    if full_path.startswith("/:"):
        return full_path[1:]
    return unquote(full_path)


if not _SQLALCHEMY_AVAILABLE:

    class _SQLiteEngine:
        def __init__(self, url: str):
            self.url = url
            self.database = _sqlite_path_from_url(url)
            if self.database != ":memory:":
                db_path = Path(self.database)
                db_path.parent.mkdir(parents=True, exist_ok=True)

        def connect(self) -> sqlite3.Connection:
            conn = sqlite3.connect(self.database, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn

        def dispose(self) -> None:  # pragma: no cover - sqlite3 connections auto-close
            return None


    class _SQLiteSession:
        def __init__(self, engine: "_SQLiteEngine"):
            self._engine = engine
            self._conn = engine.connect()
            self._ensure_schema()
            self._closed = False

        def _ensure_schema(self) -> None:
            self._conn.execute(_SQLITE_MANIFEST_DDL)
            self._conn.commit()

        def __enter__(self) -> "_SQLiteSession":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            try:
                if exc_type:
                    self._conn.rollback()
                else:
                    self._conn.commit()
            finally:
                self.close()

        def close(self) -> None:
            if not self._closed:
                self._conn.close()
                self._closed = True

        def commit(self) -> None:
            self._conn.commit()

        def rollback(self) -> None:
            self._conn.rollback()

        def connection(self) -> sqlite3.Connection:
            return self._conn


    def _sqlite_save_manifest(
        session: "_SQLiteSession", manifest_id: str, payload: Dict[str, Dict[str, str]], payload_hash: str
    ) -> Manifest:
        ts = datetime.now(timezone.utc)
        data = json.dumps(payload)
        session.connection().execute(
            """
            INSERT OR REPLACE INTO release_manifests (manifest_id, manifest_json, manifest_hash, ts)
            VALUES (?, ?, ?, ?)
            """,
            (manifest_id, data, payload_hash, ts.isoformat()),
        )
        session.connection().commit()
        return Manifest(manifest_id=manifest_id, payload=payload, ts=ts, manifest_hash=payload_hash)


    def _sqlite_fetch_manifest(session: "_SQLiteSession", manifest_id: str) -> Optional[Manifest]:
        row = session.connection().execute(
            "SELECT manifest_id, manifest_json, manifest_hash, ts FROM release_manifests WHERE manifest_id = ?",
            (manifest_id,),
        ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(row["manifest_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        ts_raw = row["ts"]
        try:
            ts = datetime.fromisoformat(ts_raw)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:  # pragma: no cover - defensive guard for corrupted data
            ts = datetime.fromtimestamp(0, tz=timezone.utc)
        return Manifest(manifest_id=row["manifest_id"], payload=payload, ts=ts, manifest_hash=row["manifest_hash"])


    def _sqlite_list_manifests(session: "_SQLiteSession", limit: Optional[int]) -> List[Manifest]:
        sql = "SELECT manifest_id, manifest_json, manifest_hash, ts FROM release_manifests ORDER BY ts DESC"
        params: Tuple[object, ...] = tuple()
        if limit is not None:
            sql += " LIMIT ?"
            params = (limit,)
        rows = session.connection().execute(sql, params).fetchall()
        manifests: List[Manifest] = []
        for row in rows:
            try:
                payload = json.loads(row["manifest_json"])
            except (TypeError, json.JSONDecodeError):
                payload = {}
            try:
                ts = datetime.fromisoformat(row["ts"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except Exception:  # pragma: no cover - defensive
                ts = datetime.fromtimestamp(0, tz=timezone.utc)
            manifests.append(
                Manifest(
                    manifest_id=row["manifest_id"],
                    payload=payload,
                    ts=ts,
                    manifest_hash=row["manifest_hash"],
                )
            )
        return manifests


    def _sqlite_backfill_hashes(session: "_SQLiteSession") -> None:
        rows = session.connection().execute(
            "SELECT manifest_id, manifest_json FROM release_manifests WHERE manifest_hash IS NULL OR manifest_hash = ''"
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["manifest_json"])
            except (TypeError, json.JSONDecodeError):
                payload = {}
            if not isinstance(payload, dict):
                continue
            payload_hash = compute_manifest_hash(payload)
            session.connection().execute(
                "UPDATE release_manifests SET manifest_hash = ? WHERE manifest_id = ?",
                (payload_hash, row["manifest_id"]),
            )
        session.connection().commit()


    def _ensure_sqlite_schema(engine: "_SQLiteEngine") -> None:
        conn = engine.connect()
        try:
            conn.execute(_SQLITE_MANIFEST_DDL)
            conn.commit()
        finally:
            conn.close()

def _create_engine(url: str) -> Engine:
    """Create a database engine suitable for the release manifest store."""

    if not _SQLALCHEMY_AVAILABLE:
        return cast(Engine, _SQLiteEngine(url))

    options: Dict[str, object] = {"future": True, "pool_pre_ping": True}

    try:
        parsed = make_url(url)
    except ArgumentError:
        parsed = None

    if parsed and parsed.drivername.startswith("sqlite"):
        sqlite_options = dict(options)
        sqlite_connect_args: Dict[str, object] = {"check_same_thread": False}
        if ":memory:" in url or parsed.database in {":memory:", None}:
            sqlite_options["poolclass"] = StaticPool
        sqlite_options["connect_args"] = sqlite_connect_args
        return create_engine(url, **sqlite_options)

    options["pool_size"] = _env_int("RELEASE_MANIFEST_POOL_SIZE", 5)
    options["max_overflow"] = _env_int("RELEASE_MANIFEST_MAX_OVERFLOW", 5)
    options["pool_timeout"] = _env_int("RELEASE_MANIFEST_POOL_TIMEOUT", 30)
    options["pool_recycle"] = _env_int("RELEASE_MANIFEST_POOL_RECYCLE", 1800)

    connect_args: Dict[str, object] = {}
    forced_sslmode = os.getenv("RELEASE_MANIFEST_SSLMODE")
    if forced_sslmode:
        connect_args["sslmode"] = forced_sslmode
    elif parsed and "sslmode" not in parsed.query and parsed.host not in {None, "localhost", "127.0.0.1"}:
        connect_args["sslmode"] = "require"

    app_name = os.getenv("RELEASE_MANIFEST_APP_NAME", "release-manifest")
    if app_name:
        connect_args["application_name"] = app_name

    if connect_args:
        options["connect_args"] = connect_args

    return create_engine(url, **options)


_MIGRATIONS_PATH = Path(__file__).resolve().parent / "release_manifest_migrations"


def run_release_manifest_migrations(database_url: Optional[str] = None) -> None:
    """Apply outstanding migrations for the release manifest schema."""

    url = (database_url or DEFAULT_RELEASE_DB_URL).strip()
    if not url:
        raise RuntimeError("Release manifest migrations require a valid database URL")

    if (command is None or Config is None) or not _SQLALCHEMY_AVAILABLE:
        LOGGER.warning(
            "Alembic is unavailable; ensuring release_manifest schema exists via a lightweight fallback."
        )
        engine = _create_engine(url)
        try:
            if _SQLALCHEMY_AVAILABLE and Base is not None:
                Base.metadata.create_all(engine)  # type: ignore[union-attr]
            else:
                _ensure_sqlite_schema(cast("_SQLiteEngine", engine))
        finally:
            if hasattr(engine, "dispose"):
                engine.dispose()
        return

    config = Config()
    config.set_main_option("script_location", str(_MIGRATIONS_PATH))
    config.set_main_option("sqlalchemy.url", url)
    config.attributes["configure_logger"] = False

    command.upgrade(config, "head")


run_release_manifest_migrations(DEFAULT_RELEASE_DB_URL)
release_engine = _create_engine(DEFAULT_RELEASE_DB_URL)


if _SQLALCHEMY_AVAILABLE:
    SessionLocal = sessionmaker(
        bind=release_engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )
else:

    class _SQLiteSessionFactory:
        def __init__(self, engine: "_SQLiteEngine"):
            self._engine = engine

        def __call__(self) -> "_SQLiteSession":
            return _SQLiteSession(self._engine)


    SessionLocal = _SQLiteSessionFactory(cast("_SQLiteEngine", release_engine))


def _backfill_manifest_hashes() -> None:
    """Populate missing manifest hashes for existing records."""

    with SessionLocal() as session:
        if _SQLALCHEMY_AVAILABLE:
            try:
                records = (
                    session.query(ReleaseManifest)
                    .filter((ReleaseManifest.manifest_hash.is_(None)) | (ReleaseManifest.manifest_hash == ""))
                    .all()
                )
            except SQLAlchemyError:  # pragma: no cover - query failure
                return
            changed = False
            for record in records:
                if not isinstance(record.manifest_json, dict):
                    continue
                record.manifest_hash = compute_manifest_hash(record.manifest_json)
                changed = True
            if changed:
                try:
                    session.commit()
                except SQLAlchemyError:  # pragma: no cover - commit failure
                    session.rollback()
        else:
            _sqlite_backfill_hashes(session)


def compute_manifest_hash(payload: Dict[str, Dict[str, str]]) -> str:
    """Return a deterministic hash for a manifest payload."""

    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_backfill_manifest_hashes()


# ---------------------------------------------------------------------------
# Manifest collection helpers
# ---------------------------------------------------------------------------


def collect_service_versions(root: Path = DEFAULT_SERVICES_DIR) -> Dict[str, str]:
    """Collect container image versions from Kubernetes deployment manifests.

    The function attempts to parse YAML manifests using PyYAML if it is
    installed.  When PyYAML is unavailable, it falls back to a lightweight
    parser that extracts ``container name`` and ``image`` pairs using regular
    expressions.  Only files ending in ``.yaml`` or ``.yml`` are considered.
    """

    if not root.exists():
        return {}

    service_versions: Dict[str, str] = {}
    for path in sorted(root.rglob("*.yml")) + sorted(root.rglob("*.yaml")):
        try:
            text = path.read_text()
        except OSError:
            continue
        if yaml is not None:
            try:
                docs = [doc for doc in yaml.safe_load_all(text) if isinstance(doc, dict)]
            except Exception:  # pragma: no cover - fallback when YAML parsing fails
                docs = []
        else:
            docs = []
        if docs:
            for doc in docs:
                containers = _extract_containers_from_yaml_dict(doc)
                service_versions.update(containers)
        else:
            service_versions.update(_extract_containers_from_text(text))
    return service_versions


def _extract_containers_from_yaml_dict(doc: Dict[str, object]) -> Dict[str, str]:
    """Extract container name -> image mappings from a YAML document."""

    containers: Dict[str, str] = {}

    def _walk(node: object) -> Iterator[Dict[str, str]]:
        if isinstance(node, dict):
            if "containers" in node and isinstance(node["containers"], list):
                for container in node["containers"]:
                    if not isinstance(container, dict):
                        continue
                    name = container.get("name")
                    image = container.get("image")
                    if isinstance(name, str) and isinstance(image, str):
                        yield {name: image}
            for value in node.values():
                yield from _walk(value)
        elif isinstance(node, list):
            for item in node:
                yield from _walk(item)

    for mapping in _walk(doc):
        containers.update(mapping)
    return containers


def _extract_containers_from_text(text: str) -> Dict[str, str]:
    """Fallback parser for container definitions when YAML is unavailable."""

    containers: Dict[str, str] = {}
    current_name: Optional[str] = None
    container_pattern = re.compile(r"^-\s+name:\s*(?P<name>[\w.-]+)")
    image_pattern = re.compile(r"image:\s*(?P<image>\S+)")
    for line in text.splitlines():
        stripped = line.strip()
        name_match = container_pattern.match(stripped)
        if name_match:
            current_name = name_match.group("name")
            continue
        image_match = image_pattern.search(stripped)
        if image_match and current_name:
            containers[current_name] = image_match.group("image")
            current_name = None
    return containers


def collect_model_versions(root: Path = DEFAULT_MODELS_DIR) -> Dict[str, str]:
    """Compute deterministic hashes for model definitions.

    Each ``.py`` file underneath ``ml/models`` contributes a version entry.  The
    hash captures the model implementation at the time of manifest creation
    without requiring an external model registry service.
    """

    if not root.exists():
        return {}

    model_versions: Dict[str, str] = {}
    for path in sorted(root.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        digest = hashlib.sha256(data).hexdigest()[:12]
        key = str(path.relative_to(root)).replace("\\", "/")
        model_versions[key] = digest
    return model_versions


def collect_config_versions(database_url: str = DEFAULT_CONFIG_DB_URL) -> Dict[str, str]:
    """Fetch the latest configuration versions from the config database.

    The function reflects the ``config_versions`` table dynamically to avoid a
    hard dependency on the ``config_service`` module.  If the database or table
    is absent the function returns an empty mapping, which keeps the manifest
    generation resilient for local development environments.
    """

    if not _SQLALCHEMY_AVAILABLE:
        return {}

    try:
        engine = _create_engine(database_url)
    except SQLAlchemyError:  # pragma: no cover - invalid URL
        return {}

    metadata = MetaData()
    try:
        table = Table("config_versions", metadata, autoload_with=engine)
    except NoSuchTableError:
        return {}
    except SQLAlchemyError:  # pragma: no cover - reflection failure
        return {}

    stmt = (
        select(
            table.c.account_id,
            table.c.key,
            func.max(table.c.version).label("version"),
        )
        .group_by(table.c.account_id, table.c.key)
        .order_by(table.c.account_id, table.c.key)
    )

    configs: Dict[str, str] = {}
    with engine.connect() as conn:
        try:
            for row in conn.execute(stmt):
                account_id = row.account_id or "global"
                key = row.key
                version = row.version
                composite_key = f"{account_id}:{key}" if account_id != "global" else key
                configs[composite_key] = str(version)
        except SQLAlchemyError:  # pragma: no cover - query failure
            return {}
    return configs


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def save_manifest(session: Session, manifest_id: str, payload: Dict[str, Dict[str, str]]) -> Manifest:
    """Persist a manifest payload in the database."""

    payload_hash = compute_manifest_hash(payload)
    if not _SQLALCHEMY_AVAILABLE:
        return _sqlite_save_manifest(cast("_SQLiteSession", session), manifest_id, payload, payload_hash)

    record = ReleaseManifest(
        manifest_id=manifest_id,
        manifest_json=payload,
        manifest_hash=payload_hash,
        ts=datetime.now(timezone.utc),
    )
    session.add(record)
    session.commit()
    session.refresh(record)
    return Manifest(
        manifest_id=record.manifest_id,
        payload=record.manifest_json,
        ts=record.ts,
        manifest_hash=record.manifest_hash,
    )


def fetch_manifest(session: Session, manifest_id: str) -> Optional[Manifest]:
    """Fetch a manifest by identifier."""

    if not _SQLALCHEMY_AVAILABLE:
        return _sqlite_fetch_manifest(cast("_SQLiteSession", session), manifest_id)

    record: Optional[ReleaseManifest] = session.get(ReleaseManifest, manifest_id)
    if not record:
        return None
    return Manifest(
        manifest_id=record.manifest_id,
        payload=record.manifest_json,
        ts=record.ts,
        manifest_hash=record.manifest_hash,
    )


def list_manifests(session: Session, limit: Optional[int] = None) -> List[Manifest]:
    """Return persisted manifests ordered by timestamp descending."""

    if not _SQLALCHEMY_AVAILABLE:
        return _sqlite_list_manifests(cast("_SQLiteSession", session), limit)

    stmt = select(ReleaseManifest).order_by(ReleaseManifest.ts.desc())
    if limit is not None:
        stmt = stmt.limit(limit)
    records = session.execute(stmt).scalars().all()
    return [
        Manifest(manifest_id=r.manifest_id, payload=r.manifest_json, ts=r.ts, manifest_hash=r.manifest_hash)
        for r in records
    ]


def write_manifest_file(manifest: Manifest, path: Path = DEFAULT_MANIFEST_FILE) -> None:
    """Serialise the manifest payload to a JSON file for downstream consumers."""

    try:
        path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
    except OSError:  # pragma: no cover - filesystem failure
        pass


def write_manifest_markdown(manifest: Manifest, path: Path = DEFAULT_MANIFEST_MARKDOWN) -> None:
    """Write a Markdown summary of the manifest contents."""

    lines = [
        f"# Release Manifest {manifest.manifest_id}",
        "",
        f"- Timestamp: {manifest.ts.isoformat()}",
        f"- Hash: {manifest.manifest_hash or 'unknown'}",
        "",
    ]

    def _append_table(title: str, items: Dict[str, str]) -> None:
        lines.extend([f"## {title}", ""])
        if not items:
            lines.append("No entries recorded.")
            lines.append("")
            return
        lines.extend(["| Name | Version |", "| --- | --- |"])
        for name, version in sorted(items.items()):
            lines.append(f"| {name} | {version} |")
        lines.append("")

    _append_table("Services", _coerce_str_mapping(manifest.payload.get("services")))
    _append_table("Models", _coerce_str_mapping(manifest.payload.get("models")))
    _append_table("Configs", _coerce_str_mapping(manifest.payload.get("configs")))

    try:
        path.write_text("\n".join(lines))
    except OSError:  # pragma: no cover - filesystem failure
        pass


def verify_release_manifest(
    manifest: Manifest,
    services_dir: Path,
    models_dir: Path,
    config_db_url: str,
) -> List[str]:
    """Return a list of verification error messages for the given manifest."""

    mismatches: List[str] = []

    expected_services = _coerce_str_mapping(manifest.payload.get("services"))
    actual_services = {k: str(v) for k, v in collect_service_versions(services_dir).items()}
    mismatches.extend(_diff_versions("services", expected_services, actual_services))

    expected_models = _coerce_str_mapping(manifest.payload.get("models"))
    actual_models = {k: str(v) for k, v in collect_model_versions(models_dir).items()}
    mismatches.extend(_diff_versions("models", expected_models, actual_models))

    expected_configs = _coerce_str_mapping(manifest.payload.get("configs"))
    actual_configs = {k: str(v) for k, v in collect_config_versions(config_db_url).items()}
    mismatches.extend(_diff_versions("configs", expected_configs, actual_configs))

    if manifest.manifest_hash:
        current_hash = compute_manifest_hash(manifest.payload)
        if current_hash != manifest.manifest_hash:
            mismatches.append(
                "manifest hash mismatch: stored hash"
                f" {manifest.manifest_hash} does not match recomputed {current_hash}"
            )

    return mismatches


def _diff_versions(category: str, expected: Dict[str, str], actual: Dict[str, str]) -> List[str]:
    """Compare expected and actual mappings and return human readable diffs."""

    messages: List[str] = []
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    changed = sorted({key for key in expected if key in actual and expected[key] != actual[key]})

    for key in missing:
        messages.append(f"{category}: missing '{key}' (expected {expected[key]!r})")
    for key in extra:
        messages.append(f"{category}: unexpected entry '{key}' with version {actual[key]!r}")
    for key in changed:
        messages.append(
            f"{category}: version mismatch for '{key}' (expected {expected[key]!r}, found {actual[key]!r})"
        )
    return messages


def _coerce_str_mapping(value: Optional[Mapping[str, object]]) -> Dict[str, str]:
    """Normalise payload fragments that may be missing or loosely typed."""

    if not isinstance(value, dict):
        return {}
    normalised: Dict[str, str] = {}
    for key, val in value.items():
        if key is None or val is None:
            continue
        normalised[str(key)] = str(val)
    return normalised


# ---------------------------------------------------------------------------
# CLI implementation
# ---------------------------------------------------------------------------


def create_command(args: argparse.Namespace) -> int:
    """Handle the ``create`` sub-command."""

    manifest_id = args.id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    services = collect_service_versions(Path(args.services_dir))
    models = collect_model_versions(Path(args.models_dir))
    configs = collect_config_versions(args.config_db)

    payload = {"services": services, "models": models, "configs": configs}

    with SessionLocal() as session:
        if fetch_manifest(session, manifest_id):
            raise SystemExit(f"Manifest id '{manifest_id}' already exists")
        manifest = save_manifest(session, manifest_id, payload)

    if args.output:
        write_manifest_file(manifest, Path(args.output))
    if args.markdown_output:
        write_manifest_markdown(manifest, Path(args.markdown_output))

    print(json.dumps(manifest.to_dict(), indent=2))
    return 0


def list_command(args: argparse.Namespace) -> int:
    """Handle the ``list`` sub-command."""

    with SessionLocal() as session:
        manifests = list_manifests(session, limit=args.limit)
    for manifest in manifests:
        print(json.dumps(manifest.to_dict(), indent=2))
    return 0


def rollback_command(args: argparse.Namespace) -> int:
    """Handle the ``rollback`` sub-command."""

    if not args.id:
        raise SystemExit("--id is required for rollback")

    with SessionLocal() as session:
        manifest = fetch_manifest(session, args.id)
    if manifest is None:
        raise SystemExit(f"Manifest '{args.id}' was not found")

    if args.output:
        write_manifest_file(manifest, Path(args.output))
    if args.markdown_output:
        write_manifest_markdown(manifest, Path(args.markdown_output))
    print(json.dumps(manifest.to_dict(), indent=2))
    return 0


def verify_command(args: argparse.Namespace) -> int:
    """Handle the ``verify`` sub-command."""

    with SessionLocal() as session:
        manifest = fetch_manifest(session, args.id)
    if manifest is None:
        raise SystemExit(f"Manifest '{args.id}' was not found")

    mismatches = verify_release_manifest(
        manifest,
        Path(args.services_dir),
        Path(args.models_dir),
        args.config_db,
    )

    if mismatches:
        for message in mismatches:
            print(message)
        return 1

    print(json.dumps(manifest.to_dict(), indent=2))
    return 0


COMMAND_HANDLERS = {
    "create": create_command,
    "list": list_command,
    "rollback": rollback_command,
    "verify": verify_command,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Release manifest management utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Generate and persist a new release manifest")
    create_parser.add_argument("--id", help="Explicit manifest identifier")
    create_parser.add_argument(
        "--services-dir",
        default=str(DEFAULT_SERVICES_DIR),
        help="Directory containing Kubernetes deployment manifests",
    )
    create_parser.add_argument(
        "--models-dir",
        default=str(DEFAULT_MODELS_DIR),
        help="Directory containing model definition files",
    )
    create_parser.add_argument(
        "--config-db",
        default=DEFAULT_CONFIG_DB_URL,
        help="SQLAlchemy database URL for the configuration service",
    )
    create_parser.add_argument("--output", default=str(DEFAULT_MANIFEST_FILE), help="JSON file for manifest export")
    create_parser.add_argument(
        "--markdown-output",
        default=str(DEFAULT_MANIFEST_MARKDOWN),
        help="Markdown file for manifest summary",
    )

    list_parser = subparsers.add_parser("list", help="List stored manifests")
    list_parser.add_argument("--limit", type=int, help="Maximum number of manifests to return")

    rollback_parser = subparsers.add_parser("rollback", help="Re-promote an existing manifest")
    rollback_parser.add_argument("--id", required=False, help="Identifier of the manifest to rollback to")
    rollback_parser.add_argument("--output", default=str(DEFAULT_MANIFEST_FILE), help="JSON file to update")
    rollback_parser.add_argument(
        "--markdown-output",
        default=str(DEFAULT_MANIFEST_MARKDOWN),
        help="Markdown summary file to update",
    )

    verify_parser = subparsers.add_parser("verify", help="Verify that the live deployment matches a manifest")
    verify_parser.add_argument("--id", required=True, help="Identifier of the manifest to verify against")
    verify_parser.add_argument(
        "--services-dir",
        default=str(DEFAULT_SERVICES_DIR),
        help="Directory containing Kubernetes deployment manifests",
    )
    verify_parser.add_argument(
        "--models-dir",
        default=str(DEFAULT_MODELS_DIR),
        help="Directory containing model definition files",
    )
    verify_parser.add_argument(
        "--config-db",
        default=DEFAULT_CONFIG_DB_URL,
        help="SQLAlchemy database URL for the configuration service",
    )

    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler = COMMAND_HANDLERS[args.command]
    return handler(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
