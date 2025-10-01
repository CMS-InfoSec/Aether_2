"""CLI utilities for creating and managing release manifests.

This module collects the versions of deployed services, machine learning models,
and configuration records so that we can pin an entire release to a reproducible
state.  The data is stored in the ``release_manifests`` table which persists a
JSON payload for each manifest along with the timestamp when it was recorded.

Usage
-----
    python release_manifest.py create [--id <manifest-id>]
    python release_manifest.py list [--limit 10]
    python release_manifest.py rollback --id <manifest-id>

The ``create`` command writes the manifest both to the database and, by default,
updates ``release_manifest_current.json`` with the latest payload.  The
``rollback`` command fetches a previous manifest and re-promotes it as the
current manifest file so that other systems can pick up the rollback.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

from sqlalchemy import JSON, Column, DateTime, MetaData, String, Table, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore


DEFAULT_RELEASE_DB_URL = os.getenv(
    "RELEASE_MANIFEST_DATABASE_URL", "sqlite+pysqlite:////tmp/release_manifests.db"
)
DEFAULT_CONFIG_DB_URL = os.getenv("CONFIG_DATABASE_URL", "sqlite+pysqlite:////tmp/config.db")
DEFAULT_SERVICES_DIR = Path("deploy/k8s/base/aether-services")
DEFAULT_MODELS_DIR = Path("ml/models")
DEFAULT_MANIFEST_FILE = Path("release_manifest_current.json")


Base = declarative_base()


class ReleaseManifest(Base):
    """ORM model for persisted release manifests."""

    __tablename__ = "release_manifests"

    manifest_id = Column(String, primary_key=True)
    manifest_json = Column(JSON, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


@dataclass
class Manifest:
    """In-memory representation of a release manifest."""

    manifest_id: str
    payload: Dict[str, Dict[str, str]]
    ts: datetime

    def to_dict(self) -> Dict[str, object]:
        return {
            "manifest_id": self.manifest_id,
            "ts": self.ts.isoformat(),
            "payload": self.payload,
        }


def _create_engine(url: str) -> Engine:
    """Create a SQLAlchemy engine with sensible defaults for SQLite."""

    kwargs = {"future": True}
    if url.startswith("sqlite"):  # pragma: no cover - defensive branch for sqlite
        connect_args = {"check_same_thread": False}
        kwargs["connect_args"] = connect_args
        if ":memory:" in url:
            kwargs["poolclass"] = StaticPool
    return create_engine(url, **kwargs)


release_engine = _create_engine(DEFAULT_RELEASE_DB_URL)
SessionLocal = sessionmaker(bind=release_engine, autoflush=False, autocommit=False, expire_on_commit=False)
Base.metadata.create_all(bind=release_engine)


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

    record = ReleaseManifest(manifest_id=manifest_id, manifest_json=payload, ts=datetime.now(timezone.utc))
    session.add(record)
    session.commit()
    session.refresh(record)
    return Manifest(manifest_id=record.manifest_id, payload=record.manifest_json, ts=record.ts)


def fetch_manifest(session: Session, manifest_id: str) -> Optional[Manifest]:
    """Fetch a manifest by identifier."""

    record: Optional[ReleaseManifest] = session.get(ReleaseManifest, manifest_id)
    if not record:
        return None
    return Manifest(manifest_id=record.manifest_id, payload=record.manifest_json, ts=record.ts)


def list_manifests(session: Session, limit: Optional[int] = None) -> List[Manifest]:
    """Return persisted manifests ordered by timestamp descending."""

    stmt = select(ReleaseManifest).order_by(ReleaseManifest.ts.desc())
    if limit is not None:
        stmt = stmt.limit(limit)
    records = session.execute(stmt).scalars().all()
    return [Manifest(manifest_id=r.manifest_id, payload=r.manifest_json, ts=r.ts) for r in records]


def write_manifest_file(payload: Dict[str, Dict[str, str]], path: Path = DEFAULT_MANIFEST_FILE) -> None:
    """Serialise the manifest payload to a JSON file for downstream consumers."""

    try:
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    except OSError:  # pragma: no cover - filesystem failure
        pass


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
        write_manifest_file(payload, Path(args.output))

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
        write_manifest_file(manifest.payload, Path(args.output))
    print(json.dumps(manifest.to_dict(), indent=2))
    return 0


COMMAND_HANDLERS = {
    "create": create_command,
    "list": list_command,
    "rollback": rollback_command,
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
    create_parser.add_argument(
        "--output",
        default=str(DEFAULT_MANIFEST_FILE),
        help="Optional file to update with the latest manifest payload",
    )

    list_parser = subparsers.add_parser("list", help="List stored manifests")
    list_parser.add_argument("--limit", type=int, help="Maximum number of manifests to return")

    rollback_parser = subparsers.add_parser("rollback", help="Re-promote an existing manifest")
    rollback_parser.add_argument("--id", required=False, help="Identifier of the manifest to rollback to")
    rollback_parser.add_argument(
        "--output",
        default=str(DEFAULT_MANIFEST_FILE),
        help="File to update with the manifest payload",
    )

    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler = COMMAND_HANDLERS[args.command]
    return handler(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
