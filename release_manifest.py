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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    MetaData,
    String,
    Table,
    create_engine,
    func,
    inspect,
    select,
    text,
)
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

LOGGER = logging.getLogger("release_manifest")
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


Base = declarative_base()


class ReleaseManifest(Base):
    """ORM model for persisted release manifests."""

    __tablename__ = "release_manifests"

    manifest_id = Column(String, primary_key=True)
    manifest_json = Column(JSON, nullable=False)
    manifest_hash = Column(String, nullable=True)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


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
Base.metadata.create_all(bind=release_engine)


def _ensure_manifest_hash_column(engine: Engine) -> None:
    """Ensure the ``manifest_hash`` column exists for backwards compatibility."""

    try:
        inspector = inspect(engine)
        columns = {col["name"] for col in inspector.get_columns("release_manifests")}
    except SQLAlchemyError:  # pragma: no cover - introspection failure
        return
    if "manifest_hash" in columns:
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE release_manifests ADD COLUMN manifest_hash VARCHAR"))
    except SQLAlchemyError:  # pragma: no cover - alteration failure
        return


_ensure_manifest_hash_column(release_engine)
SessionLocal = sessionmaker(bind=release_engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _backfill_manifest_hashes() -> None:
    """Populate missing manifest hashes for existing records."""

    with SessionLocal() as session:
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


def _coerce_str_mapping(value: Optional[Dict[str, object]]) -> Dict[str, str]:
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
