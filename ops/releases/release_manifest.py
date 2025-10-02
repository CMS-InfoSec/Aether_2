"""Utilities for building and verifying release manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from sqlalchemy import JSON, Column, DateTime, MetaData, String, Table, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore


DEFAULT_RELEASE_DB_URL = os.getenv("RELEASE_DATABASE_URL", "sqlite+pysqlite:////tmp/releases.db")
DEFAULT_CONFIG_DB_URL = os.getenv("CONFIG_DATABASE_URL", "sqlite+pysqlite:////tmp/config.db")
DEFAULT_SERVICES_DIR = Path("deploy/k8s/base/aether-services")
DEFAULT_MODELS_DIR = Path("ml/models")
DEFAULT_JSON_OUTPUT = Path("release_manifest_current.json")
DEFAULT_MARKDOWN_OUTPUT = Path("release_manifest_current.md")


Base = declarative_base()


class ReleaseRecord(Base):
    """ORM mapping for persisted release manifests."""

    __tablename__ = "releases"

    manifest_id = Column(String, primary_key=True)
    manifest_json = Column(JSON, nullable=False)
    manifest_hash = Column(String, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


@dataclass
class Manifest:
    """In-memory representation of a release manifest."""

    manifest_id: str
    payload: Dict[str, Dict[str, str]]
    manifest_hash: str
    ts: datetime

    def to_dict(self) -> Dict[str, object]:
        return {
            "manifest_id": self.manifest_id,
            "ts": self.ts.isoformat(),
            "manifest_hash": self.manifest_hash,
            "payload": self.payload,
        }


def _create_engine(url: str) -> Engine:
    kwargs = {"future": True}
    if url.startswith("sqlite"):  # pragma: no cover - sqlite specific config
        kwargs["connect_args"] = {"check_same_thread": False}
        if ":memory:" in url:
            kwargs["poolclass"] = StaticPool
    return create_engine(url, **kwargs)


release_engine = _create_engine(DEFAULT_RELEASE_DB_URL)
Base.metadata.create_all(bind=release_engine)
SessionLocal = sessionmaker(bind=release_engine, autoflush=False, autocommit=False, expire_on_commit=False)


def compute_manifest_hash(payload: Dict[str, Dict[str, str]]) -> str:
    """Return a deterministic SHA256 hash for a manifest payload."""

    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------


def collect_service_versions(root: Path = DEFAULT_SERVICES_DIR) -> Dict[str, str]:
    """Collect service -> container image mappings from Kubernetes manifests."""

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
            except Exception:  # pragma: no cover - fall back to regex parser
                docs = []
        else:
            docs = []
        if docs:
            for doc in docs:
                service_versions.update(_extract_containers_from_yaml_doc(doc))
        else:
            service_versions.update(_extract_containers_from_text(text))
    return service_versions


def _extract_containers_from_yaml_doc(doc: Dict[str, object]) -> Dict[str, str]:
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
    """Return hashes for model implementation files."""

    if not root.exists():
        return {}

    versions: Dict[str, str] = {}
    for path in sorted(root.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        digest = hashlib.sha256(data).hexdigest()[:12]
        key = str(path.relative_to(root)).replace("\\", "/")
        versions[key] = digest
    return versions


def collect_config_versions(database_url: str = DEFAULT_CONFIG_DB_URL) -> Dict[str, str]:
    """Fetch configuration versions from the config service database."""

    try:
        engine = _create_engine(database_url)
    except SQLAlchemyError:  # pragma: no cover - invalid database URL
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


def create_manifest(
    manifest_id: Optional[str] = None,
    services_dir: Path = DEFAULT_SERVICES_DIR,
    models_dir: Path = DEFAULT_MODELS_DIR,
    config_db_url: str = DEFAULT_CONFIG_DB_URL,
) -> Manifest:
    """Collect live state and build a manifest object."""

    manifest_id = manifest_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    payload = _normalise_payload(
        {
            "services": collect_service_versions(services_dir),
            "configs": collect_config_versions(config_db_url),
            "models": collect_model_versions(models_dir),
        }
    )
    manifest_hash = compute_manifest_hash(payload)
    ts = datetime.now(timezone.utc)
    return Manifest(manifest_id=manifest_id, payload=payload, manifest_hash=manifest_hash, ts=ts)


def save_manifest(session: Session, manifest: Manifest) -> Manifest:
    """Persist the manifest to the releases table."""

    record = ReleaseRecord(
        manifest_id=manifest.manifest_id,
        manifest_json=manifest.payload,
        manifest_hash=manifest.manifest_hash,
        ts=manifest.ts,
    )
    session.add(record)
    session.commit()
    session.refresh(record)
    return Manifest(
        manifest_id=record.manifest_id,
        payload=_normalise_payload(record.manifest_json),
        manifest_hash=record.manifest_hash,
        ts=record.ts,
    )


def fetch_manifest(session: Session, manifest_id: str) -> Optional[Manifest]:
    record: Optional[ReleaseRecord] = session.get(ReleaseRecord, manifest_id)
    if record is None:
        return None
    return Manifest(
        manifest_id=record.manifest_id,
        payload=_normalise_payload(record.manifest_json),
        manifest_hash=record.manifest_hash,
        ts=record.ts,
    )


def list_manifests(session: Session, limit: Optional[int] = None) -> List[Manifest]:
    stmt = select(ReleaseRecord).order_by(ReleaseRecord.ts.desc())
    if limit is not None:
        stmt = stmt.limit(limit)
    rows = session.execute(stmt).scalars().all()
    manifests: List[Manifest] = []
    for row in rows:
        manifests.append(
            Manifest(
                manifest_id=row.manifest_id,
                payload=_normalise_payload(row.manifest_json),
                manifest_hash=row.manifest_hash,
                ts=row.ts,
            )
        )
    return manifests


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def manifest_to_json(manifest: Manifest) -> str:
    return json.dumps(manifest.to_dict(), indent=2, sort_keys=True)


def manifest_to_markdown(manifest: Manifest) -> str:
    lines = [
        f"# Release Manifest {manifest.manifest_id}",
        "",
        f"- Timestamp: {manifest.ts.isoformat()}",
        f"- Hash: {manifest.manifest_hash}",
        "",
    ]

    def _append_table(title: str, values: Dict[str, str]) -> None:
        lines.extend([f"## {title}", ""])
        if not values:
            lines.append("No entries recorded.")
            lines.append("")
            return
        lines.extend(["| Name | Version |", "| --- | --- |"])
        for name, version in sorted(values.items()):
            lines.append(f"| {name} | {version} |")
        lines.append("")

    _append_table("Services", _coerce_str_mapping(manifest.payload.get("services")))
    _append_table("Configs", _coerce_str_mapping(manifest.payload.get("configs")))
    _append_table("Models", _coerce_str_mapping(manifest.payload.get("models")))

    return "\n".join(lines)


def write_manifest_artifacts(
    manifest: Manifest,
    json_path: Path = DEFAULT_JSON_OUTPUT,
    markdown_path: Path = DEFAULT_MARKDOWN_OUTPUT,
) -> None:
    try:
        json_path.write_text(manifest_to_json(manifest))
    except OSError:  # pragma: no cover - filesystem failure
        pass
    try:
        markdown_path.write_text(manifest_to_markdown(manifest))
    except OSError:  # pragma: no cover - filesystem failure
        pass


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_release_manifest(
    manifest: Manifest,
    services_dir: Path = DEFAULT_SERVICES_DIR,
    models_dir: Path = DEFAULT_MODELS_DIR,
    config_db_url: str = DEFAULT_CONFIG_DB_URL,
) -> List[str]:
    """Compare live state against a stored manifest and return mismatch messages."""

    mismatches: List[str] = []
    expected_services = _coerce_str_mapping(manifest.payload.get("services"))
    expected_configs = _coerce_str_mapping(manifest.payload.get("configs"))
    expected_models = _coerce_str_mapping(manifest.payload.get("models"))

    actual_services = {k: str(v) for k, v in collect_service_versions(services_dir).items()}
    mismatches.extend(_diff_versions("services", expected_services, actual_services))

    actual_configs = {k: str(v) for k, v in collect_config_versions(config_db_url).items()}
    mismatches.extend(_diff_versions("configs", expected_configs, actual_configs))

    actual_models = {k: str(v) for k, v in collect_model_versions(models_dir).items()}
    mismatches.extend(_diff_versions("models", expected_models, actual_models))

    recomputed_hash = compute_manifest_hash(manifest.payload)
    if recomputed_hash != manifest.manifest_hash:
        mismatches.append(
            "manifest hash mismatch: stored hash"
            f" {manifest.manifest_hash} does not match recomputed {recomputed_hash}"
        )

    return mismatches


def _diff_versions(category: str, expected: Dict[str, str], actual: Dict[str, str]) -> List[str]:
    messages: List[str] = []
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    changed = sorted(key for key in expected if key in actual and expected[key] != actual[key])

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
    if not isinstance(value, dict):
        return {}
    return {str(key): str(val) for key, val in value.items() if key is not None and val is not None}


def _normalise_payload(payload: Optional[Dict[str, object]]) -> Dict[str, Dict[str, str]]:
    if not isinstance(payload, dict):
        payload = {}
    return {
        "services": _coerce_str_mapping(payload.get("services")),
        "configs": _coerce_str_mapping(payload.get("configs")),
        "models": _coerce_str_mapping(payload.get("models")),
    }


def verify_release_manifest_by_id(
    manifest_id: str,
    *,
    services_dir: Path = DEFAULT_SERVICES_DIR,
    models_dir: Path = DEFAULT_MODELS_DIR,
    config_db_url: str = DEFAULT_CONFIG_DB_URL,
    session_factory: Callable[[], Session] = SessionLocal,
) -> Tuple[Manifest, List[str]]:
    """Fetch a stored manifest and compare it with the live environment."""

    with session_factory() as session:
        manifest = fetch_manifest(session, manifest_id)

    if manifest is None:
        raise LookupError(f"Manifest '{manifest_id}' was not found")

    mismatches = verify_release_manifest(
        manifest,
        services_dir=services_dir,
        models_dir=models_dir,
        config_db_url=config_db_url,
    )
    return manifest, mismatches


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def create_command(args: argparse.Namespace) -> int:
    manifest = create_manifest(
        manifest_id=args.id,
        services_dir=Path(args.services_dir),
        models_dir=Path(args.models_dir),
        config_db_url=args.config_db,
    )

    with SessionLocal() as session:
        if fetch_manifest(session, manifest.manifest_id):
            raise SystemExit(f"Manifest '{manifest.manifest_id}' already exists")
        manifest = save_manifest(session, manifest)

    write_manifest_artifacts(manifest, Path(args.output_json), Path(args.output_markdown))
    print(manifest_to_json(manifest))
    return 0


def list_command(args: argparse.Namespace) -> int:
    with SessionLocal() as session:
        manifests = list_manifests(session, args.limit)
    print(json.dumps([m.to_dict() for m in manifests], indent=2, sort_keys=True))
    return 0


def verify_command(args: argparse.Namespace) -> int:
    try:
        _, mismatches = verify_release_manifest_by_id(
            args.id,
            services_dir=Path(args.services_dir),
            models_dir=Path(args.models_dir),
            config_db_url=args.config_db,
        )
    except LookupError as exc:
        raise SystemExit(str(exc))

    if mismatches:
        for message in mismatches:
            print(message)
        return 1

    print("Manifest matches live state")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Release manifest tooling")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Collect versions and persist a manifest")
    create_parser.add_argument("--id", help="Optional manifest identifier")
    create_parser.add_argument(
        "--services-dir",
        default=str(DEFAULT_SERVICES_DIR),
        help="Directory containing Kubernetes service manifests",
    )
    create_parser.add_argument(
        "--models-dir",
        default=str(DEFAULT_MODELS_DIR),
        help="Directory containing model implementation files",
    )
    create_parser.add_argument(
        "--config-db",
        default=DEFAULT_CONFIG_DB_URL,
        help="SQLAlchemy database URL for the config service",
    )
    create_parser.add_argument(
        "--output-json",
        default=str(DEFAULT_JSON_OUTPUT),
        help="Path to write the JSON manifest",
    )
    create_parser.add_argument(
        "--output-markdown",
        default=str(DEFAULT_MARKDOWN_OUTPUT),
        help="Path to write the Markdown manifest",
    )
    create_parser.set_defaults(func=create_command)

    list_parser = subparsers.add_parser("list", help="List stored manifests")
    list_parser.add_argument("--limit", type=int, help="Maximum number of manifests to return")
    list_parser.set_defaults(func=list_command)

    verify_parser = subparsers.add_parser(
        "verify_release_manifest",
        help="Verify that the live deployment matches a stored manifest",
    )
    verify_parser.add_argument("--id", required=True, help="Identifier of the manifest to verify")
    verify_parser.add_argument(
        "--services-dir",
        default=str(DEFAULT_SERVICES_DIR),
        help="Directory containing Kubernetes service manifests",
    )
    verify_parser.add_argument(
        "--models-dir",
        default=str(DEFAULT_MODELS_DIR),
        help="Directory containing model implementation files",
    )
    verify_parser.add_argument(
        "--config-db",
        default=DEFAULT_CONFIG_DB_URL,
        help="SQLAlchemy database URL for the config service",
    )
    verify_parser.set_defaults(func=verify_command)

    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
