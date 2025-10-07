"""Utilities for reconstructing historical system state.

This module exposes two user-facing entry points:

* A command line interface that reconstructs state for a timestamp via
  ``python time_travel.py --ts "2025-09-30T12:00:00"``.
* A FastAPI router that surfaces the same behaviour over HTTP at
  ``GET /debug/time_travel?ts=...``.

The actual reconstruction logic is intentionally defensive.  The production
environment ships event logs and periodic database snapshots into a variety of
directories.  Local development environments, however, often lack historical
artifacts.  To ensure the tool is still helpful during development we treat
missing data sources as soft failures and return metadata describing the paths
that were inspected.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from fastapi import APIRouter, HTTPException, Query


ISO_TIMESTAMP_HELP = (
    "Timestamp must be ISO-8601 formatted, for example 2025-09-30T12:00:00 or "
    "2025-09-30T12:00:00Z"
)

# Default locations that may contain supporting artifacts.  The paths are kept
# intentionally broad – in practice deployments can point the tool at
# environment-specific locations via environment variables.
DEFAULT_EVENT_LOG_DIRS = (
    "data/event_logs",
    "logs/events",
    "ops/event_logs",
)
DEFAULT_SNAPSHOT_DIRS = (
    "data/db_snapshots",
    "snapshots",
    "ops/db_snapshots",
)
DEFAULT_FEATURE_DIRS = (
    "data/feast",
    "ml/data",
)
DEFAULT_MODEL_DIRS = (
    "ml/models",
    "services/models",
)
DEFAULT_CONFIG_DIRS = (
    "config",
    "config/env",
)
DEFAULT_OMS_DIRS = (
    "ops/oms_state",
    "data/oms",
)


@dataclass
class ArtifactSummary:
    """Summary metadata describing the files used to construct part of a state."""

    root: str
    files: List[Dict[str, Any]]


def _sanitise_directory(raw: str, env_var: str) -> Path:
    """Return a safe absolute directory path derived from ``raw``."""

    text = raw.strip()
    if not text:
        raise ValueError(f"{env_var} entries must not be empty")
    if any(ord(char) < 32 for char in text):
        raise ValueError(f"{env_var} entries must not contain control characters")

    candidate = Path(text).expanduser()
    if any(part == ".." for part in candidate.parts):
        raise ValueError(f"{env_var} entries must not contain parent directory traversal")

    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate

    candidate = candidate.absolute()

    for ancestor in (candidate, *candidate.parents):
        if ancestor.exists() and ancestor.is_symlink():
            raise ValueError(f"{env_var} entries must not reference symlinked paths")

    if candidate.exists() and not candidate.is_dir():
        raise ValueError(f"{env_var} entries must point to directories")

    return candidate


def _resolve_artifact_dirs(env_var: str, default: Iterable[str]) -> List[Path]:
    """Resolve and sanitise artifact directories for state reconstruction."""

    override = os.getenv(env_var)
    if override:
        raw_entries = [part for part in override.split(os.pathsep) if part]
    else:
        raw_entries = list(default)

    directories: List[Path] = []
    for raw in raw_entries:
        text = str(raw).strip()
        if not text:
            continue
        directories.append(_sanitise_directory(text, env_var))

    return directories


def parse_timestamp(ts: str) -> datetime:
    """Parse a timestamp string into a :class:`datetime` instance.

    We accept ISO-8601 timestamps with or without a trailing ``Z``.  Missing
    timezone information is treated as UTC to align with platform conventions.
    """

    if not ts:
        raise ValueError("timestamp is required")

    cleaned = ts.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:  # pragma: no cover - pure validation logic
        raise ValueError(f"invalid timestamp '{ts}': {ISO_TIMESTAMP_HELP}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return parsed


def _load_event_logs(timestamp: datetime, limit: int = 100) -> Dict[str, Any]:
    """Load event log entries up to ``timestamp``.

    The method scans the configured directories for ``*.json`` and ``*.jsonl``
    files, returning at most ``limit`` events ordered chronologically.  The
    payload is intentionally small to keep the API responsive.
    """

    log_dirs = _resolve_artifact_dirs("AETHER_EVENT_LOG_DIRS", DEFAULT_EVENT_LOG_DIRS)
    events: List[Dict[str, Any]] = []
    searched: List[str] = []

    for directory in log_dirs:
        searched.append(str(directory))
        if not directory.exists():
            continue

        for current_root, _, files in os.walk(directory, followlinks=False):
            current_path = Path(current_root)
            for name in files:
                if not name.endswith((".json", ".jsonl")):
                    continue

                path = current_path / name
                if path.is_symlink():
                    continue

                for entry in _read_json_entries(path):
                    entry_ts = entry.get("timestamp")
                    if entry_ts is None:
                        continue

                    try:
                        entry_dt = parse_timestamp(str(entry_ts))
                    except ValueError:
                        continue

                    if entry_dt <= timestamp:
                        events.append({"source": str(path), "event": entry})

    events.sort(key=lambda item: item["event"].get("timestamp", ""))

    return {
        "searched_paths": searched,
        "events": events[-limit:],
        "total_events": len(events),
    }


def _read_json_entries(path: Path) -> Iterable[Dict[str, Any]]:
    """Yield JSON objects from a file supporting both JSON and JSONL."""

    try:
        text = path.read_text()
    except (OSError, UnicodeDecodeError):  # pragma: no cover - defensive path
        return []

    try:
        # Attempt to interpret as a single JSON document first.
        data = json.loads(text)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
    except json.JSONDecodeError:
        pass

    entries: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            entries.append(parsed)
    return entries


def _collect_file_state(
    directories: Iterable[str],
    timestamp: datetime,
    env_var: str,
    max_entries: int = 50,
) -> Dict[str, Any]:
    """Collect metadata for files whose modification time predates ``timestamp``."""

    roots = _resolve_artifact_dirs(env_var, directories)
    summaries: List[ArtifactSummary] = []
    searched: List[str] = []

    for root in roots:
        searched.append(str(root))
        if not root.exists():
            continue

        files: List[Dict[str, Any]] = []
        try:
            for current_root, _, filenames in os.walk(root, followlinks=False):
                current_path = Path(current_root)
                for name in filenames:
                    path = current_path / name
                    if path.is_symlink():
                        continue
                    try:
                        stat = path.stat()
                    except FileNotFoundError:
                        continue
                    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                    if modified <= timestamp:
                        files.append(
                            {
                                "path": str(path.relative_to(root)),
                                "modified": modified.isoformat(),
                                "size": stat.st_size,
                            }
                        )
        except OSError:  # pragma: no cover - filesystem edge cases
            continue

        if files:
            summaries.append(ArtifactSummary(root=str(root), files=files[-max_entries:]))

    return {
        "searched_paths": searched,
        "artifacts": [summary.__dict__ for summary in summaries],
    }


def reconstruct_state(timestamp: datetime) -> Dict[str, Any]:
    """Reconstruct system state as-of ``timestamp``.

    The output is a dictionary with keys ``timestamp``, ``features``, ``models``,
    ``configs``, ``oms`` and ``events``.  Each entry contains metadata describing
    the artifacts that contributed to the reconstruction.
    """

    feature_state = _collect_file_state(
        DEFAULT_FEATURE_DIRS, timestamp, env_var="AETHER_FEATURE_DIRS"
    )
    model_state = _collect_file_state(
        DEFAULT_MODEL_DIRS, timestamp, env_var="AETHER_MODEL_DIRS"
    )
    config_state = _collect_file_state(
        DEFAULT_CONFIG_DIRS, timestamp, env_var="AETHER_CONFIG_DIRS"
    )
    oms_state = _collect_file_state(
        DEFAULT_OMS_DIRS, timestamp, env_var="AETHER_OMS_DIRS"
    )
    db_state = _collect_file_state(
        DEFAULT_SNAPSHOT_DIRS, timestamp, env_var="AETHER_SNAPSHOT_DIRS"
    )

    events = _load_event_logs(timestamp)

    return {
        "timestamp": timestamp.isoformat(),
        "features": feature_state,
        "models": model_state,
        "configs": config_state,
        "oms": oms_state,
        "db_snapshots": db_state,
        "event_logs": events,
    }


def _cli(argv: Optional[List[str]] = None) -> int:
    """Run the command line interface."""

    parser = argparse.ArgumentParser(description="Reconstruct system state")
    parser.add_argument("--ts", required=True, help=ISO_TIMESTAMP_HELP)

    args = parser.parse_args(argv)

    try:
        timestamp = parse_timestamp(args.ts)
    except ValueError as exc:
        parser.error(str(exc))
        return 2  # pragma: no cover - parser.error exits

    try:
        state = reconstruct_state(timestamp)
    except ValueError as exc:
        parser.error(str(exc))
        return 2  # pragma: no cover - parser.error exits
    json.dump(state, fp=os.sys.stdout, indent=2)
    os.sys.stdout.write("\n")
    return 0


router = APIRouter(prefix="/debug", tags=["debug"])


@router.get("/time_travel", summary="Reconstruct system state at a timestamp")
def time_travel_endpoint(ts: str = Query(..., description=ISO_TIMESTAMP_HELP)) -> Dict[str, Any]:
    """Return reconstructed state for an HTTP request."""

    try:
        timestamp = parse_timestamp(ts)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        return reconstruct_state(timestamp)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


if __name__ == "__main__":  # pragma: no cover - CLI dispatch
    raise SystemExit(_cli())

