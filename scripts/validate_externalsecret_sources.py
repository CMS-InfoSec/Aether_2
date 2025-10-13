#!/usr/bin/env python3
"""Validate that ExternalSecret remote references resolve to Vault keys.

This script parses an ExternalSecret manifest and confirms that every
`remoteRef` entry exists in Vault with the requested property. It expects the
`vault` CLI to be authenticated already (for example via `hashicorp/vault-action`).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import yaml


@dataclass(frozen=True)
class RemoteRef:
    external_secret: str
    secret_key: Optional[str]
    remote_key: str
    remote_property: str
    optional: bool = False


class VaultLookupError(RuntimeError):
    """Represents an error returned when fetching data from Vault."""

    def __init__(self, path: str, message: str) -> None:
        super().__init__(message)
        self.path = path
        self.message = message


def load_remote_refs(manifest_path: Path) -> List[RemoteRef]:
    """Parse the ExternalSecret manifest and collect Vault references."""
    with manifest_path.open("r", encoding="utf-8") as fh:
        documents = list(yaml.safe_load_all(fh))

    refs: List[RemoteRef] = []
    for document in documents:
        if not isinstance(document, dict):
            continue
        metadata = document.get("metadata", {}) or {}
        spec = document.get("spec", {}) or {}
        data_entries = spec.get("data", []) or []
        if not isinstance(data_entries, list):
            continue
        external_secret_name = metadata.get("name", "<unknown>")
        for entry in data_entries:
            if not isinstance(entry, dict):
                continue
            remote_ref = entry.get("remoteRef", {}) or {}
            remote_key = remote_ref.get("key")
            remote_property = remote_ref.get("property")
            if not remote_key or not remote_property:
                continue
            secret_key = entry.get("secretKey")
            optional = bool(entry.get("optional", False) or remote_ref.get("optional", False))
            refs.append(
                RemoteRef(
                    external_secret=external_secret_name,
                    secret_key=secret_key,
                    remote_key=str(remote_key),
                    remote_property=str(remote_property),
                    optional=optional,
                )
            )
    return refs


def fetch_vault_path(path: str) -> Dict[str, object]:
    """Return the key/value pairs stored at a Vault KV path."""
    try:
        result = subprocess.run(
            ["vault", "kv", "get", "-format=json", path],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - exercised in CI
        stderr = exc.stderr.strip() if exc.stderr else exc.stdout.strip()
        raise VaultLookupError(path, stderr or exc.args[0]) from exc

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:  # pragma: no cover - unexpected
        raise VaultLookupError(path, f"Invalid JSON returned from Vault: {exc}") from exc

    data = payload.get("data")
    if isinstance(data, dict) and "data" in data:
        inner = data["data"]
        if isinstance(inner, dict):
            return inner
    if isinstance(data, dict):
        return data

    raise VaultLookupError(path, "Unexpected Vault response structure")


def validate_refs(remote_refs: Iterable[RemoteRef]) -> List[str]:
    """Validate each remote reference against Vault and return any failures."""
    refs_by_path: Dict[str, List[RemoteRef]] = defaultdict(list)
    for ref in remote_refs:
        refs_by_path[ref.remote_key].append(ref)

    failures: List[str] = []
    for path, refs in sorted(refs_by_path.items()):
        try:
            data = fetch_vault_path(path)
        except VaultLookupError as error:
            missing_refs = [r for r in refs if not r.optional]
            if missing_refs:
                for ref in missing_refs:
                    context = (
                        f"ExternalSecret '{ref.external_secret}' expects Vault path"
                        f" '{path}' with property '{ref.remote_property}'"
                    )
                    if ref.secret_key:
                        context += f" (secretKey '{ref.secret_key}')"
                    failures.append(f"{context}. Vault query failed: {error.message or 'path not found'}")
            continue

        for ref in refs:
            if ref.optional:
                continue
            if ref.remote_property not in data:
                context = (
                    f"ExternalSecret '{ref.external_secret}' expects property"
                    f" '{ref.remote_property}' at Vault path '{path}'"
                )
                if ref.secret_key:
                    context += f" (secretKey '{ref.secret_key}')"
                failures.append(f"{context}, but it was not present in Vault.")

    return failures


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        type=Path,
        help="Path to the ExternalSecret manifest YAML file",
    )
    args = parser.parse_args(argv)

    remote_refs = load_remote_refs(args.manifest)
    if not remote_refs:
        print("No ExternalSecret remote references found; nothing to validate.")
        return 0

    failures = validate_refs(remote_refs)
    if failures:
        print("ExternalSecret validation failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("All ExternalSecret Vault keys are present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
