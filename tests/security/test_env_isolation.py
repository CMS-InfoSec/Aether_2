"""Regression checks that CI/test environments never load production secrets."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests import conftest as test_config


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            pytest.fail(f"Invalid line in {path}: {raw_line}")
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def test_env_test_file_contains_only_placeholders() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    env_path = repo_root / ".env.test"
    assert env_path.is_file(), ".env.test must exist for isolated test execution"

    env_values = _parse_env_file(env_path)
    expected = test_config._SAFE_ENV_DEFAULTS

    missing = set(expected) - set(env_values)
    assert not missing, f"Missing placeholder values in .env.test: {sorted(missing)}"

    mismatched = {
        key: env_values[key]
        for key, placeholder in expected.items()
        if env_values.get(key) != placeholder
    }
    assert not mismatched, f"Unexpected secrets present in .env.test: {mismatched}"

    for key, value in env_values.items():
        lowered = value.lower()
        assert "prod" not in lowered and "live" not in lowered, (
            f"Suspicious value for {key}: {value}"
        )


def test_dockerignore_blocks_sensitive_material() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dockerignore = repo_root / ".dockerignore"
    patterns = {
        line.strip()
        for line in dockerignore.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    required_patterns = {
        ".env",
        ".env.*",
        "config/production/",
        "deploy/credentials/",
        "secrets/production/",
        "vault/",
        "certs/",
        "**/*.pem",
        "**/*.key",
        "**/*.crt",
        "**/*.p12",
    }

    missing = required_patterns - patterns
    assert not missing, f"Sensitive Dockerignore patterns missing: {sorted(missing)}"
