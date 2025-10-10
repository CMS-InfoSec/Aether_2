"""Regression coverage for the compliance filter insecure-default fallback."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


def _reload_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("COMPLIANCE_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path / "state"))
    for name in list(sys.modules):
        if name.startswith("compliance_filter"):
            sys.modules.pop(name)
    module = importlib.import_module("compliance_filter")
    return importlib.reload(module)


def test_compliance_filter_uses_local_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _reload_module(monkeypatch, tmp_path)

    entry = module.compliance_filter.update_asset("btc-usd", "restricted", "manual review")
    assert entry.symbol == "BTC-USD"
    assert entry.status == "restricted"

    allowed, persisted = module.compliance_filter.evaluate("btc-usd")
    assert not allowed
    assert persisted is not None
    assert persisted.reason == "manual review"

    assets = module.compliance_filter.list_assets()
    assert len(assets) == 1
    assert assets[0].symbol == "BTC-USD"

    # Reload the module to ensure state survives across imports.
    module = _reload_module(monkeypatch, tmp_path)
    allowed_after, record_after = module.compliance_filter.evaluate("btc-usd")
    assert not allowed_after
    assert record_after is not None
    assert record_after.status == "restricted"
