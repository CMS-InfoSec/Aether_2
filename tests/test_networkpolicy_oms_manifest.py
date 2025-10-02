from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("yaml")
import yaml

MANIFEST = Path("deploy/k8s/base/aether-services/networkpolicy-oms.yaml")


def _load_manifest() -> dict:
    with MANIFEST.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_ingress_sources_are_restricted() -> None:
    manifest = _load_manifest()
    ingress_rules = manifest["spec"].get("ingress", [])
    assert ingress_rules, "Expected at least one ingress rule in the OMS NetworkPolicy"

    sources: list[dict] = []
    for rule in ingress_rules:
        sources.extend(rule.get("from", []))

    assert sources, "Ingress rules must define at least one allowed source"

    for source in sources:
        assert source.get("podSelector", None) != {}, "podSelector must not allow all pods"

    allows_ingress_controller = any(
        source.get("namespaceSelector", {})
        .get("matchLabels", {})
        .get("kubernetes.io/metadata.name")
        == "ingress-nginx"
        and source.get("podSelector", {})
        .get("matchLabels", {})
        .get("app.kubernetes.io/name")
        == "ingress-nginx"
        for source in sources
    )
    assert allows_ingress_controller, "OMS policy must allow traffic from the ingress-nginx controller"

    allows_trusted_namespaces = any(
        source.get("namespaceSelector", {})
        .get("matchLabels", {})
        .get("networking.aether.io/trusted")
        == "true"
        for source in sources
    )
    assert allows_trusted_namespaces, "OMS policy must allow traffic from trusted labelled namespaces"
