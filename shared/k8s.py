"""Kubernetes helpers for managing secrets within the cluster."""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Dict

try:  # pragma: no cover - optional dependency for real clusters
    from kubernetes import client, config
except ImportError:  # pragma: no cover - fallback for tests/local
    client = None  # type: ignore
    config = None  # type: ignore


def _in_memory_store() -> Dict[str, Dict[str, Dict[str, str]]]:
    """Return the global in-memory secret store."""

    if not hasattr(_in_memory_store, "_store"):
        _in_memory_store._store = {}  # type: ignore[attr-defined]
    return _in_memory_store._store  # type: ignore[attr-defined]


@dataclass
class KubernetesSecretClient:
    """Minimal client wrapper for patching secrets.

    In production this delegates to the official Kubernetes Python client.
    During tests the operations are stored in memory to avoid external
    dependencies.
    """

    namespace: str = "default"

    _store: ClassVar[Dict[str, Dict[str, Dict[str, str]]]] = _in_memory_store()

    def patch_secret(self, name: str, data: Dict[str, str]) -> None:
        """Patch or create a namespaced secret."""

        if client and config:  # pragma: no branch - runtime dependent
            config.load_incluster_config()
            body = {"stringData": data}
            core = client.CoreV1Api()
            core.patch_namespaced_secret(name=name, namespace=self.namespace, body=body)
            return

        namespace_store = self._store.setdefault(self.namespace, {})
        existing = dict(namespace_store.get(name, {}))
        existing.update(data)
        namespace_store[name] = existing

    def get_secret(self, name: str) -> Dict[str, str]:
        """Retrieve a secret from the in-memory store."""

        if client and config:  # pragma: no cover - would require real cluster
            raise RuntimeError("Direct secret inspection is not supported in-cluster")
        return dict(self._store.get(self.namespace, {}).get(name, {}))

    @classmethod
    def reset(cls) -> None:
        """Clear the in-memory store (used in tests)."""

        cls._store.clear()


__all__ = ["KubernetesSecretClient"]
