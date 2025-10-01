"""Kubernetes helpers for managing Kraken API secrets."""
from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, ClassVar, Dict, Optional

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
class KrakenSecretStore:
    """Wrapper around ``CoreV1Api`` for Kraken credential secrets."""

    namespace: str = "aether-secrets"
    core_v1: Optional[Any] = None

    _store: ClassVar[Dict[str, Dict[str, Dict[str, str]]]] = _in_memory_store()

    def __post_init__(self) -> None:
        if self.core_v1 is None and client is not None and config is not None:
            try:  # pragma: no cover - depends on cluster environment
                config.load_incluster_config()
            except Exception:  # pragma: no cover - config loading failures
                pass
            self.core_v1 = client.CoreV1Api()
        if self.core_v1 is None:
            self.core_v1 = SimpleNamespace()  # ensures attribute access works in tests

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def secret_name(self, account_id: str) -> str:
        """Return the canonical secret name for an account."""

        return f"kraken-keys-{account_id}"

    def write_credentials(self, account_id: str, *, api_key: str, api_secret: str) -> None:
        """Create or update the namespaced secret with Kraken credentials."""

        name = self.secret_name(account_id)
        payload = {"stringData": {"api_key": api_key, "api_secret": api_secret}}

        if hasattr(self.core_v1, "patch_namespaced_secret"):
            try:  # pragma: no branch - handled for ApiException absence
                self.core_v1.patch_namespaced_secret(  # type: ignore[call-arg]
                    name=name,
                    namespace=self.namespace,
                    body=payload,
                )
                return
            except Exception as exc:  # pragma: no cover - relies on k8s client
                if getattr(exc, "status", None) != 404:
                    raise
        if hasattr(self.core_v1, "create_namespaced_secret"):
            body = {
                "metadata": {"name": name, "namespace": self.namespace},
                **payload,
                "type": "Opaque",
            }
            self.core_v1.create_namespaced_secret(  # type: ignore[call-arg]
                namespace=self.namespace,
                body=body,
            )
            return

        # Fall back to in-memory store when the Kubernetes client is unavailable
        namespace_store = self._store.setdefault(self.namespace, {})
        namespace_store[name] = {"api_key": api_key, "api_secret": api_secret}

    def read_credentials(self, account_id: str) -> Dict[str, str]:
        """Retrieve credentials from the in-memory store for tests."""

        namespace_store = self._store.get(self.namespace, {})
        return dict(namespace_store.get(self.secret_name(account_id), {}))

    @classmethod
    def reset(cls) -> None:
        """Clear the in-memory store (used in tests)."""

        cls._store.clear()


# Backwards compatibility alias for legacy imports
KubernetesSecretClient = KrakenSecretStore

__all__ = ["KrakenSecretStore", "KubernetesSecretClient"]
