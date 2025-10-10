"""Subset of the Kubernetes client used by the secrets service tests."""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from types import SimpleNamespace
from typing import Any, Dict, Tuple

from . import rest

__all__ = ["CoreV1Api", "ApiClient", "rest"]


@dataclass
class ApiClient:
    """Placeholder for the real ``ApiClient`` type."""

    configuration: Any | None = None


_SECRETS: Dict[Tuple[str, str], SimpleNamespace] = {}
_LOCK = RLock()


class CoreV1Api:
    """Very small subset of the CoreV1Api secret management surface."""

    def __init__(self, api_client: ApiClient | None = None) -> None:
        self.api_client = api_client or ApiClient()

    def read_namespaced_secret(self, *, name: str, namespace: str) -> SimpleNamespace:
        with _LOCK:
            secret = _SECRETS.get((namespace, name))
        if secret is None:
            raise rest.ApiException(status=404, reason="Secret not found")
        return secret

    def patch_namespaced_secret(self, *, name: str, namespace: str, body: Dict[str, Any]) -> SimpleNamespace:
        secret = self.read_namespaced_secret(name=name, namespace=namespace)
        metadata = getattr(secret, "metadata", SimpleNamespace(name=name, annotations={})).__dict__
        annotations = metadata.setdefault("annotations", {})
        annotations.update(body.get("metadata", {}).get("annotations", {}))
        metadata["name"] = name
        secret.metadata = SimpleNamespace(**metadata)
        data = body.get("data")
        if data:
            secret.data = dict(data)
        secret.type = body.get("type", getattr(secret, "type", "Opaque"))
        return secret

    def create_namespaced_secret(self, *, namespace: str, body: Dict[str, Any]) -> SimpleNamespace:
        name = body.get("metadata", {}).get("name")
        if not name:
            raise rest.ApiException(status=422, reason="metadata.name is required")
        metadata = body.get("metadata", {})
        secret = SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                annotations=dict(metadata.get("annotations", {})),
            ),
            data=dict(body.get("data", {})),
            type=body.get("type", "Opaque"),
        )
        with _LOCK:
            _SECRETS[(namespace, name)] = secret
        return secret


# Re-export the rest module contents to mirror the real client package.
ApiException = rest.ApiException

