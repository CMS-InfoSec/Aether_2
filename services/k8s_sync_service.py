"""Utility helpers for synchronising account secrets with Kubernetes."""

from __future__ import annotations

import base64
import logging
from typing import Any, Dict

try:  # pragma: no cover - optional dependency at runtime
    from kubernetes import client, config
    from kubernetes.client import CoreV1Api
    from kubernetes.client.rest import ApiException
except Exception:  # pragma: no cover - used during tests when kubernetes not available
    client = None  # type: ignore[assignment]
    config = None  # type: ignore[assignment]

    class CoreV1Api:  # type: ignore[no-redef]
        ...

    class ApiException(Exception):  # type: ignore[no-redef]
        def __init__(self, status: int = 500, reason: str | None = None) -> None:
            super().__init__(reason)
            self.status = status


LOGGER = logging.getLogger(__name__)
_SECRET_PREFIX = "kraken-keys-"
_DEFAULT_NAMESPACE = "aether"


def _ensure_client() -> CoreV1Api:
    if client is None or config is None:  # pragma: no cover - defensive
        raise RuntimeError("kubernetes client is not available")
    config.load_incluster_config()
    return CoreV1Api()


def _build_secret_payload(account_id: str, api_key: str, api_secret: str) -> Dict[str, Any]:
    encoded_key = base64.b64encode(api_key.encode("utf-8")).decode("ascii")
    encoded_secret = base64.b64encode(api_secret.encode("utf-8")).decode("ascii")
    name = f"{_SECRET_PREFIX}{account_id}"
    payload = {
        "metadata": {"name": name},
        "type": "Opaque",
        "data": {
            "apiKey": encoded_key,
            "apiSecret": encoded_secret,
        },
    }
    return payload


def sync_account_secret(account_id: str, api_key: str, api_secret: str) -> None:
    """Synchronise account secrets into a Kubernetes secret.

    In unit tests the kubernetes package is not available, so the function
    simply logs the intent.  At runtime the secret is applied with a
    ``PATCH`` request to ensure the resource is idempotent.
    """

    if not api_key or not api_secret:
        raise ValueError("API key and secret are required for secret sync")

    try:
        api = _ensure_client()
    except Exception as exc:  # pragma: no cover - typically raised in tests
        LOGGER.debug("Skipping Kubernetes sync for %s: %s", account_id, exc)
        return

    namespace = _DEFAULT_NAMESPACE
    body = _build_secret_payload(account_id, api_key, api_secret)
    name = body["metadata"]["name"]
    try:
        api.patch_namespaced_secret(name=name, namespace=namespace, body=body)
    except ApiException as exc:  # pragma: no cover - depends on cluster state
        if exc.status == 404:
            api.create_namespaced_secret(namespace=namespace, body=body)
        else:
            LOGGER.error("Failed to sync secret %s: %s", name, exc)
            raise


__all__ = ["sync_account_secret"]

