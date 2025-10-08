"""Kubernetes helpers for managing Kraken API secrets."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, ClassVar, Dict, Optional

import base64
from services.secrets.secure_secrets import EncryptedSecretEnvelope, EnvelopeEncryptor

try:  # pragma: no cover - optional dependency for real clusters
    from kubernetes import client, config
except ImportError:  # pragma: no cover - fallback for tests/local
    client = None  # type: ignore
    config = None  # type: ignore


ANNOTATION_CREATED_AT = "aether.kraken/createdAt"
ANNOTATION_ROTATED_AT = "aether.kraken/lastRotatedAt"


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
        self._ensure_core_v1()

    def _ensure_core_v1(self) -> Any:
        """Return a Kubernetes client instance, falling back to a stub."""

        if self.core_v1 is None:
            self.core_v1 = SimpleNamespace()
        return self.core_v1

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def secret_name(self, account_id: str) -> str:
        """Return the canonical secret name for an account."""

        return f"kraken-keys-{account_id}"

    def write_credentials(self, account_id: str, *, api_key: str, api_secret: str) -> None:
        """Create or update the namespaced secret with Kraken credentials."""

        encryptor = EnvelopeEncryptor()
        envelope = encryptor.encrypt_credentials(
            account_id,
            api_key=api_key,
            api_secret=api_secret,
        )
        self.write_encrypted_secret(account_id, envelope=envelope)

    def write_encrypted_secret(
        self, account_id: str, *, envelope: EncryptedSecretEnvelope
    ) -> None:
        """Persist envelope-encrypted credentials to the secret backend."""

        name = self.secret_name(account_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        annotations = {
            ANNOTATION_CREATED_AT: now_iso,
            ANNOTATION_ROTATED_AT: now_iso,
        }
        secret_data = envelope.to_secret_data()

        payload = {
            "metadata": {"annotations": annotations},
            "data": secret_data,
            "type": "Opaque",
        }

        core_v1 = self._ensure_core_v1()

        if hasattr(core_v1, "patch_namespaced_secret"):
            try:  # pragma: no branch - handled for ApiException absence
                core_v1.patch_namespaced_secret(  # type: ignore[call-arg]
                    name=name,
                    namespace=self.namespace,
                    body=payload,
                )
                return
            except Exception as exc:  # pragma: no cover - relies on k8s client
                if getattr(exc, "status", None) != 404:
                    raise
        if hasattr(core_v1, "create_namespaced_secret"):
            body = {
                "metadata": {
                    "name": name,
                    "namespace": self.namespace,
                    "annotations": annotations,
                },
                "data": secret_data,
                "type": "Opaque",
            }
            core_v1.create_namespaced_secret(  # type: ignore[call-arg]
                namespace=self.namespace,
                body=body,
            )
            return

        namespace_store = self._store.setdefault(self.namespace, {})
        record = namespace_store.get(name, {})
        created_at = record.get("created_at") or now_iso
        record.update(secret_data)
        record.update(
            {
                "created_at": created_at,
                "rotated_at": now_iso,
                "kms_key_id_plain": envelope.kms_key_id,
            }
        )
        namespace_store[name] = record

    def get_secret(self, name: str) -> Dict[str, Any]:
        """Return the raw secret payload for the provided name."""

        core_v1 = self._ensure_core_v1()

        if hasattr(core_v1, "read_namespaced_secret"):
            try:  # pragma: no branch - mirrors the Kubernetes client API
                response = core_v1.read_namespaced_secret(  # type: ignore[call-arg]
                    name=name,
                    namespace=self.namespace,
                )
            except Exception:
                response = None
            if response is not None:
                metadata: Dict[str, Any] = {}
                raw_metadata = getattr(response, "metadata", None)
                if raw_metadata is not None:
                    if hasattr(raw_metadata, "to_dict"):
                        metadata = raw_metadata.to_dict()  # pragma: no cover - client dep
                    else:
                        metadata = dict(getattr(raw_metadata, "__dict__", {}))
                payload: Dict[str, Any] = {}
                string_data = getattr(response, "string_data", None)
                if isinstance(string_data, dict) and string_data:
                    payload = {str(k): str(v) for k, v in string_data.items()}
                else:
                    data = getattr(response, "data", None)
                    if isinstance(data, dict):
                        decoded: Dict[str, Any] = {}
                        for key, value in data.items():
                            if isinstance(value, str) and base64 is not None:
                                try:
                                    decoded_value = base64.b64decode(value).decode()
                                except Exception:
                                    decoded_value = value
                                decoded[str(key)] = decoded_value
                            else:
                                decoded[str(key)] = value
                        payload = decoded
                return {"metadata": metadata, "data": payload}

        namespace_store = self._store.get(self.namespace, {})
        secret = namespace_store.get(name)
        if not secret:
            return {}
        metadata = {
            "name": name,
            "namespace": self.namespace,
            "created_at": secret.get("created_at"),
            "rotated_at": secret.get("rotated_at"),
            "kms_key_id": secret.get("kms_key_id_plain") or secret.get("kms_key_id"),
        }
        for sensitive_field in (
            "api_key",
            "api_secret",
            "encrypted_api_key",
            "encrypted_api_secret",
        ):
            if sensitive_field in secret:
                metadata[sensitive_field] = secret.get(sensitive_field)
        annotations = {}
        if secret.get("created_at"):
            annotations[ANNOTATION_CREATED_AT] = secret["created_at"]
        if secret.get("rotated_at"):
            annotations[ANNOTATION_ROTATED_AT] = secret["rotated_at"]
        if annotations:
            metadata["annotations"] = annotations
        return {"metadata": metadata, "data": dict(secret)}

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
