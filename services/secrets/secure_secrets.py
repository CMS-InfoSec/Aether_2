"""Envelope encryption helpers for Kraken secrets."""
from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import hashlib
import hmac
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple

try:  # pragma: no cover - cryptography is optional in the test environment
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - provide a simple fallback implementation
    class AESGCM:  # type: ignore[no-redef]
        """Minimal stand-in that provides authenticated XOR encryption."""

        def __init__(self, key: bytes) -> None:
            if not isinstance(key, (bytes, bytearray)):
                raise TypeError("AESGCM key must be bytes-like")
            if len(key) == 0:
                raise ValueError("AESGCM key must not be empty")
            self._key = bytes(key)

        def _keystream(self, nonce: bytes, associated_data: bytes | None) -> bytes:
            digest = hashlib.sha256(self._key + nonce + (associated_data or b""))
            return digest.digest()

        def encrypt(self, nonce: bytes, data: bytes, associated_data: bytes | None) -> bytes:
            stream = self._keystream(nonce, associated_data)
            ciphertext = bytes(b ^ stream[i % len(stream)] for i, b in enumerate(data))
            tag = hashlib.sha256(self._key + nonce + ciphertext + (associated_data or b""))
            return ciphertext + tag.digest()

        def decrypt(self, nonce: bytes, data: bytes, associated_data: bytes | None) -> bytes:
            if len(data) < 32:
                raise ValueError("Ciphertext truncated")
            ciphertext, tag = data[:-32], data[-32:]
            expected = hashlib.sha256(self._key + nonce + ciphertext + (associated_data or b""))
            if not hmac.compare_digest(expected.digest(), tag):
                raise ValueError("Authentication failed")
            stream = self._keystream(nonce, associated_data)
            return bytes(b ^ stream[i % len(stream)] for i, b in enumerate(ciphertext))

LOGGER = logging.getLogger(__name__)


class EncryptionError(RuntimeError):
    """Raised when encryption or decryption fails."""


class MasterKeyPersistenceError(RuntimeError):
    """Raised when durable persistence of master keys fails."""


def _b64encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64decode(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii"))


def _serialize_context(context: Optional[Dict[str, str]]) -> bytes:
    if not context:
        return b""
    return json.dumps(dict(sorted(context.items())), separators=(",", ":")).encode("utf-8")


@dataclass
class MasterKeyRecord:
    """Represents a single master key version."""

    master_key_id: str
    rotated_at: datetime
    version: int


class MasterKeyPersistence:
    """Durable storage helper for local KMS master key material."""

    def __init__(self, *, path: Optional[os.PathLike[str] | str] = None) -> None:
        default_path = os.getenv("LOCAL_KMS_STATE_PATH")
        if path is None and default_path is not None:
            path = default_path
        if path is None:
            path = Path(tempfile.gettempdir()) / "aether-local-kms-master-keys.json"
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def _read_store(self) -> Dict[str, List[Dict[str, Any]]]:
        if not self._path.exists():
            return {}
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
            raise MasterKeyPersistenceError("Persisted master key store is corrupted") from exc
        except OSError as exc:  # pragma: no cover - filesystem issues
            raise MasterKeyPersistenceError("Unable to read persisted master keys") from exc
        if not isinstance(payload, dict):  # pragma: no cover - defensive guard
            raise MasterKeyPersistenceError("Unexpected master key store format")
        result: Dict[str, List[Dict[str, Any]]] = {}
        for key, value in payload.items():
            if isinstance(key, str) and isinstance(value, list):
                filtered = [item for item in value if isinstance(item, dict)]
                result[key] = filtered
        return result

    def load(self, key_id: str) -> List[Tuple[MasterKeyRecord, bytes]]:
        entries = self._read_store().get(key_id, [])
        restored: List[Tuple[MasterKeyRecord, bytes]] = []
        for entry in entries:
            try:
                master_key_id = str(entry["master_key_id"])
                rotated_at = datetime.fromisoformat(str(entry["rotated_at"]))
                version = int(entry["version"])
                encoded_key = str(entry["master_key"])
                master_key = _b64decode(encoded_key)
            except Exception as exc:  # pragma: no cover - defensive guard
                LOGGER.warning(
                    "Skipping invalid master key persistence entry", extra={"reason": str(exc)}
                )
                continue
            record = MasterKeyRecord(
                master_key_id=master_key_id,
                rotated_at=rotated_at,
                version=version,
            )
            restored.append((record, master_key))
        restored.sort(key=lambda item: item[0].version)
        return restored

    def _atomic_write(self, payload: Dict[str, List[Dict[str, Any]]]) -> None:
        tmp_fd, tmp_path = tempfile.mkstemp(dir=str(self._path.parent))
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, default=str, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self._path)
        except OSError as exc:  # pragma: no cover - filesystem issues
            raise MasterKeyPersistenceError("Unable to persist master key record") from exc
        finally:
            with suppress(FileNotFoundError):
                os.remove(tmp_path)

    def persist(self, *, key_id: str, record: MasterKeyRecord, master_key: bytes) -> None:
        store = self._read_store()
        entries = store.setdefault(key_id, [])
        encoded_key = _b64encode(master_key)
        serialised = {
            "master_key_id": record.master_key_id,
            "rotated_at": record.rotated_at.isoformat(),
            "version": record.version,
            "master_key": encoded_key,
        }
        entries = [entry for entry in entries if entry.get("master_key_id") != record.master_key_id]
        entries.append(serialised)
        entries.sort(key=lambda entry: int(entry.get("version", 0)))
        store[key_id] = entries
        self._atomic_write(store)


@dataclass
class DataKey:
    """Representation of a data key generated by the local KMS emulator."""

    plaintext: bytes
    encrypted: bytes
    kms_key_id: str
    context: bytes
    master_key_id: str
    master_key_rotated_at: datetime


class LocalKMSEmulator:
    """Simple AES-GCM based KMS emulator for envelope encryption.

    The emulator requires a base64-encoded master key either supplied directly or
    via the ``LOCAL_KMS_MASTER_KEY`` environment variable. Without a master key
    the emulator refuses to start so we never accidentally encrypt credentials
    with predictable development defaults.
    """

    default_key_id: ClassVar[str] = "local/aether-secrets"

    def __init__(
        self,
        *,
        key_id: str | None = None,
        master_key: Optional[bytes] = None,
        rotation_interval: Optional[timedelta] = None,
        persistence: Optional[MasterKeyPersistence] = None,
    ) -> None:
        self.key_id = key_id or os.getenv("LOCAL_KMS_KEY_ID", self.default_key_id)
        self._rotation_interval = rotation_interval or timedelta(days=90)
        self._master_keys: Dict[str, bytes] = {}
        self._master_key_history: List[MasterKeyRecord] = []
        self._current_version = 0
        self._persistence = persistence or MasterKeyPersistence()
        self._load_persisted_master_keys()
        if self._master_keys:
            return
        if master_key is None:
            env_value = os.getenv("LOCAL_KMS_MASTER_KEY")
            if not env_value:
                raise EncryptionError(
                    "LOCAL_KMS_MASTER_KEY environment variable must be provided"
                )
            try:
                master_key = base64.b64decode(env_value)
            except (ValueError, binascii.Error) as exc:
                raise EncryptionError("Invalid LOCAL_KMS_MASTER_KEY value") from exc
        normalized_key = self._normalize_master_key(master_key)
        self._activate_master_key(normalized_key, datetime.now(timezone.utc))

    def _load_persisted_master_keys(self) -> None:
        try:
            records = self._persistence.load(self.key_id)
        except MasterKeyPersistenceError as exc:
            LOGGER.error("Failed to load persisted master keys for %s", self.key_id)
            raise
        for record, master_key in records:
            self._master_keys[record.master_key_id] = master_key
            self._master_key_history.append(record)
            if record.version > self._current_version:
                self._current_version = record.version
                self._current_master_key_id = record.master_key_id
                self._last_rotated = record.rotated_at

    def _normalize_master_key(self, master_key: bytes) -> bytes:
        if len(master_key) not in (16, 24, 32):
            master_key = (master_key * (32 // len(master_key) + 1))[:32]
        return master_key

    def _activate_master_key(self, master_key: bytes, rotated_at: datetime) -> None:
        next_version = self._current_version + 1
        master_key_id = f"{self.key_id}:v{next_version}"
        record = MasterKeyRecord(
            master_key_id=master_key_id,
            rotated_at=rotated_at,
            version=next_version,
        )
        if self._persistence is not None:
            self._persistence.persist(key_id=self.key_id, record=record, master_key=master_key)
        self._current_version = next_version
        self._master_keys[master_key_id] = master_key
        self._current_master_key_id = master_key_id
        self._last_rotated = rotated_at
        self._master_key_history.append(record)

    def _aesgcm(self, *, master_key_id: Optional[str] = None) -> AESGCM:
        key_id = master_key_id or self._current_master_key_id
        key = self._master_keys[key_id]
        return AESGCM(key)

    def _rotate_master_key_if_due(self) -> Optional[MasterKeyRecord]:
        now = datetime.now(timezone.utc)
        last_rotated = getattr(self, "_last_rotated", None)
        if last_rotated is None or now - last_rotated >= self._rotation_interval:
            new_master = os.urandom(32)
            self._activate_master_key(new_master, now)
            record = self._master_key_history[-1]
            LOGGER.info(
                "Rotated local KMS master key",  # pragma: no cover - log-only path
                extra={"master_key_id": record.master_key_id, "version": record.version},
            )
            return record
        return None

    def rotate_master_key_if_due(self) -> Optional[MasterKeyRecord]:
        """Public helper used by background jobs to trigger rotations."""

        return self._rotate_master_key_if_due()

    def master_key_history(self) -> List[MasterKeyRecord]:
        return list(self._master_key_history)

    def generate_data_key(
        self,
        *,
        key_id: Optional[str] = None,
        context: Optional[Dict[str, str]] = None,
    ) -> DataKey:
        self._rotate_master_key_if_due()
        kms_key_id = key_id or self.key_id
        context_bytes = _serialize_context(context)
        plaintext = os.urandom(32)
        nonce = os.urandom(12)
        aesgcm = self._aesgcm()
        encrypted = nonce + aesgcm.encrypt(nonce, plaintext, context_bytes)
        return DataKey(
            plaintext=plaintext,
            encrypted=encrypted,
            kms_key_id=kms_key_id,
            context=context_bytes,
            master_key_id=self._current_master_key_id,
            master_key_rotated_at=self._last_rotated,
        )

    def decrypt_data_key(
        self,
        *,
        encrypted: bytes,
        master_key_id: Optional[str] = None,
        context: Optional[Dict[str, str]] = None,
    ) -> bytes:
        context_bytes = _serialize_context(context)
        nonce, ciphertext = encrypted[:12], encrypted[12:]
        key_id = master_key_id or self._current_master_key_id
        aesgcm = self._aesgcm(master_key_id=key_id)
        return aesgcm.decrypt(nonce, ciphertext, context_bytes)


@dataclass
class EncryptedSecretEnvelope:
    """Container describing an encrypted Kraken credential pair."""

    kms_key_id: str
    encrypted_data_key: bytes
    encryption_context: bytes
    api_key_nonce: bytes
    api_key_ciphertext: bytes
    api_secret_nonce: bytes
    api_secret_ciphertext: bytes
    master_key_id: str
    master_key_rotated_at: datetime
    version: str = "v1"

    def to_secret_data(self) -> Dict[str, str]:
        return {
            "encryption_version": _b64encode(self.version.encode("utf-8")),
            "kms_key_id": _b64encode(self.kms_key_id.encode("utf-8")),
            "encrypted_data_key": _b64encode(self.encrypted_data_key),
            "encryption_context": _b64encode(self.encryption_context),
            "encrypted_api_key": _b64encode(self.api_key_ciphertext),
            "encrypted_api_key_nonce": _b64encode(self.api_key_nonce),
            "encrypted_api_secret": _b64encode(self.api_secret_ciphertext),
            "encrypted_api_secret_nonce": _b64encode(self.api_secret_nonce),
            "master_key_id": _b64encode(self.master_key_id.encode("utf-8")),
            "master_key_rotated_at": _b64encode(
                self.master_key_rotated_at.isoformat().encode("utf-8")
            ),
        }

    @classmethod
    def from_secret_data(cls, data: Dict[str, str]) -> "EncryptedSecretEnvelope":
        try:
            version = _b64decode(data["encryption_version"]).decode("utf-8")
            kms_key_id = _b64decode(data["kms_key_id"]).decode("utf-8")
            encrypted_data_key = _b64decode(data["encrypted_data_key"])
            encryption_context = _b64decode(data.get("encryption_context", ""))
            api_key_ciphertext = _b64decode(data["encrypted_api_key"])
            api_key_nonce = _b64decode(data["encrypted_api_key_nonce"])
            api_secret_ciphertext = _b64decode(data["encrypted_api_secret"])
            api_secret_nonce = _b64decode(data["encrypted_api_secret_nonce"])
            encoded_master_id = data.get("master_key_id")
            encoded_master_rotated_at = data.get("master_key_rotated_at")
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise EncryptionError("Encrypted secret payload missing required fields") from exc
        master_key_id = (
            _b64decode(encoded_master_id).decode("utf-8")
            if encoded_master_id
            else f"{kms_key_id}:legacy"
        )
        if encoded_master_rotated_at:
            rotated_str = _b64decode(encoded_master_rotated_at).decode("utf-8")
            try:
                master_key_rotated_at = datetime.fromisoformat(rotated_str)
            except ValueError:  # pragma: no cover - defensive guard
                master_key_rotated_at = datetime.now(timezone.utc)
        else:
            master_key_rotated_at = datetime.now(timezone.utc)
        return cls(
            kms_key_id=kms_key_id,
            encrypted_data_key=encrypted_data_key,
            encryption_context=encryption_context,
            api_key_nonce=api_key_nonce,
            api_key_ciphertext=api_key_ciphertext,
            api_secret_nonce=api_secret_nonce,
            api_secret_ciphertext=api_secret_ciphertext,
            master_key_id=master_key_id,
            master_key_rotated_at=master_key_rotated_at,
            version=version,
        )


@dataclass
class DecryptedCredentials:
    api_key: str
    api_secret: str


class EnvelopeEncryptor:
    """Utility for encrypting and decrypting Kraken credentials."""

    def __init__(self, kms: LocalKMSEmulator | None = None) -> None:
        self._kms = kms or LocalKMSEmulator()

    def encrypt_credentials(
        self,
        account_id: str,
        *,
        api_key: str,
        api_secret: str,
    ) -> EncryptedSecretEnvelope:
        context = {"account_id": account_id}
        data_key = self._kms.generate_data_key(context=context)
        aesgcm = AESGCM(data_key.plaintext)

        api_key_nonce = os.urandom(12)
        api_secret_nonce = os.urandom(12)
        api_key_ciphertext = aesgcm.encrypt(api_key_nonce, api_key.encode("utf-8"), data_key.context)
        api_secret_ciphertext = aesgcm.encrypt(
            api_secret_nonce,
            api_secret.encode("utf-8"),
            data_key.context,
        )
        return EncryptedSecretEnvelope(
            kms_key_id=data_key.kms_key_id,
            encrypted_data_key=data_key.encrypted,
            encryption_context=data_key.context,
            api_key_nonce=api_key_nonce,
            api_key_ciphertext=api_key_ciphertext,
            api_secret_nonce=api_secret_nonce,
            api_secret_ciphertext=api_secret_ciphertext,
            master_key_id=data_key.master_key_id,
            master_key_rotated_at=data_key.master_key_rotated_at,
        )

    def decrypt_credentials(
        self,
        account_id: str,
        envelope: EncryptedSecretEnvelope,
    ) -> DecryptedCredentials:
        context = {"account_id": account_id}
        data_key = self._kms.decrypt_data_key(
            encrypted=envelope.encrypted_data_key,
            master_key_id=envelope.master_key_id,
            context=context,
        )
        aesgcm = AESGCM(data_key)
        api_key = aesgcm.decrypt(
            envelope.api_key_nonce,
            envelope.api_key_ciphertext,
            envelope.encryption_context,
        ).decode("utf-8")
        api_secret = aesgcm.decrypt(
            envelope.api_secret_nonce,
            envelope.api_secret_ciphertext,
            envelope.encryption_context,
        ).decode("utf-8")
        return DecryptedCredentials(api_key=api_key, api_secret=api_secret)

    def rotate_master_key_if_due(self) -> Optional[MasterKeyRecord]:
        return self._kms.rotate_master_key_if_due()

    def master_key_history(self) -> List[MasterKeyRecord]:
        return self._kms.master_key_history()


class SecretsMetadataStore:
    """In-memory emulation of ``secrets_meta`` table for metadata tracking."""

    _records: ClassVar[list[Dict[str, Any]]] = []

    def __init__(self, *, account_id: str) -> None:
        self.account_id = account_id

    def record_rotation(
        self,
        *,
        kms_key_id: str,
        last_rotated: datetime,
        actor: str,
        approvers: Iterable[str],
        ip_hash: str,
        master_key_id: str,
        master_key_rotated_at: datetime,
    ) -> Dict[str, Any]:
        entry = {
            "account_id": self.account_id,
            "kms_key_id": kms_key_id,
            "last_rotated": last_rotated,
            "ts": datetime.now(timezone.utc),
            "actor": actor,
            "approvers": sorted(set(approvers)),
            "ip_hash": ip_hash,
            "master_key_id": master_key_id,
            "master_key_rotated_at": master_key_rotated_at,
        }
        self._records.append(entry)
        return dict(entry)

    @classmethod
    def latest(cls, account_id: str) -> Optional[Dict[str, Any]]:
        relevant = [entry for entry in cls._records if entry["account_id"] == account_id]
        if not relevant:
            return None
        latest = max(relevant, key=lambda entry: entry["ts"])
        return dict(latest)

    @classmethod
    def history(cls, account_id: str) -> List[Dict[str, Any]]:
        relevant = [entry for entry in cls._records if entry["account_id"] == account_id]
        relevant.sort(key=lambda entry: entry["ts"], reverse=True)
        return [dict(entry) for entry in relevant]

    @classmethod
    def all(cls) -> Iterable[Dict[str, Any]]:
        for entry in cls._records:
            yield dict(entry)


__all__ = [
    "DataKey",
    "DecryptedCredentials",
    "EncryptedSecretEnvelope",
    "EnvelopeEncryptor",
    "MasterKeyPersistence",
    "MasterKeyPersistenceError",
    "LocalKMSEmulator",
    "MasterKeyRecord",
    "SecretsMetadataStore",
    "EncryptionError",
]
