"""Integration tests covering the local KMS persistence layer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from services.secrets.secure_secrets import EnvelopeEncryptor, LocalKMSEmulator, MasterKeyPersistence


@pytest.mark.integration
def test_master_key_persistence_survives_restart(tmp_path: Path) -> None:
    """Encrypted secrets remain decryptable after KMS rotation and restart."""

    persistence = MasterKeyPersistence(path=tmp_path / "kms-state.json")
    kms = LocalKMSEmulator(key_id="test/local", persistence=persistence)
    encryptor = EnvelopeEncryptor(kms)

    envelope = encryptor.encrypt_credentials(
        "acct-123",
        api_key="api-key-123456",
        api_secret="api-secret-abcdef",
    )

    # Simulate master key rotation and ensure the new version is persisted.
    kms._last_rotated = datetime.now(timezone.utc) - timedelta(days=200)  # type: ignore[attr-defined]
    rotated = kms.rotate_master_key_if_due()
    assert rotated is not None
    assert rotated.version >= 2

    # Re-create the emulator to simulate a service restart.
    restarted_kms = LocalKMSEmulator(
        key_id="test/local",
        persistence=MasterKeyPersistence(path=persistence.path),
    )
    restarted_encryptor = EnvelopeEncryptor(restarted_kms)

    decrypted = restarted_encryptor.decrypt_credentials("acct-123", envelope)
    assert decrypted.api_key == "api-key-123456"
    assert decrypted.api_secret == "api-secret-abcdef"

    history_ids = {record.master_key_id for record in restarted_kms.master_key_history()}
    assert envelope.master_key_id in history_ids
