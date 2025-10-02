import base64
import logging

import pytest

from services.secrets.secure_secrets import EncryptionError, LocalKMSEmulator


def test_local_kms_requires_master_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOCAL_KMS_MASTER_KEY", raising=False)
    with pytest.raises(EncryptionError):
        LocalKMSEmulator()


def test_local_kms_does_not_log_master_key(caplog: pytest.LogCaptureFixture) -> None:
    key = b"\x01" * 32
    with caplog.at_level(logging.DEBUG, logger="services.secrets.secure_secrets"):
        LocalKMSEmulator(master_key=key)
    encoded = base64.b64encode(key).decode("ascii")
    assert encoded not in caplog.text
    assert key.hex() not in caplog.text


def test_env_master_key_not_logged(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    key_bytes = b"\x02" * 32
    encoded = base64.b64encode(key_bytes).decode("ascii")
    monkeypatch.setenv("LOCAL_KMS_MASTER_KEY", encoded)
    with caplog.at_level(logging.DEBUG, logger="services.secrets.secure_secrets"):
        LocalKMSEmulator()
    assert encoded not in caplog.text
    assert key_bytes.hex() not in caplog.text
