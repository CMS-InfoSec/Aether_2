import pytest

from shared.session_config import load_session_ttl_minutes


def test_load_session_ttl_minutes_defaults_to_sixty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SESSION_TTL_MINUTES", raising=False)
    assert load_session_ttl_minutes() == 60


def test_load_session_ttl_minutes_rejects_non_numeric(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SESSION_TTL_MINUTES", "abc")
    with pytest.raises(RuntimeError, match="positive integer"):
        load_session_ttl_minutes()


def test_load_session_ttl_minutes_rejects_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SESSION_TTL_MINUTES", "0")
    with pytest.raises(RuntimeError, match="positive integer"):
        load_session_ttl_minutes()
