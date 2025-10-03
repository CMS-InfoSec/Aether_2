from __future__ import annotations

import pytest

from auth.session_client import AdminSessionManager, SessionToken


@pytest.mark.asyncio
async def test_session_manager_caches_until_expired() -> None:
    now = {"value": 1000.0}

    def _clock() -> float:
        return now["value"]

    class _StubClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def fetch_session(self, account_id: str) -> SessionToken:
            self.calls.append(account_id)
            return SessionToken(token=f"token-{len(self.calls)}", expires_at=_clock() + 30.0)

    client = _StubClient()
    manager = AdminSessionManager(client, clock=_clock, refresh_leeway=5.0)

    token_one = await manager.token_for_account("delta")
    token_two = await manager.token_for_account("delta")

    assert token_one == token_two
    assert client.calls == ["delta"]

    now["value"] += 30.0

    token_three = await manager.token_for_account("delta")
    assert token_three != token_one
    assert client.calls == ["delta", "delta"]


@pytest.mark.asyncio
async def test_session_manager_requires_account_id() -> None:
    class _StubClient:
        async def fetch_session(self, account_id: str) -> SessionToken:
            return SessionToken(token="noop")

    manager = AdminSessionManager(_StubClient())

    with pytest.raises(ValueError):
        await manager.token_for_account("   ")

