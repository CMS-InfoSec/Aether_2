import asyncio

import pytest

from shared.async_utils import dispatch_async


def test_dispatch_async_without_running_loop():
    invoked = False

    async def _stub():
        nonlocal invoked
        invoked = True

    dispatch_async(_stub(), context="test")
    assert invoked


@pytest.mark.asyncio
async def test_dispatch_async_with_running_loop():
    invoked = False

    async def _stub():
        nonlocal invoked
        invoked = True

    dispatch_async(_stub(), context="test")
    await asyncio.sleep(0)
    assert invoked
