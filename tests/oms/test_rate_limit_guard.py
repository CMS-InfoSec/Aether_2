import time

import pytest

pytest.importorskip("fastapi")

pytestmark = pytest.mark.anyio("asyncio")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

from services.oms.rate_limit_guard import RateLimitGuard


async def test_non_urgent_orders_back_off_when_bucket_is_saturated() -> None:
    guard = RateLimitGuard(ws_limit=4, ws_window=1.0, soft_ratio=0.5, sleep_floor=0.01)
    account_id = "acct"

    # Consume enough capacity to breach the soft threshold but remain below the
    # hard limit. Successful releases keep the reservation within the window.
    for _ in range(2):
        await guard.acquire(account_id, "add_order", transport="websocket")
        await guard.release(account_id, transport="websocket", successful=True)

    start = time.perf_counter()
    await guard.acquire(account_id, "add_order", transport="websocket")
    elapsed = time.perf_counter() - start
    await guard.release(account_id, transport="websocket", successful=True)

    # Non urgent orders should respect the soft back-off and delay their
    # submission by at least one spacing interval.
    assert elapsed >= 0.2


async def test_urgent_cancels_proceed_without_soft_backoff() -> None:
    guard = RateLimitGuard(ws_limit=4, ws_window=1.0, soft_ratio=0.5, sleep_floor=0.01)
    account_id = "acct"

    for _ in range(2):
        await guard.acquire(account_id, "add_order", transport="websocket")
        await guard.release(account_id, transport="websocket", successful=True)

    start = time.perf_counter()
    await guard.acquire(
        account_id,
        "cancel_order",
        transport="websocket",
        urgent=True,
    )
    elapsed = time.perf_counter() - start
    await guard.release(account_id, transport="websocket", successful=True)

    # Urgent requests should bypass the soft throttling delay.
    assert elapsed < 0.05
