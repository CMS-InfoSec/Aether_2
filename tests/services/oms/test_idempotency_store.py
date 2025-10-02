from __future__ import annotations

import asyncio
import ast
from pathlib import Path
import types
from typing import Any, Dict


def _load_idempotency_store() -> type:
    """Dynamically load the _IdempotencyStore definition from the service module."""

    source = Path("services/oms/oms_service.py").read_text(encoding="utf-8")
    module_ast = ast.parse(source)
    target_names = {"_IdempotencyEntry", "_IdempotencyStore"}
    snippets: list[str] = []

    for node in module_ast.body:
        if isinstance(node, ast.ClassDef) and node.name in target_names:
            snippet = ast.get_source_segment(source, node)
            if not snippet:
                continue
            decorators = [
                f"@{ast.get_source_segment(source, decorator)}"
                for decorator in node.decorator_list
            ]
            combined = "\n".join((*decorators, snippet)) if decorators else snippet
            snippets.append(combined)

    exec_source = "\n\n".join(snippets)
    namespace: Dict[str, Any] = {}
    exec(  # noqa: S102 - executing trusted project code for testing
        "from dataclasses import dataclass\n"
        "import asyncio\n"
        "import time\n"
        "from typing import Awaitable, Dict, Tuple\n"
        "from typing import Any\n"
        "OMSOrderStatusResponse = Any\n"
        f"{exec_source}\n",
        namespace,
    )
    return namespace["_IdempotencyStore"]


_IdempotencyStore = _load_idempotency_store()


async def _create_response(order_id: str) -> types.SimpleNamespace:
    return types.SimpleNamespace(exchange_order_id=order_id)


def test_idempotency_store_evicts_entries_after_ttl() -> None:
    async def scenario() -> None:
        store = _IdempotencyStore(ttl_seconds=0.05)

        first_response, reused_first = await store.get_or_create(
            "order", _create_response("first")
        )

        assert reused_first is False
        assert first_response.exchange_order_id == "first"

        await asyncio.sleep(0.06)

        second_response, reused_second = await store.get_or_create(
            "order", _create_response("second")
        )

        assert reused_second is False
        assert second_response.exchange_order_id == "second"

        async with store._lock:  # type: ignore[attr-defined]
            assert len(store._entries) == 1  # type: ignore[attr-defined]

    asyncio.run(scenario())


def test_idempotency_store_retains_recent_entries() -> None:
    async def scenario() -> None:
        store = _IdempotencyStore(ttl_seconds=10.0)

        first_response, reused_first = await store.get_or_create(
            "order", _create_response("initial")
        )

        assert reused_first is False

        next_factory = _create_response("should-not-run")
        second_response, reused_second = await store.get_or_create("order", next_factory)
        next_factory.close()

        assert reused_second is True
        assert second_response is first_response

        async with store._lock:  # type: ignore[attr-defined]
            assert len(store._entries) == 1  # type: ignore[attr-defined]

    asyncio.run(scenario())
