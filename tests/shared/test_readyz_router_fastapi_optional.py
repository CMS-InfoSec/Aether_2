"""Tests for the shared readiness router when FastAPI is unavailable."""

from __future__ import annotations

import importlib
import json
import sys
from contextlib import contextmanager

import pytest

from shared.readiness import ReadinessProbeError


@contextmanager
def _without_modules(*prefixes: str):
    removed: dict[str, object] = {}
    try:
        for name in list(sys.modules):
            if any(name == prefix or name.startswith(f"{prefix}.") for prefix in prefixes):
                removed[name] = sys.modules.pop(name)
        yield
    finally:  # pragma: no cover - defensive restoration
        sys.modules.update(removed)


@pytest.mark.asyncio
async def test_readyz_router_provides_fastapi_fallback() -> None:
    module_name = "shared.readyz_router"
    original_module = sys.modules.pop(module_name, None)

    try:
        with _without_modules("fastapi", "starlette"):
            module = importlib.import_module(module_name)
            module = importlib.reload(module)

            router = module.ReadyzRouter()
            router.register_probe("ok", lambda: {"detail": "ready"})

            def _failing_probe() -> None:
                raise ReadinessProbeError("database unavailable")

            router.register_probe("db", _failing_probe)

            readyz_handler = router.router.routes[0].endpoint
            response = await readyz_handler()

            assert response.status_code == 503
            payload = json.loads(response.body)
            assert payload["status"] == "error"
            assert payload["checks"]["ok"] == {"status": "ok", "detail": "ready"}
            assert payload["checks"]["db"]["status"] == "error"
    finally:
        sys.modules.pop(module_name, None)
        if original_module is not None:
            sys.modules[module_name] = original_module
        else:
            importlib.invalidate_caches()
            importlib.import_module(module_name)
