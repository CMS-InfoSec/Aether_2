from __future__ import annotations

import asyncio
import builtins
import importlib
import sys
from typing import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


def test_graceful_shutdown_imports_without_fastapi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The graceful shutdown helpers should stay usable without FastAPI installed."""

    _purge_modules(["fastapi", "starlette", "shared.graceful_shutdown"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        if name == "starlette" or name.startswith("starlette."):
            raise ModuleNotFoundError("starlette unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    graceful_shutdown = importlib.import_module("shared.graceful_shutdown")

    app = graceful_shutdown.FastAPI()
    manager = graceful_shutdown.setup_graceful_shutdown(app, service_name="observer")

    assert manager.service_name == "observer"
    assert getattr(app.state, "_graceful_shutdown_manager") is manager

    middleware = next(item for item in app.user_middleware if item.type == "http")

    async def _call_next(request):
        del request
        return graceful_shutdown.JSONResponse(
            {"status": "ok"}, status_code=graceful_shutdown.http_status.HTTP_200_OK
        )

    request = graceful_shutdown.Request(url="/orders")
    response = asyncio.run(middleware.handler(request, _call_next))

    assert response.status_code == graceful_shutdown.http_status.HTTP_200_OK

    start_route = next(route for route in app.routes if route.path == "/ops/drain/start")
    start_response = graceful_shutdown.Response()
    start_payload = asyncio.run(start_route.endpoint(start_response))

    assert start_response.status_code == graceful_shutdown.http_status.HTTP_202_ACCEPTED
    assert start_payload["draining"] is True

    blocked = asyncio.run(middleware.handler(graceful_shutdown.Request(url="/orders"), _call_next))

    assert blocked.status_code == graceful_shutdown.http_status.HTTP_503_SERVICE_UNAVAILABLE

    allowed = asyncio.run(
        middleware.handler(graceful_shutdown.Request(url="/ops/drain/status"), _call_next)
    )

    assert allowed.status_code == graceful_shutdown.http_status.HTTP_200_OK

    status_route = next(route for route in app.routes if route.path == "/ops/drain/status")
    status_payload = asyncio.run(status_route.endpoint())

    assert status_payload["draining"] is True
