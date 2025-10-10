"""Lightweight stand-ins for the :mod:`httpx` client library."""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, Optional
from urllib.parse import parse_qsl, urljoin, urlparse

try:  # pragma: no cover - prefer the real TestClient if FastAPI is installed
    from services.common.fastapi_stub import TestClient
except Exception:  # pragma: no cover - defensive fallback
    TestClient = None  # type: ignore[assignment]

__all__ = [
    "ASGITransport",
    "AsyncClient",
    "BaseTransport",
    "Client",
    "HTTPError",
    "HTTPStatusError",
    "MockTransport",
    "Request",
    "RequestError",
    "Response",
    "Timeout",
    "TimeoutException",
]


class RequestError(Exception):
    """Base error raised when a request cannot be issued."""


class HTTPError(RequestError):
    """Represents generic HTTP failures raised by the stub client."""


class TimeoutException(HTTPError):
    """Raised when a request exceeds the configured timeout."""


class HTTPStatusError(HTTPError):
    """HTTP error raised when the response status is not successful."""

    def __init__(
        self,
        message: str,
        *,
        request: Request | None = None,
        response: Response | None = None,
    ) -> None:
        super().__init__(message)
        self.request = request
        self.response = response


@dataclass
class Timeout:
    """Simplified timeout container mirroring :class:`httpx.Timeout`."""

    timeout: float | None

    def __iter__(self):  # pragma: no cover - convenience for compatibility
        yield self.timeout
        yield self.timeout
        yield self.timeout
        yield self.timeout


class Request:
    """Very small replacement for :class:`httpx.Request`."""

    def __init__(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        parsed = urlparse(url)
        self.method = method.upper()
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}
        self.params = dict(params or {})
        self.url = SimpleNamespace(
            scheme=parsed.scheme,
            host=parsed.hostname,
            path=parsed.path or "/",
            query=parsed.query,
            __str__=lambda self=parsed: self.geturl(),
        )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Request(method={self.method!r}, url={self.url.path!r})"


class Response:
    """Minimal :class:`httpx.Response` drop-in used by the stub client."""

    def __init__(
        self,
        status_code: int,
        *,
        json_data: Any | None = None,
        text: str | None = None,
        content: bytes | None = None,
        request: Request | None = None,
    ) -> None:
        self.status_code = int(status_code)
        self.request = request
        self._json = json_data if json_data is not None else _MISSING

        if text is not None:
            self.text = text
            self.content = text.encode("utf-8")
        elif content is not None:
            self.content = content
            try:
                self.text = content.decode("utf-8")
            except Exception:  # pragma: no cover - binary payloads
                self.text = ""
        elif json_data is not None:
            try:
                self.text = json.dumps(json_data)
            except (TypeError, ValueError):
                self.text = ""
            self.content = self.text.encode("utf-8")
        else:
            self.text = ""
            self.content = b""

    def json(self) -> Any:
        if self._json is not _MISSING:
            return self._json
        if not self.text:
            raise ValueError("Response does not contain JSON data")
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            raise HTTPStatusError(
                f"HTTP {self.status_code}", request=self.request, response=self
            )


class BaseTransport:
    """Base transport interface used by the lightweight ``httpx`` shim."""

    async def handle_async_request(
        self,
        method: str,
        url: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        raise NotImplementedError


class ASGITransport(BaseTransport):
    """Bridge that routes requests into a FastAPI-style application."""

    def __init__(self, *, app: Any) -> None:
        if TestClient is None:  # pragma: no cover - FastAPI stub missing
            raise RuntimeError("FastAPI compatibility shim is required for ASGITransport")
        self.app = app
        self._client: TestClient | None = None

    def _ensure_client(self) -> TestClient:
        if self._client is None:
            self._client = TestClient(self.app)
        return self._client

    async def handle_async_request(
        self,
        method: str,
        url: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        client = self._ensure_client()
        resolved_url, combined_params = _combine_url(url, params)
        request = Request(method, resolved_url, headers=headers, params=combined_params)
        path = urlparse(resolved_url).path or "/"

        loop = asyncio.get_running_loop()

        def _call() -> Any:
            return client.request(
                method,
                path,
                json=json,
                headers=headers,
                params=combined_params,
            )

        response = await loop.run_in_executor(None, _call)
        return Response(response.status_code, json_data=response.json(), request=request)


class MockTransport(BaseTransport):
    """Transport that routes requests into a user-provided handler."""

    def __init__(self, handler: Callable[[Request], Response | Awaitable[Response]]) -> None:
        self._handler = handler

    async def handle_async_request(
        self,
        method: str,
        url: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        request = Request(method, url, headers=headers, params=params)
        result = self._handler(request)
        if inspect.isawaitable(result):  # pragma: no cover - async handlers are rare
            result = await result
        if not isinstance(result, Response):
            raise TypeError("MockTransport handler must return an httpx.Response")
        return result


def _combine_url(url: str, params: Optional[Dict[str, Any]]) -> tuple[str, Dict[str, Any]]:
    parsed = urlparse(url)
    query = {key: value for key, value in parse_qsl(parsed.query, keep_blank_values=True)}
    if params:
        query.update(params)
    rebuilt = parsed._replace(query="&".join(f"{k}={v}" for k, v in query.items()))
    return rebuilt.geturl(), query


class AsyncClient:
    """Very small asynchronous client implementing the ``httpx`` API."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        transport: BaseTransport | None = None,
        timeout: float | Timeout | None = None,
    ) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self._transport = transport
        self.timeout = timeout
        self._closed = False

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True

    close = aclose

    def _prepare_url(self, url: str) -> str:
        if url.startswith("http://") or url.startswith("https://"):
            return url
        if not self.base_url:
            return url
        return urljoin(f"{self.base_url}/", url.lstrip("/"))

    async def request(
        self,
        method: str,
        url: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        if self._closed:
            raise RuntimeError("AsyncClient is closed")
        transport = self._transport
        if transport is None:
            raise RequestError(
                "httpx stub cannot perform network requests without a transport"
            )
        resolved = self._prepare_url(url)
        return await transport.handle_async_request(
            method,
            resolved,
            json=json,
            headers=headers,
            params=params,
        )

    async def get(self, url: str, **kwargs: Any) -> Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Response:
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> Response:
        return await self.request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> Response:
        return await self.request("DELETE", url, **kwargs)


_MISSING = object()


class Client:
    """Synchronous wrapper around :class:`AsyncClient` for the stub."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        transport: BaseTransport | None = None,
        timeout: float | Timeout | None = None,
    ) -> None:
        self._async_client = AsyncClient(
            base_url=base_url, transport=transport, timeout=timeout
        )

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        asyncio.run(self._async_client.aclose())

    def get(self, url: str, **kwargs: Any) -> Response:
        return asyncio.run(self._async_client.get(url, **kwargs))

    def post(self, url: str, **kwargs: Any) -> Response:
        return asyncio.run(self._async_client.post(url, **kwargs))

    def put(self, url: str, **kwargs: Any) -> Response:
        return asyncio.run(self._async_client.put(url, **kwargs))

    def delete(self, url: str, **kwargs: Any) -> Response:
        return asyncio.run(self._async_client.delete(url, **kwargs))
