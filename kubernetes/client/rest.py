"""REST layer compatibility shim for the lightweight Kubernetes client."""

from __future__ import annotations

__all__ = ["ApiException"]


class ApiException(Exception):
    """Exception type raised by the real Kubernetes client."""

    def __init__(self, status: int = 500, reason: str | None = None) -> None:
        super().__init__(reason or "Kubernetes API error")
        self.status = status
        self.reason = reason
