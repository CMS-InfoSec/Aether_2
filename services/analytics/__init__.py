"""Analytics service package."""

from .orderflow_service import router as orderflow_router

__all__ = ["orderflow_router"]
