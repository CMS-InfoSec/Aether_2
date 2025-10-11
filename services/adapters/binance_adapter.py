"""Binance spot adapter implementation mirroring the Kraken stub interface."""

from __future__ import annotations

from typing import Optional

from .kraken_spot_adapter import KrakenSpotAdapter


class BinanceAdapter(KrakenSpotAdapter):
    """Binance spot adapter with stubbed trading semantics."""

    def __init__(
        self,
        *,
        rest_url: Optional[str] = None,
        websocket_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            "binance",
            rest_url=self.resolve_url(rest_url, "BINANCE_REST_URL", "https://api.binance.com"),
            websocket_url=self.resolve_url(
                websocket_url,
                "BINANCE_WEBSOCKET_URL",
                "wss://stream.binance.com:9443/ws",
            ),
        )


__all__ = ["BinanceAdapter"]
