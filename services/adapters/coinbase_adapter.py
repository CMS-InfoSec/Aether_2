"""Coinbase spot adapter implementation mirroring the Kraken stub interface."""

from __future__ import annotations

from typing import Optional

from .kraken_spot_adapter import KrakenSpotAdapter


class CoinbaseAdapter(KrakenSpotAdapter):
    """Coinbase spot adapter with stubbed trading semantics."""

    def __init__(
        self,
        *,
        rest_url: Optional[str] = None,
        websocket_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            "coinbase",
            rest_url=self.resolve_url(
                rest_url,
                "COINBASE_REST_URL",
                "https://api.exchange.coinbase.com",
            ),
            websocket_url=self.resolve_url(
                websocket_url,
                "COINBASE_WEBSOCKET_URL",
                "wss://ws-feed.exchange.coinbase.com",
            ),
        )


__all__ = ["CoinbaseAdapter"]
