"""Public CLOB REST API — fetch order book without authentication."""

from __future__ import annotations

from typing import Any

from polybot5m.constants import CLOB_API_URL


async def fetch_order_book(token_id: str, base_url: str = CLOB_API_URL) -> Any:
    """
    Fetch order book for a token via public REST API.
    No credentials required.

    Returns object with .bids and .asks (list of {price, size}).
    Compatible with executor._best_bid_from_book.
    """
    import aiohttp

    url = f"{base_url.rstrip('/')}/book"
    params = {"token_id": token_id}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

    class _Book:
        def __init__(self, data: dict) -> None:
            bids_raw = data.get("bids", [])
            asks_raw = data.get("asks", [])
            self.bids = [
                {"price": float(b.get("price", 0)), "size": float(b.get("size", 0))}
                if isinstance(b, dict)
                else b
                for b in bids_raw
            ]
            self.asks = [
                {"price": float(a.get("price", 0)), "size": float(a.get("size", 0))}
                if isinstance(a, dict)
                else a
                for a in asks_raw
            ]

    return _Book(data)
