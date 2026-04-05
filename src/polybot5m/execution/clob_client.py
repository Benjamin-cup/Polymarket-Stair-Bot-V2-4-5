"""Thin async wrapper around py_clob_client.client.ClobClient for order signing and submission."""

from __future__ import annotations

import asyncio
from typing import Any

from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from polybot5m.constants import CLOB_API_URL, CHAIN_ID


def _order_type_from_str(s: str) -> OrderType:
    u = (s or "GTC").upper()
    if u == "FOK":
        return OrderType.FOK
    if u == "FAK":
        return OrderType.FAK
    if u == "GTD":
        return OrderType.GTD
    return OrderType.GTC


class ClobClient:
    """Async-friendly wrapper around py_clob_client.ClobClient."""

    def __init__(
        self,
        private_key: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        host: str = CLOB_API_URL,
        chain_id: int = CHAIN_ID,
        signature_type: int = 2,
        funder: str = "",
        derive_api_creds: bool = True,
    ) -> None:
        self._client = PyClobClient(
            host=host.rstrip("/") if host else CLOB_API_URL,
            chain_id=chain_id,
            key=private_key,
            creds=None,
            signature_type=signature_type,
            funder=funder or None,
        )
        if derive_api_creds:
            try:
                derived = self._client.create_or_derive_api_creds()
            except Exception as e:
                raise RuntimeError(
                    "CLOB API credential derivation failed (check PRIVATE_KEY and network). "
                    f"Original error: {e}"
                ) from e
            if derived is None:
                raise RuntimeError(
                    "CLOB create_or_derive_api_creds returned None; set derive_clob_api_creds: false "
                    "and valid POLYBOT5M_EXECUTION__API_* in .env"
                )
            self._client.set_api_creds(derived)
        else:
            if not (api_key and api_secret and api_passphrase):
                raise ValueError(
                    "derive_clob_api_creds is false but api_key/api_secret/api_passphrase are missing"
                )
            self._client.set_api_creds(
                ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
            )

    async def close(self) -> None:
        pass

    async def get_neg_risk(self, token_id: str) -> bool:
        return await asyncio.to_thread(self._client.get_neg_risk, token_id)

    async def get_fee_rate_bps(self, token_id: str) -> int:
        return await asyncio.to_thread(self._client.get_fee_rate_bps, token_id)

    async def get_tick_size(self, token_id: str) -> str:
        return await asyncio.to_thread(self._client.get_tick_size, token_id)

    async def get_order_book(self, token_id: str) -> Any:
        return await asyncio.to_thread(self._client.get_order_book, token_id)

    def create_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        neg_risk: bool = False,
        fee_rate_bps: int = 0,
        tick_size: str | None = None,
        expiration: str | None = None,
    ) -> Any:
        exp_int = 0
        if expiration is not None and str(expiration).strip() not in ("", "0"):
            exp_int = int(expiration)
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY if side.upper() == "BUY" else SELL,
            expiration=exp_int,
        )
        from py_clob_client.clob_types import PartialCreateOrderOptions

        options = PartialCreateOrderOptions(
            tick_size=tick_size if tick_size else None,
            neg_risk=neg_risk if neg_risk else None,
        )
        return self._client.create_order(order_args, options)

    async def post_order(
        self,
        signed_order: Any,
        order_type: str = "FOK",
    ) -> dict[str, Any]:
        ot = _order_type_from_str(order_type)
        return await asyncio.to_thread(
            self._client.post_order,
            signed_order,
            ot,
            False,
        )
