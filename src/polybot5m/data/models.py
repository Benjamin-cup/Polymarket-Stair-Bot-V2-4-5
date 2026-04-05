"""Pydantic models for Gamma events and markets."""

from datetime import datetime

from pydantic import BaseModel


class CryptoMarketMeta(BaseModel):
    asset: str
    interval: str
    slug: str
    expiry: datetime


class Market(BaseModel):
    condition_id: str
    asset_ids: list[str]
    question: str
    outcomes: list[str]
    meta: CryptoMarketMeta


class Event(BaseModel):
    id: str
    slug: str
    title: str
    markets: list[Market]

    def all_asset_ids(self) -> list[str]:
        out: list[str] = []
        for m in self.markets:
            out.extend(m.asset_ids)
        return out

    def condition_id_for_asset(self, asset_id: str) -> str | None:
        """Return the condition_id of the market that contains this asset_id."""
        for m in self.markets:
            if asset_id in m.asset_ids:
                return m.condition_id
        return None
