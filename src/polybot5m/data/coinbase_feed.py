"""Coinbase — WebSocket ticker for spot; REST candles for epoch open (strike)."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

import aiohttp
import websockets

from polybot5m.constants import COINBASE_REST_URL, COINBASE_WS_URL

log = logging.getLogger(__name__)

READ_TIMEOUT = 10.0
PING_INTERVAL = 10.0
RECONNECT_BASE = 3.0
RECONNECT_MAX = 30.0


def asset_to_product_id(asset: str) -> str:
    return f"{asset.upper()}-USD"


async def run_coinbase_ws(
    product_ids: list[str],
    price_store: dict[str, float],
    stop_event: asyncio.Event,
    price_notify: asyncio.Event | None = None,
) -> None:
    """Subscribe to Coinbase ticker channel and update price_store continuously."""
    reconnect_delay = RECONNECT_BASE

    while not stop_event.is_set():
        ws = None
        try:
            ws = await websockets.connect(
                COINBASE_WS_URL,
                ping_interval=PING_INTERVAL,
                ping_timeout=READ_TIMEOUT,
                close_timeout=5,
            )
            sub = json.dumps({
                "type": "subscribe",
                "channels": [{"name": "ticker", "product_ids": product_ids}],
            })
            await ws.send(sub)
            log.info("Coinbase WS connected: %s", product_ids)
            reconnect_delay = RECONNECT_BASE

            async for raw in ws:
                if stop_event.is_set():
                    break
                try:
                    data = json.loads(raw) if isinstance(raw, str) else raw
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(data, dict):
                    continue
                if data.get("type") == "ticker":
                    pid = data.get("product_id", "")
                    price_s = data.get("price", "")
                    if pid and price_s:
                        try:
                            price_store[pid] = float(price_s)
                            if price_notify is not None:
                                price_notify.set()
                        except (ValueError, TypeError):
                            pass

        except asyncio.CancelledError:
            break
        except (websockets.ConnectionClosed, OSError, Exception) as e:
            if stop_event.is_set():
                break
            log.warning("Coinbase WS error: %s, reconnecting in %ss", e, reconnect_delay)
        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass

        if stop_event.is_set():
            break
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX)

    log.info("Coinbase WS stopped")


async def fetch_candle_open(
    product_id: str,
    epoch_unix: int,
    granularity: int = 300,
) -> float | None:
    """Open price of the candle that starts at epoch_unix."""
    start = datetime.fromtimestamp(epoch_unix, tz=UTC)
    end = start + timedelta(seconds=granularity)
    url = f"{COINBASE_REST_URL}/products/{product_id}/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": str(granularity),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                body = await resp.json()
        if isinstance(body, list) and body:
            candle = body[0]
            if isinstance(candle, list) and len(candle) > 3:
                return float(candle[3])
    except Exception as e:
        log.warning("Coinbase candle fetch failed for %s@%s: %s", product_id, epoch_unix, e)
    return None
