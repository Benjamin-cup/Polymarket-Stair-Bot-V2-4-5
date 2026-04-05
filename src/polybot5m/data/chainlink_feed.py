"""Chainlink Data Streams — REST for strikes (HMAC auth, V3 benchmark decode)."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time

import aiohttp

from polybot5m.constants import CHAINLINK_PRICE_DECIMALS, CHAINLINK_REST_URL

log = logging.getLogger(__name__)


def _generate_auth_headers(
    method: str,
    path: str,
    body: bytes,
    user_id: str,
    secret: str,
) -> dict[str, str]:
    ts = str(int(time.time() * 1000))
    body_hash = hashlib.sha256(body).hexdigest()
    sig_data = f"{method} {path} {body_hash} {user_id} {ts}"
    signature = hmac.new(secret.encode(), sig_data.encode(), hashlib.sha256).hexdigest()
    return {
        "Authorization": user_id,
        "X-Authorization-Timestamp": ts,
        "X-Authorization-Signature-SHA256": signature,
    }


def _decode_v3_benchmark_price(report_hex: str) -> float | None:
    try:
        raw = bytes.fromhex(report_hex.removeprefix("0x"))
    except ValueError:
        return None

    if len(raw) < 224:
        return None

    try:
        blob_offset = int.from_bytes(raw[96:128], "big")
        blob_len = int.from_bytes(raw[blob_offset : blob_offset + 32], "big")
        blob = raw[blob_offset + 32 : blob_offset + 32 + blob_len]
    except Exception:
        return None

    if len(blob) < 224:
        return None

    bp_int = int.from_bytes(blob[192:224], "big", signed=True)
    return bp_int / CHAINLINK_PRICE_DECIMALS


async def fetch_strikes_at_timestamp(
    user_id: str,
    secret: str,
    feed_ids: dict[str, str],
    epoch_start_unix: int,
    *,
    lead_delay_s: float = 1.0,
) -> dict[str, float]:
    """Fetch benchmark prices from Chainlink REST at epoch_start_unix. Keys = asset (e.g. btc)."""
    if not feed_ids or not user_id or not secret:
        return {}

    if lead_delay_s > 0:
        await asyncio.sleep(lead_delay_s)

    result: dict[str, float] = {}
    async with aiohttp.ClientSession() as session:
        for asset, hex_id in feed_ids.items():
            path = f"/api/v1/reports?feedID={hex_id}&timestamp={epoch_start_unix}"
            url = f"{CHAINLINK_REST_URL}{path}"
            headers = _generate_auth_headers("GET", path, b"", user_id, secret)
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        log.warning("Chainlink strike fetch %s: HTTP %s", asset, resp.status)
                        continue
                    data = await resp.json()
                report = data.get("report", {}) if isinstance(data, dict) else {}
                full_report_hex = report.get("fullReport", "")
                if not full_report_hex and isinstance(data, list) and data:
                    full_report_hex = data[0].get("fullReport", "")
                if full_report_hex:
                    price = _decode_v3_benchmark_price(full_report_hex)
                    if price and price > 0:
                        result[asset] = price
                        log.info("Chainlink strike %s: $%.10f", asset, price)
            except Exception as e:
                log.warning("Chainlink strike fetch %s failed: %s", asset, e)

    return result


async def run_chainlink_spot_loop(
    asset: str,
    feed_id_hex: str,
    user_id: str,
    secret: str,
    product_id: str,
    price_store: dict[str, float],
    stop_event: asyncio.Event,
    poll_interval_s: float,
) -> None:
    """Poll Chainlink reports and mirror spot into price_store[product_id] (same key as Coinbase WS)."""
    a = asset.lower().strip()
    feed_ids = {a: feed_id_hex}
    interval = max(0.2, float(poll_interval_s))
    while not stop_event.is_set():
        ts = int(time.time())
        strikes = await fetch_strikes_at_timestamp(
            user_id,
            secret,
            feed_ids,
            ts,
            lead_delay_s=0.0,
        )
        p = strikes.get(a)
        if p is not None and p > 0:
            price_store[product_id] = float(p)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            break
