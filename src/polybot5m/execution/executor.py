"""Poll YES/NO order books until epoch end."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from polybot5m.config import StrikeSpotContext
from polybot5m.trading_process_log import append_trading_jsonl, utc_iso_z

if TYPE_CHECKING:
    from polybot5m.execution.sell_strategy import SellStrategyRunner
from polybot5m.data.chainlink_feed import run_chainlink_spot_loop
from polybot5m.data.coinbase_feed import run_coinbase_ws
from polybot5m.data.strike_price import fetch_epoch_strike


def _best_bid_from_book(book: Any) -> float:
    """Extract best (highest) bid price from order book. Returns 0.0 if empty."""
    if not book:
        return 0.0
    bids = getattr(book, "bids", None)
    if not bids or len(bids) == 0:
        return 0.0
    prices: list[float] = []
    for b in bids:
        p = getattr(b, "price", None) or (b.get("price") if isinstance(b, dict) else None)
        if p is not None and 0 < float(p) <= 1:
            prices.append(float(p))
    return max(prices) if prices else 0.0


def _best_ask_from_book(book: Any) -> float:
    """Extract best (lowest) ask price from order book. Returns 0.0 if empty."""
    if not book:
        return 0.0
    asks = getattr(book, "asks", None)
    if not asks or len(asks) == 0:
        return 0.0
    prices: list[float] = []
    for a in asks:
        p = getattr(a, "price", None) or (a.get("price") if isinstance(a, dict) else None)
        if p is not None and 0 < float(p) <= 1:
            prices.append(float(p))
    return min(prices) if prices else 0.0


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _rem_inventory_suffix(
    sell_runner: Any,
    inventory_yes: float | None,
    inventory_no: float | None,
) -> str:
    """Format remaining YES/NO shares for monitor logs (after sell tick if runner present)."""
    if sell_runner is not None:
        return f" rem_YES={sell_runner.rem_yes:.4f} rem_NO={sell_runner.rem_no:.4f}"
    if inventory_yes is not None and inventory_no is not None:
        return f" rem_YES={inventory_yes:.4f} rem_NO={inventory_no:.4f}"
    return ""


def _chainlink_feed_id_for_symbol(feed_ids: dict[str, str], symbol: str) -> str | None:
    k = symbol.lower().strip()
    v = feed_ids.get(k)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _erc1155_balances(rpc_url: str, wallet: str, yes_id: str, no_id: str) -> tuple[int, int] | None:
    from web3 import Web3

    from polybot5m.constants import CTF_ADDRESS

    abi = [
        {
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "id", "type": "uint256"},
            ],
            "name": "balanceOf",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        }
    ]
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not w3.is_connected():
            return None
        c = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=abi)
        owner = Web3.to_checksum_address(wallet)
        y = int(yes_id)
        n = int(no_id)
        by = c.functions.balanceOf(owner, y).call()
        bn = c.functions.balanceOf(owner, n).call()
        return int(by), int(bn)
    except Exception:
        return None


async def post_redeem_monitor_orderbooks(
    clob_base_url: str,
    yes_token_id: str,
    no_token_id: str,
    duration_s: float,
    tag: str,
    poll_interval_s: float,
    *,
    rpc_url: str | None = None,
    wallet_address: str | None = None,
    log_chain_balance: bool = False,
) -> None:
    """After redeem: poll public books (and optionally CTF balances) for `duration_s` seconds."""
    if duration_s <= 0:
        return
    from polybot5m.data.clob_rest import fetch_order_book

    end = time.monotonic() + duration_s
    extra = " + chain" if log_chain_balance and wallet_address and rpc_url else ""
    print(f"  {tag} POST_REDEEM monitor {duration_s:g}s{extra}")
    while time.monotonic() < end:
        try:
            book_yes, book_no = await asyncio.gather(
                fetch_order_book(yes_token_id, clob_base_url),
                fetch_order_book(no_token_id, clob_base_url),
            )
            by = _best_bid_from_book(book_yes)
            bn = _best_bid_from_book(book_no)
            line = f"  {tag} [POST_REDEEM] YES bid={by:.4f} NO bid={bn:.4f} sum={by + bn:.4f}"
            if log_chain_balance and wallet_address and rpc_url:
                bal = await asyncio.to_thread(
                    _erc1155_balances, rpc_url, wallet_address, yes_token_id, no_token_id
                )
                if bal is not None:
                    line += f" | chain YES={bal[0]} NO={bal[1]}"
                else:
                    line += " | chain=error"
            print(line)
        except Exception as e:
            print(f"  {tag} [POST_REDEEM] book error: {e}")
        await asyncio.sleep(poll_interval_s)


async def monitor_orderbook_until_epoch_end(
    clob_base_url: str,
    yes_token_id: str,
    no_token_id: str,
    epoch_end: datetime,
    tag: str = "",
    poll_interval_s: float = 0.5,
    market_log_interval_s: float = 1.0,
    monitor_verbose_seconds_before_end: float = 5.0,
    *,
    strike_spot_feed: StrikeSpotContext | None = None,
    log_strike_spot_interval_s: float = 0.0,
    run_strike_spot_oracle: bool = False,
    sell_runner: SellStrategyRunner | None = None,
    trading_process_path: Path | None = None,
    trading_process_log_interval_s: float = 0.0,
    trading_process_log_stdout: bool = False,
    inventory_yes: float | None = None,
    inventory_no: float | None = None,
) -> dict[str, Any]:
    """
    Poll YES/NO order books until `epoch_end` (UTC).
    """
    from polybot5m.data.clob_rest import fetch_order_book

    last_market_log = 0.0
    out: dict[str, Any] = {}

    og_line = ""
    if strike_spot_feed is not None:
        og_line = (
            f"; strike_feed={strike_spot_feed.strike_provider}+spot={strike_spot_feed.spot_provider} "
            f"({strike_spot_feed.symbol.upper()}-USD)"
        )
    print(f"  {tag} MONITOR (orderbook{og_line}) until epoch end")

    last_tp_mono = 0.0

    stop_oracle: asyncio.Event | None = None
    oracle_task: asyncio.Task | None = None
    price_store: dict[str, float] = {}
    strike = 0.0
    product_id: str | None = None
    last_strike_spot_log = 0.0
    spot_log_key = "spot"
    need_oracle = strike_spot_feed is not None and (
        log_strike_spot_interval_s > 0 or run_strike_spot_oracle
    )
    if need_oracle:
        stop_oracle = asyncio.Event()
        product_id = f"{strike_spot_feed.symbol.upper()}-USD"
        sp_cfg = strike_spot_feed.spot_provider.lower().strip()
        fid = _chainlink_feed_id_for_symbol(
            dict(strike_spot_feed.chainlink_feed_ids),
            strike_spot_feed.symbol,
        )
        use_chainlink_spot = (
            sp_cfg == "chainlink"
            and bool(strike_spot_feed.chainlink_user_id)
            and bool(strike_spot_feed.chainlink_secret)
            and fid is not None
        )
        if use_chainlink_spot:
            spot_log_key = "chainlink_spot"
            oracle_task = asyncio.create_task(
                run_chainlink_spot_loop(
                    strike_spot_feed.symbol,
                    fid,
                    strike_spot_feed.chainlink_user_id,
                    strike_spot_feed.chainlink_secret,
                    product_id,
                    price_store,
                    stop_oracle,
                    strike_spot_feed.chainlink_spot_poll_interval_s,
                ),
            )
        else:
            if sp_cfg == "chainlink":
                print(
                    f"  {tag} spot: chainlink requested but missing user/secret or feed_id for "
                    f"{strike_spot_feed.symbol} — falling back to Coinbase WebSocket",
                )
            spot_log_key = "coinbase_spot"
            oracle_task = asyncio.create_task(
                run_coinbase_ws([product_id], price_store, stop_oracle, None),
            )
        for _ in range(50):
            if price_store.get(product_id):
                break
            await asyncio.sleep(0.1)
        strike = await fetch_epoch_strike(
            strike_spot_feed.symbol.lower(),
            strike_spot_feed.epoch_start_unix,
            price_store,
            strike_spot_feed.strike_provider,
            strike_spot_feed.interval_secs,
            strike_spot_feed.chainlink_user_id,
            strike_spot_feed.chainlink_secret,
            dict(strike_spot_feed.chainlink_feed_ids),
            market_slug=strike_spot_feed.market_slug or "",
        )
        spot0 = price_store.get(product_id)
        sk = f"{strike:g}" if strike and strike > 0 else "—"
        sp = f"{spot0:g}" if spot0 and spot0 > 0 else "—"
        print(
            f"  {tag} strike_spot init target={sk} {spot_log_key}={sp} provider={strike_spot_feed.strike_provider}",
        )

    try:
        while _utc_now() < epoch_end:
            remaining_s = max(0.0, (epoch_end - _utc_now()).total_seconds())
            now_mono = time.monotonic()

            if strike_spot_feed is not None and product_id is not None and log_strike_spot_interval_s > 0:
                if now_mono - last_strike_spot_log >= log_strike_spot_interval_s:
                    spot_cur = price_store.get(product_id)
                    ts = f"{strike:g}" if strike and strike > 0 else "—"
                    ss = f"{spot_cur:g}" if spot_cur and spot_cur > 0 else "—"
                    print(f"  {tag} [STRIKE_SPOT] target={ts} {spot_log_key}={ss}")
                    last_strike_spot_log = now_mono

            try:
                book_yes, book_no = await asyncio.gather(
                    fetch_order_book(yes_token_id, clob_base_url),
                    fetch_order_book(no_token_id, clob_base_url),
                )
                best_bid_yes = _best_bid_from_book(book_yes)
                best_bid_no = _best_bid_from_book(book_no)
                best_ask_yes = _best_ask_from_book(book_yes)
                best_ask_no = _best_ask_from_book(book_no)
            except Exception as e:
                print(f"  {tag} [MONITOR] book fetch error: {e}")
                await asyncio.sleep(poll_interval_s)
                continue

            spot_this_poll: float | None = None
            if strike_spot_feed is not None and product_id is not None:
                spot_this_poll = price_store.get(product_id)

            if sell_runner is not None:
                sk = strike if strike and strike > 0 else None
                sp = spot_this_poll if spot_this_poll is not None and spot_this_poll > 0 else None
                await sell_runner.on_tick(
                    best_bid_yes,
                    best_bid_no,
                    remaining_s,
                    strike=sk,
                    spot=sp,
                    ask_yes=best_ask_yes if best_ask_yes > 0 else None,
                    ask_no=best_ask_no if best_ask_no > 0 else None,
                )
            elif trading_process_path is not None:
                now_tp = time.monotonic()
                if trading_process_log_interval_s <= 0 or (
                    now_tp - last_tp_mono >= trading_process_log_interval_s
                ):
                    last_tp_mono = now_tp
                    append_trading_jsonl(
                        trading_process_path,
                        {
                            "event": "MONITOR_TICK",
                            "ts_utc": utc_iso_z(),
                            "tag": tag,
                            "bid_yes": best_bid_yes,
                            "bid_no": best_bid_no,
                            "sum_bids": best_bid_yes + best_bid_no,
                            "t_minus_s": remaining_s,
                            "rem_yes": inventory_yes,
                            "rem_no": inventory_no,
                            "sell_strategy_enabled": False,
                        },
                    )
                    if trading_process_log_stdout:
                        print(
                            f"  {tag} [TRADING_PROCESS] t={remaining_s:.1f}s YES={best_bid_yes:.4f} "
                            f"NO={best_bid_no:.4f} sum={best_bid_yes + best_bid_no:.4f}",
                        )

            rem_suffix = _rem_inventory_suffix(sell_runner, inventory_yes, inventory_no)

            in_verbose = (
                monitor_verbose_seconds_before_end > 0
                and 0 < remaining_s <= monitor_verbose_seconds_before_end
            )
            if in_verbose:
                tick_extra = ""
                if strike_spot_feed is not None and product_id is not None:
                    ts = f"{strike:g}" if strike and strike > 0 else "—"
                    ss = (
                        f"{spot_this_poll:g}"
                        if spot_this_poll is not None and spot_this_poll > 0
                        else "—"
                    )
                    tick_extra = f" strike={ts} spot={ss}"
                print(
                    f"  {tag} [MARKET_TICK] 🟢YES best_bid={best_bid_yes:.4f} "
                    f"🔴NO best_bid={best_bid_no:.4f} ⏰t_minus={remaining_s:.2f}s{tick_extra}"
                    f"{rem_suffix}"
                )
            elif now_mono - last_market_log >= market_log_interval_s:
                print(
                    f"  {tag} [MARKET] 🟢YES best_bid={best_bid_yes:.4f} 🔴NO best_bid={best_bid_no:.4f} "
                    f"⏰t_minus={remaining_s:.1f}s{rem_suffix}"
                )
                last_market_log = now_mono
            elif rem_suffix:
                print(
                    f"  {tag} [REM]{rem_suffix} ⏰t_minus={remaining_s:.2f}s",
                )

            await asyncio.sleep(poll_interval_s)

    finally:
        if stop_oracle is not None:
            stop_oracle.set()
        if oracle_task is not None:
            try:
                await asyncio.wait_for(oracle_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                oracle_task.cancel()

    print(f"  {tag} [MONITOR_END] epoch end reached")
    if trading_process_path is not None:
        append_trading_jsonl(
            trading_process_path,
            {"event": "MONITOR_END", "ts_utc": utc_iso_z(), "tag": tag},
        )
    return out
