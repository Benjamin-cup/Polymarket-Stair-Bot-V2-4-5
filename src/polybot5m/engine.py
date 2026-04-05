"""Split → monitor orderbook → redeem."""

from __future__ import annotations

import asyncio
import enum
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from polybot5m.config import MarketTarget, Settings, StrikeSpotContext
from polybot5m.constants import INTERVAL_SECONDS
from polybot5m.data.slug_builder import compute_epoch_slugs
from polybot5m.time_utils import format_utc_iso_z
from polybot5m.trading_process_log import append_trading_jsonl, resolve_trading_process_path, utc_iso_z


class Phase(enum.Enum):
    IDLE = "IDLE"
    SPLIT = "SPLIT"
    MONITOR = "MONITOR"
    REDEEM = "REDEEM"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _append_export(export_path: Path | None, record: dict) -> None:
    if not export_path:
        return
    try:
        export_path.parent.mkdir(parents=True, exist_ok=True)
        existing: list[dict] = []
        if export_path.exists() and export_path.stat().st_size > 0:
            with open(export_path) as f:
                existing = json.load(f)
        existing.append(record)
        with open(export_path, "w") as f:
            json.dump(existing, f, indent=2)
    except Exception as e:
        print(f"  Export write error: {e}")


def _tag(symbol: str, epoch: str) -> str:
    return f"[{symbol}/{epoch}]"


async def run_market_cycle(
    target: MarketTarget,
    settings: Settings,
    export_path: Path | None = None,
    market_index: int = 0,
    clob_client: Any | None = None,
) -> dict[str, Any]:
    """Run one full cycle: split, monitor order book, redeem."""
    import aiohttp
    import os

    from polybot5m.data.gamma import GammaClient
    from polybot5m.execution.executor import (
        monitor_orderbook_until_epoch_end,
        post_redeem_monitor_orderbooks,
    )
    from polybot5m.execution.paper_report import build_market_summary
    from polybot5m.execution.sell_strategy import build_sell_runner
    from polybot5m.execution.redeem import redeem_positions_batch

    lm = settings.liquidity_maker
    exe = settings.execution
    tag = _tag(target.symbol, target.epoch)
    paper_trading = getattr(settings.bot, "paper_trading", False)

    interval = target.epoch
    interval_secs = INTERVAL_SECONDS.get(interval, 300)

    slugs = compute_epoch_slugs(target.symbol, interval)
    slug = slugs.current_slug
    epoch_start = slugs.current_start
    epoch_end = epoch_start + timedelta(seconds=interval_secs)

    summary: dict[str, Any] = {
        "symbol": target.symbol,
        "epoch": interval,
        "slug": slug,
        "epoch_start": epoch_start.isoformat(),
        "epoch_end": epoch_end.isoformat(),
        "phase": Phase.IDLE.value,
        "split_tx": None,
        "redeem_tx": None,
        "error": None,
        "paper_summary": None,
    }

    poll_interval = getattr(lm, "monitor_poll_interval_s", 0.5)
    log_interval = getattr(lm, "monitor_log_interval_s", 1.0)
    verbose_before = float(getattr(lm, "monitor_verbose_seconds_before_end", 5.0))

    print(f"  {tag} slug={slug}")

    tp_path = resolve_trading_process_path(settings)
    if tp_path:
        append_trading_jsonl(
            tp_path,
            {
                "event": "MARKET_CYCLE_START",
                "ts_utc": utc_iso_z(),
                "tag": tag,
                "symbol": target.symbol,
                "epoch": interval,
                "slug": slug,
                "epoch_end": epoch_end.isoformat(),
                "allocation_usdc": lm.portfolio_allocation_usdc,
                "paper_trading": paper_trading,
                "dry_run": settings.bot.dry_run,
                "sell_strategy_enabled": settings.sell_strategy.enabled,
            },
        )

    async with aiohttp.ClientSession() as session:
        gamma = GammaClient(settings.api.gamma_url, session)
        try:
            event = await gamma.fetch_event_by_slug(slug)
        except ValueError as e:
            summary["error"] = f"No event for slug: {e}"
            print(f"  {tag} ERROR: {summary['error']}")
            return summary

    asset_ids = event.all_asset_ids()
    if len(asset_ids) != 2:
        summary["error"] = f"Expected 2 asset IDs (YES/NO), got {len(asset_ids)}"
        print(f"  {tag} ERROR: {summary['error']}")
        return summary

    market = event.markets[0]
    condition_id = market.condition_id
    yes_token = asset_ids[0]
    no_token = asset_ids[1]

    print(f"  {tag} condition_id={condition_id[:24]}...")

    private_key = exe.private_key
    chain_id = exe.chain_id
    builder_api_key = os.getenv("POLYBOT5M_EXECUTION__BUILDER_API_KEY", "")
    builder_api_secret = os.getenv("POLYBOT5M_EXECUTION__BUILDER_API_SECRET", "")
    builder_api_passphrase = os.getenv("POLYBOT5M_EXECUTION__BUILDER_API_PASSPHRASE", "")

    allocation = lm.portfolio_allocation_usdc
    shares_yes = allocation
    shares_no = allocation

    log_strike_spot_iv = float(getattr(lm, "log_strike_spot_interval_s", 0.0) or 0.0)
    ss_cfg = settings.sell_strategy
    hedge_needs_oracle = bool(
        ss_cfg.enabled and getattr(ss_cfg, "hedge_enabled", False)
    )
    strike_spot_feed: StrikeSpotContext | None = None
    if log_strike_spot_iv > 0 or hedge_needs_oracle:
        pf = settings.price_feed
        strike_spot_feed = StrikeSpotContext(
            symbol=target.symbol.lower(),
            epoch_start_unix=int(epoch_start.timestamp()),
            interval_secs=interval_secs,
            strike_provider=pf.provider,
            chainlink_user_id=pf.chainlink.streams_user_id,
            chainlink_secret=pf.chainlink.streams_secret,
            chainlink_feed_ids=dict(pf.chainlink.feed_ids),
            market_slug=slug,
            spot_provider=pf.spot_provider,
            chainlink_spot_poll_interval_s=float(pf.chainlink_spot_poll_interval_s or 1.0),
        )

    sell_clob = None if paper_trading else clob_client
    tp_interval = float(getattr(lm, "trading_process_log_interval_s", 0.0) or 0.0)
    tp_stdout = bool(getattr(lm, "trading_process_log_stdout", False))
    sell_runner = build_sell_runner(
        settings.sell_strategy,
        tag,
        yes_token,
        no_token,
        allocation,
        allocation,
        sell_clob,
        dry_run=settings.bot.dry_run,
        paper_trading=paper_trading,
        portfolio_allocation_usdc=allocation,
        process_log_path=tp_path if settings.sell_strategy.enabled else None,
        process_log_interval_s=tp_interval,
        process_log_stdout=tp_stdout,
    )

    async def _monitor() -> dict[str, Any]:
        return await monitor_orderbook_until_epoch_end(
            settings.api.clob_url,
            yes_token,
            no_token,
            epoch_end,
            tag=tag,
            poll_interval_s=poll_interval,
            market_log_interval_s=log_interval,
            monitor_verbose_seconds_before_end=verbose_before,
            strike_spot_feed=strike_spot_feed,
            log_strike_spot_interval_s=log_strike_spot_iv,
            run_strike_spot_oracle=hedge_needs_oracle,
            sell_runner=sell_runner,
            trading_process_path=tp_path,
            trading_process_log_interval_s=tp_interval,
            trading_process_log_stdout=tp_stdout,
            inventory_yes=allocation,
            inventory_no=allocation,
        )

    # ── PAPER TRADING PATH ───────────────────────────────────────
    if paper_trading:
        if tp_path:
            append_trading_jsonl(
                tp_path,
                {
                    "event": "PHASE_PAPER",
                    "ts_utc": utc_iso_z(),
                    "tag": tag,
                    "allocation_usdc": allocation,
                    "shares_yes": shares_yes,
                    "shares_no": shares_no,
                },
            )
        print(
            f"  {tag} PAPER: simulated split {allocation} USDC → {shares_yes} YES + {shares_no} NO; "
            "monitor order book only"
        )
        if _utc_now() < epoch_end:
            await _monitor()
        else:
            print(f"  {tag} MONITOR skip: no time left before epoch end")

        print(f"  {tag} RESOLVED (paper)")
        paper_fill_rows = list(sell_runner.paper_fills) if sell_runner is not None else []
        paper_summ = build_market_summary(
            paper_fill_rows,
            [],
            target.symbol,
            interval,
            slug,
            condition_id,
            epoch_end,
            allocation,
            split_yes_shares=shares_yes,
            split_no_shares=shares_no,
        )
        summary["paper_summary"] = paper_summ
        summary["split_tx"] = "paper"
        summary["redeem_tx"] = "paper"
        summary["phase"] = Phase.IDLE.value
        if tp_path:
            append_trading_jsonl(
                tp_path,
                {
                    "event": "MARKET_CYCLE_END",
                    "ts_utc": utc_iso_z(),
                    "tag": tag,
                    "mode": "paper",
                    "error": None,
                },
            )
        return summary

    # ── PHASE 1: SPLIT ───────────────────────────────────────────
    summary["phase"] = Phase.SPLIT.value

    if settings.bot.dry_run:
        print(f"  {tag} SPLIT ${allocation} (dry-run)")
        summary["split_tx"] = "dry_run"
    else:
        from polybot5m.execution.split import split_position

        print(f"  {tag} SPLIT ${allocation}...")
        split_result = await split_position(
            condition_id=condition_id,
            collateral_amount_usdc=allocation,
            private_key=private_key,
            chain_id=chain_id,
            approve_first=lm.split_approve_first,
            cred_index=market_index,
            api_key=builder_api_key or None,
            api_secret=builder_api_secret or None,
            api_passphrase=builder_api_passphrase or None,
        )
        if split_result.get("error"):
            summary["error"] = f"Split failed: {split_result['error']}"
            print(f"  {tag} ❌SPLIT ERROR: {split_result['error']}")
            if tp_path:
                append_trading_jsonl(
                    tp_path,
                    {
                        "event": "MARKET_CYCLE_END",
                        "ts_utc": utc_iso_z(),
                        "tag": tag,
                        "error": summary["error"],
                    },
                )
            return summary
        summary["split_tx"] = split_result.get("tx_hash")
        print(f"  {tag} 🚀SPLIT OK tx={summary['split_tx']}")

    if tp_path:
        append_trading_jsonl(
            tp_path,
            {
                "event": "PHASE_SPLIT_DONE",
                "ts_utc": utc_iso_z(),
                "tag": tag,
                "split_tx": summary.get("split_tx"),
                "amount_usdc": allocation,
                "error": summary.get("error"),
            },
        )

    _append_export(export_path, {
        "ts_utc": format_utc_iso_z(_utc_now()),
        "type": "split",
        "symbol": target.symbol,
        "epoch": interval,
        "slug": slug,
        "condition_id": condition_id,
        "amount_usdc": allocation,
        "tx_hash": summary["split_tx"],
    })

    # ── PHASE 2: MONITOR ─────────────────────────────────────────
    summary["phase"] = Phase.MONITOR.value
    remaining = max(0, (epoch_end - _utc_now()).total_seconds())
    print(f"  {tag} MONITOR {int(remaining)}s until resolution...")
    if tp_path:
        append_trading_jsonl(
            tp_path,
            {
                "event": "PHASE_MONITOR",
                "ts_utc": utc_iso_z(),
                "tag": tag,
                "seconds_until_epoch_end": remaining,
            },
        )

    monitor_ok = not settings.bot.dry_run and _utc_now() < epoch_end
    if monitor_ok:
        await _monitor()
    elif settings.bot.dry_run and _utc_now() < epoch_end:
        await _monitor()
    else:
        skip_reasons: list[str] = []
        if settings.bot.dry_run and _utc_now() >= epoch_end:
            skip_reasons.append("dry_run+no_time")
        elif _utc_now() >= epoch_end:
            skip_reasons.append("no_time_remaining")
        print(f"  {tag} MONITOR skip: {', '.join(skip_reasons) or '—'} — sleeping until epoch end")
        while _utc_now() < epoch_end:
            await asyncio.sleep(5)

    print(f"  {tag} RESOLVED")
    if tp_path:
        append_trading_jsonl(
            tp_path,
            {"event": "PHASE_RESOLVED", "ts_utc": utc_iso_z(), "tag": tag},
        )

    # ── PHASE 3: REDEEM ──────────────────────────────────────────
    summary["phase"] = Phase.REDEEM.value

    redeem_at = epoch_end + timedelta(seconds=lm.redeem_delay_seconds)
    wait_s = max(0, (redeem_at - _utc_now()).total_seconds())
    if wait_s > 0:
        print(f"  {tag} REDEEM waiting {int(wait_s)}s...")
        await asyncio.sleep(wait_s)

    stagger = market_index * lm.stagger_delay_seconds
    if stagger > 0 and not settings.bot.dry_run:
        print(f"  {tag} REDEEM stagger {stagger}s...")
        await asyncio.sleep(stagger)

    if settings.bot.dry_run:
        print(f"  {tag} REDEEM (dry-run)")
        summary["redeem_tx"] = "dry_run"
    else:
        print(f"  {tag} REDEEM {condition_id[:24]}...")
        redeem_result = await redeem_positions_batch(
            condition_ids=[condition_id],
            private_key=private_key,
            rpc_url=exe.rpc_url,
            chain_id=chain_id,
            use_relayer=True,
            cred_index=market_index,
            api_key=builder_api_key or None,
            api_secret=builder_api_secret or None,
            api_passphrase=builder_api_passphrase or None,
        )
        if redeem_result.get("error"):
            summary["error"] = f"Redeem failed: {redeem_result['error']}"
            print(f"  {tag} REDEEM ERROR: {redeem_result['error']}")
        else:
            summary["redeem_tx"] = redeem_result.get("tx_hash")
            print(f"  {tag} REDEEM OK tx={summary['redeem_tx']}")

    if tp_path:
        append_trading_jsonl(
            tp_path,
            {
                "event": "PHASE_REDEEM_DONE",
                "ts_utc": utc_iso_z(),
                "tag": tag,
                "redeem_tx": summary.get("redeem_tx"),
                "error": summary.get("error"),
            },
        )

    ss = settings.sell_strategy
    post_redeem_s = float(getattr(lm, "post_redeem_monitor_seconds", 0.0) or 0.0)
    if ss.enabled:
        post_redeem_s = max(post_redeem_s, float(ss.post_redeem_monitor_seconds or 0.0))
    if ss.post_redeem_log_chain_balance and post_redeem_s < 0.1:
        post_redeem_s = max(post_redeem_s, 30.0)
    if post_redeem_s > 0:
        from eth_account import Account

        if tp_path:
            append_trading_jsonl(
                tp_path,
                {
                    "event": "PHASE_POST_REDEEM",
                    "ts_utc": utc_iso_z(),
                    "tag": tag,
                    "duration_s": post_redeem_s,
                    "log_chain_balance": bool(ss.post_redeem_log_chain_balance),
                },
            )
        wallet_addr = (exe.funder or "").strip()
        if not wallet_addr and exe.private_key:
            wallet_addr = Account.from_key(exe.private_key).address
        await post_redeem_monitor_orderbooks(
            settings.api.clob_url,
            yes_token,
            no_token,
            post_redeem_s,
            tag,
            poll_interval,
            rpc_url=exe.rpc_url,
            wallet_address=wallet_addr or None,
            log_chain_balance=bool(ss.post_redeem_log_chain_balance),
        )

    _append_export(export_path, {
        "ts_utc": format_utc_iso_z(_utc_now()),
        "type": "redeem",
        "symbol": target.symbol,
        "epoch": interval,
        "slug": slug,
        "condition_id": condition_id,
        "tx_hash": summary.get("redeem_tx"),
        "error": summary.get("error"),
    })

    if tp_path:
        append_trading_jsonl(
            tp_path,
            {
                "event": "MARKET_CYCLE_END",
                "ts_utc": utc_iso_z(),
                "tag": tag,
                "error": summary.get("error"),
            },
        )

    summary["phase"] = Phase.IDLE.value
    return summary


async def run_all_markets(
    targets: list[MarketTarget],
    settings: Settings,
    export_path: Path | None = None,
    clob_client: Any | None = None,
) -> list[dict[str, Any]]:
    """Run cycles for all target markets with staggered starts."""
    stagger = settings.liquidity_maker.stagger_delay_seconds
    export_dir = settings.liquidity_maker.export_dir if settings.liquidity_maker.export_dir else None
    paper_trading = getattr(settings.bot, "paper_trading", False)

    async def _run_with_delay(i: int, target: MarketTarget) -> dict[str, Any]:
        if i > 0 and not paper_trading:
            delay = i * stagger
            print(f"  [{target.symbol}/{target.epoch}] waiting {delay}s before split...")
            await asyncio.sleep(delay)
        return await run_market_cycle(
            target,
            settings,
            export_path=export_path,
            market_index=i,
            clob_client=clob_client,
        )

    tasks = [_run_with_delay(i, t) for i, t in enumerate(targets)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    summaries: list[dict[str, Any]] = []
    for i, r in enumerate(results):
        if isinstance(r, BaseException):
            print(f"  [{targets[i].symbol}/{targets[i].epoch}] FATAL: {r}")
            summaries.append({
                "symbol": targets[i].symbol,
                "epoch": targets[i].epoch,
                "error": str(r),
            })
        else:
            summaries.append(r)

    if paper_trading and export_dir:
        from polybot5m.execution.paper_report import write_paper_report

        paper_summaries = [s["paper_summary"] for s in summaries if s.get("paper_summary")]
        if paper_summaries:
            json_path, csv_path = write_paper_report(paper_summaries, export_dir, _utc_now())
            if json_path:
                print(f"\n  Paper report: {json_path}")
            if csv_path:
                print(f"  Paper CSV: {csv_path}")

    return summaries
