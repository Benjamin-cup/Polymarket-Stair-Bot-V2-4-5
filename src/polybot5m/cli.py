"""CLI for Polymarket split → monitor → redeem bot."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click
from rich.console import Console

from polybot5m.config import load_config
from polybot5m.constants import INTERVAL_SECONDS
from polybot5m.log_setup import install_run_logging

console = Console()


def _utc_now() -> datetime:
    return datetime.now(UTC)


@click.group()
@click.option("--config", "-c", default="config/default.yaml", help="Config file path")
@click.pass_context
def cli(ctx: click.Context, config: str) -> None:
    """Polymarket split → monitor order book → redeem."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


@cli.command()
@click.option("--dry-run", is_flag=True, help="Simulate without splitting or redeeming on chain")
@click.option("--paper", "paper_trading", is_flag=True, help="Paper: no chain; poll order book only")
@click.option("--cycles", type=int, default=None, help="Max cycles (0 = run forever)")
@click.option("--allocation", type=float, default=None, help="Override portfolio_allocation_usdc")
@click.option("--log-file", type=str, default=None, help="Tee stdout/stderr to this path (empty disables)")
@click.option("--log-append", is_flag=True, help="Append to log file instead of truncating")
@click.option("--log-timestamp-name", is_flag=True, help="Use polybot5m_YYYYMMDD_HHMMSS.log under log path")
@click.pass_context
def run(
    ctx: click.Context,
    dry_run: bool,
    paper_trading: bool,
    cycles: int | None,
    allocation: float | None,
    log_file: str | None,
    log_append: bool,
    log_timestamp_name: bool,
) -> None:
    """Run: split → monitor order book → redeem, repeat."""
    settings = load_config(ctx.obj["config_path"])

    if dry_run:
        settings.bot.dry_run = True
    if paper_trading:
        settings.bot.paper_trading = True
        settings.bot.dry_run = False
    if allocation is not None:
        settings.liquidity_maker.portfolio_allocation_usdc = allocation
    if cycles is not None:
        settings.liquidity_maker.cycles = cycles
    if log_file is not None:
        settings.bot.log_file = log_file
    if log_append:
        settings.bot.log_append = True
    if log_timestamp_name:
        settings.bot.log_timestamp_name = True

    cleanup_log = install_run_logging(
        settings.bot.log_file,
        log_append=settings.bot.log_append,
        log_timestamp_name=settings.bot.log_timestamp_name,
    )
    try:
        _run_with_logging(settings)
    finally:
        cleanup_log()


def _run_with_logging(settings) -> None:
    lm = settings.liquidity_maker
    console.print("[bold green]Polymarket split → monitor → redeem[/bold green]")
    console.print(f"  dry_run={settings.bot.dry_run}")
    console.print(f"  paper_trading={settings.bot.paper_trading}")
    console.print(f"  allocation={lm.portfolio_allocation_usdc} USDC")
    console.print(
        f"  monitor_poll={lm.monitor_poll_interval_s}s log={lm.monitor_log_interval_s}s "
        f"verbose_last={lm.monitor_verbose_seconds_before_end}s "
        f"strike_spot_log={lm.log_strike_spot_interval_s}s "
        f"post_redeem_monitor={lm.post_redeem_monitor_seconds:g}s",
    )
    if settings.bot.log_file.strip():
        console.print(
            f"  log_file={settings.bot.log_file!r} append={settings.bot.log_append} "
            f"timestamp_name={settings.bot.log_timestamp_name}"
        )
    tpj = (lm.trading_process_jsonl or "").strip()
    if tpj:
        console.print(
            f"  trading_process_jsonl={tpj!r} interval_s={lm.trading_process_log_interval_s} "
            f"stdout={lm.trading_process_log_stdout}",
        )
    ss = settings.sell_strategy
    if ss.enabled:
        console.print(
            f"  sell_strategy: window={ss.sell_window_seconds_before_end:g}s "
            f"trigger=[{ss.trigger_high_bid_min},{ss.trigger_high_bid_max}] "
            f"consec>{ss.one_side_bid_threshold}={ss.one_side_consecutive_cycles} "
            f"sum>{ss.sum_bids_above_partial} max_cycle={ss.high_partial_max_cycles} "
            f"high_bid_snap={ss.high_bid_change_decimals}dp "
            f"jsonl={ss.trade_history_jsonl!r}",
        )

    asyncio.run(_run_loop(settings))


async def _run_loop(settings) -> None:
    from polybot5m.engine import run_all_markets
    from polybot5m.execution.clob_client import ClobClient

    lm = settings.liquidity_maker
    clob_client = None
    exe = settings.execution
    ss = settings.sell_strategy
    if (
        ss.enabled
        and not settings.bot.paper_trading
        and not settings.bot.dry_run
        and (exe.private_key or "").strip()
    ):
        try:
            clob_client = ClobClient(
                private_key=exe.private_key,
                api_key=exe.api_key,
                api_secret=exe.api_secret,
                api_passphrase=exe.api_passphrase,
                host=settings.api.clob_url,
                chain_id=exe.chain_id,
                signature_type=exe.signature_type,
                funder=exe.funder or "",
                derive_api_creds=exe.derive_clob_api_creds,
            )
        except Exception as e:
            print(f"  WARNING: CLOB client init failed ({e}); limit sells will be simulated only")

    targets = lm.markets
    if not targets:
        print("ERROR: No markets configured in liquidity_maker.markets")
        return

    print(f"Markets: {[(t.symbol, t.epoch) for t in targets]}")

    export_path: Path | None = None
    if lm.export_dir:
        export_path = Path(lm.export_dir).resolve() / "liquidity_maker_activity.json"

    max_cycles = lm.cycles
    cycle_count = 0

    try:
        while max_cycles == 0 or cycle_count < max_cycles:
            min_interval = min(INTERVAL_SECONDS.get(t.epoch, 300) for t in targets)
            now = _utc_now()
            epoch_ts = (int(now.timestamp()) // min_interval) * min_interval
            epoch_end = datetime.fromtimestamp(epoch_ts + min_interval, tz=UTC)

            print(f"\n{'='*50}")
            print(f"CYCLE {cycle_count + 1}  epoch_end={epoch_end.isoformat()}")
            print(f"{'='*50}")

            summaries = await run_all_markets(
                targets,
                settings,
                export_path,
                clob_client=clob_client,
            )

            for s in summaries:
                symbol = s.get("symbol", "?")
                epoch = s.get("epoch", "?")
                err = s.get("error")
                if err:
                    print(f"  [{symbol}/{epoch}] ERROR: {err}")
                else:
                    split_tx = s.get("split_tx", "")
                    redeem_tx = s.get("redeem_tx", "")
                    if split_tx and redeem_tx:
                        print(
                            f"  [{symbol}/{epoch}] OK  split={str(split_tx)[:16]}...  "
                            f"redeem={str(redeem_tx)[:16]}..."
                        )
                    else:
                        print(f"  [{symbol}/{epoch}] OK")

            cycle_count += 1
            if max_cycles > 0 and cycle_count >= max_cycles:
                break

            while _utc_now() < epoch_end + timedelta(seconds=2):
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("\nInterrupted by user.")
    finally:
        if clob_client is not None:
            await clob_client.close()
        print(f"Shutdown. Cycles completed: {cycle_count}")
