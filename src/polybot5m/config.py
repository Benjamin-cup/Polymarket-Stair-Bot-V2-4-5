"""Configuration — YAML + env (POLYBOT5M_ prefix)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings

from dotenv import load_dotenv


class BotConfig(BaseModel):
    dry_run: bool = True
    paper_trading: bool = False  # Simulated: no chain split; monitor order book only
    log_level: str = "INFO"
    log_file: str = ""
    log_append: bool = False
    log_timestamp_name: bool = False


class ApiConfig(BaseModel):
    gamma_url: str = "https://gamma-api.polymarket.com"
    clob_url: str = "https://clob.polymarket.com"
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com"


class ExecutionConfig(BaseModel):
    """Builder-relayer (split/redeem) + optional CLOB API for limit sells."""
    enabled: bool = False
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    private_key: str = ""
    funder: str = ""
    chain_id: int = 137
    signature_type: int = 2
    rpc_url: str = "https://rpc.ankr.com/polygon/b7025907c7c47329edc930ec748839dd7d71e4d5ce738db39aa03eec87bdd3f2"
    # When true, derive L2 API creds from private key (requires network). Set false and set API_* if offline.
    derive_clob_api_creds: bool = True


class SellStrategyConfig(BaseModel):
    """Limit sells in the last sell_window_seconds_before_end before epoch end; JSONL trade log."""

    enabled: bool = False
    sell_window_seconds_before_end: float = 120.0
    sell_order_type: str = "GTC"
    trigger_high_bid_min: float = 0.91
    trigger_high_bid_max: float = 0.96
    one_side_bid_threshold: float = 0.90
    one_side_consecutive_cycles: int = 6
    # Staged high-leg: require pair_sum >= this (pair_sum = low_sell_price + current high bid).
    sum_bids_above_partial: float = 1.01
    high_partial_max_cycles: int = 10
    # Round held high bid to this many decimals to detect "change" (2 = cent ticks 0.90..0.99).
    high_bid_change_decimals: int = 2
    high_bid_full_sell_threshold: float = 0.98
    final_seconds_before_end: float = 5.0
    final_unwind_if_high_below: float = 0.98
    min_sell_shares: float = 0.01
    trade_history_jsonl: str = "exports/sell_trade_history.jsonl"
    # If >0, combined with liquidity_maker.post_redeem_monitor_seconds via max() when sell_strategy.enabled.
    post_redeem_monitor_seconds: float = 0.0
    post_redeem_log_chain_balance: bool = False
    # Hedge: after LOW_FULL, optional buy sold-out leg (multiplier × allocation) + sell opposite at book; needs strike vs spot.
    hedge_enabled: bool = False
    hedge_opposite_bid_below: float = 0.5
    hedge_sold_side_bid_above: float = 0.5
    hedge_target_vs_spot_cycles: int = 2
    hedge_buy_size_multiplier: float = 1.5
    hedge_order_type: str = "GTC"
    # Live CLOB: retry create+post on errors / bad API response; refresh limit from last book poll between tries.
    order_submit_max_retries: int = 10
    order_submit_retry_delay_s: float = 0.4
    order_refresh_price_on_retry: bool = True


class MarketTarget(BaseModel):
    """A single market+epoch to run."""
    symbol: str
    epoch: str


class ChainlinkConfig(BaseModel):
    streams_user_id: str = ""
    streams_secret: str = ""
    feed_ids: dict[str, str] = {}


class PriceFeedConfig(BaseModel):
    """Strike: chainlink = Data Streams API (Polymarket resolution); polymarket = UI scrape; coinbase = candle open."""
    provider: str = "chainlink"
    chainlink: ChainlinkConfig = ChainlinkConfig()
    # Spot for [STRIKE_SPOT] logging: coinbase = WebSocket ticker; chainlink = poll Data Streams (same feed_ids).
    spot_provider: str = "coinbase"
    chainlink_spot_poll_interval_s: float = 1.0


@dataclass(frozen=True)
class StrikeSpotContext:
    """Strike + spot feeds for optional [STRIKE_SPOT] logging during monitor."""

    symbol: str
    epoch_start_unix: int
    interval_secs: int
    strike_provider: str
    chainlink_user_id: str
    chainlink_secret: str
    chainlink_feed_ids: dict[str, str]
    market_slug: str = ""
    spot_provider: str = "coinbase"
    chainlink_spot_poll_interval_s: float = 1.0


class BacktestRiskConfig(BaseModel):
    """Optional stress on execution-time books (near resolution)."""

    spread_widen_seconds_before_end: float = 30.0
    # Effective bid shrink: multiply top-of-book sizes by this factor when inside window (simulates widening).
    spread_widen_depth_mult: float = 0.55
    liquidity_eviction_probability: float = 0.05
    # Fraction of top-level size removed when eviction fires.
    eviction_size_fraction: float = 0.35
    # Rare: no bid liquidity at execution (failed exit attempt).
    no_exit_liquidity_probability: float = 0.02


class BacktestSimulationConfig(BaseModel):
    """Realistic execution simulation for historical replay (nearest to live trading)."""

    enabled: bool = False
    latency_ms_min: float = 50.0
    latency_ms_max: float = 500.0
    # Taker-style fee on notional proceeds (Polymarket varies; 0 = off).
    fee_bps: float = 0.0
    # Bernoulli: limit order reaches matching engine and fills (rest = no fill this attempt).
    limit_order_fill_probability: float = 0.92
    # If True, treat each sell as aggressive (walk book); if False, still walk book but limit fill prob applies.
    market_style_sell: bool = True
    random_seed: int | None = 42
    risk: BacktestRiskConfig = BacktestRiskConfig()


class BacktestRootConfig(BaseModel):
    """Nested under `backtest:` in YAML."""

    simulation: BacktestSimulationConfig = BacktestSimulationConfig()
    # Hedge in replay: use bid/ask gates only (no strike vs chain spot cycles). Live trading ignores this.
    hedge_without_chain_compare: bool = True
    # Tee backtest stdout/stderr to file (same behavior as bot.log_file; empty = off).
    log_file: str = ""
    log_append: bool = False
    log_timestamp_name: bool = True


class LiquidityMakerConfig(BaseModel):
    """Split → monitor order book → redeem."""

    portfolio_allocation_usdc: float = 1000.0
    # If true (live / dry-run): read proxy USDC.e balance via RPC before split; allocation = balance × fraction.
    portfolio_allocation_from_balance: bool = False
    portfolio_allocation_balance_fraction: float = 0.30
    portfolio_allocation_min_usdc: float = 1.0
    portfolio_allocation_max_usdc: float | None = None
    split_approve_first: bool = True
    redeem_delay_seconds: int = 120
    stagger_delay_seconds: int = 5
    export_dir: str = "exports"
    cycles: int = 0
    monitor_poll_interval_s: float = 0.2
    monitor_log_interval_s: float = 1.0
    # In the last N seconds before epoch end, log every poll ([MARKET_TICK]).
    monitor_verbose_seconds_before_end: float = 5.0
    # Log market target (strike) and spot every N seconds (0 = off). Implies spot feed + strike fetch for this market.
    log_strike_spot_interval_s: float = 0.0
    # After redeem, poll public order books for this many seconds (0 = off).
    post_redeem_monitor_seconds: float = 0.0
    # NDJSON log of phases + every monitor tick (bids, inventory, sell state). Empty = off.
    trading_process_jsonl: str = ""
    # 0 = log every poll; >0 = min seconds between MONITOR_TICK lines (reduces file size).
    trading_process_log_interval_s: float = 0.0
    # Echo compact [TRADING_PROCESS] lines to stdout when trading_process_jsonl is set.
    trading_process_log_stdout: bool = False
    epoch: str = "5m"
    symbols: list[str] = []
    markets: list[MarketTarget] = []

    @model_validator(mode="after")
    def _symbols_to_markets(self) -> LiquidityMakerConfig:
        if self.symbols:
            self.markets = [
                MarketTarget(symbol=str(s).strip().lower(), epoch=self.epoch)
                for s in self.symbols
                if str(s).strip()
            ]
        return self


class Settings(BaseSettings):
    bot: BotConfig = BotConfig()
    api: ApiConfig = ApiConfig()
    execution: ExecutionConfig = ExecutionConfig()
    liquidity_maker: LiquidityMakerConfig = LiquidityMakerConfig()
    price_feed: PriceFeedConfig = PriceFeedConfig()
    sell_strategy: SellStrategyConfig = SellStrategyConfig()
    backtest: BacktestRootConfig = BacktestRootConfig()

    model_config = {"env_prefix": "POLYBOT5M_", "env_nested_delimiter": "__"}


def load_config(path: str = "config/default.yaml") -> Settings:
    config_path = Path(path).resolve()
    for base in (config_path.parent.parent, config_path.parent, Path.cwd()):
        env_file = base / ".env"
        if env_file.is_file():
            load_dotenv(dotenv_path=str(env_file), override=True)
            break
    load_dotenv(override=True)
    data: dict = {}
    if config_path.exists():
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
    return Settings(**data)
