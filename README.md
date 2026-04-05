# Polymarket split → monitor → redeem

Splits USDC.e into YES/NO conditional tokens, polls the public order book until the epoch ends (no CLOB sells), then redeems winning shares via the builder-relayer.

## Strategy

```
IDLE → SPLIT → MONITOR → REDEEM → IDLE
```

1. **SPLIT** — Convert USDC.e into YES + NO outcome tokens via CTF `splitPosition`
2. **MONITOR** — Poll YES/NO best bids (no orders placed or cancelled)
3. **REDEEM** — Redeem winning tokens → USDC.e via builder-relayer (gasless)

Runs across multiple markets and intervals in parallel (BTC, ETH, SOL, XRP × 5m, 15m).

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[relayer]"
```

Copy `.env.example` to `.env` and fill in credentials:

```bash
cp .env.example .env
```

Required credentials:
- **Wallet** — `PRIVATE_KEY` (and `FUNDER` if your setup requires it)
- **Builder-Relayer** — `BUILDER_API_KEY`, `BUILDER_API_SECRET`, `BUILDER_API_PASSPHRASE` (split + redeem)

## Configuration

**`config/default.yaml`** — Bot settings, execution credentials, liquidity maker parameters.

Target assets and interval (all symbols share one `epoch`):

```yaml
liquidity_maker:
  epoch: 5m
  symbols: [btc, sol, eth]
```

You can also use a YAML list:

```yaml
  symbols:
    - btc
    - sol
    - eth
```

**Legacy:** if `symbols` is omitted or empty, you can set explicit rows with `markets` (different epoch per row):

```yaml
  markets:
    - { symbol: btc, epoch: 5m }
    - { symbol: eth, epoch: 15m }
```

If `symbols` is non-empty, it overrides and fills `markets` using `epoch`.

Key parameters in `liquidity_maker`:
- `portfolio_allocation_usdc` — USDC.e to split per market/epoch (default: 1000)
- `monitor_poll_interval_s` / `monitor_log_interval_s` — Order book polling and `[MARKET]` log cadence
- `redeem_delay_seconds` — Wait after resolution before redeeming (default: 120)
- `cycles` — Number of cycles to run (0 = forever)

## Usage

```bash
# Run with CLI
polybot5m run

# Dry run (no split/redeem on chain)
polybot5m run --dry-run

# Paper — no chain; polls real order book until epoch end; PNL assumes full YES/NO inventory
polybot5m run --paper

# Custom allocation and cycles
polybot5m run --allocation 500 --cycles 5

# Via script
python scripts/run_liquidity_maker.py run
python scripts/run_liquidity_maker.py run --dry-run --cycles 1
```

### Paper mode

No on-chain split or redeem. The bot polls the **public** order book until the epoch ends, then writes a paper PNL report assuming you hold the full YES and NO split size (no simulated sells).

- Outputs to `exports/paper_summary_{timestamp}.json` and `exports/paper_pnl_{timestamp}.csv`

No wallet or relayer credentials needed for paper mode.

### Standalone Redeem

```bash
python scripts/test_redeem.py --condition-id 0x...
python scripts/test_redeem.py --from-export exports --last 5
```

### Derive Credentials

```bash
python scripts/derive_polymarket_creds.py
```

## Architecture

```
src/polybot5m/
├── cli.py              # Click CLI entry point
├── config.py           # Settings (YAML + env + TOML markets)
├── constants.py        # Contract addresses, intervals
├── engine.py           # Liquidity maker state machine
├── data/
│   ├── gamma.py        # Gamma API (event/market discovery)
│   ├── clob_ws.py      # CLOB WebSocket (orderbook)
│   ├── clob_rest.py    # Public REST API (order book fetch for paper trading)
│   ├── orderbook.py    # In-memory orderbook store
│   ├── slug_builder.py # Epoch slug computation
│   └── models.py       # Pydantic models (Event, Market)
└── execution/
    ├── executor.py     # Order book monitor
    ├── split.py        # CTF splitPosition via relayer
    ├── redeem.py       # CTF redeemPositions via relayer
    ├── paper_exchange.py # Virtual orders for paper trading
    └── paper_report.py  # PNL summary and export (JSON/CSV)
```

## How It Works

For each market (e.g. BTC-5m) per epoch:

1. Compute epoch slug → fetch event from Gamma API → get condition_id + asset_ids
2. Call CTF `splitPosition` via builder-relayer to convert USDC.e → YES tokens + NO tokens
3. Poll the public order book until the epoch ends (no CLOB orders)
4. Redeem winning outcome tokens → USDC.e

At resolution, one side pays $1 per share held.

## Testing

### Dry Run (No Transactions)

```bash
polybot5m run --dry-run --cycles 1
```

Logs dry-run split/monitor/redeem for each market. No on-chain transactions.

### Config Validation

```bash
python3 -c "
from polybot5m.config import load_config
cfg = load_config()
print('allocation:', cfg.liquidity_maker.portfolio_allocation_usdc)
print('markets:', [(m.symbol, m.epoch) for m in cfg.liquidity_maker.markets])
"
```

### Slug + Gamma Fetch

```bash
python3 -c "
import asyncio, aiohttp
from polybot5m.data.gamma import GammaClient
from polybot5m.data.slug_builder import compute_epoch_slugs

async def test():
    slugs = compute_epoch_slugs('btc', '5m')
    async with aiohttp.ClientSession() as s:
        g = GammaClient('https://gamma-api.polymarket.com', s)
        event = await g.fetch_event_by_slug(slugs.current_slug)
        print('title:', event.title)
        print('asset_ids:', len(event.all_asset_ids()))
        print('condition_id:', event.markets[0].condition_id[:32] + '...')
asyncio.run(test())
"
```

### Live Single-Cycle Test

```bash
polybot5m run --cycles 1 --allocation 1
```

Splits $1 USDC.e, monitors the book until resolution, redeems. Check `exports/liquidity_maker_activity.json`.

### Standalone Redeem

```bash
python scripts/test_redeem.py --condition-id 0x...
python scripts/test_redeem.py --from-export exports --last 5
```

### Troubleshooting

- **"Relayer deps missing"** — Run `pip install -e ".[relayer]"`
- **"No markets found"** — Set `liquidity_maker.symbols` or `liquidity_maker.markets` in `config/default.yaml`
- **Rate limit errors** — Add more builder API credentials (`_KEY_1..N` in `.env`)
- **Split fails with approval error** — Ensure `split_approve_first: true` in config
- **Missing credentials** — Set `POLYBOT5M_EXECUTION__*` in `.env` or config

## Logging

Activity is exported to `exports/liquidity_maker_activity.json` with records for splits and redeems.
