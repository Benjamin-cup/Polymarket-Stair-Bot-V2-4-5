"""Constants for Polymarket Liquidity Maker Bot."""

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com"
WS_MSG_BOOK = "book"
WS_MSG_PRICE_CHANGE = "price_change"

INTERVAL_SECONDS = {"5m": 300, "15m": 900}

STRUCTURED_SLUG_INTERVALS = {"5m", "15m"}

# CLOB execution
DEFAULT_TICK_SIZE = "0.01"
CHAIN_ID = 137  # Polygon mainnet
CHAIN_ID_AMOY = 80002  # Polygon Amoy testnet
ORDER_TYPE_FOK = "FOK"
ORDER_TYPE_GTC = "GTC"

# CTF contracts (Polygon mainnet)
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # Conditional Tokens
USDCe_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e collateral

# Coinbase spot (WebSocket ticker)
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST_URL = "https://api.exchange.coinbase.com"

# Chainlink Data Streams (strike at epoch; optional)
CHAINLINK_REST_URL = "https://api.dataengine.chain.link"
CHAINLINK_WS_URL = "wss://ws.dataengine.chain.link"
CHAINLINK_PRICE_DECIMALS = 1e18
