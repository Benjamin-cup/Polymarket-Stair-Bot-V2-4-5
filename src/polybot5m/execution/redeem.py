"""Redeem winning conditional tokens (CTF) after market resolution.

Supports two paths:
- Relayer (gasless): use Polymarket relayer + builder API key — same as TypeScript claimWinnings.
- Direct: sign and send tx with Web3 (you pay gas).

When rate limited, retries with next builder cred from pool (POLYBOT5M_EXECUTION__BUILDER_API_KEY_1..5).
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from eth_abi import encode
from eth_utils import keccak, to_checksum_address

from polybot5m.constants import CTF_ADDRESS, USDCe_ADDRESS

RELAYER_URL = "https://relayer-v2.polymarket.com/"

# Same as TypeScript: ctfInterface.encodeFunctionData("redeemPositions", [
#   USDC_E, ethers.constants.HashZero, marketConditionId, [1, 2]
# ])
REDEEM_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "outputs": [],
    }
]

# ethers.constants.HashZero = 32 zero bytes
HASH_ZERO = b"\x00" * 32

# Env prefix for builder API creds
BUILDER_ENV_PREFIX = "POLYBOT5M_EXECUTION__BUILDER_API_"


def _is_rate_limit_error(err: str) -> bool:
    """True if error looks like a rate limit / quota response."""
    s = (err or "").lower()
    return any(
        x in s
        for x in ("rate limit", "rate limit exceeded", "429", "too many requests", "quota exceeded", "throttl")
    )


def _load_builder_creds_pool() -> list[tuple[str, str, str]]:
    """
    Load builder API credentials from env. Supports:
    - POLYBOT5M_EXECUTION__BUILDER_API_KEY_1, _SECRET_1, _PASSPHRASE_1 (through _5 or more)
    - Legacy: BUILDER_API_KEY, BUILDER_API_SECRET, BUILDER_API_PASSPHRASE (no suffix)

    Returns list of (key, secret, passphrase) tuples. Numbered creds first, then legacy.
    """
    pool: list[tuple[str, str, str]] = []
    for i in range(1, 20):  # support 1..19
        key = os.getenv(f"{BUILDER_ENV_PREFIX}KEY_{i}", "")
        secret = os.getenv(f"{BUILDER_ENV_PREFIX}SECRET_{i}", "")
        passphrase = os.getenv(f"{BUILDER_ENV_PREFIX}PASSPHRASE_{i}", "")
        if key and secret and passphrase:
            pool.append((key, secret, passphrase))
    # Legacy single cred (no number)
    key = os.getenv("POLYBOT5M_EXECUTION__BUILDER_API_KEY", "")
    secret = os.getenv("POLYBOT5M_EXECUTION__BUILDER_API_SECRET", "")
    passphrase = os.getenv("POLYBOT5M_EXECUTION__BUILDER_API_PASSPHRASE", "")
    if key and secret and passphrase:
        legacy = (key, secret, passphrase)
        if legacy not in pool:
            pool.append(legacy)
    return pool


def _condition_id_to_bytes32(condition_id: str) -> bytes:
    """Convert hex condition_id (0x... or raw hex) to 32-byte bytes."""
    raw = condition_id.strip()
    if raw.startswith("0x"):
        raw = raw[2:]
    b = bytes.fromhex(raw)
    if len(b) > 32:
        return b[-32:]
    if len(b) < 32:
        return b"\x00" * (32 - len(b)) + b
    return b

def _function_selector(signature: str) -> bytes:
    """First 4 bytes of Keccak-256 of the function signature."""
    return keccak(text=signature)[:4]


def _encode_redeem_calldata(
    collateral_token: str,
    condition_id_b32: bytes,
    index_sets: list[int],
) -> str:
    """Return hex-encoded calldata for redeemPositions(collateralToken, parentCollectionId, conditionId, indexSets).

    Same pattern as: selector = keccak(sig)[:4]; data = selector + encode(types, args).
    Matches TypeScript: ctfInterface.encodeFunctionData("redeemPositions", [USDC_E, HashZero, marketConditionId, [1,2]])
    """
    selector = _function_selector("redeemPositions(address,bytes32,bytes32,uint256[])")
    encoded_args = encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [to_checksum_address(collateral_token), HASH_ZERO, condition_id_b32, index_sets],
    )
    return "0x" + (selector + encoded_args).hex()


def _redeem_via_relayer(
    condition_id: str,
    private_key: str,
    chain_id: int,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    rpc_url: str = "https://rpc.ankr.com/polygon/b7025907c7c47329edc930ec748839dd7d71e4d5ce738db39aa03eec87bdd3f2",
    relayer_url: str = RELAYER_URL,
    ctf_address: str = CTF_ADDRESS,
    collateral_token: str = USDCe_ADDRESS,
    index_sets: list[int] | None = None,
) -> dict[str, Any]:
    """
    Execute redeem via Polymarket relayer (gasless). Matches TypeScript claimWinnings flow.
    Requires builder API credentials (POLY_BUILDER_API_KEY, etc.).
    """
    return _redeem_batch_via_relayer(
        condition_ids=[condition_id],
        private_key=private_key,
        chain_id=chain_id,
        builder_api_key=builder_api_key,
        builder_api_secret=builder_api_secret,
        builder_api_passphrase=builder_api_passphrase,
        rpc_url=rpc_url,
        relayer_url=relayer_url,
        ctf_address=ctf_address,
        collateral_token=collateral_token,
        index_sets=index_sets,
    )


def _redeem_batch_via_relayer(
    condition_ids: list[str],
    private_key: str,
    chain_id: int,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    rpc_url: str = "https://rpc.ankr.com/polygon/b7025907c7c47329edc930ec748839dd7d71e4d5ce738db39aa03eec87bdd3f2",
    relayer_url: str = RELAYER_URL,
    ctf_address: str = CTF_ADDRESS,
    collateral_token: str = USDCe_ADDRESS,
    index_sets: list[int] | None = None,
) -> dict[str, Any]:
    """
    Batch redeem multiple markets in one relayer call.
    Builds array of SafeTransaction (one redeemPositions per condition_id), passes to execute([tx1, tx2, ...]).
    Returns {tx_hash, error, condition_ids} where condition_ids are the ones included in the batch.
    """
    if not condition_ids:
        return {"tx_hash": None, "error": "No condition_ids to redeem", "condition_ids": []}

    index_sets = index_sets if index_sets is not None else [1, 2]
    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds

    except ImportError as e:
        return {
            "tx_hash": None,
            "error": f"Relayer deps missing: {e}. pip install py-builder-relayer-client py-builder-signing-sdk",
            "condition_ids": condition_ids,
        }

    try:
        txs: list[SafeTransaction] = []
        for condition_id in condition_ids:
            condition_id_b32 = _condition_id_to_bytes32(condition_id)
            data_hex = _encode_redeem_calldata(collateral_token, condition_id_b32, index_sets)
            txs.append(
                SafeTransaction(
                    to=ctf_address,
                    operation=OperationType.Call,
                    data=data_hex,
                    value="0",
                )
            )

        if len(txs) > 1:
            print(f"  Batch redeeming {len(txs)} condition_ids in one call")
        else:
            print(f"data_hex: {_encode_redeem_calldata(collateral_token, _condition_id_to_bytes32(condition_ids[0]), index_sets)}")

        builder_config = BuilderConfig(
            local_builder_creds=BuilderApiKeyCreds(
                key=builder_api_key,
                secret=builder_api_secret,
                passphrase=builder_api_passphrase,
            )
        )

        client = RelayClient(
            relayer_url=relayer_url,
            chain_id=chain_id,
            private_key=private_key,
            builder_config=builder_config,
        )

        response = client.execute(txs, "Redeem winnings")
        result = response.wait()
        tx_hash = None
        if result:
            tx_hash = result.get("transactionHash") or result.get("transaction_hash")
        if not tx_hash and getattr(response, "transaction_hash", None):
            tx_hash = response.transaction_hash
        if tx_hash:
            return {"tx_hash": tx_hash, "error": None, "condition_ids": condition_ids}
        return {"tx_hash": None, "error": "Relayer did not return transaction hash", "condition_ids": condition_ids}
    except Exception as e:
        return {"tx_hash": None, "error": str(e), "condition_ids": condition_ids}


async def redeem_positions(
    condition_id: str,
    private_key: str,
    rpc_url: str,
    chain_id: int = 137,
    ctf_address: str = CTF_ADDRESS,
    collateral_token: str = USDCe_ADDRESS,
    index_sets: list[int] | None = None,
    *,
    use_relayer: bool = True,
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
    relayer_url: str = RELAYER_URL,
) -> dict[str, Any]:
    """
    Redeem winning CTF positions for a single resolved market.

    If use_relayer is True and api_key, api_secret, api_passphrase are all set,
    uses Polymarket relayer (gasless, same as TypeScript claimWinnings).
    """
    return await redeem_positions_batch(
        condition_ids=[condition_id],
        private_key=private_key,
        rpc_url=rpc_url,
        chain_id=chain_id,
        ctf_address=ctf_address,
        collateral_token=collateral_token,
        index_sets=index_sets,
        use_relayer=use_relayer,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        relayer_url=relayer_url,
    )


async def redeem_positions_batch(
    condition_ids: list[str],
    private_key: str,
    rpc_url: str,
    chain_id: int = 137,
    ctf_address: str = CTF_ADDRESS,
    collateral_token: str = USDCe_ADDRESS,
    index_sets: list[int] | None = None,
    *,
    use_relayer: bool = True,
    cred_index: int = 0,
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
    relayer_url: str = RELAYER_URL,
) -> dict[str, Any]:
    """
    Batch redeem winning CTF positions for multiple resolved markets in one relayer call.

    cred_index rotates which builder credential to start with, so parallel
    redeems each use a different credential (e.g. market 0 → cred 0, market 1 → cred 1).
    On rate-limit, retries with the next credential in the pool.
    """
    if not condition_ids:
        return {"tx_hash": None, "error": "No condition_ids to redeem", "condition_ids": []}

    cred_pool = _load_builder_creds_pool()
    if not cred_pool and use_relayer and api_key and api_secret and api_passphrase:
        cred_pool = [(api_key, api_secret, api_passphrase)]
    elif not cred_pool:
        return {
            "tx_hash": None,
            "error": "Batch redeem requires relayer. Set POLYBOT5M_EXECUTION__BUILDER_API_KEY_1..N or api_key/secret/passphrase.",
            "condition_ids": condition_ids,
        }
    elif api_key and api_secret and api_passphrase:
        single = (api_key, api_secret, api_passphrase)
        if single not in cred_pool:
            cred_pool = [single] + cred_pool

    # Rotate starting credential so parallel redeems don't collide
    n = len(cred_pool)
    ordered_pool = [cred_pool[(cred_index + i) % n] for i in range(n)]

    last_error: str | None = None
    for idx, (key, secret, passphrase) in enumerate(ordered_pool):
        result = await asyncio.to_thread(
            _redeem_batch_via_relayer,
            condition_ids=condition_ids,
            private_key=private_key,
            chain_id=chain_id,
            builder_api_key=key,
            builder_api_secret=secret,
            builder_api_passphrase=passphrase,
            rpc_url=rpc_url,
            relayer_url=relayer_url,
            ctf_address=ctf_address,
            collateral_token=collateral_token,
            index_sets=index_sets,
        )
        err = result.get("error")
        if not err:
            return result
        last_error = err
        if _is_rate_limit_error(err) and idx < n - 1:
            print(f"  Rate limit — retrying with next builder cred ({idx + 2}/{n})")
        else:
            break

    return {
        "tx_hash": None,
        "error": last_error or "Redeem failed",
        "condition_ids": condition_ids,
    }
