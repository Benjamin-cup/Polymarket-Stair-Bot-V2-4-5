"""Split USDC.e collateral into YES+NO conditional tokens via CTF splitPosition.

Uses the same builder-relayer-client pattern as redeem.py (gasless via Polymarket relayer).
Optionally includes ERC-20 approve for USDC.e → CTF if needed.
"""

from __future__ import annotations

import asyncio
from typing import Any

from eth_abi import encode
from eth_utils import keccak, to_checksum_address

from polybot5m.constants import CTF_ADDRESS, USDCe_ADDRESS
from polybot5m.execution.redeem import (
    RELAYER_URL,
    HASH_ZERO,
    _condition_id_to_bytes32,
    _is_rate_limit_error,
    _load_builder_creds_pool,
)

MAX_UINT256 = 2**256 - 1


def _function_selector(signature: str) -> bytes:
    return keccak(text=signature)[:4]


def _encode_approve_calldata(spender: str, amount: int = MAX_UINT256) -> str:
    """ERC-20 approve(spender, amount) calldata."""
    selector = _function_selector("approve(address,uint256)")
    args = encode(["address", "uint256"], [to_checksum_address(spender), amount])
    return "0x" + (selector + args).hex()


def _encode_split_calldata(
    collateral_token: str,
    condition_id_b32: bytes,
    partition: list[int],
    amount: int,
) -> str:
    """Encode splitPosition(address,bytes32,bytes32,uint256[],uint256) calldata.

    Matches CTF contract:
      splitPosition(collateralToken, parentCollectionId, conditionId, partition, amount)
    """
    selector = _function_selector(
        "splitPosition(address,bytes32,bytes32,uint256[],uint256)"
    )
    args = encode(
        ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
        [
            to_checksum_address(collateral_token),
            HASH_ZERO,
            condition_id_b32,
            partition,
            amount,
        ],
    )
    return "0x" + (selector + args).hex()


def _split_via_relayer(
    condition_id: str,
    collateral_amount_raw: int,
    private_key: str,
    chain_id: int,
    builder_api_key: str,
    builder_api_secret: str,
    builder_api_passphrase: str,
    *,
    approve_first: bool = True,
    relayer_url: str = RELAYER_URL,
    ctf_address: str = CTF_ADDRESS,
    collateral_token: str = USDCe_ADDRESS,
    partition: list[int] | None = None,
) -> dict[str, Any]:
    """Execute splitPosition via Polymarket relayer (gasless).

    Optionally prepends an ERC-20 approve tx so the CTF can pull USDC.e.
    """
    partition = partition if partition is not None else [1, 2]

    try:
        from py_builder_relayer_client.client import RelayClient
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds
    except ImportError as e:
        return {
            "tx_hash": None,
            "error": f"Relayer deps missing: {e}. pip install py-builder-relayer-client py-builder-signing-sdk",
        }

    try:
        txs: list[SafeTransaction] = []

        if approve_first:
            approve_data = _encode_approve_calldata(ctf_address, MAX_UINT256)
            txs.append(
                SafeTransaction(
                    to=collateral_token,
                    operation=OperationType.Call,
                    data=approve_data,
                    value="0",
                )
            )

        condition_id_b32 = _condition_id_to_bytes32(condition_id)
        split_data = _encode_split_calldata(
            collateral_token, condition_id_b32, partition, collateral_amount_raw
        )
        txs.append(
            SafeTransaction(
                to=ctf_address,
                operation=OperationType.Call,
                data=split_data,
                value="0",
            )
        )

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

        response = client.execute(txs, "Split USDC.e into YES+NO tokens")
        result = response.wait()
        tx_hash = None
        if result:
            tx_hash = result.get("transactionHash") or result.get("transaction_hash")
        if not tx_hash and getattr(response, "transaction_hash", None):
            tx_hash = response.transaction_hash
        if tx_hash:
            return {"tx_hash": tx_hash, "error": None}
        return {"tx_hash": None, "error": "Relayer did not return transaction hash"}
    except Exception as e:
        return {"tx_hash": None, "error": str(e)}


async def split_position(
    condition_id: str,
    collateral_amount_usdc: float,
    private_key: str,
    chain_id: int = 137,
    *,
    approve_first: bool = True,
    cred_index: int = 0,
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
    relayer_url: str = RELAYER_URL,
    ctf_address: str = CTF_ADDRESS,
    collateral_token: str = USDCe_ADDRESS,
    partition: list[int] | None = None,
) -> dict[str, Any]:
    """Split USDC.e collateral into YES+NO conditional tokens.

    collateral_amount_usdc is in human-readable units (e.g. 100.0 = $100).
    USDC.e has 6 decimals, so raw amount = int(collateral_amount_usdc * 1e6).

    cred_index rotates which builder credential to start with, so parallel
    splits each use a different credential (e.g. market 0 → cred 0, market 1 → cred 1).
    On failure/rate-limit, retries with the next credential in the pool.
    """
    collateral_amount_raw = int(collateral_amount_usdc * 1_000_000)
    if collateral_amount_raw <= 0:
        return {"tx_hash": None, "error": "collateral_amount_usdc must be > 0"}

    cred_pool = _load_builder_creds_pool()
    if not cred_pool and api_key and api_secret and api_passphrase:
        cred_pool = [(api_key, api_secret, api_passphrase)]
    elif not cred_pool:
        return {
            "tx_hash": None,
            "error": "Split requires relayer. Set POLYBOT5M_EXECUTION__BUILDER_API_KEY_1..N or api_key/secret/passphrase.",
        }
    elif api_key and api_secret and api_passphrase:
        single = (api_key, api_secret, api_passphrase)
        if single not in cred_pool:
            cred_pool = [single] + cred_pool

    # Rotate starting credential so parallel splits don't collide
    n = len(cred_pool)
    ordered_pool = [cred_pool[(cred_index + i) % n] for i in range(n)]

    last_error: str | None = None
    for idx, (key, secret, passphrase) in enumerate(ordered_pool):
        result = await asyncio.to_thread(
            _split_via_relayer,
            condition_id=condition_id,
            collateral_amount_raw=collateral_amount_raw,
            private_key=private_key,
            chain_id=chain_id,
            builder_api_key=key,
            builder_api_secret=secret,
            builder_api_passphrase=passphrase,
            approve_first=approve_first,
            relayer_url=relayer_url,
            ctf_address=ctf_address,
            collateral_token=collateral_token,
            partition=partition,
        )
        err = result.get("error")
        if not err:
            return result
        last_error = err
        if _is_rate_limit_error(err) and idx < n - 1:
            print(f"  Rate limit on split — retrying with next builder cred ({idx + 2}/{n})")
        else:
            break

    return {"tx_hash": None, "error": last_error or "Split failed"}
