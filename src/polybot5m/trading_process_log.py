"""Append-only NDJSON log for full trading lifecycle (phases + monitor ticks)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_iso_z() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def resolve_trading_process_path(settings: Any) -> Path | None:
    """Read liquidity_maker.trading_process_jsonl."""
    lm = getattr(settings, "liquidity_maker", None)
    if lm is None:
        return None
    raw = getattr(lm, "trading_process_jsonl", "") or ""
    raw = str(raw).strip()
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


def append_trading_jsonl(path: str | Path | None, row: dict[str, Any]) -> None:
    if path is None:
        return
    raw = str(path).strip()
    if not raw:
        return
    p = Path(raw).expanduser()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, default=str) + "\n"
        with open(p, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        print(f"  trading_process_jsonl write error: {e}")
