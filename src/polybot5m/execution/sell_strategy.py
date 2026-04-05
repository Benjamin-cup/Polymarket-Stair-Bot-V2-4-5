"""Limit sell strategy: last-window trigger, low-leg full exit, staged high leg, final unwind."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polybot5m.config import SellStrategyConfig
from polybot5m.execution.paper_report import PaperFill
from polybot5m.trading_process_log import append_trading_jsonl, utc_iso_z


def _utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _snap(p: float, nd: int = 4) -> float:
    return round(float(p), nd)


@dataclass
class SellStrategyRunner:
    """Stateful sell logic driven once per monitor poll."""

    cfg: SellStrategyConfig
    tag: str
    yes_token_id: str
    no_token_id: str
    rem_yes: float
    rem_no: float
    clob: Any | None
    trade_log_path: Path | None
    dry_run: bool
    paper_trading: bool
    # Split allocation (USDC); per-leg share count matches this in paper — used for HIGH_PARTIAL chunk size.
    portfolio_allocation_usdc: float
    process_log_path: Path | None = None
    process_log_interval_s: float = 0.0
    process_log_stdout: bool = False

    triggered: bool = False
    low_sold: bool = False
    consecutive_above_90: int = 0
    any_sell_done: bool = False
    last_high_bid_snap: float | None = None
    high_partial_started: bool = False
    final_unwind_fired: bool = False
    # After LOW_FULL: fixed outcome token we still hold (higher bid at time of low exit).
    locked_high_side: str | None = None
    # Limit price used for the full low-leg exit (for pair_sum = low_sell_price + current high bid).
    low_sell_price: float | None = None

    hedge_executed: bool = False
    hedge_cycles_strike_lt_spot: int = 0
    hedge_cycles_strike_gt_spot: int = 0
    _hedge_warned_no_strike_spot: bool = False

    _fee_cache: dict[str, int] = field(default_factory=dict)
    _neg_cache: dict[str, bool] = field(default_factory=dict)
    _tick_cache: dict[str, str] = field(default_factory=dict)
    _last_process_log_mono: float = 0.0
    _warned_missing_low_sell_price: bool = False
    _last_low_full_skip_log_mono: float = 0.0
    # Paper trading: each simulated sell as a full fill at limit price (strategy uses best bid as limit).
    paper_fills: list[PaperFill] = field(default_factory=list)
    # Optional: realistic backtest (latency, VWAP, fees) — see polybot5m.backtest.simulation
    paper_execution_hook: Any | None = None
    _last_sell_vwap: float | None = None
    # Latest tick context for execution hook (set in on_tick).
    _ctx_bid_yes: float = 0.0
    _ctx_bid_no: float = 0.0
    _ctx_remaining_s: float = 0.0
    _ctx_snapshot_ts: datetime | None = None
    _ctx_orderbook_yes: Any | None = None
    _ctx_orderbook_no: Any | None = None
    # True = require strike vs spot cycle count (live). False = backtest hedge on book gates only.
    hedge_require_strike_spot: bool = True

    def _high_low(self, bid_yes: float, bid_no: float) -> tuple[str, str, float, float]:
        if bid_yes >= bid_no:
            return "yes", "no", bid_yes, bid_no
        return "no", "yes", bid_no, bid_yes

    def _rem_for_side(self, side: str) -> float:
        return self.rem_yes if side == "yes" else self.rem_no

    def _set_rem(self, side: str, v: float) -> None:
        if side == "yes":
            self.rem_yes = max(0.0, v)
        else:
            self.rem_no = max(0.0, v)

    def _token_id(self, side: str) -> str:
        return self.yes_token_id if side == "yes" else self.no_token_id

    def _bid_for_side(self, bid_yes: float, bid_no: float, side: str) -> float:
        return bid_yes if side == "yes" else bid_no

    def _high_partial_chunk(self, rem_high: float) -> float:
        """Shares per staged high-leg slice: portfolio_allocation_usdc / max_cycles, capped by remaining."""
        cfg = self.cfg
        if cfg.high_partial_max_cycles <= 0:
            return rem_high
        denom = float(cfg.high_partial_max_cycles)
        raw = self.portfolio_allocation_usdc / denom
        chunk = max(0.0, _snap(raw, 6))
        chunk = min(chunk, rem_high)
        if chunk < cfg.min_sell_shares:
            chunk = rem_high
        return chunk

    def _pair_sum(self, bid_yes: float, bid_no: float) -> float | None:
        """
        Sum used for all post–low-sell logic: executed low-leg price + current high token best bid.
        None before LOW_FULL or if low_sell_price is missing.
        """
        if not self.low_sold or self.locked_high_side is None or self.low_sell_price is None:
            return None
        bid_h = self._bid_for_side(bid_yes, bid_no, self.locked_high_side)
        return self.low_sell_price + bid_h

    def _append_log(self, row: dict[str, Any]) -> None:
        if not self.trade_log_path:
            return
        try:
            self.trade_log_path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(row, default=str) + "\n"
            with open(self.trade_log_path, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            print(f"  {self.tag} trade log write error: {e}")

    async def _place_sell_limit(
        self,
        token_side: str,
        price: float,
        size: float,
        reason: str,
    ) -> bool:
        cfg = self.cfg
        tid = self._token_id(token_side)
        min_s = cfg.min_sell_shares
        if size < min_s:
            return False
        price = max(0.01, min(0.99, _snap(price, 6)))
        size = _snap(size, 6)

        row: dict[str, Any] = {
            "ts_utc": _utc_iso(),
            "tag": self.tag,
            "event": "SELL_LIMIT",
            "token_side": token_side.upper(),
            "token_id": tid,
            "price": price,
            "size": size,
            "reason": reason,
            "dry_run": self.dry_run,
            "paper": self.paper_trading,
        }

        if self.clob is None or self.dry_run or self.paper_trading:
            row["status"] = "simulated" if (self.dry_run or self.paper_trading) else "no_clob"
            self._append_log(row)
            print(
                f"  {self.tag} [SELL] {reason} {token_side.upper()} px={price:.4f} sz={size:.4f} "
                f"({'PAPER' if self.paper_trading else 'DRY' if self.dry_run else 'SKIP'})"
            )
            fill_size = size
            self._last_sell_vwap = None
            if self.paper_trading:
                label = "YES" if token_side.lower() == "yes" else "NO"
                if self.paper_execution_hook is not None:
                    ts = self._ctx_snapshot_ts or datetime.now(UTC)
                    res = await self.paper_execution_hook.execute_sell_limit(
                        token_side=token_side,
                        limit_price=price,
                        size=size,
                        reason=reason,
                        decision_ts=ts,
                        bid_yes=self._ctx_bid_yes,
                        bid_no=self._ctx_bid_no,
                        remaining_s=self._ctx_remaining_s,
                        orderbook_yes=self._ctx_orderbook_yes,
                        orderbook_no=self._ctx_orderbook_no,
                    )
                    if not res.ok or res.filled_size < min_s:
                        return False
                    fill_size = res.filled_size
                    self._last_sell_vwap = res.vwap_price
                    now = ts
                    self.paper_fills.append(
                        PaperFill(
                            token_id=tid,
                            side_label=label,
                            price=res.vwap_price,
                            size=fill_size,
                            usdc_proceeds=_snap(res.proceeds_after_fees, 6),
                            filled_at=now,
                            best_bid_at_fill=res.best_bid_or_ask_at_fill,
                            reason=reason,
                            fee_usdc=res.fee_usdc,
                            latency_ms=res.latency_ms,
                            vwap_price=res.vwap_price,
                            limit_price_at_decision=price,
                            slippage_vs_limit=res.slippage_vs_limit,
                            best_bid_at_decision=res.best_bid_at_decision,
                        )
                    )
                else:
                    now = datetime.now(UTC)
                    proceeds = _snap(price * size, 6)
                    self._last_sell_vwap = price
                    self.paper_fills.append(
                        PaperFill(
                            token_id=tid,
                            side_label=label,
                            price=price,
                            size=size,
                            usdc_proceeds=proceeds,
                            filled_at=now,
                            best_bid_at_fill=price,
                            reason=reason,
                        )
                    )
            self._set_rem(token_side, self._rem_for_side(token_side) - fill_size)
            self.any_sell_done = True
            return True

        try:
            if tid not in self._neg_cache:
                self._neg_cache[tid] = await self.clob.get_neg_risk(tid)
            if tid not in self._tick_cache:
                self._tick_cache[tid] = await self.clob.get_tick_size(tid)
            if tid not in self._fee_cache:
                self._fee_cache[tid] = await self.clob.get_fee_rate_bps(tid)

            signed = await asyncio.to_thread(
                self.clob.create_order,
                tid,
                "SELL",
                price,
                size,
                self._neg_cache[tid],
                self._fee_cache[tid],
                self._tick_cache[tid],
            )
            resp = await self.clob.post_order(signed, cfg.sell_order_type)
            row["status"] = "submitted"
            row["response"] = resp
            self._append_log(row)
            print(f"  {self.tag} [SELL] {reason} {token_side.upper()} px={price:.4f} sz={size:.4f} ok")
            self._set_rem(token_side, self._rem_for_side(token_side) - size)
            self.any_sell_done = True
            return True
        except Exception as e:
            row["status"] = "error"
            row["error"] = str(e)
            self._append_log(row)
            print(f"  {self.tag} [SELL ERROR] {reason}: {e}")
            return False

    async def _place_buy_limit(
        self,
        token_side: str,
        price: float,
        size: float,
        reason: str,
    ) -> bool:
        cfg = self.cfg
        tid = self._token_id(token_side)
        min_s = cfg.min_sell_shares
        if size < min_s:
            return False
        price = max(0.01, min(0.99, _snap(price, 6)))
        size = _snap(size, 6)
        order_type = cfg.hedge_order_type or cfg.sell_order_type

        row: dict[str, Any] = {
            "ts_utc": _utc_iso(),
            "tag": self.tag,
            "event": "BUY_LIMIT",
            "token_side": token_side.upper(),
            "token_id": tid,
            "price": price,
            "size": size,
            "reason": reason,
            "dry_run": self.dry_run,
            "paper": self.paper_trading,
        }

        if self.clob is None or self.dry_run or self.paper_trading:
            row["status"] = "simulated" if (self.dry_run or self.paper_trading) else "no_clob"
            self._append_log(row)
            print(
                f"  {self.tag} [BUY] {reason} {token_side.upper()} px={price:.4f} sz={size:.4f} "
                f"({'PAPER' if self.paper_trading else 'DRY' if self.dry_run else 'SKIP'})"
            )
            buy_fill = size
            if self.paper_trading and self.paper_execution_hook is not None:
                ts = self._ctx_snapshot_ts or datetime.now(UTC)
                res = await self.paper_execution_hook.execute_buy_limit(
                    token_side=token_side,
                    limit_price=price,
                    size=size,
                    reason=reason,
                    decision_ts=ts,
                    bid_yes=self._ctx_bid_yes,
                    bid_no=self._ctx_bid_no,
                    remaining_s=self._ctx_remaining_s,
                    orderbook_yes=self._ctx_orderbook_yes,
                    orderbook_no=self._ctx_orderbook_no,
                )
                if not res.ok or res.filled_size < min_s:
                    return False
                buy_fill = res.filled_size
            self._set_rem(token_side, self._rem_for_side(token_side) + buy_fill)
            return True

        try:
            if tid not in self._neg_cache:
                self._neg_cache[tid] = await self.clob.get_neg_risk(tid)
            if tid not in self._tick_cache:
                self._tick_cache[tid] = await self.clob.get_tick_size(tid)
            if tid not in self._fee_cache:
                self._fee_cache[tid] = await self.clob.get_fee_rate_bps(tid)

            signed = await asyncio.to_thread(
                self.clob.create_order,
                tid,
                "BUY",
                price,
                size,
                self._neg_cache[tid],
                self._fee_cache[tid],
                self._tick_cache[tid],
            )
            resp = await self.clob.post_order(signed, order_type)
            row["status"] = "submitted"
            row["response"] = resp
            self._append_log(row)
            print(f"  {self.tag} [BUY] {reason} {token_side.upper()} px={price:.4f} sz={size:.4f} ok")
            self._set_rem(token_side, self._rem_for_side(token_side) + size)
            return True
        except Exception as e:
            row["status"] = "error"
            row["error"] = str(e)
            self._append_log(row)
            print(f"  {self.tag} [BUY ERROR] {reason}: {e}")
            return False

    def _append_process_snapshot(self, bid_yes: float, bid_no: float, remaining_s: float) -> None:
        if not self.process_log_path:
            return
        now = time.monotonic()
        if (
            self.process_log_interval_s > 0
            and (now - self._last_process_log_mono) < self.process_log_interval_s
        ):
            return
        self._last_process_log_mono = now
        cfg = self.cfg
        hs = self.locked_high_side
        bid_h = self._bid_for_side(bid_yes, bid_no, hs) if hs else max(bid_yes, bid_no)
        pair_sum = self._pair_sum(bid_yes, bid_no)
        row: dict[str, Any] = {
            "event": "MONITOR_TICK",
            "ts_utc": utc_iso_z(),
            "tag": self.tag,
            "bid_yes": bid_yes,
            "bid_no": bid_no,
            "sum_bids": bid_yes + bid_no,
            "low_sell_price": self.low_sell_price,
            "pair_sum": pair_sum,
            "t_minus_s": remaining_s,
            "rem_yes": self.rem_yes,
            "rem_no": self.rem_no,
            "sell_strategy_enabled": True,
            "in_sell_window": remaining_s <= cfg.sell_window_seconds_before_end,
            "triggered": self.triggered,
            "low_sold": self.low_sold,
            "locked_high_side": self.locked_high_side,
            "held_high_bid": bid_h,
            "consecutive_above_90": self.consecutive_above_90,
            "last_high_bid_snap": self.last_high_bid_snap,
            "high_partial_started": self.high_partial_started,
            "final_unwind_fired": self.final_unwind_fired,
            "any_sell_done": self.any_sell_done,
            "hedge_executed": self.hedge_executed,
            "hedge_cycles_strike_lt_spot": self.hedge_cycles_strike_lt_spot,
            "hedge_cycles_strike_gt_spot": self.hedge_cycles_strike_gt_spot,
        }
        append_trading_jsonl(self.process_log_path, row)
        if self.process_log_stdout:
            ps = f" pair_sum={pair_sum:.4f}" if pair_sum is not None else " pair_sum=—"
            print(
                f"  {self.tag} [TRADING_PROCESS] t={remaining_s:.1f}s YES={bid_yes:.4f} NO={bid_no:.4f} "
                f"book_sum={bid_yes + bid_no:.4f}{ps} rem_y={self.rem_yes:.4f} rem_n={self.rem_no:.4f} "
                f"trig={self.triggered} low_sold={self.low_sold} lock={self.locked_high_side} "
                f"hedge_done={self.hedge_executed}",
            )

    async def on_tick(
        self,
        bid_yes: float,
        bid_no: float,
        remaining_s: float,
        *,
        strike: float | None = None,
        spot: float | None = None,
        ask_yes: float | None = None,
        ask_no: float | None = None,
        snapshot_ts: datetime | None = None,
        orderbook_yes: Any | None = None,
        orderbook_no: Any | None = None,
    ) -> None:
        self._ctx_bid_yes = bid_yes
        self._ctx_bid_no = bid_no
        self._ctx_remaining_s = remaining_s
        self._ctx_snapshot_ts = snapshot_ts
        self._ctx_orderbook_yes = orderbook_yes
        self._ctx_orderbook_no = orderbook_no
        try:
            if not self.hedge_executed:
                await self._maybe_hedge_pair(bid_yes, bid_no, strike, spot, ask_yes, ask_no)
            if not self.hedge_executed:
                await self._on_tick_inner(bid_yes, bid_no, remaining_s)
        finally:
            self._append_process_snapshot(bid_yes, bid_no, remaining_s)

    async def _maybe_hedge_pair(
        self,
        bid_yes: float,
        bid_no: float,
        strike: float | None,
        spot: float | None,
        ask_yes: float | None,
        ask_no: float | None,
    ) -> None:
        cfg = self.cfg
        if not cfg.hedge_enabled:
            return
        if self.hedge_executed or not self.low_sold or self.locked_high_side is None:
            return
        min_s = cfg.min_sell_shares
        hs = self.locked_high_side
        opp_max = float(cfg.hedge_opposite_bid_below)
        sold_min = float(cfg.hedge_sold_side_bid_above)
        n_need = max(1, int(cfg.hedge_target_vs_spot_cycles))
        buy_sz = _snap(float(self.portfolio_allocation_usdc) * float(cfg.hedge_buy_size_multiplier), 6)

        def yes_sold_first_ok() -> bool:
            return (
                hs == "no"
                and self.rem_yes < min_s
                and self.rem_no >= min_s
                and bid_no < opp_max
                and bid_yes > sold_min
            )

        def no_sold_first_ok() -> bool:
            return (
                hs == "yes"
                and self.rem_no < min_s
                and self.rem_yes >= min_s
                and bid_yes < opp_max
                and bid_no > sold_min
            )

        # Backtest: hedge when book gates pass; no strike vs chain spot (not available in replay).
        if not self.hedge_require_strike_spot:
            if yes_sold_first_ok():
                if buy_sz < min_s:
                    return
                buy_px = float(ask_yes) if ask_yes and ask_yes > 0 else bid_yes
                ok_buy = await self._place_buy_limit("yes", buy_px, buy_sz, "HEDGE_BUY_YES")
                if not ok_buy:
                    return
                sell_no = self.rem_no
                ok_sell = await self._place_sell_limit("no", bid_no, sell_no, "HEDGE_SELL_NO")
                if ok_sell:
                    self.hedge_executed = True
                    print(
                        f"  {self.tag} [HEDGE] buy YES + sell NO (backtest, book gates only)",
                    )
                return
            if no_sold_first_ok():
                if buy_sz < min_s:
                    return
                buy_px = float(ask_no) if ask_no and ask_no > 0 else bid_no
                ok_buy = await self._place_buy_limit("no", buy_px, buy_sz, "HEDGE_BUY_NO")
                if not ok_buy:
                    return
                sell_yes = self.rem_yes
                ok_sell = await self._place_sell_limit("yes", bid_yes, sell_yes, "HEDGE_SELL_YES")
                if ok_sell:
                    self.hedge_executed = True
                    print(
                        f"  {self.tag} [HEDGE] buy NO + sell YES (backtest, book gates only)",
                    )
                return
            self.hedge_cycles_strike_lt_spot = 0
            self.hedge_cycles_strike_gt_spot = 0
            return

        if strike is None or spot is None or float(strike) <= 0 or float(spot) <= 0:
            self.hedge_cycles_strike_lt_spot = 0
            self.hedge_cycles_strike_gt_spot = 0
            if (
                (yes_sold_first_ok() or no_sold_first_ok())
                and not self._hedge_warned_no_strike_spot
            ):
                self._hedge_warned_no_strike_spot = True
                print(
                    f"  {self.tag} [HEDGE] need strike+chain spot (enable price_feed + oracle)",
                )
            return

        st = float(strike)
        sp = float(spot)

        if yes_sold_first_ok():
            self.hedge_cycles_strike_gt_spot = 0
            if st < sp:
                self.hedge_cycles_strike_lt_spot += 1
            else:
                self.hedge_cycles_strike_lt_spot = 0
            if self.hedge_cycles_strike_lt_spot < n_need or buy_sz < min_s:
                return
            buy_px = float(ask_yes) if ask_yes and ask_yes > 0 else bid_yes
            ok_buy = await self._place_buy_limit("yes", buy_px, buy_sz, "HEDGE_BUY_YES")
            if not ok_buy:
                return
            sell_no = self.rem_no
            ok_sell = await self._place_sell_limit("no", bid_no, sell_no, "HEDGE_SELL_NO")
            if ok_sell:
                self.hedge_executed = True
                print(f"  {self.tag} [HEDGE] buy YES + sell NO complete (no further stair sells)")
            return

        if no_sold_first_ok():
            self.hedge_cycles_strike_lt_spot = 0
            if st > sp:
                self.hedge_cycles_strike_gt_spot += 1
            else:
                self.hedge_cycles_strike_gt_spot = 0
            if self.hedge_cycles_strike_gt_spot < n_need or buy_sz < min_s:
                return
            buy_px = float(ask_no) if ask_no and ask_no > 0 else bid_no
            ok_buy = await self._place_buy_limit("no", buy_px, buy_sz, "HEDGE_BUY_NO")
            if not ok_buy:
                return
            sell_yes = self.rem_yes
            ok_sell = await self._place_sell_limit("yes", bid_yes, sell_yes, "HEDGE_SELL_YES")
            if ok_sell:
                self.hedge_executed = True
                print(f"  {self.tag} [HEDGE] buy NO + sell YES complete (no further stair sells)")
            return

        self.hedge_cycles_strike_lt_spot = 0
        self.hedge_cycles_strike_gt_spot = 0

    async def _on_tick_inner(self, bid_yes: float, bid_no: float, remaining_s: float) -> None:
        if self.hedge_executed:
            return
        cfg = self.cfg
        high_side, low_side, high_bid, low_bid = self._high_low(bid_yes, bid_no)
        mx = max(bid_yes, bid_no)
        held_high_bid = (
            self._bid_for_side(bid_yes, bid_no, self.locked_high_side)
            if self.locked_high_side
            else high_bid
        )
        pair_sum_pre = self._pair_sum(bid_yes, bid_no)
        # Final unwind: last N seconds; use pair_sum vs low_sell+threshold when low leg was sold (same as bid_h < threshold).
        unwind_price_too_low = False
        if pair_sum_pre is not None and self.low_sell_price is not None:
            unwind_price_too_low = pair_sum_pre < (
                self.low_sell_price + cfg.final_unwind_if_high_below
            )
        else:
            unwind_price_too_low = held_high_bid < cfg.final_unwind_if_high_below

        # Final unwind: sold something, high not at exit target, last N seconds (once)
        if (
            remaining_s <= cfg.final_seconds_before_end
            and self.any_sell_done
            and unwind_price_too_low
            and not self.final_unwind_fired
        ):
            self.final_unwind_fired = True
            if self.rem_yes >= cfg.min_sell_shares:
                await self._place_sell_limit("yes", bid_yes, self.rem_yes, "FINAL_UNWIND_YES")
            if self.rem_no >= cfg.min_sell_shares:
                await self._place_sell_limit("no", bid_no, self.rem_no, "FINAL_UNWIND_NO")
            return

        # Only operate in the last sell_window (unless already triggered and progressing)
        in_window = remaining_s <= cfg.sell_window_seconds_before_end
        if not self.triggered:
            if not in_window:
                return
            if cfg.trigger_high_bid_min <= mx <= cfg.trigger_high_bid_max:
                self.triggered = True
                print(
                    f"  {self.tag} [SELL_ARM] window={cfg.sell_window_seconds_before_end:g}s "
                    f"max_bid={mx:.4f} in [{cfg.trigger_high_bid_min},{cfg.trigger_high_bid_max}]"
                )
            else:
                return

        # Before low leg sold: consecutive cycles with dominant side above threshold
        if not self.low_sold:
            if mx > cfg.one_side_bid_threshold:
                self.consecutive_above_90 += 1
            else:
                self.consecutive_above_90 = 0
            if self.consecutive_above_90 >= cfg.one_side_consecutive_cycles:
                rem_low = self._rem_for_side(low_side)
                if rem_low >= cfg.min_sell_shares:
                    # Same band as SELL_ARM: high token best bid must be in [trigger_high_bid_min, max]
                    # on this monitoring tick (avoids dumping the low leg while high has run to e.g. 0.98).
                    high_ok = (
                        cfg.trigger_high_bid_min <= high_bid <= cfg.trigger_high_bid_max
                    )
                    if not high_ok:
                        now = time.monotonic()
                        if now - self._last_low_full_skip_log_mono >= 5.0:
                            self._last_low_full_skip_log_mono = now
                            print(
                                f"  {self.tag} [SELL] LOW_FULL_OPPOSITE wait: high_bid={high_bid:.4f} "
                                f"not in [{cfg.trigger_high_bid_min:g},{cfg.trigger_high_bid_max:g}] "
                                f"(consec={self.consecutive_above_90})",
                            )
                    else:
                        px = max(0.01, min(0.99, _snap(low_bid, 6)))
                        ok = await self._place_sell_limit(
                            low_side, low_bid, rem_low, "LOW_FULL_OPPOSITE"
                        )
                        if ok:
                            self.low_sold = True
                            self.locked_high_side = high_side
                            self.low_sell_price = (
                                _snap(self._last_sell_vwap, 6)
                                if self._last_sell_vwap is not None
                                else px
                            )
                            self.consecutive_above_90 = 0
                            self.last_high_bid_snap = None
                            self.high_partial_started = False
            return

        # After low sold: full exit high leg if bid reaches threshold
        hs = self.locked_high_side
        if hs is None:
            return
        bid_h = self._bid_for_side(bid_yes, bid_no, hs)
        rem_high = self._rem_for_side(hs)
        pair_sum = self._pair_sum(bid_yes, bid_no)
        if pair_sum is None:
            if not self._warned_missing_low_sell_price:
                print(
                    f"  {self.tag} [SELL] low_sold set but low_sell_price missing — "
                    "pair_sum unavailable; set low_sell_price on LOW_FULL only",
                )
                self._warned_missing_low_sell_price = True
            if rem_high >= cfg.min_sell_shares and bid_h >= cfg.high_bid_full_sell_threshold:
                await self._place_sell_limit(hs, bid_h, rem_high, "HIGH_FULL_0.98_FALLBACK_BID")
                self.last_high_bid_snap = _snap(bid_h, cfg.high_bid_change_decimals)
            return

        # HIGH_FULL: pair_sum >= low_sell_price + threshold  ⇔  bid_h >= threshold
        if (
            rem_high >= cfg.min_sell_shares
            and pair_sum >= self.low_sell_price + cfg.high_bid_full_sell_threshold
        ):
            await self._place_sell_limit(hs, bid_h, rem_high, "HIGH_FULL_0.98")
            self.last_high_bid_snap = _snap(bid_h, cfg.high_bid_change_decimals)
            return

        # Staged high-leg sells when pair_sum >= threshold (>= so 1.01 exact triggers)
        rem_high = self._rem_for_side(hs)
        if (
            pair_sum >= cfg.sum_bids_above_partial
            and rem_high >= cfg.min_sell_shares
            and cfg.high_partial_max_cycles > 0
        ):
            chunk = self._high_partial_chunk(rem_high)
            nd = cfg.high_bid_change_decimals
            snap_h = _snap(bid_h, nd)

            if not self.high_partial_started:
                await self._place_sell_limit(hs, bid_h, chunk, "HIGH_PARTIAL_FIRST")
                self.high_partial_started = True
                self.last_high_bid_snap = snap_h
                return

            if self.last_high_bid_snap is not None and snap_h != self.last_high_bid_snap:
                await self._place_sell_limit(hs, bid_h, chunk, "HIGH_PARTIAL_ON_BID_CHANGE")
                self.last_high_bid_snap = snap_h
                return

def build_sell_runner(
    cfg: SellStrategyConfig,
    tag: str,
    yes_token_id: str,
    no_token_id: str,
    initial_yes: float,
    initial_no: float,
    clob: Any | None,
    *,
    dry_run: bool,
    paper_trading: bool,
    portfolio_allocation_usdc: float | None = None,
    process_log_path: Path | None = None,
    process_log_interval_s: float = 0.0,
    process_log_stdout: bool = False,
    paper_execution_hook: Any | None = None,
    hedge_require_strike_spot: bool = True,
) -> SellStrategyRunner | None:
    if not cfg.enabled:
        return None
    path = Path(cfg.trade_history_jsonl).resolve() if cfg.trade_history_jsonl.strip() else None
    pa = float(portfolio_allocation_usdc) if portfolio_allocation_usdc is not None else float(initial_yes)
    if pa <= 0:
        pa = float(initial_yes)
    return SellStrategyRunner(
        cfg=cfg,
        tag=tag,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
        rem_yes=float(initial_yes),
        rem_no=float(initial_no),
        clob=clob,
        trade_log_path=path,
        dry_run=dry_run,
        paper_trading=paper_trading,
        portfolio_allocation_usdc=pa,
        process_log_path=process_log_path,
        process_log_interval_s=process_log_interval_s,
        process_log_stdout=process_log_stdout,
        paper_execution_hook=paper_execution_hook,
        hedge_require_strike_spot=hedge_require_strike_spot,
    )
