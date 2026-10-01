"""A thread-safe store for the three dashboard views: Scan, Active, Closed.

Decoupled from HOW it gets fed -- a live streaming runtime and a replay/backtest
harness both just call the same `record_*` methods below. The dashboard app
reads only from this store, never from SymbolStateStore/PositionState/etc.
directly, so the read side has one stable, simple contract regardless of what
changes on the pipeline side.

Same lock-then-copy-then-release discipline used throughout this codebase:
writes happen under a brief lock; reads copy out plain dicts and release the
lock before any rendering happens.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .symbol_state import SymbolSnapshot
from .entry_pipeline import EntryDecision
from .exit_pipeline import PositionState, ExitDecision


@dataclass
class ClosedTrade:
    trade_id: str
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    entry_ts: float
    closed_ts: float
    peak_gain_pct: float
    final_gain_pct: float
    exit_reasons: tuple
    quantity: int = 1
    broker_confirmed: bool = False
    broker_entry_fill: float = 0.0
    broker_exit_fill: float = 0.0


class DashboardStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        # symbol -> latest (snapshot, entry_decision, recorded_ts)
        self._scan: Dict[str, tuple] = {}
        # trade_id -> (position, latest exit decision, recorded_ts)
        self._active: Dict[str, tuple] = {}
        self._closed: List[ClosedTrade] = []
        self._signals = deque(maxlen=500)
        self._signal_keys = set()
        self._signal_key_order = deque()

    # -- writes --

    def record_signal(self, symbol: str, direction: str, decision: EntryDecision,
                      now: float, source: str, bar_ts: Optional[float] = None) -> bool:
        with self._lock:
            key = (symbol, direction, source, bar_ts, decision.action,
                   tuple(decision.reasons), decision.momentum_state)
            if key in self._signal_keys:
                return False
            self._signal_keys.add(key)
            self._signal_key_order.append(key)
            if len(self._signal_key_order) > 2000:
                self._signal_keys.discard(self._signal_key_order.popleft())
            self._signals.appendleft({"ts": now, "symbol": symbol,
                "direction": direction, "source": source, "action": decision.action,
                "bar_ts": bar_ts,
                "reasons": list(decision.reasons),
                "momentum": decision.momentum_state,
                "uw_ready": decision.uw.ready, "uw_supports": decision.uw.supports})
            return True

    def signal_view(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._signals)

    def record_scan(self, snap: SymbolSnapshot, decision: Optional[EntryDecision] = None,
                     now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()
        with self._lock:
            prior = self._scan.get(snap.symbol)
            # Ticks update market state continuously; retain the most recent
            # PSAR decision until another flip produces a new one.
            last_decision = decision if decision is not None else (prior[1] if prior else None)
            decision_ts = now if decision is not None else (prior[3] if prior else None)
            self._scan[snap.symbol] = (snap, last_decision, now, decision_ts)

    def record_open(self, trade_id: str, pos: PositionState, now: Optional[float] = None) -> None:
        now = now if now is not None else time.time()
        with self._lock:
            self._active[trade_id] = (pos, None, now)

    def record_exit_check(self, trade_id: str, pos: PositionState, decision: ExitDecision,
                           now: Optional[float] = None, finalize: bool = True) -> None:
        now = now if now is not None else time.time()
        with self._lock:
            if trade_id not in self._active:
                return  # already closed or never opened -- ignore stale updates
            self._active[trade_id] = (pos, decision, now)
            if decision.state == "EXIT_PENDING" and finalize:
                self._close_locked(trade_id, pos, decision, now)

    def _close_locked(self, trade_id: str, pos: PositionState, decision: ExitDecision, now: float) -> None:
        self._active.pop(trade_id, None)
        self._closed.append(ClosedTrade(
            trade_id=trade_id, symbol=pos.symbol, direction=pos.direction,
            entry_price=pos.entry_option_price, exit_price=pos.current_option_price,
            entry_ts=pos.opened_ts, closed_ts=now,
            peak_gain_pct=round(pos.peak_gain_pct(), 2),
            final_gain_pct=round(decision.gain_pct, 2),
            exit_reasons=decision.reasons,
            quantity=pos.quantity, broker_confirmed=bool(pos.entry_order_id),
            broker_entry_fill=pos.broker_entry_fill,
        ))

    def close_manually(self, trade_id: str, exit_price: float, reasons: tuple,
                        now: Optional[float] = None,
                        live_exit_bid: Optional[float] = None) -> None:
        """For a close that isn't the result of evaluate_exit's own EXIT_PENDING
        (e.g. a manual close, or a fill-confirmation path). Kept separate from
        record_exit_check so the two ways a trade can end are both explicit,
        not one silently standing in for the other."""
        now = now if now is not None else time.time()
        with self._lock:
            entry = self._active.pop(trade_id, None)
            if entry is None:
                return
            pos, _, _ = entry
            if not pos.entry_order_id:
                pos.current_option_price = exit_price
            elif live_exit_bid is not None and live_exit_bid > 0:
                pos.current_option_price = live_exit_bid
            else:
                pos.current_option_price = 0.0
            self._closed.append(ClosedTrade(
                trade_id=trade_id, symbol=pos.symbol, direction=pos.direction,
                entry_price=pos.entry_option_price,
                exit_price=pos.current_option_price if pos.entry_order_id else exit_price,
                entry_ts=pos.opened_ts, closed_ts=now,
                peak_gain_pct=round(pos.peak_gain_pct(), 2),
                final_gain_pct=round(pos.gain_pct(), 2),
                exit_reasons=reasons,
                quantity=pos.quantity, broker_confirmed=bool(pos.entry_order_id),
                broker_entry_fill=pos.broker_entry_fill,
                broker_exit_fill=exit_price if pos.entry_order_id else 0.0,
            ))

    # -- reads: plain, JSON-serializable dicts, copied out under the lock --

    def scan_view(self) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._scan.values())
        rows = []
        for snap, decision, recorded_ts, decision_ts in sorted(items, key=lambda x: x[0].symbol):
            rows.append({
                "symbol": snap.symbol,
                "price": snap.price,
                "price_state": snap.price_state,
                "connected": snap.connected,
                "bar_state": snap.bar_state,
                "flow_30s": snap.flow_30s, "flow_1m": snap.flow_1m,
                "flow_3m": snap.flow_3m, "flow_5m": snap.flow_5m,
                "net_flow_state": snap.net_flow_state,
                "gamma_path": snap.gamma_path,
                "gex_state": snap.gex_state,
                "action": decision.action if decision else None,
                "decision_ts": decision_ts,
                "reasons": list(decision.reasons) if decision else [],
                "momentum_state": decision.momentum_state if decision else None,
                "recorded_ts": recorded_ts,
            })
        return rows

    def active_view(self) -> List[Dict[str, Any]]:
        with self._lock:
            items = dict(self._active)
        rows = []
        for trade_id, (pos, decision, recorded_ts) in items.items():
            rows.append({
                "trade_id": trade_id,
                "symbol": pos.symbol,
                "direction": pos.direction,
                "quantity": pos.quantity,
                "occ_symbol": pos.occ_symbol,
                "broker_entry_order_id": pos.entry_order_id,
                "entry_price": pos.entry_option_price,
                "broker_entry_fill": pos.broker_entry_fill if pos.entry_order_id else None,
                "current_price": pos.current_option_price,
                "gain_pct": round(pos.gain_pct(), 2),
                "peak_gain_pct": round(pos.peak_gain_pct(), 2),
                "armed": pos.armed,
                "state": decision.state if decision else "PENDING",
                "reasons": list(decision.reasons) if decision else [],
                "opened_ts": pos.opened_ts,
                "held_sec": round(recorded_ts - pos.opened_ts, 1),
            })
        return sorted(rows, key=lambda r: -r["held_sec"])

    def closed_view(self) -> List[Dict[str, Any]]:
        with self._lock:
            trades = list(self._closed)
        rows = []
        for t in sorted(trades, key=lambda x: -x.closed_ts):
            rows.append({
                "trade_id": t.trade_id, "symbol": t.symbol, "direction": t.direction,
                "entry_price": t.entry_price, "exit_price": t.exit_price,
                "quantity": t.quantity, "broker_confirmed": t.broker_confirmed,
                "live_quote_pnl_usd": round((t.exit_price - t.entry_price) * t.quantity * 100, 2)
                    if t.broker_confirmed and t.exit_price > 0 else None,
                "peak_gain_pct": t.peak_gain_pct, "final_gain_pct": t.final_gain_pct,
                "exit_reasons": list(t.exit_reasons),
                "duration_sec": round(t.closed_ts - t.entry_ts, 1),
            })
        return rows
