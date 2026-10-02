"""The runtime engine. Ties SymbolStateStore, the entry pipeline, the exit
pipeline, and the dashboard together into something that actually runs.

Transport-agnostic by design: this module has no knowledge of UW's WebSocket,
Tradier's WebSocket, or any specific wire format. It exposes plain ingest
methods (`on_underlying_tick`, `on_option_quote`, `on_net_flow`, etc.) that a
real stream client calls as messages arrive, and a replay harness calls the
same way when feeding historical data. Wiring an actual live connection later
means writing a thin adapter that parses that transport's messages and calls
these same methods -- it does not mean touching this file.

Strategy evaluation runs on underlying ticks and fresh option bids. The
sandbox broker worker also checks the EOD deadline and market-feed health.

One position per symbol at a time, matching the reference system's own
constraint and keeping the orchestration simple.
"""
from __future__ import annotations

import itertools
import functools
import threading
import time
from datetime import datetime, time as clock_time
from zoneinfo import ZoneInfo
from typing import Callable, Dict, Optional

from .symbol_state import (
    Bar, GexSample, IntervalFlowSample, MarketTideSample, NetFlowSample,
    SymbolStateStore, completed_bars,
)
from .entry_pipeline import (EntryConfig, DEFAULT_ENTRY_CONFIG, evaluate_entry,
                             latest_psar_flip, PSARParams, uw_confluence,
                             compute_psar, momentum_from_bars)
from .exit_pipeline import (ExitConfig, DEFAULT_EXIT_CONFIG, PositionState,
                            evaluate_exit, structure_from_bars)
from .reversal_signals import ReversalSignals, ReversalSettings
from .bid_exit_shadow import read_bid_exit
from .bleed_out_shadow import read_bleed_out_exit
from .contract_selection import OptionEntryResult
from .dashboard_store import DashboardStore


_trade_id_counter = itertools.count(1)


def _serialized(method):
    @functools.wraps(method)
    def call(self, *args, **kwargs):
        with self._state_lock:
            return method(self, *args, **kwargs)
    return call


class TradingRuntime:
    def __init__(self, dashboard: Optional[DashboardStore] = None,
                 entry_config: EntryConfig = DEFAULT_ENTRY_CONFIG,
                 exit_config: ExitConfig = DEFAULT_EXIT_CONFIG,
                 bar_seconds: float = 120.0,
                 order_executor=None,
                 max_market_age_sec: float = 10.0,
                 feed_timeout_sec: float = 15.0,
                 option_quote_timeout_sec: float = 20.0,
                 option_quote_recovery=None,
                 reversal_settings: ReversalSettings = ReversalSettings(),
                 option_entry_price_provider: Optional[Callable[[str, str, float, float], Optional[OptionEntryResult]]] = None,
                 on_trade_opened: Optional[Callable[[str, PositionState], None]] = None,
                 on_trade_closed: Optional[Callable[[str, PositionState], None]] = None,
                 audit=None):
        self._state_lock = threading.RLock()
        self.store = SymbolStateStore(bar_seconds=bar_seconds)
        self.bar_seconds = float(bar_seconds)
        self.dashboard = dashboard if dashboard is not None else DashboardStore()
        self.entry_config = entry_config
        self.exit_config = exit_config
        self.reversal_settings = reversal_settings
        self._reversal_engines: Dict[str, ReversalSignals] = {}
        self.positions: Dict[str, tuple] = {}       # underlying symbol -> (trade_id, PositionState)
        # occ option symbol -> underlying symbol. Quotes arrive keyed by the
        # specific contract (e.g. "AAPL260115C00150000"), but positions are
        # tracked by underlying -- this is what lets on_option_quote() find
        # the right position from a contract-level quote.
        self._occ_to_underlying: Dict[str, str] = {}
        # A real wire-up supplies this: (symbol, direction, now, underlying_price)
        # -> option ask at the moment of fill, sourced from a live option-chain
        # /quote feed -- the same ask-first rule validated throughout this
        # codebase. underlying_price is passed through explicitly (from the
        # SymbolSnapshot already in hand here) because contract selection
        # needs it to pick a strike; inferring it indirectly inside the
        # provider was tried and rejected while building contract_selection.py.
        # Without a working provider, TAKE decisions are refused rather than
        # silently priced off the underlying, which would look plausible and
        # be wrong.
        self.option_entry_price_provider = option_entry_price_provider
        self.order_executor = order_executor
        self.max_market_age_sec = max_market_age_sec
        self.feed_timeout_sec = feed_timeout_sec
        self.option_quote_timeout_sec = option_quote_timeout_sec
        self.option_quote_recovery = option_quote_recovery
        self._last_underlying_wall: Dict[str, float] = {}
        self._feed_warning: set[str] = set()
        self.pending_symbols: set[str] = set()
        self.blocked_symbols: set[str] = set()
        self._processed_flip: Dict[str, tuple] = {}
        self.on_trade_opened = on_trade_opened
        self.on_trade_closed = on_trade_closed
        self.audit = audit
        self._exit_audit_state = {}
        self._bid_shadow_first = {}
        self._bleed_out_shadow_first = {}
        self._connected = None
        self._unavailable_log = {}
        self._position_mark_wall = {}
        self._latest_option_quote = {}
        self._option_feed_warning = set()
        self._option_recovery_last = {}
        self._session_closed_logged = set()
        self._last_observation_wall = {}
        self._last_observation_bar = {}
        self._uw_health_state = {}

    @staticmethod
    def _entry_session(ts: float) -> bool:
        et = datetime.fromtimestamp(ts, ZoneInfo("America/New_York"))
        return et.weekday() < 5 and clock_time(9, 30) <= et.time() < clock_time(15, 30)

    @staticmethod
    def _regular_session(ts: float) -> bool:
        et = datetime.fromtimestamp(ts, ZoneInfo("America/New_York"))
        return et.weekday() < 5 and clock_time(9, 30) <= et.time() < clock_time(16, 0)

    def _quote_fresh(self, trade_id: str, now_wall: float) -> bool:
        quote = self._latest_option_quote.get(trade_id)
        return bool(quote and 0 <= now_wall - quote["market_ts"] <= self.option_quote_timeout_sec
                    and 0 <= now_wall - quote["received_at_ts"] <= self.option_quote_timeout_sec)

    def _position_context(self, symbol, trade_id, pos, snap, now, quote_fresh):
        """All inputs and counterfactual UW context needed for an EOD review.

        UW shadow fields are observations only; evaluate_exit remains the sole
        source of executable exit decisions.
        """
        quote = self._latest_option_quote.get(trade_id, {})
        spread = pos.last_quote_spread if quote_fresh else 0.0
        earned = max(0.0, pos.peak_option_price - pos.entry_option_price)
        peak_spread = pos.peak_quote_spread or spread
        trail = max(self.exit_config.spread_trail_multiple * peak_spread,
                    self.exit_config.peak_profit_giveback_fraction * earned)
        giveback = max(0.0, pos.peak_option_price - pos.current_option_price)
        bars = completed_bars(snap.bars,
                              snap.bars[-1].ts-snap.bars[-2].ts if len(snap.bars) >= 2 else 120., now)
        psar = compute_psar(bars, self.exit_config.psar) if snap.price_state == snap.bar_state == "FRESH" and len(bars) >= 3 else ()
        opposite = "PUT" if pos.direction == "CALL" else "CALL"
        uw = uw_confluence(snap, opposite)
        last_bar = bars[-1] if bars else None
        frame = (self._reversal_frame(symbol, bars, emit_events=False)
                 if snap.price_state == snap.bar_state == "FRESH"
                 else (self._reversal_engines[symbol].frames[-1]
                       if symbol in self._reversal_engines and self._reversal_engines[symbol].frames else None))
        marker = next((e for e in frame.events if e.kind == "MOMENTUM_COMPLETE"), None) if frame else None
        reversal_side, reversal_perfected = (marker.direction, marker.perfected) if marker else ("", False)
        reversal_opposes = bool(last_bar and reversal_side == opposite and
                                last_bar.ts >= pos.opened_ts and
                                snap.price_state == snap.bar_state == "FRESH")
        return dict(symbol=symbol, trade_id=trade_id, occ_symbol=pos.occ_symbol,
                    direction=pos.direction, market_ts=now, opened_ts=pos.opened_ts,
                    underlying_price=snap.price, price_state=snap.price_state,
                    bar_state=snap.bar_state, completed_bar_count=len(bars),
                    completed_bar=vars(last_bar).copy() if last_bar else None,
                    reversal_phase_side=reversal_side or None,
                    reversal_phase_perfected=reversal_perfected,
                    reversal_phase_opposes_position=reversal_opposes,
                    reversal_bid_retreat=round(giveback, 4),
                    reversal_perfect_exit_candidate=bool(reversal_opposes and reversal_perfected and
                                                         quote_fresh and pos.current_option_price > 0),
                    psar_direction=psar[-1].direction if psar else None,
                    structure=structure_from_bars(bars) if bars else "MIXED",
                    momentum=momentum_from_bars(bars) if bars else "NOT_READY",
                    option_bid=pos.current_option_price, option_ask=quote.get("ask"),
                    option_quote_market_ts=quote.get("market_ts"),
                    option_quote_received_at_ts=quote.get("received_at_ts"),
                    option_quote_fresh=quote_fresh,
                    option_quote_age_sec=round(time.time()-quote["market_ts"], 3) if quote.get("market_ts") else None,
                    option_spread=pos.last_quote_spread, peak_option_spread=pos.peak_quote_spread,
                    entry_live_ask=pos.entry_option_price, broker_entry_fill=pos.broker_entry_fill or None,
                    peak_bid=pos.peak_option_price, last_new_peak_ts=pos.last_new_peak_ts,
                    gain_pct=pos.gain_pct(), peak_gain_pct=pos.peak_gain_pct(),
                    armed=pos.armed, earned_premium=earned, giveback_premium=giveback,
                    arm_required_gain=max(self.exit_config.spread_arm_multiple * peak_spread,
                                          self.exit_config.premium_arm_fraction * pos.entry_option_price),
                    active_trail=trail if spread > 0 else None,
                    uw_flow_30s=snap.flow_30s, uw_flow_1m=snap.flow_1m,
                    uw_flow_3m=snap.flow_3m, uw_flow_5m=snap.flow_5m,
                    uw_net_flow_state=snap.net_flow_state,
                    uw_interval_direction=snap.interval_direction,
                    uw_aggressor_direction=snap.aggressor_direction,
                    uw_aggressor_strength=snap.aggressor_strength,
                    uw_delta_flow_direction=snap.delta_flow_direction,
                    uw_vega_flow_direction=snap.vega_flow_direction,
                    uw_avg_dte=snap.avg_dte, uw_interval_flow_state=snap.interval_flow_state,
                    uw_market_tide_direction=snap.market_tide_direction,
                    uw_market_tide_state=snap.market_tide_state,
                    uw_opposite_ready=uw.ready, uw_opposite_supports=uw.supports,
                    uw_opposite_agreeing=uw.agreeing_signals,
                    uw_shadow_tightened_trail=trail/2 if uw.supports and pos.armed and spread > 0 else None,
                    uw_shadow_exit=bool(uw.supports and pos.armed and spread > 0
                                        and giveback >= trail/2 - self.exit_config.epsilon),
                    gamma_path=snap.gamma_path, gex_state=snap.gex_state)

    def option_feed_view(self) -> list[dict]:
        with self._state_lock:
            return [{"symbol": symbol, "side": "", "option": pos.occ_symbol,
                     "status": "OPTION_FEED_STALE", "order_id": pos.entry_order_id,
                     "detail": "No fresh option bid; new entries paused, REST recovery active"}
                    for symbol, (_, pos) in self.positions.items()
                    if symbol in self._option_feed_warning]

    def _audit_signal(self, symbol, direction, decision, now, source, bar_ts, snap):
        if self.dashboard.record_signal(symbol, direction, decision, now, source, bar_ts):
            if self.audit:
                signal_bar = next((b for b in reversed(snap.bars) if b.ts == bar_ts), None)
                self.audit.emit("SIGNAL_DECISION", symbol=symbol, direction=direction,
                                source=source, action=decision.action, reasons=list(decision.reasons),
                                bar_ts=bar_ts, signal_bar=vars(signal_bar).copy() if signal_bar else None,
                                market_ts=now, underlying_price=snap.price,
                                price_state=snap.price_state, bar_state=snap.bar_state,
                                momentum=decision.momentum_state,
                                target_price=decision.target.target if decision.target else None,
                                target_source=decision.target.source if decision.target else None,
                                underlying_stop=decision.target.stop if decision.target else None,
                                underlying_rr=decision.target.rr if decision.target else None,
                                uw_ready=decision.uw.ready, uw_supports=decision.uw.supports,
                                uw_flow_30s=snap.flow_30s, uw_flow_1m=snap.flow_1m,
                                uw_flow_3m=snap.flow_3m, uw_flow_5m=snap.flow_5m,
                                uw_net_flow_state=snap.net_flow_state,
                                uw_interval_flow_state=snap.interval_flow_state,
                                uw_aggressor_direction=snap.aggressor_direction,
                                uw_aggressor_strength=snap.aggressor_strength,
                                uw_opposite_votes=decision.uw.agreeing_signals,
                                uw_market_tide_state=snap.market_tide_state,
                                uw_market_tide_direction=snap.market_tide_direction,
                                gamma_path=snap.gamma_path, gex_state=snap.gex_state)

    def _audit_uw_sample(self, symbol, sample_type, ts, raw):
        if self.audit and symbol in self.positions:
            trade_id, pos = self.positions[symbol]
            evaluation_ts = time.time() if self.order_executor else ts
            snap = self.store.snapshot(symbol, now=evaluation_ts)
            self.audit.emit("UW_POSITION_SAMPLE", durable=False, sample_type=sample_type,
                            sample_market_ts=ts, raw=raw,
                            **self._position_context(symbol, trade_id, pos, snap, evaluation_ts,
                                                     self._quote_fresh(trade_id, time.time())
                                                     if self.order_executor else bool(self._latest_option_quote.get(trade_id))))

    # -- ingestion: called by a real stream client or a replay driver --

    @_serialized
    def on_underlying_tick(self, symbol: str, ts: float, price: float, volume: float = 0.0) -> None:
        if self.order_executor and abs(time.time() - ts) > self.max_market_age_sec:
            return  # delayed or future market event cannot trigger broker orders
        self._last_underlying_wall[symbol] = time.time()
        if symbol in self._feed_warning:
            self._feed_warning.discard(symbol)
            if self.audit:
                self.audit.emit("FEED_RECOVERED", symbol=symbol, market_ts=ts)
        self.store.ingest_tick(symbol, ts, price, volume=volume)
        self._evaluate_symbol(symbol, now=ts)

    @_serialized
    def on_option_quote(self, occ_symbol: str, ts: float, bid: float, ask: float) -> None:
        """Updates the matching open position's price, using the validated
        ask-first-entry / bid-first-current-and-peak discipline. Entry price
        is captured once by _open_position(), not here.

        Keyed by the option contract's OWN symbol (e.g.
        "AAPL260115C00150000"), not the underlying -- quotes arrive this way
        from a real stream subscribed to the specific contract a position is
        holding. Looks up which underlying position that contract belongs to
        via _occ_to_underlying; a quote for a contract with no tracked
        position (already closed, or never opened) is a no-op, not an error.
        """
        underlying = self._occ_to_underlying.get(occ_symbol)
        if underlying is None:
            return
        entry = self.positions.get(underlying)
        if entry is None:
            return
        trade_id, pos = entry
        mark = bid  # a long option can only be closed at the bid
        received_at = time.time()
        previous_quote = self._latest_option_quote.get(trade_id)
        invalid = ("NONPOSITIVE_BID" if mark <= 0 else
                   "STALE_TIMESTAMP" if self.order_executor and abs(received_at - ts) > self.max_market_age_sec else
                   "OUT_OF_ORDER_TIMESTAMP" if previous_quote and ts < previous_quote["market_ts"] else None)
        if self.audit:
            self.audit.emit("OPTION_QUOTE", durable=False, symbol=underlying,
                            trade_id=trade_id, occ_symbol=occ_symbol, market_ts=ts,
                            received_at_ts=received_at, bid=bid, ask=ask,
                            quote_age_sec=round(received_at-ts, 3), accepted=invalid is None,
                            rejected_reason=invalid,
                            spread_valid=ask > bid > 0,
                            spread_warning="ASK_BELOW_BID" if ask < bid else
                                           "NO_VALID_ASK" if ask <= 0 else None)
        if invalid:
            return
        if underlying in self._option_feed_warning:
            self._option_feed_warning.discard(underlying)
            if self.audit:
                self.audit.emit("OPTION_FEED_RECOVERED", symbol=underlying, trade_id=trade_id,
                                market_ts=ts)
        self._latest_option_quote[trade_id] = {"bid": bid, "ask": ask,
                                                "market_ts": ts, "received_at_ts": received_at}
        pos.update_price(mark, ts, ask=ask)
        if self.audit and time.time() - self._position_mark_wall.get(trade_id, 0) >= 30:
            self._position_mark_wall[trade_id] = time.time()
            self.audit.emit("POSITION_MARK", symbol=underlying, trade_id=trade_id,
                            occ_symbol=occ_symbol, market_ts=ts, option_bid=mark,
                            entry_live_ask=pos.entry_option_price,
                            option_ask=ask, option_spread=pos.last_quote_spread,
                            peak_bid=pos.peak_option_price, last_new_peak_ts=pos.last_new_peak_ts,
                            gain_pct=round(pos.gain_pct(), 2),
                            peak_gain_pct=round(pos.peak_gain_pct(), 2))
        if self.order_executor:
            self.order_executor.remember_position(pos)
        self._evaluate_symbol(underlying, now=ts, entries_allowed=False,
                              observation_source="OPTION_QUOTE")

    @_serialized
    @_serialized
    def on_clock(self, now: Optional[float] = None) -> None:
        """Independent EOD check and feed-health alarm; never creates entries."""
        now = time.time() if now is None else now
        if not self.order_executor:
            return
        from .exit_pipeline import _past_et_cutoff
        et = datetime.fromtimestamp(now, ZoneInfo("America/New_York"))
        if (et.weekday() < 5 and et.hour < 16 and
                _past_et_cutoff(now, self.exit_config.eod_force_close_et)):
            for symbol in list(self.positions):
                if symbol not in self.pending_symbols and symbol not in self.blocked_symbols:
                    self._evaluate_symbol(symbol, now=now, entries_allowed=False, eod_only=True)
            return  # do not delay a forced close with a REST quote recovery
        if self._regular_session(now):
            for symbol, (trade_id, pos) in list(self.positions.items()):
                if self.audit:
                    state = self.store.snapshot(symbol, now=now)
                    health = (state.net_flow_state, state.interval_flow_state,
                              state.market_tide_state, state.gex_state)
                    if self._uw_health_state.get(trade_id) != health:
                        self._uw_health_state[trade_id] = health
                        self.audit.emit("UW_FEED_HEALTH", symbol=symbol, trade_id=trade_id,
                                        net_flow_state=health[0], interval_flow_state=health[1],
                                        market_tide_state=health[2], gex_state=health[3],
                                        market_ts=now)
                if self._quote_fresh(trade_id, time.time()):
                    continue
                age = round(time.time() - (self._latest_option_quote.get(trade_id) or {}).get(
                    "received_at_ts", pos.opened_ts), 1)
                if age < self.option_quote_timeout_sec:
                    continue
                if symbol not in self._option_feed_warning:
                    self._option_feed_warning.add(symbol)
                    if self.audit:
                        self.audit.emit("OPTION_FEED_STALE", symbol=symbol, trade_id=trade_id,
                                        occ_symbol=pos.occ_symbol, seconds_since_quote=age,
                                        entries_paused=True)
                    print(f"[market-data] OPTION FEED STALE {symbol} {pos.occ_symbol}; new entries paused", flush=True)
                if (self.option_quote_recovery and
                        time.time() - self._option_recovery_last.get(trade_id, 0) >= 15):
                    self._option_recovery_last[trade_id] = time.time()
                    try:
                        quote = self.option_quote_recovery(pos.occ_symbol)
                        if self.audit:
                            self.audit.emit("OPTION_QUOTE_RECOVERY_ATTEMPT", symbol=symbol,
                                            trade_id=trade_id, quote_returned=bool(quote))
                        if quote:
                            self.on_option_quote(pos.occ_symbol, *quote)
                    except Exception as exc:
                        if self.audit:
                            self.audit.emit("OPTION_QUOTE_RECOVERY_ERROR", symbol=symbol,
                                            trade_id=trade_id, error=str(exc))
        for symbol in list(self.store.symbols()) if self._regular_session(now) else []:
            last = self._last_underlying_wall.get(symbol)
            if last is not None and time.time() - last > self.feed_timeout_sec and symbol not in self._feed_warning:
                self._feed_warning.add(symbol)
                if self.audit:
                    self.audit.emit("FEED_STALE", symbol=symbol, seconds_since_tick=round(time.time() - last, 1))
                print(f"[market-data] STALE {symbol}: no fresh underlying tick; entries paused", flush=True)

    @_serialized
    def on_net_flow(self, symbol: str, ts: float, dir_delta_flow: float) -> None:
        self.store.ingest_net_flow(symbol, NetFlowSample(ts=ts, dir_delta_flow=dir_delta_flow))
        self._audit_uw_sample(symbol, "NET_FLOW", ts, {"dir_delta_flow": dir_delta_flow})

    @_serialized
    def on_interval_flow(self, symbol: str, sample: IntervalFlowSample) -> None:
        self.store.ingest_interval_flow(symbol, sample)
        self._audit_uw_sample(symbol, "INTERVAL_FLOW", sample.ts, vars(sample).copy())

    @_serialized
    def on_market_tide(self, ts: float, net_call_premium: float, net_put_premium: float) -> None:
        for symbol in self.store.symbols():
            self.store.ingest_market_tide(symbol, MarketTideSample(
                ts=ts, net_call_premium=net_call_premium, net_put_premium=net_put_premium))
            self._audit_uw_sample(symbol, "MARKET_TIDE", ts,
                                  {"net_call_premium": net_call_premium,
                                   "net_put_premium": net_put_premium})

    @_serialized
    def on_gex(self, symbol: str, sample: GexSample) -> None:
        self.store.ingest_gex(symbol, sample)
        self._audit_uw_sample(symbol, "GEX", sample.ts, vars(sample).copy())

    def set_connected(self, connected: bool) -> None:
        if connected != self._connected:
            self._connected = connected
            if self.audit:
                self.audit.emit("FEED_CONNECTION", connected=connected)
        self.store.set_connected(connected)

    @_serialized
    def seed_history(self, symbol: str, bars: list[Bar]) -> None:
        """Prime completed bars without replaying a pre-startup PSAR trade."""
        for bar in bars:
            self.store.ingest_bar(symbol, bar)
        self._reversal_frame(symbol, tuple(bars), emit_events=False)
        if bars:
            flip = latest_psar_flip(tuple(bars), self.entry_config.psar)
            if flip:
                self._processed_flip[symbol] = (bars[-1].ts, flip.direction)

    # -- core orchestration --

    def _reversal_frame(self, symbol, bars, *, emit_events=True):
        """Advance each symbol once per closed bar; never replay past alerts."""
        engine = self._reversal_engines.setdefault(symbol, ReversalSignals(self.reversal_settings))
        if bars and engine.bars and bars[-1].ts < engine.bars[-1].ts:
            # A delayed option quote can evaluate with an older market time;
            # it must not rewind state or reissue completed-bar triggers.
            return engine.frames[-1]
        for bar in bars:
            if engine.bars and bar.ts <= engine.bars[-1].ts:
                continue
            if engine.bars and bar.ts - engine.bars[-1].ts > self.bar_seconds * 1.5:
                engine = self._reversal_engines[symbol] = ReversalSignals(self.reversal_settings)
            frame = engine.update(bar)
            if emit_events:
                for event in frame.events:
                    if event.kind not in {"MOMENTUM_COUNT", "EXHAUSTION_COUNT"}:
                        self.dashboard.record_reversal(symbol, event)
            if emit_events and self.audit:
                for event in frame.events:
                    if event.kind not in {"MOMENTUM_COUNT", "EXHAUSTION_COUNT"}:
                        self.audit.emit("REVERSAL_SIGNAL", durable=False, symbol=symbol,
                                        market_ts=bar.ts, trigger=event.kind,
                                        direction=event.direction, perfected=event.perfected,
                                        count=event.count, level=event.value,
                                        visible=event.visible, pine_alert=event.alert,
                                        detail=event.detail)
        return engine.frames[-1] if engine.frames else None

    def _evaluate_symbol(self, symbol: str, now: float,
                         entries_allowed: bool = True, eod_only: bool = False,
                         observation_source: str = "UNDERLYING_OR_CLOCK") -> None:
        snap = self.store.snapshot(symbol, now=now)
        indicator_bars = completed_bars(snap.bars, self.bar_seconds, now)
        reversal_frame = (self._reversal_frame(symbol, indicator_bars)
                          if snap.price_state == snap.bar_state == "FRESH" else None)

        if symbol in self.pending_symbols or symbol in self.blocked_symbols:
            if self.audit and symbol in self.positions and observation_source == "OPTION_QUOTE":
                trade_id, pos = self.positions[symbol]
                self.audit.emit("POSITION_OBSERVATION", durable=False,
                                observation_source=observation_source,
                                evaluation_skipped="PENDING_ORDER" if symbol in self.pending_symbols else "BLOCKED_ORDER",
                                **self._position_context(symbol, trade_id, pos, snap, now,
                                    self._quote_fresh(trade_id, time.time()) if self.order_executor else True))
            self.dashboard.record_scan(snap, None, now=now)
            return

        if symbol in self.positions:
            trade_id, pos = self.positions[symbol]
            quote_fresh = (not self.order_executor or self._quote_fresh(trade_id, time.time()))
            decision = evaluate_exit(snap, pos, now=now, config=self.exit_config,
                                     option_quote_fresh=quote_fresh, reversal_frame=reversal_frame)
            if self.audit:
                shadow = read_bid_exit(pos, now, quote_fresh, self.exit_config)
                if shadow.reason and trade_id not in self._bid_shadow_first:
                    self._bid_shadow_first[trade_id] = (now, shadow.reason, pos.current_option_price)
                    self.audit.emit("BID_EXIT_SHADOW_TRIGGER", symbol=symbol, trade_id=trade_id,
                                    reason=shadow.reason, market_ts=now, option_bid=pos.current_option_price,
                                    entry_live_ask=pos.entry_option_price, peak_bid=pos.peak_option_price,
                                    spread=pos.last_quote_spread, trail=shadow.trail,
                                    giveback=shadow.giveback, gain_pct=round(shadow.gain_pct, 2),
                                    peak_gain_pct=round(shadow.peak_gain_pct, 2),
                                    option_quote_fresh=quote_fresh)
                if decision.state == "EXIT_PENDING":
                    self.audit.emit("BID_EXIT_SHADOW_AT_ACTIVE_EXIT", symbol=symbol,
                                    trade_id=trade_id, active_reasons=list(decision.reasons),
                                    shadow_reason=shadow.reason, market_ts=now,
                                    option_bid=pos.current_option_price, entry_live_ask=pos.entry_option_price,
                                    peak_bid=pos.peak_option_price, armed=shadow.armed,
                                    trail=shadow.trail, giveback=shadow.giveback,
                                    option_quote_fresh=quote_fresh)
                bleed = read_bleed_out_exit(pos, quote_fresh, self.exit_config)
                if bleed.reason and trade_id not in self._bleed_out_shadow_first:
                    self._bleed_out_shadow_first[trade_id] = (now, bleed.reason, pos.current_option_price)
                    self.audit.emit("BLEED_OUT_SHADOW_TRIGGER", symbol=symbol, trade_id=trade_id,
                                    reason=bleed.reason, market_ts=now, option_bid=pos.current_option_price,
                                    entry_live_ask=pos.entry_option_price, gain_pct=round(bleed.gain_pct, 2),
                                    peak_gain_pct=round(bleed.peak_gain_pct, 2), option_quote_fresh=quote_fresh)
                if decision.state == "EXIT_PENDING":
                    self.audit.emit("BLEED_OUT_SHADOW_AT_ACTIVE_EXIT", symbol=symbol,
                                    trade_id=trade_id, active_reasons=list(decision.reasons),
                                    shadow_reason=bleed.reason, market_ts=now,
                                    gain_pct=round(bleed.gain_pct, 2), peak_gain_pct=round(bleed.peak_gain_pct, 2),
                                    option_quote_fresh=quote_fresh)
            if eod_only and "EOD_FORCE_CLOSE" not in decision.reasons:
                return
            if self.audit:
                closed = completed_bars(snap.bars, self.bar_seconds, now)
                bar_ts = closed[-1].ts if closed else None
                wall = time.time()
                if (observation_source == "OPTION_QUOTE" or decision.state == "EXIT_PENDING"
                        or self._last_observation_bar.get(trade_id) != bar_ts
                        or wall - self._last_observation_wall.get(trade_id, 0) >= 30):
                    self._last_observation_bar[trade_id] = bar_ts
                    self._last_observation_wall[trade_id] = wall
                    self.audit.emit("POSITION_OBSERVATION", durable=decision.state == "EXIT_PENDING",
                                    observation_source=observation_source,
                                    decision_state=decision.state,
                                    decision_reasons=list(decision.reasons),
                                    **self._position_context(symbol, trade_id, pos, snap, now, quote_fresh))
            state = (decision.state, tuple(decision.reasons))
            if self.audit and (state != self._exit_audit_state.get(trade_id) or decision.state == "EXIT_PENDING"):
                quote = self._latest_option_quote.get(trade_id, {})
                self.audit.emit("EXIT_DECISION", symbol=symbol, trade_id=trade_id,
                                state=decision.state, reasons=list(decision.reasons), market_ts=now,
                                option_bid=pos.current_option_price, entry_fill=pos.entry_option_price,
                                peak_option_bid=pos.peak_option_price,
                                option_spread=pos.last_quote_spread,
                                peak_option_spread=pos.peak_quote_spread,
                                profit_trail=max(self.exit_config.spread_trail_multiple *
                                                 (pos.peak_quote_spread or pos.last_quote_spread),
                                                 self.exit_config.peak_profit_giveback_fraction *
                                                 max(0.0, pos.peak_option_price-pos.entry_option_price)),
                                option_giveback=max(0.0, pos.peak_option_price-pos.current_option_price),
                                profit_armed=pos.armed, last_new_peak_ts=pos.last_new_peak_ts,
                                gain_pct=round(decision.gain_pct, 2),
                                peak_gain_pct=round(pos.peak_gain_pct(), 2),
                                underlying_price=snap.price, price_state=snap.price_state,
                                option_ask=quote.get("ask"), option_quote_market_ts=quote.get("market_ts"),
                                option_quote_fresh=quote_fresh,
                                option_quote_age_sec=round(time.time()-quote["market_ts"], 3)
                                    if quote.get("market_ts") else None)
                self._exit_audit_state[trade_id] = state
            self.dashboard.record_exit_check(trade_id, pos, decision, now=now,
                                             finalize=self.order_executor is None)
            if self.order_executor:
                self.order_executor.remember_position(pos)
            if decision.state == "EXIT_PENDING":
                if self.order_executor:
                    self.pending_symbols.add(symbol)
                    try:
                        quote = self._latest_option_quote.get(trade_id, {})
                        observation = {"bid": pos.current_option_price if pos.current_option_price > 0 else None,
                                       "ask": quote.get("ask"), "peak_bid": pos.peak_option_price,
                                       "market_ts": quote.get("market_ts"),
                                       "received_at_ts": quote.get("received_at_ts"),
                                       "quote_age_sec": round(time.time()-quote["market_ts"], 3)
                                           if quote.get("market_ts") else None,
                                       "underlying_price": snap.price, "reason": list(decision.reasons)}
                        self.order_executor.submit_exit(symbol, pos, decision.reasons, now,
                                                        observation=observation)
                    except Exception as exc:
                        self.blocked_symbols.add(symbol)
                        self.order_executor.block(symbol, str(exc))
                        print(f"[sandbox] EXIT BLOCKED {symbol}: {exc}", flush=True)
                else:
                    del self.positions[symbol]
                    if pos.occ_symbol:
                        self._occ_to_underlying.pop(pos.occ_symbol, None)
                    # This is the no-order_executor direct-close path -- a
                    # second place a position can be removed, besides the
                    # broker-fill-mediated close below. Found via a genuine
                    # test failure while wiring bleed_out_shadow in: this path
                    # was missing shadow-tracking cleanup entirely, meaning
                    # _bid_shadow_first (pre-existing) and _bleed_out_shadow_
                    # first would both leak a stale entry per trade closed
                    # this way, forever. Fixed here for both.
                    self._bid_shadow_first.pop(trade_id, None)
                    self._bleed_out_shadow_first.pop(trade_id, None)
                    if self.on_trade_closed:
                        self.on_trade_closed(trade_id, pos)
            self.dashboard.record_scan(snap, None, now=now)
            return

        if not entries_allowed:
            return

        session_ts = time.time() if self.order_executor else now
        if not self._entry_session(session_ts):
            key = (symbol, datetime.fromtimestamp(session_ts, ZoneInfo("America/New_York")).date())
            if self.audit and key not in self._session_closed_logged:
                self._session_closed_logged.add(key)
                self.audit.emit("ENTRY_SESSION_CLOSED", symbol=symbol, market_ts=now,
                                cutoff_et="15:30", candidate_cleared=True)
            self.dashboard.record_scan(snap, None, now=now)
            return
        if self.order_executor and self._option_feed_warning:
            self.dashboard.record_scan(snap, None, now=now)
            return
        if snap.price_state != "FRESH" or snap.bar_state != "FRESH":
            return

        closed_bars = completed_bars(snap.bars, bar_seconds=self.bar_seconds, now=now)
        flip = latest_psar_flip(closed_bars, self.entry_config.psar)
        flip_key = (closed_bars[-1].ts, flip.direction) if flip else None
        if flip is None:
            self.dashboard.record_scan(snap, None, now=now)
            return
        previous_flip = self._processed_flip.get(symbol)
        if previous_flip == flip_key:
            self.dashboard.record_scan(snap, None, now=now)
            return
        if previous_flip and previous_flip[1] == flip.direction:
            # A PSAR trend cannot flip twice into the same direction. A
            # recalculated history can report a later bar as another flip;
            # suppress it until a genuine opposite-direction flip occurs.
            if self.audit:
                self.audit.emit("PSAR_HISTORY_DIVERGENCE", symbol=symbol,
                                market_ts=now, previous_bar_ts=previous_flip[0],
                                candidate_bar_ts=flip.ts, direction=flip.direction,
                                reason="CONSECUTIVE_SAME_DIRECTION_FLIP")
            self.dashboard.record_scan(snap, None, now=now)
            return
        if self.audit:
            self.audit.emit("PSAR_FLIP_DETAILS", symbol=symbol, market_ts=now,
                            bar_ts=flip.ts, direction=flip.direction, sar=flip.sar,
                            bar_count=len(closed_bars),
                            recent_bars=[vars(bar).copy() for bar in closed_bars[-3:]],
                            parameters=vars(self.entry_config.psar).copy(),
                            previous_flip=previous_flip)
        self._processed_flip[symbol] = flip_key

        decision = evaluate_entry(snap, flip.direction, now=now, config=self.entry_config)
        self._audit_signal(symbol, flip.direction, decision, now, "PSAR_FLIP", closed_bars[-1].ts, snap)
        if decision.action == "TAKE":
            if not self._open_position(symbol, flip.direction, snap, now):
                self._entry_unavailable(symbol, flip.direction, "LIVE_OPTION_QUOTE_UNAVAILABLE_AT_FLIP", now)
        self.dashboard.record_scan(snap, decision, now=now)

    def _open_position(self, symbol: str, direction: str, snap, now: float) -> bool:
        # Ask-first entry reference: for a long option, ask is what you
        # actually pay -- validated throughout this codebase. Without a real
        # provider wired in, TAKE is refused rather than silently priced off
        # the underlying, which would look plausible on a dashboard and be
        # flatly wrong (underlying price moves are not option price moves).
        if self.option_entry_price_provider is None:
            self._entry_unavailable(symbol, direction, "NO_OPTION_PROVIDER", now)
            return False
        result = self.option_entry_price_provider(symbol, direction, now, snap.price)
        if result is None or result.price <= 0:
            self._entry_unavailable(symbol, direction, "NO_VALID_OPTION_ASK", now)
            return False
        if self.audit:
            self.audit.emit("CONTRACT_SELECTED", symbol=symbol, direction=direction,
                            occ_symbol=result.occ_symbol, ask=result.price, market_ts=now)
        if self.order_executor:
            if not result.occ_symbol:
                self.blocked_symbols.add(symbol)
                self.order_executor.block(symbol, "missing OCC contract")
                print(f"[sandbox] ENTRY BLOCKED {symbol}: missing OCC contract", flush=True)
                return True
            self.pending_symbols.add(symbol)
            try:
                self.order_executor.submit_entry(symbol, direction, result.occ_symbol, now, result.price)
            except Exception as exc:
                self.blocked_symbols.add(symbol)
                self.order_executor.block(symbol, str(exc))
                print(f"[sandbox] ENTRY BLOCKED {symbol}: {exc}", flush=True)
            return True  # no internal position until a broker fill is confirmed
        trade_id = f"{symbol}-{direction}-{next(_trade_id_counter)}"
        pos = PositionState(symbol=symbol, direction=direction, opened_ts=now,
                             entry_option_price=result.price, occ_symbol=result.occ_symbol)
        pos.update_price(result.price, now)
        self.positions[symbol] = (trade_id, pos)
        if result.occ_symbol:
            self._occ_to_underlying[result.occ_symbol] = symbol
        self.dashboard.record_open(trade_id, pos, now=now)
        if self.on_trade_opened:
            self.on_trade_opened(trade_id, pos)
        return True

    def _entry_unavailable(self, symbol, direction, reason, now):
        key = (symbol, direction, reason)
        if self.audit and now - self._unavailable_log.get(key, float("-inf")) >= 120:
            self._unavailable_log[key] = now
            self.audit.emit("ENTRY_UNAVAILABLE", symbol=symbol, reason=reason,
                            direction=direction, market_ts=now)

    @_serialized
    def restore_broker_position(self, record: dict) -> None:
        symbol = record['symbol']
        if symbol in self.positions:
            raise RuntimeError(f"Duplicate restored position for {symbol}")
        live_ask = float(record.get('live_entry_ask') or 0)
        if live_ask <= 0:
            raise RuntimeError(f"{symbol} broker position lacks its live entry ask; reconcile the existing journal before automated exits.")
        pos = PositionState(symbol=symbol, direction=record['direction'],
                            opened_ts=record['opened_ts'], entry_option_price=live_ask,
                            occ_symbol=record['occ_symbol'], quantity=record['quantity'],
                            entry_order_id=record['entry_order_id'],
                            broker_entry_fill=record['entry_fill'])
        pos.current_option_price = float(record.get('current_bid') or 0)
        pos.peak_option_price = float(record.get('peak_bid') or 0)
        pos.armed = bool(record.get('armed'))
        pos.last_new_peak_ts = float(record.get('last_new_peak_ts') or 0)
        pos.last_quote_spread = float(record.get('last_quote_spread') or 0)
        pos.peak_quote_spread = float(record.get('peak_quote_spread') or 0)
        trade_id = record['tag']
        self.positions[symbol] = (trade_id, pos)
        self._occ_to_underlying[pos.occ_symbol] = symbol
        self.dashboard.record_open(trade_id, pos, now=record['opened_ts'])
        if self.on_trade_opened:
            self.on_trade_opened(trade_id, pos)

    @_serialized
    def broker_entry_filled(self, record: dict) -> None:
        if record['symbol'] in self.positions:
            self.pending_symbols.discard(record['symbol'])
            return
        self.pending_symbols.discard(record['symbol'])
        self.restore_broker_position(record)

    @_serialized
    def broker_exit_filled(self, symbol: str, record: dict, fill_price: float) -> None:
        if symbol not in self.positions:
            self.pending_symbols.discard(symbol)
            return
        trade_id, pos = self.positions[symbol]
        if pos.occ_symbol != record['occ_symbol'] or pos.quantity != record['quantity']:
            raise RuntimeError('Close receipt does not match internal position')
        quote = self._latest_option_quote.get(trade_id)
        record["fill_observation"] = ({**quote, "observed_at_ts": time.time()} if quote else None)
        live_bid = (record.get('exit_trigger') or {}).get('bid')
        self.dashboard.close_manually(trade_id, fill_price, tuple(record['exit_reasons']),
                                      live_exit_bid=float(live_bid) if live_bid else None)
        del self.positions[symbol]
        self._occ_to_underlying.pop(pos.occ_symbol, None)
        self._latest_option_quote.pop(trade_id, None)
        self._bid_shadow_first.pop(trade_id, None)
        self._bleed_out_shadow_first.pop(trade_id, None)
        self._option_feed_warning.discard(symbol)
        self.pending_symbols.discard(symbol)
        if self.on_trade_closed:
            self.on_trade_closed(trade_id, pos)

    @_serialized
    def broker_order_rejected(self, symbol: str, record: dict) -> None:
        self.pending_symbols.discard(symbol)
        self.blocked_symbols.add(symbol)
        print(f"[sandbox] {record['side']} rejected for {symbol}; manual review required", flush=True)
