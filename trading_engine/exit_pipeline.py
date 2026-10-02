"""The exit ladder. Reads only from a SymbolSnapshot (symbol_state.py) and a
PositionState (this module). Nothing here performs network I/O -- exit
evaluation is event-driven, called whenever new data arrives for a symbol
with an open position, not on a polling schedule. There is no fast loop left
to stall, die, or fall behind -- that entire class of bug (the original
tick-loop stall, the sequential option-quote bottleneck, the 2h14m silent
death from an uncaught PermissionError) is structurally removed by not having
a scheduler at all, not patched around.

Priority order below is a direct, evidenced port of the validated ladder,
re-read against the new streaming state:
  1. EOD_FORCE_CLOSE       -- unconditional, checked first, always
  2. EMERGENCY_OPTION_STOP -- the -30% floor, epsilon-tolerant
  3. OPPOSITE_PSAR_CONFIRMED_BY_PRICE -- PSAR flip + structure/momentum confirms
  3b. OPPOSITE_PSAR_BARE_PROVEN -- see note below, NOT carried forward blindly
  4. THESIS_BROKEN_PRICE_FLOW -- structure+momentum+UW all agree against
  5. PROFIT_GIVEBACK_EXIT  -- live spread and earned premium set the trail
  6. PROFIT_LOCK           -- a watch status, not an exit, once armed
  7. STALL_EXIT            -- unarmed, no new bid high through a completed bar
  default: HOLD / HOLD_STRONG

Two deliberate departures from a straight port, both evidenced by real
investigation, not assumption:

(a) PSAR direction is recomputed fresh from SymbolSnapshot.bars on every call,
    via stage 2's compute_psar()/latest_psar_flip(). It is NOT read from a
    cached, entry-time direction field. Investigating a real gap (six real
    trades in one session peaked 8-12% then gave back 16-26 points each with
    zero protection) surfaced that a Khan/PSAR flip is a discrete EVENT, not
    a continuously-updating STATE -- re-reading a stale snapshot field for it
    during ongoing management is structurally the wrong approach regardless
    of log format. Recomputing fresh is the only correct way to ask "has this
    reversed since I opened."

(b) OPPOSITE_PSAR_BARE_PROVEN is a genuinely new, SEPARATE trigger, not
    silently folded into OPPOSITE_PSAR_CONFIRMED_BY_PRICE. The hypothesis:
    once a position has cleared its observed option spread, a bare opposite PSAR flip -- without
    waiting for full structure/momentum confirmation -- may be a more
    reliable, earlier signal than it would be on an unproven position, where
    a bare flip is known to be noisy. This was reasoned from real data (the
    six gray-zone losses were all eventually caught by the confirmed version,
    just too late) but could NOT be backtested against real data, because the
    old log format doesn't expose a live-recomputable PSAR series to replay
    against. It ships enabled by default but as its own distinct, clearly
    logged reason -- specifically so it can be monitored, compared against
    the confirmed trigger's timing, and disabled in one place
    (require_price_confirmation_after_proven=True) if real data shows it
    firing too early on trades that were still going to recover. Do not treat
    this the way OPPOSITE_PSAR_CONFIRMED_BY_PRICE is treated -- that one is
    validated; this one is reasoned and waiting on evidence.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional, Tuple

from .symbol_state import Bar, SymbolSnapshot, completed_bars
from .entry_pipeline import (
    PSARParams, DEFAULT_PSAR, compute_psar,
    momentum_from_bars, momentum_ok, uw_confluence,
)
from .reversal_signals import ReversalFrame, replay_reversal


def structure_from_bars(bars: Tuple[Bar, ...], lookback: int = 6) -> str:
    """Exit-only higher-high/lower-low confirmation of an opposing move."""
    recent = bars[-lookback:]
    if len(recent) < 3:
        return "MIXED"
    highs = [b.high for b in recent]
    lows = [b.low for b in recent]
    if all(highs[i] >= highs[i-1] and lows[i] >= lows[i-1] for i in range(1, len(recent))):
        return "HH/HL"
    if all(highs[i] <= highs[i-1] and lows[i] <= lows[i-1] for i in range(1, len(recent))):
        return "LH/LL"
    return "MIXED"


def structure_ok(direction: str, structure: str) -> bool:
    return (direction == "CALL" and structure == "HH/HL") or (direction == "PUT" and structure == "LH/LL")


def completed_reversal_phase(bars: Tuple[Bar, ...]) -> Tuple[str, bool]:
    """Compatibility wrapper around the complete independent indicator."""
    if not bars:
        return "", False
    frame = replay_reversal(bars)[-1]
    for event in frame.events:
        if event.kind == "MOMENTUM_COMPLETE":
            return event.direction, event.perfected
    return "", False


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExitConfig:
    eod_force_close_et: str = "15:50"
    emergency_loss_pct: float = -30.0
    spread_arm_multiple: float = 8.0
    # A narrow one-cent spread alone must not arm a trail after a tiny
    # percentage move in an expensive option.
    premium_arm_fraction: float = 0.08
    stall_progress_multiple: float = 2.0
    stall_exit_enabled: bool = False
    spread_trail_multiple: float = 4.0
    peak_profit_giveback_fraction: float = 0.25
    require_price_confirmation_after_proven: bool = False  # see module docstring (b)
    reversal_phase_exit: bool = True
    # Reasoned from replaying 39 real closed trades across 2026-09-30 and
    # 2026-10-01 (bleed_out_shadow.py): a never-armed position whose gain
    # already exceeds this loss, regardless of bar-level peak resets, is a
    # case STALL_EXIT's bar-based "no new high" check can still miss --
    # small intermediate bounces keep resetting last_new_peak_ts while the
    # position quietly grinds to a much larger loss (observed live:
    # unprotected until -17.57% and -26.19% on two real trades). -10.0% is a
    # round-number starting point from that replay, not a tuned value --
    # NOT yet a live gate; see bleed_out_shadow.py's module docstring.
    bleed_out_loss_pct: float = -10.0
    psar: PSARParams = DEFAULT_PSAR
    # Tolerance for price comparisons after floating-point quote arithmetic.
    epsilon: float = 1e-6


DEFAULT_EXIT_CONFIG = ExitConfig()


# ---------------------------------------------------------------------------
# Position state
# ---------------------------------------------------------------------------

@dataclass
class PositionState:
    """Minimal per-position state. Entry price is the production quote's live
    ask at the trigger; the sandbox fill is stored separately for accounting.
    current/peak use the live bid (what a long option can sell for). Armed is
    a one-way gate once the peak clears the measured spread cost.
    """
    symbol: str
    direction: str  # "CALL" or "PUT"
    opened_ts: float
    entry_option_price: float  # ask-first, captured once
    occ_symbol: str = ""  # the specific option contract's own symbol, e.g.
                           # "AAPL260115C00150000" -- needed to subscribe to
                           # this contract's live quotes, distinct from `symbol`
                           # (the underlying)
    quantity: int = 1
    entry_order_id: str = ""
    broker_entry_fill: float = 0.0
    current_option_price: float = 0.0
    peak_option_price: float = 0.0
    armed: bool = False
    last_new_peak_ts: float = 0.0
    stall_confirmed: bool = False
    last_quote_spread: float = 0.0
    peak_quote_spread: float = 0.0

    def update_price(self, price: float, now: float, ask: Optional[float] = None) -> None:
        if price <= 0:
            return
        if ask is not None:
            self.last_quote_spread = ask - price if ask > price else 0.0
        self.current_option_price = price
        if self.last_new_peak_ts == 0.0:
            self.last_new_peak_ts = now
        if price > self.peak_option_price:
            self.peak_option_price = price
            self.last_new_peak_ts = now
            if self.last_quote_spread > 0:
                self.peak_quote_spread = self.last_quote_spread

    def gain_pct(self) -> float:
        if self.entry_option_price <= 0 or self.current_option_price <= 0:
            return 0.0
        return (self.current_option_price / self.entry_option_price - 1.0) * 100.0

    def peak_gain_pct(self) -> float:
        if self.entry_option_price <= 0 or self.peak_option_price <= 0:
            return 0.0
        return (self.peak_option_price / self.entry_option_price - 1.0) * 100.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hm(value: str, default: Tuple[int, int]) -> Tuple[int, int]:
    try:
        h, m = value.split(":")
        return int(h), int(m)
    except Exception:
        return default


def _past_et_cutoff(now: float, cutoff: str) -> bool:
    import datetime as dt
    from zoneinfo import ZoneInfo
    et = dt.datetime.fromtimestamp(now, tz=ZoneInfo("America/New_York"))
    h, m = _hm(cutoff, (15, 50))
    return (et.hour, et.minute) >= (h, m)


@dataclass(frozen=True)
class ExitDecision:
    state: str  # "HOLD", "HOLD_STRONG", "PROFIT_LOCK", or "EXIT_PENDING"
    reasons: Tuple[str, ...]
    gain_pct: float
    peak_gain_pct: float


# ---------------------------------------------------------------------------
# The ladder
# ---------------------------------------------------------------------------

def evaluate_exit(snap: SymbolSnapshot, pos: PositionState, now: Optional[float] = None,
                   config: ExitConfig = DEFAULT_EXIT_CONFIG,
                   option_quote_fresh: bool = True,
                   reversal_frame: Optional[ReversalFrame] = None) -> ExitDecision:
    now = now if now is not None else time.time()

    # 1. EOD -- unconditional, checked first, regardless of anything else below.
    if _past_et_cutoff(now, config.eod_force_close_et):
        return ExitDecision(state="EXIT_PENDING", reasons=("EOD_FORCE_CLOSE",),
                             gain_pct=pos.gain_pct(), peak_gain_pct=pos.peak_gain_pct())

    gain = pos.gain_pct()
    peak = pos.peak_gain_pct()
    eps = config.epsilon

    # 2. Emergency floor.
    if option_quote_fresh and gain <= config.emergency_loss_pct + eps:
        return ExitDecision(state="EXIT_PENDING", reasons=("EMERGENCY_OPTION_STOP",),
                             gain_pct=gain, peak_gain_pct=peak)

    direction = pos.direction
    opposite = "PUT" if direction == "CALL" else "CALL"

    bar_seconds = (snap.bars[-1].ts - snap.bars[-2].ts) if len(snap.bars) >= 2 else 120.0
    bars = completed_bars(snap.bars, bar_seconds=bar_seconds, now=now)
    underlying_fresh = snap.price_state == "FRESH" and snap.bar_state == "FRESH"
    psar_points = compute_psar(bars, config.psar) if underlying_fresh and len(bars) >= 3 else ()
    psar_direction = psar_points[-1].direction if psar_points else None
    opposite_psar = psar_direction == opposite

    structure = structure_from_bars(bars) if underlying_fresh and bars else "MIXED"
    momentum_state = momentum_from_bars(bars) if underlying_fresh and bars else "NOT_READY"
    structure_broken = structure_ok(opposite, structure)  # structure now supports the OTHER side
    opposite_momentum = momentum_ok(opposite, momentum_state)
    uw = uw_confluence(snap, opposite)

    spread = pos.last_quote_spread if option_quote_fresh else 0.0
    peak_spread = pos.peak_quote_spread or spread
    arm_gain = max(config.spread_arm_multiple * peak_spread,
                   config.premium_arm_fraction * pos.entry_option_price)
    if (option_quote_fresh and spread > 0 and peak_spread > 0 and
            pos.peak_option_price >= pos.entry_option_price + arm_gain - eps):
        pos.armed = True

    # The chart's perfected opposing P is actionable at the close of its bar.
    # A red P exits a CALL; a green P exits a PUT. The live bid must be fresh
    # and positive for a broker sell, but a giveback threshold is not required.
    if underlying_fresh and bars:
        frame = (reversal_frame if reversal_frame and reversal_frame.bar_ts == bars[-1].ts
                 else replay_reversal(bars)[-1])
        marker = next((e for e in frame.events if e.kind == "MOMENTUM_COMPLETE"), None)
        reversal_side, perfected = (marker.direction, marker.perfected) if marker else ("", False)
    else:
        reversal_side, perfected = "", False
    if (config.reversal_phase_exit and option_quote_fresh and pos.current_option_price > 0
            and perfected and reversal_side == opposite
            and bars[-1].ts >= pos.opened_ts):
        return ExitDecision(state="EXIT_PENDING", reasons=("OPPOSITE_PERFECT_REVERSAL",),
                             gain_pct=gain, peak_gain_pct=peak)

    # 3. Opposite PSAR, confirmed by price action -- the validated mechanism.
    if opposite_psar and (structure_broken or opposite_momentum):
        return ExitDecision(state="EXIT_PENDING", reasons=("OPPOSITE_PSAR_CONFIRMED_BY_PRICE",),
                             gain_pct=gain, peak_gain_pct=peak)

    # 3b. Bare opposite PSAR on an already-proven position -- reasoned, not yet
    # evidenced; see module docstring (b). Only engages once real progress has
    # been shown, specifically to avoid the noisy-bare-flip problem a fresh,
    # unproven entry has.
    if (option_quote_fresh and opposite_psar and not config.require_price_confirmation_after_proven
            and pos.armed):
        return ExitDecision(state="EXIT_PENDING", reasons=("OPPOSITE_PSAR_BARE_PROVEN",),
                             gain_pct=gain, peak_gain_pct=peak)

    # 4. Thesis broken: structure, momentum, and UW all agree against us.
    bid_weakening = (option_quote_fresh and spread > 0
                     and pos.peak_option_price - pos.current_option_price >= spread - eps)
    if underlying_fresh and structure_broken and opposite_momentum and uw.supports and bid_weakening:
        return ExitDecision(state="EXIT_PENDING", reasons=("THESIS_BROKEN_PRICE_FLOW",),
                             gain_pct=gain, peak_gain_pct=peak)

    # Anchor to the spread seen at the bid high. A widened spread on the
    # falling quote must not move the trailing threshold farther away.
    earned = max(0.0, pos.peak_option_price - pos.entry_option_price)
    trail = max(config.spread_trail_multiple * peak_spread,
                config.peak_profit_giveback_fraction * earned)
    giveback = max(0.0, pos.peak_option_price - pos.current_option_price)
    if pos.armed and option_quote_fresh and spread > 0:
        if giveback >= trail - eps:
            return ExitDecision(state="EXIT_PENDING", reasons=("PROFIT_GIVEBACK_EXIT",),
                                 gain_pct=gain, peak_gain_pct=peak)
        if giveback >= trail / 2 - eps:
            return ExitDecision(state="PROFIT_LOCK", reasons=("PROFIT_GIVEBACK_WATCH",),
                                 gain_pct=gain, peak_gain_pct=peak)
    elif (config.stall_exit_enabled and not pos.armed and option_quote_fresh and spread > 0 and peak_spread > 0
            and underlying_fresh and bars
            and bars[-1].ts >= pos.opened_ts and pos.last_new_peak_ts > 0
            and pos.last_new_peak_ts <= bars[-1].ts
            and pos.peak_option_price <= pos.entry_option_price +
                config.stall_progress_multiple * peak_spread
            and pos.current_option_price <= pos.entry_option_price + spread
            and not momentum_ok(direction, momentum_state)):
        # A complete bar since the fill has passed without a new bid high;
        # direction has stopped expanding and the bid has not cleared spread.
        return ExitDecision(state="EXIT_PENDING", reasons=("STALL_EXIT",),
                             gain_pct=gain, peak_gain_pct=peak)

    momentum_confirms = momentum_ok(direction, momentum_state)
    if not momentum_confirms:
        return ExitDecision(state="HOLD", reasons=("EXPANSION_PAUSED",), gain_pct=gain, peak_gain_pct=peak)
    return ExitDecision(state="HOLD_STRONG", reasons=("THESIS_EXPANDING",), gain_pct=gain, peak_gain_pct=peak)
