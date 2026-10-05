"""PSAR trigger -> confluence check -> approximate target -> entry decision.

Reads only from a SymbolSnapshot (symbol_state.py). Nothing here performs
network I/O or blocks -- every input is already in the streamed, freshness-
gated state built in stage 1.

PSAR uses Pine ta.sar calculation ordering and everget's SAR < close
signal definition. Defaults remain (0.03, 0.02, 0.20); main.py accepts
PSAR_START, PSAR_INCREMENT and PSAR_MAXIMUM. Historical replay results
from the prior implementation do not validate this corrected implementation.

UW confluence is deliberately NOT a straight port of the old majority-vote
derivation. That approach was replayed against a real trading day and agreed
with the legacy system on only 27.8% of the cases where it fired, including
direct directional contradictions. Until a UW confluence rule here has been
shadow-validated the same way, it requires much stronger agreement across
independent signals before contributing anything beyond a supportive bonus,
and never enters as a hard blocker on its own.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .symbol_state import Bar, SymbolSnapshot, completed_bars


# ---------------------------------------------------------------------------
# PSAR
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PSARParams:
    start: float = 0.03
    increment: float = 0.02
    maximum: float = 0.20


DEFAULT_PSAR = PSARParams()


@dataclass(frozen=True)
class PSARPoint:
    ts: float
    sar: float
    direction: str  # "CALL" while price is above SAR, "PUT" while below
    is_flip: bool


def compute_psar(bars: Tuple[Bar, ...], params: PSARParams = DEFAULT_PSAR) -> Tuple[PSARPoint, ...]:
    """Pine ta.sar ordering; signals follow everget's SAR-versus-close rule.

    The first valid SAR is on index 1. Reversal is checked before the
    previous-two-bar clamp, and first trend bars do not increase acceleration.
    """
    if len(bars) < 2:
        return ()
    below = bars[1].close > bars[0].close
    ep = bars[1].high if below else bars[1].low
    sar = bars[0].low if below else bars[0].high
    af = params.start
    previous_direction = None
    out = []
    for i in range(1, len(bars)):
        bar = bars[i]
        first_trend_bar = i == 1
        sar += af * (ep - sar)
        if below and sar > bar.low:
            below = False
            first_trend_bar = True
            sar = max(bar.high, ep)
            ep = bar.low
            af = params.start
        elif not below and sar < bar.high:
            below = True
            first_trend_bar = True
            sar = min(bar.low, ep)
            ep = bar.high
            af = params.start
        if not first_trend_bar:
            if below and bar.high > ep:
                ep = bar.high
                af = min(af + params.increment, params.maximum)
            elif not below and bar.low < ep:
                ep = bar.low
                af = min(af + params.increment, params.maximum)
        if below:
            sar = min(sar, bars[i - 1].low)
            if i > 1:
                sar = min(sar, bars[i - 2].low)
        else:
            sar = max(sar, bars[i - 1].high)
            if i > 1:
                sar = max(sar, bars[i - 2].high)
        direction = "CALL" if sar < bar.close else "PUT"
        out.append(PSARPoint(bar.ts, sar, direction,
                             previous_direction is not None and direction != previous_direction))
        previous_direction = direction
    return tuple(out)


def latest_psar_flip(bars: Tuple[Bar, ...], params: PSARParams = DEFAULT_PSAR) -> Optional[PSARPoint]:
    """The most recent bar-close direction flip, if the last completed bar is
    the one that flipped. Returns None if there is no flip on the latest bar
    (the caller is not currently looking at a fresh trigger) or not enough
    bar history exists yet.
    """
    points = compute_psar(bars, params)
    if not points:
        return None
    latest = points[-1]
    return latest if latest.is_flip else None


# ---------------------------------------------------------------------------
# Structure and momentum, computed directly from completed bars
# ---------------------------------------------------------------------------

def _ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def momentum_from_bars(bars: Tuple[Bar, ...], fast: int = 5, slow: int = 10) -> str:
    """BULL_EXPANDING / BEAR_EXPANDING / BULL_DECELERATING / BEAR_DECELERATING
    / NOT_READY, from a fast/slow EMA of closes plus the fast EMA's own slope.
    Direction comes from fast-vs-slow EMA separation; "expanding" vs
    "decelerating" comes from whether that separation is currently growing or
    shrinking bar over bar.
    """
    closes = [b.close for b in bars]
    if len(closes) < slow + 2:
        return "NOT_READY"
    fast_now = _ema(closes, fast)
    slow_now = _ema(closes, slow)
    fast_prev = _ema(closes[:-1], fast)
    slow_prev = _ema(closes[:-1], slow)
    if fast_now is None or slow_now is None or fast_prev is None or slow_prev is None:
        return "NOT_READY"
    sep_now = fast_now - slow_now
    sep_prev = fast_prev - slow_prev
    direction = "BULL" if sep_now >= 0 else "BEAR"
    expanding = abs(sep_now) >= abs(sep_prev)
    return f"{direction}_{'EXPANDING' if expanding else 'DECELERATING'}"


def momentum_ok(direction: str, momentum_state: str) -> bool:
    return (direction == "CALL" and momentum_state == "BULL_EXPANDING") or \
           (direction == "PUT" and momentum_state == "BEAR_EXPANDING")


# ---------------------------------------------------------------------------
# UW confluence -- deliberately conservative, see module docstring
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class UWConfluenceRead:
    supports: bool          # True only on strong, broad agreement
    opposes: bool           # True only on strong, broad agreement in the other direction
    ready: bool             # False if not enough fresh evidence to say anything at all
    agreeing_signals: int
    total_signals: int


def uw_confluence(snap: SymbolSnapshot, direction: str) -> UWConfluenceRead:
    """Two distinct feed families, not statistically independent votes.

    Net flow contributes ONE read only when its 1m/3m/5m horizons agree
    after five minutes of observations. Interval aggressor activity must
    persist for 30 seconds with >=20% imbalance. A fresh opposing market
    tide vetoes confirmation; market tide never adds another ticker vote.
    This is UW evidence only and never blocks a PSAR entry.
    """
    opposite = "PUT" if direction == "CALL" else "CALL"
    horizons = (snap.flow_1m, snap.flow_3m, snap.flow_5m)
    net = (horizons[0] if snap.net_flow_state == "FRESH"
           and snap.net_flow_coverage_sec >= 300 and len(set(horizons)) == 1
           and horizons[0] in ("CALL", "PUT") else "NOT_READY")
    interval = (snap.aggressor_confirmed_direction
                if snap.interval_flow_state == "FRESH" else "NOT_READY")
    usable = [r for r in (net, interval) if r in ("CALL", "PUT")]
    agree = sum(1 for r in usable if r == direction)
    disagree = sum(1 for r in usable if r == opposite)
    tide = snap.market_tide_direction if snap.market_tide_state == "FRESH" else "NOT_READY"
    return UWConfluenceRead(supports=agree == 2 and tide != opposite,
                             opposes=disagree == 2 and tide != direction,
                             ready=len(usable) == 2,
                             agreeing_signals=agree, total_signals=2)


# ---------------------------------------------------------------------------
# Approximate target
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TargetResult:
    target: float
    stop: float
    rr: float
    source: str  # "GEX_WALL" only; never fabricate a target


def approximate_target(snap: SymbolSnapshot, direction: str, price: float, atr: float) -> Optional[TargetResult]:
    """Assess a fresh, directional GEX level against volatility-based risk.
    A missing GEX level yields no target. A nearby level retains its true R:R
    so evaluate_entry can reject it instead of replacing it with a made-up one.
    """
    if price <= 0 or atr <= 0:
        return None
    risk = max(atr * 0.60, price * 0.0015, 0.01)
    stop = price - risk if direction == "CALL" else price + risk

    gex_target = None
    if snap.gex_state == "FRESH":
        candidates = list(snap.target_candidates)
        wall = snap.call_wall if direction == "CALL" else snap.put_wall
        if wall is not None:
            candidates.append(wall)
        directional = [
            v for v in candidates
            if (direction == "CALL" and v > price) or (direction == "PUT" and 0 < v < price)
        ]
        if directional:
            gex_target = min(directional, key=lambda v: abs(v - price))

    if gex_target is None:
        return None
    rr = abs(gex_target - price) / risk
    return TargetResult(target=gex_target, stop=stop, rr=round(rr, 3), source="GEX_WALL")


# ---------------------------------------------------------------------------
# Entry decision
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EntryDecision:
    action: str  # "TAKE" or "SKIP" on a completed PSAR flip
    symbol: str
    direction: str
    reasons: Tuple[str, ...]
    momentum_state: str
    uw: UWConfluenceRead
    target: Optional[TargetResult]


@dataclass(frozen=True)
class EntryConfig:
    psar: PSARParams = DEFAULT_PSAR
    momentum_fast: int = 5
    momentum_slow: int = 10
    min_rr: float = 1.25


DEFAULT_ENTRY_CONFIG = EntryConfig()


def evaluate_entry(snap: SymbolSnapshot, direction: str, now: Optional[float] = None,
                    config: EntryConfig = DEFAULT_ENTRY_CONFIG) -> EntryDecision:
    """The confluence check. Called once on a completed PSAR flip.
    Never fetches anything -- every
    input comes from `snap`, which is already streamed and freshness-gated.
    """
    now = now if now is not None else time.time()
    reasons: List[str] = []

    if snap.price_state == "NOT_READY" or snap.price is None:
        return EntryDecision(action="SKIP", symbol=snap.symbol, direction=direction,
                              reasons=("PRICE_NOT_READY",),
                              momentum_state="NOT_READY", uw=UWConfluenceRead(False, False, False, 0, 0),
                              target=None)

    bars = completed_bars(snap.bars, bar_seconds=snap.bars[-1].ts - snap.bars[-2].ts if len(snap.bars) >= 2 else 120.0, now=now)
    momentum_state = momentum_from_bars(bars, fast=config.momentum_fast, slow=config.momentum_slow)
    uw = uw_confluence(snap, direction)

    m_ok = momentum_ok(direction, momentum_state)

    if uw.opposes:
        reasons.append("UW_OPPOSING_OBSERVATION")

    if not m_ok:
        reasons.append("MOMENTUM_NOT_EXPANDING_OBSERVATION" if momentum_state != "NOT_READY"
                       else "MOMENTUM_NOT_READY_OBSERVATION")
    else:
        reasons.append("MOMENTUM_EXPANDING")
    if uw.supports:
        reasons.append("UW_SUPPORTIVE")
    elif not uw.ready:
        reasons.append("UW_NOT_READY")

    target = approximate_target(snap, direction, snap.price, atr=_atr_from_bars(bars))
    if target is not None and target.rr < config.min_rr:
        reasons.append("TARGET_RR_TOO_LOW")
        return EntryDecision(action="SKIP", symbol=snap.symbol, direction=direction, reasons=tuple(reasons),
                              momentum_state=momentum_state, uw=uw, target=target)
    if target is None:
        reasons.append("NO_VERIFIED_TARGET")

    reasons.append("ENTRY_AUTHORIZED")
    return EntryDecision(action="TAKE", symbol=snap.symbol, direction=direction, reasons=tuple(reasons),
                          momentum_state=momentum_state, uw=uw, target=target)


def _atr_from_bars(bars: Tuple[Bar, ...], period: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i].high, bars[i].low, bars[i - 1].close
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    window = trs[-period:] if len(trs) >= period else trs
    return sum(window) / len(window) if window else 0.0
