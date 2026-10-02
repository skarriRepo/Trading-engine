"""Independent completed-bar port of Reversal Signals [LuxAlgo] (Pine v5).

Original © LuxAlgo, CC BY-NC-SA 4.0. Adapted to Python from the source supplied
by the user. https://creativecommons.org/licenses/by-nc-sa/4.0/

All calculations run on the engine's own underlying OHLC bars. Pine plot
styles and chart labels become structured events; no TradingView connection,
chart alert, or sandbox quote is read here. Only completed bars may be passed
to update(). The default settings reproduce the supplied script's inputs.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence

from .symbol_state import Bar


@dataclass(frozen=True)
class ReversalSettings:
    momentum_display: str = "Completed"  # Completed, Detailed, None
    support_resistance: bool = True
    level_style: str = "Step Line w/ Diamonds"
    momentum_risk: bool = False
    momentum_risk_style: str = "Circles"
    exhaustion_display: str = "Completed"
    exhaustion_risk: bool = False
    exhaustion_target: bool = False
    trade_setups: str = "None"  # None, Momentum, Exhaustion, Qualified
    setup_warnings: bool = False

    def __post_init__(self) -> None:
        if self.momentum_display not in {"Completed", "Detailed", "None"}:
            raise ValueError("invalid momentum display")
        if self.exhaustion_display not in {"Completed", "Detailed", "None"}:
            raise ValueError("invalid exhaustion display")
        if self.trade_setups not in {"None", "Momentum", "Exhaustion", "Qualified"}:
            raise ValueError("invalid trade setup mode")


@dataclass(frozen=True)
class ReversalEvent:
    kind: str
    direction: str  # CALL (bullish) or PUT (bearish)
    bar_ts: float
    value: Optional[float] = None
    count: int = 0
    perfected: bool = False
    visible: bool = True
    alert: bool = False
    detail: str = ""


@dataclass(frozen=True)
class ReversalFrame:
    bar_ts: float
    bullish_count: int
    bearish_count: int
    bullish_exhaustion: int
    bearish_exhaustion: int
    resistance: float
    support: float
    bullish_momentum_risk: float
    bearish_momentum_risk: float
    bullish_exhaustion_target: float
    bullish_exhaustion_risk: float
    bearish_exhaustion_target: float
    bearish_exhaustion_risk: float
    events: tuple[ReversalEvent, ...]

    def perfected_momentum(self) -> str:
        for event in self.events:
            if event.kind == "MOMENTUM_COMPLETE" and event.perfected:
                return event.direction
        return ""


class ReversalSignals:
    """Stateful sequential evaluator. One instance per symbol and session."""

    def __init__(self, settings: ReversalSettings = ReversalSettings()):
        self.settings = settings
        self.bars: list[Bar] = []
        self.frames: list[ReversalFrame] = []
        self.b_count = self.s_count = 0
        self.b_ex = self.s_ex = 0
        self.resistance = self.support = 0.0
        self.b_mrisk = self.s_mrisk = 0.0
        self.b_target = self.b_risk = self.s_target = self.s_risk = 0.0
        self.b_low = self.b_high_at_low = self.s_high = self.s_low_at_high = None
        self.b_ex_low = self.b_ex_high = self.b_ex_low_at_high = self.b_ex_high_at_low = None
        self.s_ex_low = self.s_ex_high = self.s_ex_low_at_high = self.s_ex_high_at_low = None
        self.b_eight_close = self.s_eight_close = None
        self.b_phase = self.s_phase = False
        self.prev_b_perfect = self.prev_s_perfect = False
        self.b_nines: list[int] = []
        self.s_nines: list[int] = []
        self.b_thirteens: list[int] = []
        self.s_thirteens: list[int] = []
        self.b_setup_pending = self.s_setup_pending = False
        self.b_flip_prev = self.s_flip_prev = False
        self.long_active = self.short_active = False

    def update(self, bar: Bar) -> ReversalFrame:
        if self.bars and bar.ts <= self.bars[-1].ts:
            raise ValueError("reversal bars must be strictly increasing and completed")
        self.bars.append(bar)
        i = len(self.bars) - 1
        s = self.settings
        events: list[ReversalEvent] = []

        def emit(kind: str, direction: str, *, value=None, count=0, perfected=False,
                 visible=True, alert=False, detail="") -> None:
            events.append(ReversalEvent(kind, direction, bar.ts, value, count,
                                        perfected, visible, alert, detail))

        prev_b, prev_s = self.b_count, self.s_count
        prev_be, prev_se = self.b_ex, self.s_ex
        prev_res, prev_sup = self.resistance, self.support
        prev_bmr, prev_smr = self.b_mrisk, self.s_mrisk
        prev_br, prev_sr = self.b_risk, self.s_risk
        prev_bt, prev_st = self.b_target, self.s_target
        # Pine's close[4] is na until five candles exist. Do not invent a
        # bearish count from the first four missing comparisons.
        if i >= 4:
            if bar.close < self.bars[i - 4].close:
                self.b_count, self.s_count = (1 if prev_b == 9 else prev_b + 1), 0
            else:
                self.s_count, self.b_count = (1 if prev_s == 9 else prev_s + 1), 0
        bp = i >= 3 and ((bar.low <= self.bars[i-3].low and bar.low <= self.bars[i-2].low)
                         or (self.bars[i-1].low <= self.bars[i-3].low and self.bars[i-1].low <= self.bars[i-2].low))
        sp = i >= 3 and ((bar.high >= self.bars[i-3].high and bar.high >= self.bars[i-2].high)
                         or (self.bars[i-1].high >= self.bars[i-3].high and self.bars[i-1].high >= self.bars[i-2].high))
        early_b = prev_b == 8 and self.s_count == 1
        early_s = prev_s == 8 and self.b_count == 1

        for direction, count, perfect, early in (("CALL", self.b_count, bp, early_b),
                                                  ("PUT", self.s_count, sp, early_s)):
            if 0 < count < 9 and s.momentum_display == "Detailed":
                emit("MOMENTUM_COUNT", direction, count=count, perfected=perfect,
                     detail="8 perfected" if count == 8 and perfect else "")
            if count == 9:
                emit("MOMENTUM_COMPLETE", direction, count=9, perfected=perfect,
                     visible=s.momentum_display != "None", alert=s.momentum_display == "Completed")
            if early:
                emit("MOMENTUM_EARLY", direction, count=8,
                     visible=s.momentum_display != "None")

        if self.b_count == 9:
            self.b_nines.append(i)
        if self.s_count == 9:
            self.s_nines.append(i)
        if self.b_count == 9 or early_b:
            self.resistance = max(x.high for x in self.bars[max(0, i-8):i+1])
        elif self.resistance and bar.close > self.resistance:
            self.resistance = 0.0
        if self.s_count == 9 or early_s:
            self.support = min(x.low for x in self.bars[max(0, i-8):i+1])
        elif self.support and bar.close < self.support:
            self.support = 0.0
        if self.resistance and self.resistance != prev_res:
            emit("RESISTANCE_SET", "PUT", value=self.resistance, visible=s.support_resistance)
        if self.support and self.support != prev_sup:
            emit("SUPPORT_SET", "CALL", value=self.support, visible=s.support_resistance)
        if self.resistance and not prev_res:
            emit("BEARISH_TREND_MARK", "PUT", value=bar.close, visible=False, alert=True)
        if self.support and not prev_sup:
            emit("BULLISH_TREND_MARK", "CALL", value=bar.close, visible=False, alert=True)

        if self.b_count == 1:
            self.b_low = bar.low
        if self.b_count:
            if self.b_low is None or bar.low <= self.b_low:
                self.b_low, self.b_high_at_low = bar.low, bar.high
        if self.s_count == 1:
            self.s_high = bar.high
        if self.s_count:
            if self.s_high is None or bar.high >= self.s_high:
                self.s_high, self.s_low_at_high = bar.high, bar.low
        if self.b_count == 9 and self.b_high_at_low is not None:
            self.b_mrisk = 2*self.b_low-self.b_high_at_low
        elif (self.b_mrisk and bar.close < self.b_mrisk) or self.s_count == 9:
            self.b_mrisk = 0.0
        if self.s_count == 9 and self.s_low_at_high is not None:
            self.s_mrisk = 2*self.s_high-self.s_low_at_high
        elif (self.s_mrisk and bar.close > self.s_mrisk) or self.b_count == 9:
            self.s_mrisk = 0.0

        b_cond = i >= 2 and bar.close <= self.bars[i-2].low
        b_13 = b_cond and self.b_eight_close is not None and bar.low >= self.b_eight_close
        if self.b_count == 9 and prev_be == 0 and (bp or self.prev_b_perfect):
            self.b_phase = True
        elif self.s_count == 9 or prev_be == 13 or bar.close > self.resistance:
            self.b_phase = False
        if self.b_phase:
            self.b_ex = (1 if b_cond else 0) if self.b_count == 9 else (prev_be+1 if b_cond else prev_be)
        else:
            self.b_ex = 0
        if self.b_ex == 13 and b_13:
            self.b_ex -= 1
        if self.b_ex == 8 and self.b_ex != prev_be:
            self.b_eight_close = bar.close
        if self.b_ex == 1:
            self.b_ex_low = bar.low
            self.b_ex_high = bar.high
        if self.b_phase:
            if self.b_ex_high is None or bar.high >= self.b_ex_high:
                self.b_ex_high, self.b_ex_low_at_high = bar.high, bar.low
            if self.b_ex_low is None or bar.low <= self.b_ex_low:
                self.b_ex_low, self.b_ex_high_at_low = bar.low, bar.high
        if self.b_ex == 13 and prev_be != 13:
            self.b_thirteens.append(i)
            emit("EXHAUSTION_COMPLETE", "CALL", count=13,
                 visible=s.exhaustion_display != "None", alert=s.exhaustion_display == "Completed")
        elif self.b_phase and self.b_ex != prev_be and 0 < self.b_ex < 13 and s.exhaustion_display == "Detailed":
            emit("EXHAUSTION_COUNT", "CALL", count=self.b_ex)
        if self.b_ex == 12 and prev_be == 12 and b_13 and s.exhaustion_display == "Detailed":
            emit("EXHAUSTION_DEFERRED", "CALL", count=12)
        if self.b_ex == 13 and self.b_ex_high is not None and self.b_ex_low_at_high is not None:
            self.b_target = 2*self.b_ex_high-self.b_ex_low_at_high
        elif (self.b_target and bar.close > self.b_target) or (prev_br == 0 and prev_se == 13):
            self.b_target = 0.0
        if self.b_ex == 13 and self.b_ex_low is not None and self.b_ex_high_at_low is not None:
            self.b_risk = 2*self.b_ex_low-self.b_ex_high_at_low
        elif (self.b_risk and bar.close < self.b_risk) or (self.b_target == 0 and prev_se == 13):
            self.b_risk = 0.0

        s_cond = i >= 2 and bar.close >= self.bars[i-2].high
        s_13 = s_cond and self.s_eight_close is not None and bar.high <= self.s_eight_close
        if self.s_count == 9 and prev_se == 0 and (sp or self.prev_s_perfect):
            self.s_phase = True
        elif self.b_count == 9 or prev_se == 13 or bar.close < self.support:
            self.s_phase = False
        if self.s_phase:
            self.s_ex = (1 if s_cond else 0) if self.s_count == 9 else (prev_se+1 if s_cond else prev_se)
        else:
            self.s_ex = 0
        if self.s_ex == 13 and s_13:
            self.s_ex -= 1
        if self.s_ex == 8 and self.s_ex != prev_se:
            self.s_eight_close = bar.close
        if self.s_ex == 1:
            self.s_ex_low = bar.low
            self.s_ex_high = bar.high
        if self.s_phase:
            if self.s_ex_high is None or bar.high >= self.s_ex_high:
                self.s_ex_high, self.s_ex_low_at_high = bar.high, bar.low
            if self.s_ex_low is None or bar.low <= self.s_ex_low:
                self.s_ex_low, self.s_ex_high_at_low = bar.low, bar.high
        if self.s_ex == 13 and prev_se != 13:
            self.s_thirteens.append(i)
            emit("EXHAUSTION_COMPLETE", "PUT", count=13,
                 visible=s.exhaustion_display != "None", alert=s.exhaustion_display == "Completed")
        elif self.s_phase and self.s_ex != prev_se and 0 < self.s_ex < 13 and s.exhaustion_display == "Detailed":
            emit("EXHAUSTION_COUNT", "PUT", count=self.s_ex)
        if self.s_ex == 12 and prev_se == 12 and s_13 and s.exhaustion_display == "Detailed":
            emit("EXHAUSTION_DEFERRED", "PUT", count=12)
        if self.s_ex == 13 and self.s_ex_high is not None and self.s_ex_low_at_high is not None:
            self.s_risk = 2*self.s_ex_high-self.s_ex_low_at_high
        elif (self.s_risk and bar.close > self.s_risk) or (prev_st == 0 and self.b_ex == 13):
            self.s_risk = 0.0
        if self.s_ex == 13 and self.s_ex_low is not None and self.s_ex_high_at_low is not None:
            self.s_target = 2*self.s_ex_low-self.s_ex_high_at_low
        elif (self.s_target and bar.close < self.s_target) or (self.s_risk == 0 and self.b_ex == 13):
            self.s_target = 0.0

        for level_name, direction, value, prior, enabled in (
            ("MOMENTUM_RISK", "CALL", self.b_mrisk, prev_bmr, s.momentum_risk),
            ("MOMENTUM_RISK", "PUT", self.s_mrisk, prev_smr, s.momentum_risk),
            ("EXHAUSTION_TARGET", "CALL", self.b_target, prev_bt, s.exhaustion_target),
            ("EXHAUSTION_RISK", "CALL", self.b_risk, prev_br, s.exhaustion_risk),
            ("EXHAUSTION_TARGET", "PUT", self.s_target, prev_st, s.exhaustion_target),
            ("EXHAUSTION_RISK", "PUT", self.s_risk, prev_sr, s.exhaustion_risk),
        ):
            if value != prior:
                emit("LEVEL_SET" if value else "LEVEL_CLEARED", direction,
                     value=value or None, visible=enabled, detail=level_name)

        for name, direction, value, prior, enabled in (
            ("RESISTANCE", "PUT", self.resistance, prev_res, s.support_resistance),
            ("SUPPORT", "CALL", self.support, prev_sup, s.support_resistance),
            ("MOMENTUM_RISK", "CALL", self.b_mrisk, prev_bmr, s.momentum_risk),
            ("MOMENTUM_RISK", "PUT", self.s_mrisk, prev_smr, s.momentum_risk),
            ("EXHAUSTION_RISK", "CALL", self.b_risk, prev_br, s.exhaustion_risk),
            ("EXHAUSTION_RISK", "PUT", self.s_risk, prev_sr, s.exhaustion_risk),
        ):
            if i and value and ((value > bar.close and value < self.bars[i-1].close) or
                                 (value < bar.close and value > self.bars[i-1].close)):
                # Pine's two risk-alert conditions are `CALL_cross or
                # PUT_cross and show`; preserve that exact precedence.
                pine_alert = enabled or (direction == "CALL" and name in
                                         {"MOMENTUM_RISK", "EXHAUSTION_RISK"})
                emit("LEVEL_CROSS", direction, value=value, visible=False,
                     alert=pine_alert, detail=name)

        mode = s.trade_setups
        b_q = (len(self.b_nines) >= 2 and self.b_thirteens and self.s_nines and
               self.b_nines[-1] > self.b_thirteens[-1] > self.b_nines[-2] > self.s_nines[-1])
        s_q = (len(self.s_nines) >= 2 and self.s_thirteens and self.b_nines and
               self.s_nines[-1] > self.s_thirteens[-1] > self.s_nines[-2] > self.b_nines[-1])
        b_start = ((self.b_count == 9 or early_b) if mode == "Momentum" else
                   (i >= 5 and self.frames[i-5].bullish_exhaustion == 13) if mode == "Exhaustion" else
                   (self.b_count == 9 and bool(b_q)) if mode == "Qualified" else False)
        s_start = ((self.s_count == 9 or early_s) if mode == "Momentum" else
                   (i >= 5 and self.frames[i-5].bearish_exhaustion == 13) if mode == "Exhaustion" else
                   (self.s_count == 9 and bool(s_q)) if mode == "Qualified" else False)
        self.s_setup_pending = bool(s_start or (self.s_setup_pending and not self.s_flip_prev))
        self.b_setup_pending = bool(b_start or (self.b_setup_pending and not self.b_flip_prev))
        s_flip = bool(i >= 5 and self.s_setup_pending and bar.close < self.bars[i-4].close
                      and self.bars[i-1].close > self.bars[i-5].close)
        b_flip = bool(i >= 5 and self.b_setup_pending and bar.close > self.bars[i-4].close
                      and self.bars[i-1].close < self.bars[i-5].close)
        if mode != "None" and s_flip:
            stop = self.s_risk if mode == "Exhaustion" else self.s_mrisk
            emit("SETUP_SHORT", "PUT", value=bar.close, visible=True, alert=True,
                 detail=f"mode={mode} stop={stop or bar.high} target={self.support} risky={not bool(stop)}")
            self.short_active, self.long_active = True, False
        if s.setup_warnings and self.short_active and bar.open < bar.close and self.s_count == 2 and not self.long_active:
            emit("SETUP_WARNING", "PUT", value=bar.close, detail="bullish price flip")
            self.short_active = False
        if s.setup_warnings and self.short_active and bar.open < bar.close and (
                (prev_smr and bar.close > prev_smr) or (prev_sr and bar.close > prev_sr)):
            emit("SETUP_CRITICAL", "PUT", value=bar.close, detail="risk level breached")
            self.short_active = False
        if mode != "None" and b_flip:
            stop = self.b_risk if mode == "Exhaustion" else self.b_mrisk
            emit("SETUP_LONG", "CALL", value=bar.close, visible=True, alert=True,
                 detail=f"mode={mode} stop={stop or bar.low} target={self.b_target if mode == 'Exhaustion' else self.resistance} risky={not bool(stop)}")
            self.long_active, self.short_active = True, False
        if s.setup_warnings and self.long_active and bar.open > bar.close and self.b_count == 2 and not self.short_active:
            emit("SETUP_WARNING", "CALL", value=bar.close, detail="bearish price flip")
            self.long_active = False
        if s.setup_warnings and self.long_active and bar.open > bar.close and (
                (prev_bmr and bar.close < prev_bmr) or (prev_br and bar.close < prev_br)):
            emit("SETUP_CRITICAL", "CALL", value=bar.close, detail="risk level breached")
            self.long_active = False
        self.s_flip_prev, self.b_flip_prev = s_flip, b_flip
        self.prev_b_perfect, self.prev_s_perfect = bp, sp
        frame = ReversalFrame(bar.ts, self.b_count, self.s_count, self.b_ex, self.s_ex,
                              self.resistance, self.support, self.b_mrisk, self.s_mrisk,
                              self.b_target, self.b_risk, self.s_target, self.s_risk,
                              tuple(events))
        self.frames.append(frame)
        return frame


def replay_reversal(bars: Sequence[Bar], settings: ReversalSettings = ReversalSettings()) -> tuple[ReversalFrame, ...]:
    engine = ReversalSignals(settings)
    return tuple(engine.update(bar) for bar in bars)
