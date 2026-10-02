"""Streaming-first, single-source-of-truth market state, one instance per symbol.

Every entry/exit decision in the redesigned system reads from this store.
Nothing in the decision path ever makes a blocking network call -- all data
arrives via streaming pushes (UW WebSocket, price/bar feed), and a stream
consumer thread's job is only to call the `ingest_*` methods below. Decisions
call `snapshot()` and read the result; they never touch a live connection.

This directly replaces the split, disagreeing legacy-REST vs shadow-streaming
UW paths found on 2026-09-28: the streaming-derived UW direction agreed with
the legacy REST computation on only 5 of 18 real cases where it fired (27.8%),
including direct directional contradictions on the same symbol at the same
moment (COIN, BA, SOFI, ARM: derived CALL vs legacy PUT). Making streaming the
*only* path removes the disagreement by removing the second system, rather
than trying to reconcile two independently-evolved computations.

Freshness discipline (carried forward deliberately, this is the one piece of
the old design proven correct by an external review before this rewrite):
  - Every feed has its own TTL and its own freshness state -- FRESH / STALE /
    NOT_READY. There is no single overall "is this fresh" flag, because a
    global `min(fresh_ages)`-style check is exactly the bug found earlier: one
    live feed can hide another stale one behind it.
  - A feed that has never been seen, or whose age exceeds its TTL, becomes
    NOT_READY. It is never silently treated as NEUTRAL, zero, or "whatever we
    last saw" -- missing evidence must read as missing evidence to anything
    downstream.
  - A transport disconnect (`set_connected(False)`) immediately makes every
    live feed NOT_READY for every symbol, rather than continuing to serve
    increasingly-stale last-known values with nothing forcing a check.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple


def _dir_from_signed(v: Optional[float]) -> str:
    if v is None:
        return "NOT_READY"
    return "CALL" if v > 0 else "PUT" if v < 0 else "NEUTRAL"


class FreshnessState:
    FRESH = "FRESH"
    STALE = "STALE"
    NOT_READY = "NOT_READY"


@dataclass(frozen=True)
class FeedTTL:
    """Seconds before a feed's most recent sample becomes NOT_READY.

    Starting values, not validated ones -- these are deliberately conservative
    guesses. They need the same treatment STALL_EXIT's 1500s and the DTE
    guard's 2% threshold got: recalibrate against real observed update
    cadence once this is running and logging, before trusting the defaults.
    """
    price: float = 5.0
    bar: float = 150.0  # a 2-minute bar is stale once ~2.5 bars old
    net_flow: float = 5.0
    interval_flow: float = 10.0
    market_tide: float = 10.0
    gex: float = 15.0


DEFAULT_TTL = FeedTTL()


@dataclass
class Bar:
    ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class NetFlowSample:
    ts: float
    dir_delta_flow: float


@dataclass
class IntervalFlowSample:
    ts: float
    call_vol_ask_side: float = 0.0
    call_vol_bid_side: float = 0.0
    put_vol_ask_side: float = 0.0
    put_vol_bid_side: float = 0.0
    dir_delta_flow: float = 0.0
    dir_vega_flow: float = 0.0
    avg_dte: Optional[float] = None


@dataclass
class MarketTideSample:
    ts: float
    net_call_premium: float
    net_put_premium: float


@dataclass
class GexSample:
    ts: float
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    gamma_path: str = "N/A"
    target_candidates: Tuple[float, ...] = ()


def completed_bars(bars: Tuple[Bar, ...], bar_seconds: float, now: float) -> Tuple[Bar, ...]:
    """Filter out the still-forming current bar, leaving only bars whose
    interval has fully closed. PSAR/structure computation (stage 2) must run
    on completed bars only -- evaluating against a bar that's still
    accumulating ticks is a different, noisier signal than the reference
    system intended (it always closed one bar before handing PSAR the series).
    A bar is complete once `now` has moved past its bucket's end time.
    """
    if not bars:
        return ()
    return tuple(b for b in bars if now >= b.ts + bar_seconds)


def _net_delta_window(samples: List[NetFlowSample], now: float, window_sec: float) -> Optional[float]:
    cutoff = now - window_sec
    in_window = [s for s in samples if s.ts >= cutoff]
    if not in_window:
        return None
    return sum(s.dir_delta_flow for s in in_window)


@dataclass(frozen=True)
class SymbolSnapshot:
    """An immutable, consistent read of one symbol's streaming state at one
    instant. Every field that can go stale carries its own freshness state
    alongside it -- deliberately no single overall freshness flag.

    flow_30s/flow_1m/flow_3m/flow_5m, interval_direction, aggressor_direction,
    delta_flow_direction, and vega_flow_direction are each computed
    independently from raw signed flow, with no reference to any assumed
    trade direction -- they answer "what does the market's own flow say right
    now", not "does the evidence support this specific direction I'm asking
    about". Direction-conditional questions (does this data support CALL vs
    PUT) belong in the confluence-check stage that reads this snapshot, not
    in the snapshot itself.
    """
    symbol: str
    now: float
    connected: bool

    price: Optional[float]
    price_state: str

    bars: Tuple[Bar, ...]
    bar_state: str

    flow_30s: str
    flow_1m: str
    flow_3m: str
    flow_5m: str
    net_flow_state: str

    interval_direction: str
    aggressor_direction: str
    aggressor_strength: float
    delta_flow_direction: str
    vega_flow_direction: str
    avg_dte: Optional[float]
    interval_flow_state: str

    market_tide_direction: str
    market_tide_state: str

    gamma_path: str
    call_wall: Optional[float]
    put_wall: Optional[float]
    target_candidates: Tuple[float, ...]
    gex_state: str

    @staticmethod
    def build(symbol: str, now: float, ttl: FeedTTL, connected: bool,
              last_price: Optional[Tuple[float, float]], bars: List[Bar],
              net_flow: List[NetFlowSample], interval_flow: List[IntervalFlowSample],
              market_tide: List[MarketTideSample], gex: Optional[GexSample]) -> "SymbolSnapshot":
        # -- price --
        if not connected or last_price is None:
            price, price_state = None, FreshnessState.NOT_READY
        else:
            p_ts, p_val = last_price
            age = now - p_ts
            price = p_val
            price_state = FreshnessState.FRESH if age <= ttl.price else FreshnessState.STALE

        # -- bars (for PSAR / structure) --
        if not bars:
            bar_state = FreshnessState.NOT_READY
        else:
            age = now - max(b.ts for b in bars)
            bar_state = FreshnessState.FRESH if age <= ttl.bar else FreshnessState.STALE

        # -- net flow: four independent horizons --
        d30 = d60 = d180 = d300 = None
        if not connected:
            net_flow_state = FreshnessState.NOT_READY
        elif not net_flow:
            net_flow_state = FreshnessState.NOT_READY
        else:
            # max(), not net_flow[-1] -- append order matches arrival order for
            # a well-behaved live stream, but must not be assumed: network
            # delivery can reorder slightly, and a defensively-correct
            # freshness check shouldn't silently break if it does.
            age = now - max(s.ts for s in net_flow)
            if age > ttl.net_flow:
                net_flow_state = FreshnessState.STALE
            else:
                net_flow_state = FreshnessState.FRESH
                d30 = _net_delta_window(net_flow, now, 30.0)
                d60 = _net_delta_window(net_flow, now, 60.0)
                d180 = _net_delta_window(net_flow, now, 180.0)
                d300 = _net_delta_window(net_flow, now, 300.0)
        flow_30s = _dir_from_signed(d30)
        flow_1m = _dir_from_signed(d60)
        flow_3m = _dir_from_signed(d180)
        flow_5m = _dir_from_signed(d300)

        # -- interval / aggressor / Greek flow --
        interval_direction = aggressor_direction = "NOT_READY"
        delta_flow_direction = vega_flow_direction = "NOT_READY"
        aggressor_strength = 0.0
        avg_dte = None
        if not connected or not interval_flow:
            interval_flow_state = FreshnessState.NOT_READY
        else:
            recent = max(interval_flow, key=lambda s: s.ts)
            age = now - recent.ts
            if age > ttl.interval_flow:
                interval_flow_state = FreshnessState.STALE
            else:
                interval_flow_state = FreshnessState.FRESH
                interval_signed = (
                    recent.call_vol_ask_side - recent.call_vol_bid_side
                    - recent.put_vol_ask_side + recent.put_vol_bid_side
                )
                interval_direction = _dir_from_signed(interval_signed)
                aggr_signed = (
                    recent.call_vol_ask_side - recent.call_vol_bid_side
                    + recent.put_vol_bid_side - recent.put_vol_ask_side
                )
                aggr_gross = (
                    abs(recent.call_vol_ask_side) + abs(recent.call_vol_bid_side)
                    + abs(recent.put_vol_ask_side) + abs(recent.put_vol_bid_side)
                )
                aggressor_direction = _dir_from_signed(aggr_signed)
                aggressor_strength = (
                    round(min(100.0, abs(aggr_signed) / aggr_gross * 100.0), 1)
                    if aggr_gross > 0 else 0.0
                )
                delta_flow_direction = _dir_from_signed(recent.dir_delta_flow)
                vega_flow_direction = _dir_from_signed(recent.dir_vega_flow)
                avg_dte = recent.avg_dte

        # -- market tide --
        if not connected or not market_tide:
            market_tide_direction, market_tide_state = "NOT_READY", FreshnessState.NOT_READY
        else:
            recent = max(market_tide, key=lambda s: s.ts)
            age = now - recent.ts
            if age > ttl.market_tide:
                market_tide_direction, market_tide_state = "NOT_READY", FreshnessState.STALE
            else:
                tide_signed = recent.net_call_premium - recent.net_put_premium
                market_tide_direction = _dir_from_signed(tide_signed)
                market_tide_state = FreshnessState.FRESH

        # -- GEX / walls --
        if not connected or gex is None:
            gamma_path, call_wall, put_wall = "N/A", None, None
            target_candidates: Tuple[float, ...] = ()
            gex_state = FreshnessState.NOT_READY
        else:
            age = now - gex.ts
            if age > ttl.gex:
                gamma_path, call_wall, put_wall = "N/A", None, None
                target_candidates = ()
                gex_state = FreshnessState.STALE
            else:
                gamma_path = gex.gamma_path
                call_wall, put_wall = gex.call_wall, gex.put_wall
                target_candidates = tuple(gex.target_candidates)
                gex_state = FreshnessState.FRESH

        return SymbolSnapshot(
            symbol=symbol, now=now, connected=connected,
            price=price, price_state=price_state,
            bars=tuple(bars), bar_state=bar_state,
            flow_30s=flow_30s, flow_1m=flow_1m, flow_3m=flow_3m, flow_5m=flow_5m,
            net_flow_state=net_flow_state,
            interval_direction=interval_direction, aggressor_direction=aggressor_direction,
            aggressor_strength=aggressor_strength, delta_flow_direction=delta_flow_direction,
            vega_flow_direction=vega_flow_direction, avg_dte=avg_dte,
            interval_flow_state=interval_flow_state,
            market_tide_direction=market_tide_direction, market_tide_state=market_tide_state,
            gamma_path=gamma_path, call_wall=call_wall, put_wall=put_wall,
            target_candidates=target_candidates, gex_state=gex_state,
        )


class SymbolStream:
    """All streaming state for one symbol.

    Written to only by stream-consumer threads via the `ingest_*` methods,
    each of which acquires the lock only long enough to append/replace one
    value. Read only via `snapshot()`, which copies everything out under the
    lock and releases it before any computation happens -- the same
    lock-then-copy-then-release discipline already validated for the UW
    shadow collector this replaces.
    """

    def __init__(self, symbol: str, ttl: FeedTTL = DEFAULT_TTL, connected: bool = True,
                 bar_seconds: float = 120.0):
        self.symbol = symbol
        self.ttl = ttl
        self.bar_seconds = float(bar_seconds)
        self._lock = threading.RLock()
        self._connected = connected
        self._last_price: Optional[Tuple[float, float]] = None
        self._bars: Deque[Bar] = deque(maxlen=200)
        self._net_flow: Deque[NetFlowSample] = deque(maxlen=600)
        self._interval_flow: Deque[IntervalFlowSample] = deque(maxlen=50)
        self._market_tide: Deque[MarketTideSample] = deque(maxlen=50)
        self._gex: Optional[GexSample] = None

    # -- ingestion --

    def set_connected(self, connected: bool) -> None:
        with self._lock:
            self._connected = connected

    def ingest_price(self, ts: float, price: float) -> None:
        with self._lock:
            self._last_price = (ts, price)

    def ingest_tick(self, ts: float, price: float, volume: float = 0.0) -> None:
        """Primary real-time streaming entry point for a Tradier quote/trade
        event. Updates the raw last-price read AND buckets the tick into the
        in-progress bar for this symbol -- the same current-minute
        accumulation the reference system's MinuteCandleStore did by manually
        polling and bucketing REST quotes, just unified here so price and
        bars can never drift out of sync with each other.
        """
        if price <= 0:
            return
        with self._lock:
            # A delayed trade must never replace the current quote or append an
            # older bucket after a completed candle. Both corrupt PSAR history.
            if self._last_price is not None and ts < self._last_price[0]:
                return
            bucket_ts = int(ts // self.bar_seconds) * self.bar_seconds
            if self._bars and bucket_ts < self._bars[-1].ts:
                return
            self._last_price = (ts, price)
            if self._bars and self._bars[-1].ts == bucket_ts:
                b = self._bars[-1]
                b.high = max(b.high, price)
                b.low = min(b.low, price)
                b.close = price
                b.volume += volume
            else:
                self._bars.append(Bar(ts=bucket_ts, open=price, high=price,
                                       low=price, close=price, volume=volume))

    def ingest_bar(self, bar: Bar) -> None:
        """Direct bar injection -- for replaying an existing candle source
        (e.g. historical v18_candles.json data) rather than building bars
        from live ticks. Real-time streaming should use ingest_tick()."""
        with self._lock:
            if self._bars and self._bars[-1].ts == bar.ts:
                self._bars[-1] = bar  # update the in-progress bar in place
            else:
                self._bars.append(bar)

    def ingest_net_flow(self, sample: NetFlowSample) -> None:
        with self._lock:
            self._net_flow.append(sample)

    def ingest_interval_flow(self, sample: IntervalFlowSample) -> None:
        with self._lock:
            self._interval_flow.append(sample)

    def ingest_market_tide(self, sample: MarketTideSample) -> None:
        with self._lock:
            self._market_tide.append(sample)

    def ingest_gex(self, sample: GexSample) -> None:
        with self._lock:
            self._gex = sample

    # -- read --

    def snapshot(self, now: Optional[float] = None) -> SymbolSnapshot:
        now = now if now is not None else time.time()
        with self._lock:
            connected = self._connected
            last_price = self._last_price
            bars = list(self._bars)
            net_flow = list(self._net_flow)
            interval_flow = list(self._interval_flow)
            market_tide = list(self._market_tide)
            gex = self._gex
        return SymbolSnapshot.build(
            symbol=self.symbol, now=now, ttl=self.ttl, connected=connected,
            last_price=last_price, bars=bars, net_flow=net_flow,
            interval_flow=interval_flow, market_tide=market_tide, gex=gex,
        )


class SymbolStateStore:
    """Owns one SymbolStream per symbol. The single object every stream
    consumer thread writes into and every decision reads from."""

    def __init__(self, ttl: FeedTTL = DEFAULT_TTL, bar_seconds: float = 120.0):
        self._ttl = ttl
        self._bar_seconds = float(bar_seconds)
        self._lock = threading.RLock()
        self._streams: Dict[str, SymbolStream] = {}

    def _get_or_create(self, symbol: str) -> SymbolStream:
        symbol = symbol.upper()
        with self._lock:
            stream = self._streams.get(symbol)
            if stream is None:
                stream = SymbolStream(symbol, ttl=self._ttl, bar_seconds=self._bar_seconds)
                self._streams[symbol] = stream
            return stream

    def set_connected(self, connected: bool) -> None:
        """Call on transport connect/disconnect. Applies to every tracked
        symbol at once: a dropped connection must make every live feed
        NOT_READY immediately, not continue silently serving last-known
        values with nothing forcing a check -- the exact gap found in the
        earlier review of the collector this replaces."""
        with self._lock:
            streams = list(self._streams.values())
        for s in streams:
            s.set_connected(connected)

    def symbols(self) -> Tuple[str, ...]:
        with self._lock:
            return tuple(self._streams.keys())

    def ingest_price(self, symbol: str, ts: float, price: float) -> None:
        self._get_or_create(symbol).ingest_price(ts, price)

    def ingest_tick(self, symbol: str, ts: float, price: float, volume: float = 0.0) -> None:
        self._get_or_create(symbol).ingest_tick(ts, price, volume=volume)

    def ingest_bar(self, symbol: str, bar: Bar) -> None:
        self._get_or_create(symbol).ingest_bar(bar)

    def ingest_net_flow(self, symbol: str, sample: NetFlowSample) -> None:
        self._get_or_create(symbol).ingest_net_flow(sample)

    def ingest_interval_flow(self, symbol: str, sample: IntervalFlowSample) -> None:
        self._get_or_create(symbol).ingest_interval_flow(sample)

    def ingest_market_tide(self, symbol: str, sample: MarketTideSample) -> None:
        self._get_or_create(symbol).ingest_market_tide(sample)

    def ingest_gex(self, symbol: str, sample: GexSample) -> None:
        self._get_or_create(symbol).ingest_gex(sample)

    def snapshot(self, symbol: str, now: Optional[float] = None) -> SymbolSnapshot:
        return self._get_or_create(symbol).snapshot(now=now)
