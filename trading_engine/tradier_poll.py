"""Tradier REST-polling fallback -- for sandbox testing ONLY.

Confirmed directly from Tradier's own documentation (their FAQ: "Presently,
we do not offer a delayed streaming endpoint for paper trading") and
multiple independent sources: Tradier's sandbox/paper environment has NO
streaming market data access at all, categorically, regardless of token
validity. tradier_stream.py's WebSocket connection will always be rejected
(401) against a sandbox token. Streaming requires a live, funded Tradier
brokerage account with real-time market data entitlement.

This module exists so the pipeline can be exercised mechanically end-to-end
against sandbox -- confirming contracts get selected, positions open and
close, the dashboard updates -- before ever pointing at a live account. It
is explicitly NOT a substitute for real-time data, and must never be
presented as one:

  - Sandbox quotes are delayed ~15 minutes (Tradier's own documented
    industry-standard sandbox delay), not real-time.
  - Polling on an interval is fundamentally coarser than a push stream --
    expect flatter, less naturalistic bars than real streaming would
    produce, since the underlying data itself only updates as often as the
    delayed feed refreshes, independent of how often this polls.
  - PSAR/structure/momentum quality validated earlier in this project was
    tested against real, live-cadence data. Results from this mode say
    nothing about strategy quality -- only whether the pipeline's plumbing
    works. Do not draw trading conclusions from a sandbox-polled session.

Mirrors TradierStreamClient's public interface exactly (symbols, add_symbol,
remove_symbol, on_tick, on_connected, on_quote) so main.py's wiring code
does not need to branch on which transport is active -- only main.py's
transport *selection* (based on TRADIER_ENV) needs to know the difference.
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Dict, List, Optional

from .tradier_client import TradierRestClient, TradierAuthError


class TradierPollClient:
    def __init__(self, symbols: List[str], rest_client: TradierRestClient,
                 on_tick: Callable[[str, float, float, float], None],
                 on_quote: Optional[Callable[[str, float, float, float], None]] = None,
                 on_connected: Optional[Callable[[bool], None]] = None,
                 poll_interval_sec: float = 5.0):
        self._symbols_lock = threading.Lock()
        self.symbols = list(symbols)
        self.rest_client = rest_client
        self.on_tick = on_tick
        self.on_quote = on_quote
        self.on_connected = on_connected
        self.poll_interval_sec = poll_interval_sec
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_event_ts: Dict[str, float] = {}

    # -- dynamic subscription: same interface as TradierStreamClient --

    def add_symbol(self, symbol: str) -> None:
        with self._symbols_lock:
            if symbol not in self.symbols:
                self.symbols.append(symbol)

    def remove_symbol(self, symbol: str) -> None:
        with self._symbols_lock:
            if symbol in self.symbols:
                self.symbols.remove(symbol)

    # -- polling --

    @staticmethod
    def _event_time(raw) -> Optional[float]:
        try:
            value = float(raw)
            if value <= 0:
                return None
            return value / 1000.0 if value > 10_000_000_000 else value
        except (TypeError, ValueError):
            return None

    def _dispatch_quote(self, symbol: str, q: Dict) -> None:
        """One parsed Tradier quote row -> on_tick (equity) or on_quote
        (option), based on the quote's own "type" field -- the same
        /markets/quotes endpoint returns both shapes, distinguished this way.
        """
        is_option = str(q.get("type", "")).lower() == "option"
        # Never turn a repeated, delayed last price into a new live tick/bar.
        ts = self._event_time(q.get("bid_date") if is_option else q.get("trade_date"))
        if ts is None or ts > time.time() + 5:
            return
        if ts <= self._last_event_ts.get(symbol, 0):
            return
        if is_option and self.on_quote is not None:
            bid = float(q.get("bid") or 0.0)
            ask = float(q.get("ask") or 0.0)
            if bid > 0 or ask > 0:
                self._last_event_ts[symbol] = ts
                self.on_quote(symbol, ts, bid, ask)
        elif not is_option:
            last = q.get("last")
            if last:
                self._last_event_ts[symbol] = ts
                self.on_tick(symbol, ts, float(last), float(q.get("last_volume") or 0.0))

    def _run(self) -> None:
        if self.on_connected:
            self.on_connected(True)
        while not self._stop.is_set():
            with self._symbols_lock:
                symbols = list(self.symbols)
            if symbols:
                try:
                    quotes = self.rest_client.quotes(symbols)
                    if self.on_connected:
                        self.on_connected(True)
                    for symbol, q in quotes.items():
                        self._dispatch_quote(symbol, q)
                except TradierAuthError as exc:
                    if self.on_connected:
                        self.on_connected(False)
                    print(f"[market-data] authentication failed: {exc}", flush=True)
                    return
                except Exception as exc:
                    if self.on_connected:
                        self.on_connected(False)
                    print(f"[market-data] Tradier REST poll failed: {exc}", flush=True)
            self._stop.wait(self.poll_interval_sec)
        if self.on_connected:
            self.on_connected(False)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
