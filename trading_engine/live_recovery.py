"""Bounded production REST recovery alongside the streaming transport."""
import math
import threading
import time
from .tradier_client import PRODUCTION_BASE
from .tradier_poll import TradierPollClient

class LiveQuoteRecovery:
    def __init__(self, client, runtime, symbols, interval=3):
        if client.config.base_url.rstrip('/') != PRODUCTION_BASE:
            raise ValueError('Recovery requires production market data')
        self.client, self.runtime, self.symbols = client, runtime, tuple(symbols)
        self.interval = interval
        self.stop = threading.Event()
        self.thread = None
        self.last = {}
        self.last_log = 0

    def poll_once(self):
        rt = self.runtime
        now = time.time()
        with rt._state_lock:
            if not rt._regular_session(now):
                return
            needed = []
            for symbol in self.symbols:
                snap = rt.store.snapshot(symbol, now=now)
                if snap.price_state != 'FRESH' or now - rt._last_underlying_wall.get(symbol, 0) > 5:
                    needed.append(symbol)
            for tid, pos in rt.positions.values():
                q = rt._latest_option_quote.get(tid, {})
                if now - q.get('market_ts', 0) > 5 and pos.occ_symbol:
                    needed.append(pos.occ_symbol)
        if not needed:
            return
        rows = self.client.quotes(list(dict.fromkeys(needed)))
        accepted = []
        wall = time.time()
        for symbol, row in rows.items():
            option = symbol not in self.symbols
            ts = TradierPollClient._event_time(row.get('bid_date' if option else 'trade_date'))
            wall = time.time()
            if ts is None or not math.isfinite(ts) or abs(wall-ts) > rt.max_market_age_sec:
                continue
            if ts <= self.last.get(symbol, 0):
                continue
            if option:
                bid, ask = float(row.get('bid') or 0), float(row.get('ask') or 0)
                if not math.isfinite(bid) or not math.isfinite(ask) or bid <= 0:
                    continue
                rt.on_option_quote(symbol, ts, bid, ask)
            else:
                price = float(row.get('last') or 0)
                if not math.isfinite(price) or price <= 0:
                    continue
                # Quote last_volume is not a volume delta; never count it twice.
                rt.on_underlying_tick(symbol, ts, price, 0)
            self.last[symbol] = ts
            accepted.append(symbol)
        if rt.audit and wall - self.last_log >= 30:
            rt.audit.emit('LIVE_REST_RECOVERY', durable=False,
                          requested=needed, fresh_returned=accepted)
            self.last_log = wall

    def start(self):
        def run():
            while not self.stop.is_set():
                try:
                    self.poll_once()
                except Exception as exc:
                    if self.runtime.audit:
                        self.runtime.audit.emit('LIVE_REST_RECOVERY_ERROR', durable=False,
                                                error=type(exc).__name__)
                self.stop.wait(self.interval)
        self.thread = threading.Thread(target=run, name='live-rest-recovery', daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=6)
