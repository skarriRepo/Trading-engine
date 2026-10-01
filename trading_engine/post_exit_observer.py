"""Observe live option quotes after a confirmed close; never submit orders."""
from __future__ import annotations

import math
import threading
import time
from dataclasses import replace

from .bid_exit_shadow import read_bid_exit
from .exit_pipeline import DEFAULT_EXIT_CONFIG


class PostExitQuoteObserver:
    def __init__(self, stream, audit, seconds: float = 600, clock=time.time,
                 timer_factory=threading.Timer, max_quote_age_sec: float = 10,
                 exit_config=DEFAULT_EXIT_CONFIG):
        if not 0 < seconds <= 3600:
            raise ValueError("POST_EXIT_OBSERVE_SEC must be between 1 and 3600")
        self.stream, self.audit = stream, audit
        self.seconds, self.clock = seconds, clock
        self.timer_factory = timer_factory
        self.max_quote_age_sec = max_quote_age_sec
        self.exit_config = exit_config
        self._lock = threading.RLock()
        self._active = set()
        self._shadow = {}  # OCC -> trade_id -> observation state

    def opened(self, trade_id, pos):
        if not pos.occ_symbol:
            return
        with self._lock:
            self._active.add(pos.occ_symbol)
            self.stream.add_symbol(pos.occ_symbol)

    def closed(self, trade_id, pos):
        if not pos.occ_symbol:
            return
        occ = pos.occ_symbol
        with self._lock:
            self._active.discard(occ)
            self._shadow.setdefault(occ, {})[trade_id] = {
                "symbol": pos.symbol, "direction": pos.direction,
                "entry_ask": pos.entry_option_price,
                "exit_reference_bid": pos.current_option_price if pos.current_option_price > 0 else None,
                "end_at": self.clock() + self.seconds,
                "last_market_ts": float("-inf"), "peak_bid": None,
                "last_bid": None, "accepted_quotes": 0,
                "shadow_pos": replace(pos) if hasattr(pos, "last_quote_spread") else None,
                "first_bid_exit": None,
            }
            self.audit.emit("POST_EXIT_SHADOW_START", trade_id=trade_id, symbol=pos.symbol,
                            occ_symbol=occ, entry_live_ask=pos.entry_option_price,
                            exit_reference_bid=self._shadow[occ][trade_id]["exit_reference_bid"],
                            observe_sec=self.seconds)
        timer = self.timer_factory(self.seconds, self.finish, args=(occ, trade_id))
        timer.daemon = True
        timer.start()

    def quote(self, occ, market_ts, bid, ask):
        received_at = self.clock()
        with self._lock:
            for trade_id, row in self._shadow.get(occ, {}).items():
                if received_at > row["end_at"]:
                    continue
                valid = (all(math.isfinite(float(v)) for v in (market_ts, bid, ask))
                         and 0 < bid <= ask and 0 <= received_at - market_ts <= self.max_quote_age_sec
                         and market_ts > row["last_market_ts"])
                if not valid:
                    continue
                row["last_market_ts"] = market_ts
                row["last_bid"] = bid
                row["peak_bid"] = max(bid, row["peak_bid"] or bid)
                row["accepted_quotes"] += 1
                shadow_pos = row["shadow_pos"]
                if shadow_pos is not None:
                    shadow_pos.update_price(bid, market_ts, ask=ask)
                    read = read_bid_exit(shadow_pos, market_ts, True, self.exit_config)
                    if read.reason and row["first_bid_exit"] is None:
                        row["first_bid_exit"] = {"reason": read.reason, "market_ts": market_ts,
                                                 "bid": bid, "peak_bid": shadow_pos.peak_option_price,
                                                 "trail": read.trail, "giveback": read.giveback}
                        self.audit.emit("POST_EXIT_BID_SHADOW_TRIGGER", trade_id=trade_id,
                                        symbol=row["symbol"], occ_symbol=occ,
                                        **row["first_bid_exit"])
                self.audit.emit("POST_EXIT_OPTION_QUOTE", durable=False, trade_id=trade_id,
                                symbol=row["symbol"], direction=row["direction"],
                                occ_symbol=occ, market_ts=market_ts, received_at_ts=received_at,
                                bid=bid, ask=ask, spread=ask-bid,
                                entry_live_ask=row["entry_ask"],
                                exit_reference_bid=row["exit_reference_bid"],
                                ask_to_bid_usd_per_contract=round((bid-row["entry_ask"])*100, 2))

    def finish(self, occ, trade_id):
        with self._lock:
            row = self._shadow.get(occ, {}).pop(trade_id, None)
            if row is None:
                return
            self.audit.emit("POST_EXIT_SHADOW_END", trade_id=trade_id,
                            symbol=row["symbol"], occ_symbol=occ,
                            accepted_quotes=row["accepted_quotes"], peak_bid=row["peak_bid"],
                            last_bid=row["last_bid"], entry_live_ask=row["entry_ask"],
                            exit_reference_bid=row["exit_reference_bid"],
                            first_bid_exit=row["first_bid_exit"],
                            observation_complete=row["accepted_quotes"] > 0)
            if not self._shadow[occ]:
                del self._shadow[occ]
                if occ not in self._active:
                    self.stream.remove_symbol(occ)
