import time
import tempfile
import json
from unittest.mock import patch
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from trading_engine.runtime import TradingRuntime
from trading_engine.exit_pipeline import PositionState
from trading_engine.audit_log import AuditLog


class Execution:
    def __init__(self):
        self.exits = []
    def remember_position(self, pos):
        pass
    def submit_exit(self, symbol, pos, reasons, now, observation=None):
        self.exits.append((symbol, reasons))
    def block(self, symbol, reason):
        raise AssertionError(reason)


class BrokerExitEvents(unittest.TestCase):
    def setUp(self):
        self.executor = Execution()
        self.rt = TradingRuntime(order_executor=self.executor)
        self.pos = PositionState(symbol="SPY", direction="CALL", opened_ts=time.time()-60,
                                 entry_option_price=1., occ_symbol="SPY260930C00600000")
        self.rt.positions["SPY"] = ("test", self.pos)
        self.rt._occ_to_underlying[self.pos.occ_symbol] = "SPY"
        self.rt.dashboard.record_open("test", self.pos)

    def test_fresh_option_bid_triggers_stop_without_underlying_tick(self):
        test_time = datetime(2026, 9, 29, 10, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        with patch("trading_engine.runtime.time.time", return_value=test_time):
            self.rt.on_option_quote(self.pos.occ_symbol, test_time, .65, .70)
        self.assertEqual(self.executor.exits[0][1], ("EMERGENCY_OPTION_STOP",))

    def test_stale_option_bid_does_not_trigger_stop(self):
        self.rt.on_option_quote(self.pos.occ_symbol, time.time()-900, .65, .70)
        self.assertEqual(self.executor.exits, [])

    def test_every_held_option_quote_is_observed_with_bid_ask_and_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.rt.audit = AuditLog(tmp)
            self.rt.on_option_quote(self.pos.occ_symbol, time.time(), 1.10, 1.15)
            self.rt.on_option_quote(self.pos.occ_symbol, time.time(), 1.12, 1.16)
            self.rt.on_option_quote(self.pos.occ_symbol, time.time()-900, 1.40, 1.45)
            import pathlib
            records = [json.loads(line) for line in next(pathlib.Path(tmp).glob("engine_*.jsonl")).read_text().splitlines()]
            quotes = [r for r in records if r["event"] == "OPTION_QUOTE"]
            self.assertEqual(len(quotes), 3)
            self.assertEqual([q["accepted"] for q in quotes], [True, True, False])
            self.assertEqual(quotes[1]["ask"], 1.16)
            self.assertEqual(quotes[2]["rejected_reason"], "STALE_TIMESTAMP")

    def test_older_option_quote_cannot_overwrite_new_bid(self):
        at = datetime(2026, 9, 29, 10, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        with patch("trading_engine.runtime.time.time", return_value=at):
            self.rt.on_option_quote(self.pos.occ_symbol, at, 1.10, 1.12)
            self.rt.on_option_quote(self.pos.occ_symbol, at-2, .60, .62)
        self.assertEqual(self.pos.current_option_price, 1.10)
        self.assertEqual(self.executor.exits, [])

    def test_eod_clock_triggers_without_underlying_tick(self):
        cutoff = datetime(2026, 9, 29, 15, 50, tzinfo=ZoneInfo("America/New_York")).timestamp()
        self.rt.option_quote_recovery = lambda occ: self.fail("EOD exit must not wait for REST quote")
        self.rt.on_clock(cutoff)
        self.assertEqual(self.executor.exits[0][1], ("EOD_FORCE_CLOSE",))

    def test_delayed_underlying_quote_cannot_trigger_broker_logic(self):
        self.rt.on_underlying_tick("SPY", time.time()-900, 600.)
        self.assertEqual(self.rt.store.symbols(), ())
        self.assertEqual(self.executor.exits, [])

    def test_stale_option_bid_does_not_trigger_premium_stop_on_underlying_tick(self):
        at = datetime(2026, 9, 29, 10, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        self.pos.current_option_price = .65
        self.pos.peak_option_price = 1.3
        self.rt._latest_option_quote["test"] = {"bid": .65, "ask": .70,
                                                   "market_ts": at-60, "received_at_ts": at-60}
        with patch("trading_engine.runtime.time.time", return_value=at):
            self.rt._evaluate_symbol("SPY", at, entries_allowed=False)
            self.assertEqual(self.executor.exits, [])
            self.rt.on_option_quote(self.pos.occ_symbol, at, .65, .70)
        self.assertEqual(self.executor.exits[0][1], ("EMERGENCY_OPTION_STOP",))

    def test_dead_option_feed_pauses_entries_and_recovers_from_independent_quote(self):
        at = datetime(2026, 9, 29, 10, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        self.pos.opened_ts = at-30
        calls = []
        self.rt.option_quote_recovery = lambda occ: (calls.append(occ) or (at, 1.10, 1.12))
        with patch("trading_engine.runtime.time.time", return_value=at):
            self.rt.on_clock(at)
        self.assertEqual(calls, [self.pos.occ_symbol])
        self.assertFalse(self.rt._option_feed_warning)
        self.assertEqual(self.pos.current_option_price, 1.10)

    def test_dead_option_feed_exposes_at_risk_position_without_forced_market_sell(self):
        at = datetime(2026, 9, 29, 10, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
        self.pos.opened_ts = at-30
        with patch("trading_engine.runtime.time.time", return_value=at):
            self.rt.on_clock(at)
            warnings = self.rt.option_feed_view()
        self.assertEqual(warnings[0]["status"], "OPTION_FEED_STALE")
        self.assertEqual(self.executor.exits, [])

    def test_entry_cutoff_prevents_contract_selection(self):
        at = datetime(2026, 9, 29, 15, 44, tzinfo=ZoneInfo("America/New_York")).timestamp()
        called = []
        self.rt.option_entry_price_provider = lambda *a: called.append(a)
        self.rt.store.ingest_price("AAPL", at, 330.)
        with patch("trading_engine.runtime.time.time", return_value=at):
            self.rt._evaluate_symbol("AAPL", at)
        self.assertFalse(called)
        self.assertNotIn("AAPL", self.rt.blocked_symbols)


if __name__ == "__main__":
    unittest.main()
