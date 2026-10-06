import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trading_engine.audit_log import AuditLog
from trading_engine.dashboard_store import DashboardStore
from trading_engine.entry_pipeline import evaluate_entry
from trading_engine.runtime import TradingRuntime
from trading_engine.exit_pipeline import PositionState
from trading_engine.symbol_state import IntervalFlowSample
from test_dashboard import real_uptrend_snapshot


class AuditLogTests(unittest.TestCase):
    def test_report_lock_does_not_block_live_emit(self):
        import threading
        with tempfile.TemporaryDirectory() as d:
            audit = AuditLog(d)
            done = threading.Event()
            with audit._report_lock:
                worker = threading.Thread(target=lambda: (audit.emit('TEST'), done.set()))
                worker.start()
                self.assertTrue(done.wait(1))
            worker.join()

    def test_intraday_report_refresh_is_throttled(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = AuditLog(tmp)
            now = datetime(2026, 9, 30, 10, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
            with patch.object(audit, "report") as report:
                audit.maybe_report("journal.json", now=now)
                audit.maybe_report("journal.json", now=now + 60)
            report.assert_called_once_with("2026-09-30", "journal.json")

    def test_each_held_quote_and_uw_sample_has_replayable_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = AuditLog(tmp)
            rt = TradingRuntime(audit=audit)
            # This quote-context test must not close its fixture after market hours.
            from dataclasses import replace
            rt.exit_config = replace(rt.exit_config, eod_force_close_et="23:59")
            now = datetime.now(ZoneInfo("America/New_York")).timestamp()
            rt.store.ingest_tick("SPY", now-20, 100.)
            pos = PositionState("SPY", "CALL", now-20, 1., occ_symbol="SPY-TEST")
            rt.positions["SPY"] = ("trade-1", pos)
            rt._occ_to_underlying["SPY-TEST"] = "SPY"
            rt.on_interval_flow("SPY", IntervalFlowSample(ts=now-2, put_vol_ask_side=50))
            rt.on_net_flow("SPY", now-1, -100.)
            rt.on_option_quote("SPY-TEST", now, 1.20, 1.25)
            rt.on_option_quote("SPY-TEST", now+1, 1.19, 1.24)
            rt.on_option_quote("SPY-TEST", now-5, 1.50, 1.55)  # rejected out of order
            rows = [json.loads(x) for x in
                    (Path(tmp) / f"engine_{datetime.now(ZoneInfo('America/New_York')).date()}.jsonl")
                    .read_text().splitlines()]
            samples = [r for r in rows if r["event"] == "UW_POSITION_SAMPLE"]
            observations = [r for r in rows if r["event"] == "POSITION_OBSERVATION"
                            and r.get("observation_source") == "OPTION_QUOTE"]
            self.assertEqual(len(samples), 2)
            self.assertEqual(len(observations), 2)
            rejected = [r for r in rows if r["event"] == "OPTION_QUOTE" and not r["accepted"]]
            self.assertEqual(rejected[0]["rejected_reason"], "OUT_OF_ORDER_TIMESTAMP")
            self.assertEqual(observations[-1]["option_bid"], 1.19)
            self.assertEqual(observations[-1]["uw_flow_30s"], "PUT")
            self.assertIn("uw_shadow_exit", observations[-1])
            self.assertIn("decision_reasons", observations[-1])

    def test_signal_dedup_retains_new_bar(self):
        snap, now = real_uptrend_snapshot()
        decision = evaluate_entry(snap, "CALL", now=now)
        dashboard = DashboardStore()
        self.assertTrue(dashboard.record_signal("AAPL", "CALL", decision, now, "DEFERRED", 100))
        self.assertFalse(dashboard.record_signal("AAPL", "CALL", decision, now + 1, "DEFERRED", 100))
        self.assertTrue(dashboard.record_signal("AAPL", "CALL", decision, now + 120, "DEFERRED", 220))
        self.assertEqual(len(dashboard.signal_view()), 2)

    def test_rebuild_eod_from_events_and_confirmed_journal_fills(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = AuditLog(os.path.join(tmp, "logs"))
            day = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
            audit.emit("SIGNAL_DECISION", symbol="SPY", action="DEFER", source="DEFERRED",
                       reasons=["MOMENTUM_NOT_EXPANDING", "UW_NOT_READY"])
            audit.emit("BROKER_EXIT_FILLED", symbol="SPY", realized_pnl_usd_before_fees=-15)
            journal = Path(tmp) / "sandbox_orders.json"
            journal.write_text(json.dumps({"closed": [{"symbol": "SPY", "day_et": day,
                "pnl": -15, "exit_reasons": ["EOD_FORCE_CLOSE"]}],
                "pending": {"QQQ": {}}, "open": {}, "blocked": {}}))
            report = json.loads(audit.report(day, str(journal)).read_text())
            self.assertIn(report["report_status"], ("INTRADAY_SNAPSHOT", "POST_SESSION_SNAPSHOT"))
            self.assertIsNotNone(report["last_logged_event_at_et"])
            self.assertEqual(report["signals"]["decisions"], {"DEFER": 1})
            self.assertEqual(report["signals"]["reasons"]["UW_NOT_READY"], 1)
            self.assertEqual(report["live_quotes"]["known_outcomes"], 0)
            self.assertEqual(report["live_quotes"]["unknown_outcomes"], 1)
            self.assertNotIn("realized_pnl_usd_before_fees", report["broker"])
            self.assertEqual(report["broker"]["exit_reasons"], {"EOD_FORCE_CLOSE": 1})
            self.assertIn("SPY:None", report["evidence_coverage"]["missing_by_closed_trade"])
            self.assertEqual(report["broker"]["pending_at_report"], ["QQQ"])
            self.assertEqual(json.loads(audit.report(day, str(journal)).read_text())["broker"]["closed_trades"], 1)

    def test_eod_post_exit_quotes_use_live_ask_and_bid(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = AuditLog(os.path.join(tmp, "logs"))
            day = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
            journal = Path(tmp) / "sandbox_orders.json"
            journal.write_text(json.dumps({"closed": [{"symbol": "SPY", "trade_id": "t1",
                "day_et": day, "live_entry_ask": .63, "live_pnl": -5, "pnl": 100000}],
                "pending": {}, "open": {}, "blocked": {}}))
            audit.emit("POST_EXIT_OPTION_QUOTE", trade_id="t1", bid=.75, ask=.77)
            report = json.loads(audit.report(day, str(journal)).read_text())
            row = report["broker"]["trade_observations"][0]
            self.assertEqual(row["post_exit_peak_bid"], .75)
            self.assertEqual(row["post_exit_peak_ask_to_bid_usd_per_contract"], 12.)
            self.assertEqual(report["live_quotes"]["ask_to_exit_bid_usd"], -5.)

    def test_eod_flags_missing_context_and_preserves_uw_shadow_timing(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = AuditLog(os.path.join(tmp, "logs"))
            day = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
            audit.emit("OPTION_QUOTE", trade_id="t1", accepted=True, bid=1.2, ask=1.25)
            audit.emit("UW_POSITION_SAMPLE", trade_id="t1", sample_type="NET_FLOW",
                       uw_opposite_supports=True, uw_shadow_exit=True)
            audit.emit("POSITION_OBSERVATION", trade_id="t1", observation_source="OPTION_QUOTE",
                       decision_state="EXIT_PENDING", decision_reasons=["PROFIT_GIVEBACK_EXIT"])
            journal = Path(tmp) / "journal.json"
            journal.write_text(json.dumps({"closed": [{"symbol": "SPY", "trade_id": "t1",
                "day_et": day, "pnl": 5., "exit_reasons": ["PROFIT_GIVEBACK_EXIT"],
                "exit_order_id": "o1", "exit_fill": 1.19, "exit_trigger": {"bid": 1.2, "peak_bid": 1.4}}],
                "pending": {}, "open": {}, "blocked": {}}))
            report = json.loads(audit.report(day, str(journal)).read_text())
            trade = report["broker"]["trade_observations"][0]
            self.assertTrue(trade["evidence_available"]["uw_sample_history"])
            self.assertTrue(trade["first_uw_shadow_exit"]["uw_shadow_exit"])
            self.assertEqual(trade["exit_decision_context"]["decision_reasons"], ["PROFIT_GIVEBACK_EXIT"])
            self.assertEqual(report["evidence_coverage"]["missing_by_closed_trade"], {})


if __name__ == "__main__":
    unittest.main()
