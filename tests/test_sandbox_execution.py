"""Order lifecycle checks against a deterministic fake Tradier account."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime as RealDate

from trading_engine.sandbox_execution import SandboxExecution
from trading_engine.tradier_orders import OrderResult, TradierOrderClient, TradierOrderError
from trading_engine.runtime import TradingRuntime
from trading_engine.audit_log import AuditLog
import json

OCC = "SPY260930C00600000"


class SessionDate:
    @staticmethod
    def now(tz):
        return RealDate(2026, 9, 29, 10, 0, tzinfo=tz)


class Broker:
    account_id = "SANDBOX1"
    rest_client = SimpleNamespace(config=SimpleNamespace(base_url="https://sandbox.tradier.com/v1"))

    def __init__(self):
        self.rows = []
        self.held = []
        self.post_count = 0
        self.fail_post = False
        self.canceled = []

    def positions(self):
        return list(self.held)

    def orders(self):
        return list(self.rows)

    def get_order(self, order_id):
        return next(x for x in self.rows if str(x["id"]) == order_id)

    def preview(self, occ, symbol, side, qty, *, tag, order_type, limit_price):
        return OrderResult(None, "ok", {})

    def cancel_order(self, order_id):
        self.canceled.append(order_id)

    def _submit(self, occ, symbol, side, qty, tag):
        self.post_count += 1
        row = dict(id=self.post_count, option_symbol=occ, symbol=symbol,
                   side=side, quantity=qty, tag=tag, status="pending")
        self.rows.append(row)
        if self.fail_post:
            raise TimeoutError("response lost after broker received POST")
        return OrderResult(str(row["id"]), "ok", {})

    def buy_to_open(self, occ, symbol, qty, *, order_type, limit_price, tag):
        assert order_type == "limit" and limit_price > 0
        return self._submit(occ, symbol, "buy_to_open", qty, tag)

    def sell_to_close(self, occ, symbol, qty, *, tag):
        return self._submit(occ, symbol, "sell_to_close", qty, tag)


class SandboxOrderLifecycle(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "orders.json")
        self.broker = Broker()
        self.executor = SandboxExecution(self.broker, journal_path=self.path)
        self.rt = TradingRuntime(order_executor=self.executor)
        self.executor.attach(self.rt)

    def enter(self):
        with patch("trading_engine.sandbox_execution.datetime", SessionDate):
            self.executor.submit_entry("SPY", "CALL", OCC, 1., 1.20)

    def filled_entry(self):
        self.enter()
        self.broker.rows[0].update(status="filled", exec_quantity=1, avg_fill_price=1.23)
        self.broker.held = [dict(symbol=OCC, quantity=1)]
        self.executor.reconcile()

    def test_entry_then_exit_only_after_confirmed_broker_fills(self):
        self.enter()
        self.assertEqual(self.broker.post_count, 1)
        self.assertFalse(self.rt.positions)
        self.executor.reconcile()
        self.assertFalse(self.rt.positions)
        self.broker.rows[0].update(status="filled", exec_quantity=1, avg_fill_price=1.23)
        self.broker.held = [dict(symbol=OCC, quantity=1)]
        self.executor.reconcile()
        self.assertEqual(self.rt.positions["SPY"][1].entry_option_price, 1.20)
        self.assertEqual(self.rt.positions["SPY"][1].broker_entry_fill, 1.23)
        self.assertEqual(len(self.rt.dashboard.active_view()), 1)
        self.executor.submit_exit("SPY", self.rt.positions["SPY"][1], ("TEST_EXIT",), 2.,
                                  observation={"bid": 1.30, "ask": 1.35, "peak_bid": 1.45,
                                               "market_ts": 2., "quote_age_sec": .2})
        self.assertIn("SPY", self.rt.positions)
        self.broker.rows[1].update(status="filled", exec_quantity=1, avg_fill_price=1.40)
        self.broker.held = []
        self.executor.reconcile()
        self.assertFalse(self.rt.positions)
        self.assertEqual(self.rt.dashboard.closed_view()[0]["exit_price"], 1.30)

    def test_eod_analysis_uses_live_quotes_and_keeps_order_receipts(self):
        self.executor.audit = AuditLog(str(Path(self.temp.name) / "logs"))
        self.filled_entry()
        self.executor.audit.emit("OPTION_QUOTE", symbol="SPY", trade_id=self.executor.open["SPY"]["tag"],
                                 bid=1.45, ask=1.50, market_ts=2., quote_age_sec=.2)
        self.rt._latest_option_quote[self.executor.open["SPY"]["tag"]] = {
            "bid": 1.35, "ask": 1.38, "market_ts": 2., "received_at_ts": 2.}
        self.executor.submit_exit("SPY", self.rt.positions["SPY"][1], ("TEST_EXIT",), 2.,
                                  observation={"bid": 1.30, "ask": 1.35, "peak_bid": 1.45,
                                               "market_ts": 2., "quote_age_sec": .2})
        self.broker.rows[1].update(status="filled", exec_quantity=1, avg_fill_price=1.40)
        self.broker.held = []
        self.executor.reconcile()
        day = RealDate.now().astimezone(__import__("zoneinfo").ZoneInfo("America/New_York")).date().isoformat()
        report = json.loads(self.executor.audit.report(day, self.path).read_text())
        self.assertEqual(report["live_quotes"]["ask_to_exit_bid_usd"], 10.0)
        self.assertEqual(report["live_quotes"]["known_outcomes"], 1)
        self.assertNotIn("realized_pnl_usd_before_fees", report["broker"])
        self.assertEqual(report["broker"]["exit_reasons"], {"TEST_EXIT": 1})
        self.assertEqual(report["event_counts"]["BROKER_ENTRY_FILLED"], 1)
        self.assertEqual(report["event_counts"]["BROKER_EXIT_FILLED"], 1)
        obs = report["broker"]["trade_observations"][0]
        self.assertTrue(obs["complete"])
        self.assertEqual(obs["peak_to_trigger_usd_per_contract"], 15.0)
        self.assertEqual(obs["trigger_bid_to_fill_usd_per_contract"], -10.0)
        self.assertEqual(obs["latest_quote_at_reconciliation"]["bid"], 1.35)

    def test_rejected_entry_does_not_create_position(self):
        self.enter()
        self.broker.rows[0]["status"] = "rejected"
        self.executor.reconcile()
        self.assertFalse(self.rt.positions)
        self.assertIn("SPY", self.executor.blocked)

    def test_confirmed_zero_fill_cancel_releases_symbol_for_next_signal(self):
        self.enter()
        self.broker.rows[0].update(status="canceled", exec_quantity=0)
        self.executor.reconcile()
        self.assertNotIn("SPY", self.executor.pending)
        self.assertNotIn("SPY", self.executor.blocked)
        self.assertNotIn("SPY", self.rt.pending_symbols)
        self.assertNotIn("SPY", self.rt.blocked_symbols)
        self.enter()
        self.assertEqual(self.broker.post_count, 2)

    def test_startup_releases_persisted_confirmed_cancel_only(self):
        self.enter()
        self.broker.rows[0].update(status="canceled", exec_quantity=0)
        self.executor.reconcile()
        self.executor.block("SPY", "Broker buy_to_open canceled (1)")
        restarted = SandboxExecution(self.broker, journal_path=self.path)
        new_rt = TradingRuntime(order_executor=restarted)
        restarted.attach(new_rt)
        self.assertNotIn("SPY", restarted.blocked)
        self.assertNotIn("SPY", new_rt.blocked_symbols)

    def test_missing_execution_quantity_keeps_cancel_blocked(self):
        self.enter()
        self.broker.rows[0]["status"] = "canceled"
        self.executor.reconcile()
        self.assertIn("SPY", self.executor.blocked)

    def test_other_working_order_keeps_cancel_blocked(self):
        self.enter()
        self.broker.rows[0].update(status="canceled", exec_quantity=0)
        self.broker.rows.append(dict(id=2, symbol="SPY", option_symbol=OCC,
                                     status="pending", side="buy_to_open"))
        self.executor.reconcile()
        self.assertIn("SPY", self.executor.blocked)

    def test_held_contract_keeps_cancel_blocked(self):
        self.enter()
        self.broker.rows[0].update(status="canceled", exec_quantity=0)
        self.broker.held = [dict(symbol=OCC, quantity=1)]
        self.executor.reconcile()
        self.assertIn("SPY", self.executor.pending)
        self.assertEqual(self.broker.post_count, 1)

    def test_mismatched_contract_keeps_cancel_blocked(self):
        self.enter()
        self.broker.rows[0].update(status="canceled", exec_quantity=0,
                                   option_symbol="SPY260930P00600000")
        self.executor.reconcile()
        self.assertIn("SPY", self.executor.blocked)

    def test_unknown_post_recovers_by_tag_without_resubmitting(self):
        self.broker.fail_post = True
        self.enter()
        self.assertEqual(self.executor.pending["SPY"]["status"], "UNKNOWN")
        self.executor.reconcile()
        self.assertEqual(self.broker.post_count, 1)
        self.assertEqual(self.executor.pending["SPY"]["order_id"], "1")
        restarted = SandboxExecution(self.broker, journal_path=self.path)
        new_rt = TradingRuntime(order_executor=restarted)
        restarted.attach(new_rt)
        self.assertIn("SPY", new_rt.pending_symbols)
        self.assertEqual(self.broker.post_count, 1)

    def test_empty_tradier_account_attaches_without_orders(self):
        rest = SimpleNamespace(config=SimpleNamespace(base_url="https://sandbox.tradier.com/v1"),
                               _get=lambda path, params: {"positions": "null"})
        client = TradierOrderClient(rest, "SANDBOX1")
        executor = SandboxExecution(client, journal_path=str(Path(self.temp.name) / "empty.json"))
        executor.attach(TradingRuntime(order_executor=executor))
        self.assertEqual(executor.open, {})

    def test_partial_terminal_order_stays_unresolved(self):
        self.enter()
        self.broker.rows[0].update(status="canceled", exec_quantity=0.5)
        self.executor.reconcile()
        self.assertIn("SPY", self.executor.pending)
        self.assertFalse(self.rt.positions)

    def test_extra_broker_contract_fails_restart(self):
        self.filled_entry()
        self.broker.held = [dict(symbol=OCC, quantity=2)]
        restarted = SandboxExecution(self.broker, journal_path=self.path)
        with self.assertRaisesRegex(Exception, "differs from broker quantity 2"):
            restarted.attach(TradingRuntime(order_executor=restarted))

    def test_confirmed_exit_filled_before_restart_reconciles(self):
        self.filled_entry()
        self.executor.submit_exit("SPY", self.rt.positions["SPY"][1], ("TEST_EXIT",), 2.,
                                  observation={"bid": 1.30})
        self.broker.rows[1].update(status="filled", exec_quantity=1, avg_fill_price=1.40)
        self.broker.held = []
        restarted = SandboxExecution(self.broker, journal_path=self.path)
        new_rt = TradingRuntime(order_executor=restarted)
        restarted.attach(new_rt)
        self.assertNotIn("SPY", restarted.open)
        self.assertNotIn("SPY", restarted.pending)
        self.assertEqual(len(restarted.closed), 1)
        self.assertEqual(self.broker.post_count, 2)

    def test_missing_broker_position_without_journaled_exit_still_stops(self):
        self.filled_entry()
        self.broker.held = []
        restarted = SandboxExecution(self.broker, journal_path=self.path)
        with self.assertRaisesRegex(Exception, "broker quantity 0"):
            restarted.attach(TradingRuntime(order_executor=restarted))

    def test_manual_broker_close_with_unique_filled_receipt_is_journaled(self):
        self.filled_entry()
        self.executor.block("SPY", "exit preview internal server error")
        self.broker.rows.append(dict(id=2, side="sell_to_close", option_symbol=OCC,
                                     status="filled", quantity=1, exec_quantity=1,
                                     avg_fill_price=1.35))
        self.broker.held = []
        restarted = SandboxExecution(self.broker, journal_path=self.path)
        restarted.attach(TradingRuntime(order_executor=restarted))
        self.assertNotIn("SPY", restarted.open)
        self.assertNotIn("SPY", restarted.blocked)
        self.assertEqual(restarted.closed[-1]["exit_reasons"], ["MANUAL_BROKER_CLOSE"])
        self.assertEqual(restarted.closed[-1]["exit_order_id"], "2")

    def test_exit_preview_internal_server_error_still_submits_once(self):
        self.filled_entry()
        original = self.broker.preview
        def preview(occ, symbol, side, qty, **kwargs):
            if side == "sell_to_close":
                raise TradierOrderError("HTTP 400: Unexpected server error")
            return original(occ, symbol, side, qty, **kwargs)
        with patch.object(self.broker, "preview", side_effect=preview):
            self.executor.submit_exit("SPY", self.rt.positions["SPY"][1],
                                      ("PROFIT_GIVEBACK_EXIT",), 2.)
        self.assertEqual(self.broker.post_count, 2)
        self.assertEqual(self.executor.pending["SPY"]["side"], "sell_to_close")

    def test_exit_preview_validation_error_stops_submission(self):
        self.filled_entry()
        with patch.object(self.broker, "preview", side_effect=TradierOrderError("invalid quantity")):
            with self.assertRaisesRegex(TradierOrderError, "invalid quantity"):
                self.executor.submit_exit("SPY", self.rt.positions["SPY"][1], ("EXIT",), 2.)
        self.assertEqual(self.broker.post_count, 1)

    def test_ambiguous_manual_broker_close_does_not_auto_reconcile(self):
        self.filled_entry()
        self.broker.rows.extend(dict(id=n, side="sell_to_close", option_symbol=OCC,
                                     status="filled", quantity=1, exec_quantity=1,
                                     avg_fill_price=1.35) for n in (2, 3))
        self.broker.held = []
        restarted = SandboxExecution(self.broker, journal_path=self.path)
        with self.assertRaisesRegex(Exception, "broker quantity 0"):
            restarted.attach(TradingRuntime(order_executor=restarted))

    def test_entry_debit_cap_blocks_before_submission(self):
        with patch("trading_engine.sandbox_execution.datetime", SessionDate):
            with self.assertRaisesRegex(Exception, "exceeds"):
                self.executor.submit_entry("SPY", "CALL", OCC, 1., 10.00)
        self.assertEqual(self.broker.post_count, 0)

    def test_transient_windows_replace_denial_retries_without_order_duplication(self):
        import os
        real_replace = os.replace
        attempts = []
        def flaky_replace(src, dst):
            attempts.append((src, dst))
            if len(attempts) <= 2:
                raise PermissionError(13, "Access denied", str(dst))
            return real_replace(src, dst)
        with patch("trading_engine.sandbox_execution.os.replace", side_effect=flaky_replace), \
             patch("trading_engine.sandbox_execution.time.sleep"):
            self.enter()
        self.assertGreaterEqual(len(attempts), 3)
        self.assertEqual(self.broker.post_count, 1)
        self.assertIn("SPY", json.loads(Path(self.path).read_text())["pending"])

    def test_daily_realized_loss_cap_blocks_next_entry(self):
        self.executor.max_daily_loss = 100
        self.executor.closed.append({"day_et": RealDate(2026, 9, 29).date().isoformat(),
                                     "pnl": -110, "live_pnl": -110})
        with patch("trading_engine.sandbox_execution.datetime", SessionDate):
            with self.assertRaisesRegex(Exception, "Daily sandbox loss"):
                self.executor.submit_entry("SPY", "CALL", OCC, 1., 1.20)
        self.assertEqual(self.broker.post_count, 0)

    def test_manual_close_missing_exit_quote_uses_live_entry_risk_bound(self):
        self.executor.max_daily_loss = 500
        self.executor.closed.append({"day_et": "2026-09-29", "symbol": "NVDA",
                                     "live_entry_ask": 1.51, "live_entry_limit": 1.59,
                                     "live_pnl": None, "quantity": 1, "pnl": 100000})
        self.enter()
        self.assertEqual(self.broker.post_count, 1)

    def test_missing_quote_bound_blocks_or_respects_daily_cap(self):
        self.executor.closed.append({"day_et": "2026-09-29", "live_pnl": None,
                                     "quantity": 1, "pnl": 100000})
        with patch("trading_engine.sandbox_execution.datetime", SessionDate):
            with self.assertRaisesRegex(TradierOrderError, "live entry quote"):
                self.executor.submit_entry("SPY", "CALL", OCC, 1., 1.20)
        self.executor.closed[-1]["live_entry_ask"] = 4.50
        self.executor.max_daily_loss = 500
        with patch("trading_engine.sandbox_execution.datetime", SessionDate):
            with self.assertRaisesRegex(TradierOrderError, "Daily sandbox loss"):
                self.executor.submit_entry("SPY", "CALL", OCC, 1., 1.20)
        self.assertEqual(self.broker.post_count, 0)

    def test_expired_entry_limit_cancel_requested_once(self):
        self.enter()
        self.executor.pending["SPY"]["submitted_at_wall"] -= 60
        self.executor.reconcile()
        self.executor.reconcile()
        self.assertEqual(self.broker.canceled, ["1"])
        self.assertIn("SPY", self.executor.pending)

    def test_tradier_submission_uses_form_encoded_sandbox_option_payload(self):
        seen = []
        class Response:
            status_code = 200
            def json(self):
                return {"order": {"id": 123, "status": "ok", "result": True}}
        class Session:
            def post(self, url, **kwargs):
                seen.append((url, kwargs))
                return Response()
        rest = SimpleNamespace(config=SimpleNamespace(
            base_url="https://sandbox.tradier.com/v1", timeout_sec=5),
            _session=Session(), _headers=lambda: {"Authorization": "Bearer TEST"})
        client = TradierOrderClient(rest, "SANDBOX1")
        self.assertEqual(client.buy_to_open(OCC, "SPY", 1, tag="test").order_id, "123")
        url, kwargs = seen[0]
        self.assertEqual(url, "https://sandbox.tradier.com/v1/accounts/SANDBOX1/orders")
        self.assertEqual(kwargs["data"], {"class": "option", "symbol": "SPY",
                         "option_symbol": OCC, "side": "buy_to_open", "quantity": "1",
                         "type": "market", "duration": "day", "tag": "test"})
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/x-www-form-urlencoded")


if __name__ == "__main__":
    unittest.main()
