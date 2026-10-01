import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import threading
import time
import unittest

from trading_engine.symbol_state import SymbolStream
from trading_engine.entry_pipeline import evaluate_entry
from trading_engine.exit_pipeline import PositionState, evaluate_exit, ExitConfig
from trading_engine.dashboard_store import DashboardStore


def real_uptrend_snapshot(symbol='AAPL', bar_seconds=120.0):
    stream = SymbolStream(symbol, bar_seconds=bar_seconds)
    closes = [100 + i + (i * i) * 0.03 for i in range(20)]
    t = 1768494600.0  # safe, mid-session ET timestamp
    for c in closes:
        stream.ingest_tick(ts=t, price=c)
        t += bar_seconds
    now = t + 130.0
    return stream.snapshot(now=now), now


class TestScanFlow(unittest.TestCase):
    def test_real_entry_decision_flows_through_to_the_scan_view(self):
        snap, now = real_uptrend_snapshot('AAPL')
        decision = evaluate_entry(snap, 'CALL', now=now)
        self.assertEqual(decision.action, 'TAKE')  # sanity: this is the known-good scenario from stage 2's own tests

        store = DashboardStore()
        store.record_scan(snap, decision, now=now)
        rows = store.scan_view()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row['symbol'], 'AAPL')
        self.assertEqual(row['action'], 'TAKE')
        self.assertNotIn('structure', row)
        self.assertIn('ENTRY_AUTHORIZED', row['reasons'])

    def test_scan_view_is_the_latest_record_per_symbol_not_a_log(self):
        store = DashboardStore()
        snap1, now1 = real_uptrend_snapshot('AAPL')
        store.record_scan(snap1, None, now=now1)
        snap2, now2 = real_uptrend_snapshot('AAPL')  # same symbol, later refresh
        store.record_scan(snap2, None, now=now2)
        rows = store.scan_view()
        self.assertEqual(len(rows), 1)  # not two -- latest overwrites, doesn't accumulate

    def test_last_psar_decision_survives_market_scan_refresh(self):
        store = DashboardStore()
        snap, now = real_uptrend_snapshot('AAPL')
        decision = evaluate_entry(snap, 'CALL', now=now)
        store.record_scan(snap, decision, now=now)
        store.record_scan(snap, None, now=now + 1)
        row = store.scan_view()[0]
        self.assertEqual(row['action'], decision.action)
        self.assertEqual(row['decision_ts'], now)
        self.assertEqual(row['recorded_ts'], now + 1)

    def test_multiple_symbols_are_sorted_alphabetically(self):
        store = DashboardStore()
        for sym in ['TSLA', 'AAPL', 'MSFT']:
            snap, now = real_uptrend_snapshot(sym)
            store.record_scan(snap, None, now=now)
        rows = store.scan_view()
        self.assertEqual([r['symbol'] for r in rows], ['AAPL', 'MSFT', 'TSLA'])


class TestActiveAndClosedFlow(unittest.TestCase):
    def test_broker_fill_dollars_are_distinct_from_internal_quote_results(self):
        store = DashboardStore()
        broker = PositionState(symbol='NVDA', direction='PUT', opened_ts=1000.0,
                               entry_option_price=1.10, occ_symbol='NVDA_OPTION',
                               quantity=2, entry_order_id='entry-1', broker_entry_fill=1.12)
        store.record_open('broker', broker, now=1000.0)
        self.assertEqual(store.active_view()[0]['occ_symbol'], 'NVDA_OPTION')
        store.close_manually('broker', exit_price=1.15, reasons=('FILLED',), now=1100.0,
                             live_exit_bid=1.14)
        internal = PositionState(symbol='SPY', direction='CALL', opened_ts=1000.0,
                                 entry_option_price=1.0)
        store.record_open('internal', internal, now=1000.0)
        store.close_manually('internal', exit_price=1.5, reasons=('SIMULATED',), now=1100.0)
        rows = {row['trade_id']: row for row in store.closed_view()}
        self.assertAlmostEqual(rows['broker']['live_quote_pnl_usd'], 8.0)
        self.assertAlmostEqual(rows['broker']['entry_price'], 1.10)
        self.assertAlmostEqual(rows['broker']['exit_price'], 1.14)
        self.assertTrue(rows['broker']['broker_confirmed'])
        self.assertIsNone(rows['internal']['live_quote_pnl_usd'])

    def test_open_position_appears_in_active_not_closed(self):
        store = DashboardStore()
        pos = PositionState(symbol='AAPL', direction='CALL', opened_ts=1000.0, entry_option_price=1.0)
        store.record_open('t1', pos, now=1001.0)
        self.assertEqual(len(store.active_view()), 1)
        self.assertEqual(len(store.closed_view()), 0)

    def test_real_exit_pending_decision_moves_position_from_active_to_closed(self):
        store = DashboardStore()
        snap, now = real_uptrend_snapshot('AAPL')
        pos = PositionState(symbol='AAPL', direction='CALL', opened_ts=now - 500, entry_option_price=1.0)
        # Recalibrated against the real arm formula (spread_arm_multiple=8.0,
        # premium_arm_fraction=0.08): peak must clear entry + max(8*spread,
        # 0.08*entry). peak=1.35/spread=0.05 (arm_gain=0.4) fell just short
        # at 0.35 -- verified directly against evaluate_exit() before fixing.
        pos.update_price(1.45, now - 200, ask=1.50)
        pos.update_price(1.20, now, ask=1.25)
        store.record_open('t1', pos, now=now - 500)

        decision = evaluate_exit(snap, pos, now=now)
        self.assertEqual(decision.state, 'EXIT_PENDING')  # sanity check on the real ladder
        self.assertEqual(decision.reasons, ('PROFIT_GIVEBACK_EXIT',))

        store.record_exit_check('t1', pos, decision, now=now)
        self.assertEqual(len(store.active_view()), 0)
        closed = store.closed_view()
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0]['exit_reasons'], ['PROFIT_GIVEBACK_EXIT'])
        self.assertAlmostEqual(closed[0]['peak_gain_pct'], 45.0, places=1)  # peak 1.45 vs entry 1.0

    def test_hold_decision_keeps_position_active(self):
        store = DashboardStore()
        snap, now = real_uptrend_snapshot('AAPL')
        pos = PositionState(symbol='AAPL', direction='CALL', opened_ts=now - 100, entry_option_price=1.0)
        pos.update_price(1.02, now)
        store.record_open('t1', pos, now=now - 100)
        decision = evaluate_exit(snap, pos, now=now)
        store.record_exit_check('t1', pos, decision, now=now)
        self.assertEqual(len(store.active_view()), 1)
        self.assertEqual(len(store.closed_view()), 0)

    def test_update_on_an_already_closed_trade_id_is_ignored(self):
        store = DashboardStore()
        pos = PositionState(symbol='AAPL', direction='CALL', opened_ts=1000.0, entry_option_price=1.0)
        store.record_open('t1', pos, now=1001.0)
        store.close_manually('t1', exit_price=1.1, reasons=('MANUAL',), now=1002.0)
        self.assertEqual(len(store.closed_view()), 1)
        # A stale update arriving after the close must not resurrect it or double-close it.
        from trading_engine.exit_pipeline import ExitDecision
        stale_decision = ExitDecision(state='HOLD', reasons=('THESIS_EXPANDING',), gain_pct=1.0, peak_gain_pct=1.0)
        store.record_exit_check('t1', pos, stale_decision, now=1003.0)
        self.assertEqual(len(store.active_view()), 0)
        self.assertEqual(len(store.closed_view()), 1)

    def test_manual_close_path(self):
        store = DashboardStore()
        pos = PositionState(symbol='TSLA', direction='PUT', opened_ts=1000.0, entry_option_price=2.0)
        pos.update_price(2.4, 1050.0)
        store.record_open('t2', pos, now=1001.0)
        store.close_manually('t2', exit_price=2.1, reasons=('MANUAL_CLOSE',), now=1100.0)
        closed = store.closed_view()
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0]['exit_price'], 2.1)
        self.assertEqual(closed[0]['exit_reasons'], ['MANUAL_CLOSE'])


class TestThreadSafety(unittest.TestCase):
    def test_concurrent_writes_and_reads_do_not_crash(self):
        store = DashboardStore()
        stop = threading.Event()
        errors = []

        def writer():
            i = 0
            while not stop.is_set():
                try:
                    pos = PositionState(symbol='AAPL', direction='CALL', opened_ts=time.time(), entry_option_price=1.0)
                    tid = f't{i}'
                    store.record_open(tid, pos, now=time.time())
                    if i % 3 == 0:
                        store.close_manually(tid, exit_price=1.05, reasons=('T',), now=time.time())
                    i += 1
                except Exception as exc:
                    errors.append(exc)

        def reader():
            while not stop.is_set():
                try:
                    store.active_view(); store.closed_view(); store.scan_view()
                except Exception as exc:
                    errors.append(exc)

        threads = [threading.Thread(target=writer) for _ in range(2)] + [threading.Thread(target=reader) for _ in range(2)]
        for t in threads: t.start()
        time.sleep(0.3)
        stop.set()
        for t in threads: t.join(timeout=2)
        self.assertEqual(errors, [])


class TestFastAPIApp(unittest.TestCase):
    """End-to-end through the real HTTP layer, not just the store directly."""

    def setUp(self):
        from fastapi.testclient import TestClient
        import dashboard_app
        dashboard_app.store = DashboardStore()  # fresh store per test
        self.app_module = dashboard_app
        self.client = TestClient(dashboard_app.app)

    def test_index_serves_html(self):
        r = self.client.get('/')
        self.assertEqual(r.status_code, 200)
        self.assertIn('Scan', r.text)
        self.assertIn('Active', r.text)
        self.assertIn('Closed', r.text)

    def test_scan_endpoint_reflects_a_real_recorded_snapshot(self):
        snap, now = real_uptrend_snapshot('NVDA')
        decision = evaluate_entry(snap, 'CALL', now=now)
        self.app_module.store.record_scan(snap, decision, now=now)
        r = self.client.get('/api/scan')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['symbol'], 'NVDA')
        self.assertEqual(data[0]['action'], 'TAKE')

    def test_active_and_closed_endpoints_reflect_a_real_exit(self):
        snap, now = real_uptrend_snapshot('AAPL')
        pos = PositionState(symbol='AAPL', direction='CALL', opened_ts=now - 500, entry_option_price=1.0)
        # Same recalibration as test_real_exit_pending_decision_... above:
        # a real spread is required to arm at all under the spread-based
        # design, and peak must clear entry + max(8*spread, 0.08*entry).
        pos.update_price(1.45, now - 200, ask=1.50)
        pos.update_price(1.20, now, ask=1.25)
        self.app_module.store.record_open('t1', pos, now=now - 500)
        self.assertEqual(len(self.client.get('/api/active').json()), 1)

        decision = evaluate_exit(snap, pos, now=now)
        self.app_module.store.record_exit_check('t1', pos, decision, now=now)

        self.assertEqual(len(self.client.get('/api/active').json()), 0)
        closed = self.client.get('/api/closed').json()
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0]['exit_reasons'], ['PROFIT_GIVEBACK_EXIT'])


if __name__ == '__main__':
    unittest.main()
