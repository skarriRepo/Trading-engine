import tempfile
import threading
import time
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock
from trading_engine.runtime import TradingRuntime
from trading_engine.sandbox_execution import SandboxExecution
from test_sandbox_execution import Broker

class RecoveryTests(unittest.TestCase):
    def test_only_verified_temporary_blocks_release(self):
        with tempfile.TemporaryDirectory() as d:
            broker=Broker()
            ex=SandboxExecution(broker,journal_path=str(Path(d)/'journal.json'))
            ex.runtime=SimpleNamespace(blocked_symbols={'SPY','NVDA'})
            ex.blocked={'SPY':'Broker has a working order for SPY260930C00600000; no duplicate submitted.',
                        'NVDA':'submission response lost'}
            broker.rows=[dict(symbol='SPY',option_symbol='SPY260930C00600000',status='open')]
            ex._release_clear_working_order_blocks()
            self.assertIn('SPY',ex.blocked)
            broker.rows[0]['status']='canceled'
            ex._release_clear_working_order_blocks()
            self.assertNotIn('SPY',ex.blocked)
            self.assertNotIn('SPY',ex.runtime.blocked_symbols)
            self.assertIn('NVDA',ex.blocked)

    def test_slow_entry_provider_does_not_hold_tick_lock(self):
        started=threading.Event();release=threading.Event()
        def provider(*args):
            started.set();release.wait(2);return None
        rt=TradingRuntime(option_entry_price_provider=provider,order_executor=Mock())
        rt.async_entries=True
        rt.entry_workers=ThreadPoolExecutor(max_workers=1)
        try:
            now=time.time();rt.store.ingest_tick('SPY',now,100)
            with rt._state_lock:
                rt._open_position('SPY','CALL',rt.store.snapshot('SPY',now),now)
            self.assertTrue(started.wait(1))
            self.assertTrue(rt._state_lock.acquire(timeout=.1))
            rt._state_lock.release()
            self.assertIn('SPY',rt.pending_symbols)
            release.set()
        finally:rt.entry_workers.shutdown(wait=True)
        self.assertNotIn('SPY',rt.pending_symbols)

    def test_legacy_caps_and_preview_failure_release_only_when_flat(self):
        with tempfile.TemporaryDirectory() as d:
            broker = Broker()
            ex = SandboxExecution(broker, journal_path=str(Path(d)/'journal.json'))
            ex.runtime = SimpleNamespace(blocked_symbols={'SPY'})
            for reason in ('Order debit $499.00 exceeds $300.00 cap.',
                           'Total sandbox debit cap reached.',
                           "Tradier rejected the order (HTTP 400): Unexpected server error"):
                ex.blocked = {'SPY': reason}
                broker.held = [dict(symbol='SPY260930C00600000', quantity=1)]
                ex._release_clear_working_order_blocks()
                self.assertIn('SPY', ex.blocked)
                broker.held = []
                ex._release_clear_working_order_blocks()
                self.assertNotIn('SPY', ex.blocked)

    def test_rejected_entry_receipt_requires_zero_fill(self):
        with tempfile.TemporaryDirectory() as d:
            broker = Broker()
            ex = SandboxExecution(broker, journal_path=str(Path(d)/'journal.json'))
            ex.blocked = {'SPY': 'Broker buy_to_open rejected (123)'}
            broker.rows = [dict(id=123, status='rejected', side='buy_to_open',
                                option_symbol='SPY260930C00600000', exec_quantity=1)]
            ex._release_old_canceled_entries()
            self.assertIn('SPY', ex.blocked)
            broker.rows[0]['exec_quantity'] = 0
            ex._release_old_canceled_entries()
            self.assertNotIn('SPY', ex.blocked)

    def test_cap_failure_does_not_post_or_poison_ticker(self):
        from unittest.mock import patch
        from test_sandbox_execution import SessionDate
        with tempfile.TemporaryDirectory() as d:
            broker = Broker()
            ex = SandboxExecution(broker, journal_path=str(Path(d)/'journal.json'))
            ex.runtime = SimpleNamespace(pending_symbols={'SPY'})
            with patch('trading_engine.sandbox_execution.datetime', SessionDate):
                ex.submit_entry('SPY', 'CALL', 'SPY260930C00600000', time.time(), 10)
            self.assertEqual(broker.post_count, 0)
            self.assertNotIn('SPY', ex.pending)
            self.assertNotIn('SPY', ex.blocked)
            self.assertNotIn('SPY', ex.runtime.pending_symbols)
