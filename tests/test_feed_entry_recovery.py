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
