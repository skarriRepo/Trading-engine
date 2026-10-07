import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from trading_engine.live_recovery import LiveQuoteRecovery
from trading_engine.tradier_client import PRODUCTION_BASE, SANDBOX_BASE

class LiveRecoveryTests(unittest.TestCase):
    def runtime(self):
        rt = Mock()
        rt._state_lock = threading.RLock()
        rt._regular_session.return_value = True
        rt.store.snapshot.return_value = SimpleNamespace(price_state='STALE')
        rt._last_underlying_wall = {}
        rt.positions = {'SPY': ('trade', SimpleNamespace(occ_symbol='SPY-OPTION'))}
        rt._latest_option_quote = {}
        rt.max_market_age_sec = 10
        rt.audit = None
        return rt

    def test_live_only_fresh_and_deduplicated(self):
        rt = self.runtime()
        client = Mock(config=SimpleNamespace(base_url=PRODUCTION_BASE))
        client.quotes.return_value = {
            'SPY': {'trade_date': 1000000, 'last': 100, 'last_volume': 400},
            'SPY-OPTION': {'bid_date': 1000000, 'bid': 2, 'ask': 2.1}}
        recovery = LiveQuoteRecovery(client, rt, ['SPY'])
        with patch('trading_engine.live_recovery.time.time', return_value=1000001):
            recovery.poll_once(); recovery.poll_once()
        rt.on_underlying_tick.assert_called_once_with('SPY', 1000000, 100, 0)
        rt.on_option_quote.assert_called_once_with('SPY-OPTION', 1000000, 2, 2.1)

    def test_rejects_delayed_and_future_data(self):
        rt = self.runtime()
        client = Mock(config=SimpleNamespace(base_url=PRODUCTION_BASE))
        client.quotes.return_value = {'SPY': {'trade_date': 999000, 'last': 100},
                                     'SPY-OPTION': {'bid_date': 1000100, 'bid': 2, 'ask': 2.1}}
        with patch('trading_engine.live_recovery.time.time', return_value=1000001):
            LiveQuoteRecovery(client, rt, ['SPY']).poll_once()
        rt.on_underlying_tick.assert_not_called()
        rt.on_option_quote.assert_not_called()

    def test_sandbox_client_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'production'):
            LiveQuoteRecovery(Mock(config=SimpleNamespace(base_url=SANDBOX_BASE)), self.runtime(), ['SPY'])
