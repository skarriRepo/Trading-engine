import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import time
import unittest
from unittest.mock import MagicMock, patch

from trading_engine.tradier_poll import TradierPollClient


def _rest_client():
    with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
        from trading_engine.tradier_client import TradierRestClient
        return TradierRestClient(session=MagicMock())


class TestQuoteDispatch(unittest.TestCase):
    def test_equity_quote_dispatches_to_on_tick(self):
        ticks = []
        client = TradierPollClient(symbols=["AAPL"], rest_client=_rest_client(),
                                    on_tick=lambda *a: ticks.append(a))
        client._dispatch_quote("AAPL", {"type": "stock", "last": 150.25, "trade_date": int(time.time()*1000)})
        self.assertEqual(len(ticks), 1)
        self.assertEqual(ticks[0][0], "AAPL")
        self.assertEqual(ticks[0][2], 150.25)

    def test_option_quote_dispatches_to_on_quote_not_on_tick(self):
        ticks, quotes = [], []
        client = TradierPollClient(symbols=["AAPL260115C00150000"], rest_client=_rest_client(),
                                    on_tick=lambda *a: ticks.append(a),
                                    on_quote=lambda *a: quotes.append(a))
        client._dispatch_quote("AAPL260115C00150000", {"type": "option", "bid": 1.20, "ask": 1.25, "bid_date": int(time.time()*1000)})
        self.assertEqual(len(ticks), 0)
        self.assertEqual(len(quotes), 1)
        self.assertEqual(quotes[0][2:], (1.20, 1.25))  # (symbol, ts, bid, ask) -- skip symbol and ts

    def test_repeated_delayed_trade_does_not_create_new_bar(self):
        ticks = []
        client = TradierPollClient(symbols=["SPY"], rest_client=_rest_client(), on_tick=lambda *a: ticks.append(a))
        delayed = int((time.time() - 900)*1000)
        for _ in range(5):
            client._dispatch_quote("SPY", {"type":"etf", "last": 600, "trade_date": delayed})
        self.assertEqual(len(ticks), 1)
        self.assertAlmostEqual(ticks[0][1], delayed / 1000, places=3)

    def test_option_quote_with_no_on_quote_handler_is_silently_skipped(self):
        client = TradierPollClient(symbols=["X"], rest_client=_rest_client(), on_tick=lambda *a: None)
        client._dispatch_quote("X", {"type": "option", "bid": 1.0, "ask": 1.05})  # must not raise

    def test_missing_last_price_for_equity_does_not_call_on_tick(self):
        ticks = []
        client = TradierPollClient(symbols=["AAPL"], rest_client=_rest_client(),
                                    on_tick=lambda *a: ticks.append(a))
        client._dispatch_quote("AAPL", {"type": "stock"})
        self.assertEqual(ticks, [])

    def test_zero_bid_and_ask_for_option_does_not_call_on_quote(self):
        quotes = []
        client = TradierPollClient(symbols=["X"], rest_client=_rest_client(), on_tick=lambda *a: None,
                                    on_quote=lambda *a: quotes.append(a))
        client._dispatch_quote("X", {"type": "option", "bid": 0.0, "ask": 0.0})
        self.assertEqual(quotes, [])


class TestDynamicSubscription(unittest.TestCase):
    def test_add_and_remove_symbol_mirror_the_stream_client_interface(self):
        client = TradierPollClient(symbols=["SPY"], rest_client=_rest_client(), on_tick=lambda *a: None)
        client.add_symbol("AAPL260115C00150000")
        self.assertIn("AAPL260115C00150000", client.symbols)
        client.remove_symbol("AAPL260115C00150000")
        self.assertNotIn("AAPL260115C00150000", client.symbols)

    def test_add_symbol_is_idempotent(self):
        client = TradierPollClient(symbols=["SPY"], rest_client=_rest_client(), on_tick=lambda *a: None)
        client.add_symbol("SPY")
        self.assertEqual(client.symbols.count("SPY"), 1)

    def test_remove_missing_symbol_is_a_safe_no_op(self):
        client = TradierPollClient(symbols=["SPY"], rest_client=_rest_client(), on_tick=lambda *a: None)
        client.remove_symbol("NOT_THERE")  # must not raise


class TestPollLoop(unittest.TestCase):
    def test_a_poll_cycle_calls_quotes_with_the_current_symbol_list_and_dispatches_results(self):
        rest_client = _rest_client()
        rest_client.quotes = MagicMock(return_value={
            "AAPL": {"type": "stock", "last": 150.25, "trade_date": int(time.time()*1000)},
            "MSFT260115C00400000": {"type": "option", "bid": 2.0, "ask": 2.05, "bid_date": int(time.time()*1000)},
        })
        ticks, quotes, connected_states = [], [], []
        client = TradierPollClient(symbols=["AAPL", "MSFT260115C00400000"], rest_client=rest_client,
                                    on_tick=lambda *a: ticks.append(a), on_quote=lambda *a: quotes.append(a),
                                    on_connected=lambda c: connected_states.append(c),
                                    poll_interval_sec=0.05)
        client.start()
        time.sleep(0.15)
        client.close()
        client._thread.join(timeout=2)

        self.assertGreaterEqual(len(ticks), 1)
        self.assertGreaterEqual(len(quotes), 1)
        self.assertIn(True, connected_states)
        self.assertIn(False, connected_states)

    def test_auth_error_during_polling_propagates_and_signals_disconnected(self):
        rest_client = _rest_client()
        from trading_engine.tradier_client import TradierAuthError
        rest_client.quotes = MagicMock(side_effect=TradierAuthError("401"))
        connected_states = []
        client = TradierPollClient(symbols=["AAPL"], rest_client=rest_client, on_tick=lambda *a: None,
                                    on_connected=lambda c: connected_states.append(c), poll_interval_sec=0.05)
        client.start()
        time.sleep(0.15)
        client.close()
        self.assertIn(False, connected_states)


class TestEndToEndWithRealRuntime(unittest.TestCase):
    """Confirms polled quotes actually drive a real TradingRuntime, the same
    bar it was held to for the streaming client."""

    def test_polled_equity_ticks_flow_into_the_real_runtime(self):
        from trading_engine.runtime import TradingRuntime
        rt = TradingRuntime()
        base_ts = 1768494600.0
        for i in range(15):
            client = TradierPollClient(symbols=["AAPL"], rest_client=_rest_client(), on_tick=rt.on_underlying_tick)
            client._dispatch_quote("AAPL", {"type": "stock", "last": 100.0 + i,
                                             "trade_date": int((time.time() - 60 + i)*1000)})
        snap = rt.store.snapshot("AAPL", now=time.time() + 1.0)
        self.assertIsNotNone(snap.price)
        self.assertGreater(len(snap.bars), 0)


if __name__ == '__main__':
    unittest.main()
