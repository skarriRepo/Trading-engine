import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import json
import unittest
from unittest.mock import MagicMock, patch

from trading_engine.tradier_stream import parse_stream_message, ParsedTick, ParsedQuote, TradierStreamClient, _to_ws_scheme


class TestTradeMessages(unittest.TestCase):
    def test_option_contract_trade_cannot_become_an_underlying_psar_bar(self):
        self.assertIsNone(parse_stream_message({"type": "trade",
            "symbol": "NVDA260930C00230000", "price": "1.25",
            "size": "10", "date": "1790775600000"}))

    def test_realistic_trade_message_parses_to_a_tick(self):
        # Matches Tradier's documented "trade" message shape.
        raw = {"type": "trade", "symbol": "AAPL", "exch": "Q", "price": "150.25",
               "size": "100", "cvol": "125000", "date": "1768494600000", "last": "150.25"}
        parsed = parse_stream_message(raw)
        self.assertIsInstance(parsed, ParsedTick)
        self.assertEqual(parsed.symbol, "AAPL")
        self.assertEqual(parsed.price, 150.25)
        self.assertEqual(parsed.volume, 100.0)
        self.assertAlmostEqual(parsed.ts, 1768494600.0, places=3)

    def test_realistic_timesale_message_parses_to_a_tick(self):
        raw = {"type": "timesale", "symbol": "MSFT", "exch": "Q", "bid": "410.05",
               "ask": "410.15", "last": "410.10", "size": "50", "date": "1768494720000",
               "seq": 12345, "flag": "", "cancel": False, "correction": ""}
        parsed = parse_stream_message(raw)
        self.assertIsInstance(parsed, ParsedTick)
        self.assertEqual(parsed.price, 410.10)
        self.assertEqual(parsed.volume, 50.0)

    def test_trade_message_missing_price_and_last_returns_none(self):
        raw = {"type": "trade", "symbol": "AAPL", "size": "100", "date": "1768494600000"}
        self.assertIsNone(parse_stream_message(raw))

    def test_trade_with_missing_size_defaults_volume_to_zero_not_a_crash(self):
        raw = {"type": "trade", "symbol": "AAPL", "price": "150.0", "date": "1768494600000"}
        parsed = parse_stream_message(raw)
        self.assertEqual(parsed.volume, 0.0)


class TestQuoteMessages(unittest.TestCase):
    def test_missing_exchange_timestamp_is_not_assumed_live(self):
        self.assertIsNone(parse_stream_message({"type": "quote", "symbol": "SPY",
                                                "bid": "1.0", "ask": "1.1"}))

    def test_realistic_quote_message_parses_to_a_quote(self):
        raw = {"type": "quote", "symbol": "AAPL", "bid": "150.20", "bidsz": "5",
               "bidexch": "Q", "biddate": "1768494600000", "ask": "150.30",
               "asksz": "3", "askexch": "Q", "askdate": "1768494601000"}
        parsed = parse_stream_message(raw)
        self.assertIsInstance(parsed, ParsedQuote)
        self.assertEqual(parsed.bid, 150.20)
        self.assertEqual(parsed.ask, 150.30)

    def test_quote_with_malformed_bid_returns_none(self):
        raw = {"type": "quote", "symbol": "AAPL", "bid": "not-a-number", "ask": "150.30",
               "biddate": "1768494600000"}
        self.assertIsNone(parse_stream_message(raw))


class TestOtherMessageTypes(unittest.TestCase):
    def test_summary_is_expected_and_option_trade_is_not_an_underlying_tick(self):
        ticks, diagnostics = [], []
        client = TradierStreamClient(
            symbols=["NVDA"], rest_client=MagicMock(),
            on_tick=lambda *args: ticks.append(args),
            on_diagnostic=lambda *args: diagnostics.append(args))
        client._on_message(None, json.dumps({"type": "summary", "symbol": "NVDA", "last": "230"}))
        client._on_message(None, json.dumps({"type": "trade",
            "symbol": "NVDA260930C00230000", "price": "1.25", "date": "1790775600000"}))
        self.assertEqual(ticks, [])
        self.assertEqual(diagnostics, [])

    def test_summary_message_returns_none_not_an_error(self):
        raw = {"type": "summary", "symbol": "AAPL", "open": "149.0", "high": "151.0",
               "low": "148.5", "prevClose": "148.9"}
        self.assertIsNone(parse_stream_message(raw))

    def test_unknown_future_message_type_returns_none_gracefully(self):
        # A protocol addition Tradier ships later must not crash the stream.
        raw = {"type": "some_new_type_from_the_future", "symbol": "AAPL", "foo": "bar"}
        self.assertIsNone(parse_stream_message(raw))

    def test_message_with_no_symbol_returns_none(self):
        raw = {"type": "trade", "price": "150.0", "date": "1768494600000"}
        self.assertIsNone(parse_stream_message(raw))


class TestEndToEndWithRuntime(unittest.TestCase):
    """Confirms parsed messages actually drive a real TradingRuntime tick,
    not just that parsing produces a plausible-looking dataclass."""

    def test_a_stream_of_realistic_trade_messages_flows_into_the_real_runtime(self):
        from trading_engine.runtime import TradingRuntime
        rt = TradingRuntime()
        base_ts = 1768494600000  # ms, matching the stream's own units
        for i in range(15):
            raw = {"type": "trade", "symbol": "AAPL", "price": str(100.0 + i),
                   "size": "10", "date": str(base_ts + i * 120000)}
            parsed = parse_stream_message(raw)
            rt.on_underlying_tick(parsed.symbol, parsed.ts, parsed.price, parsed.volume)
        snap = rt.store.snapshot("AAPL", now=(base_ts / 1000.0) + 1900.0)
        self.assertEqual(snap.price, 114.0)
        self.assertGreater(len(snap.bars), 0)


class TestWsSchemeNormalization(unittest.TestCase):
    """Directly reproduces a real, observed failure: a live Tradier session
    response returned an https:// URL, which websocket-client's own
    parse_url() rejects outright with "scheme https is invalid" rather than
    failing gracefully. _get_session() must never hand that URL to
    WebSocketApp unnormalized.
    """

    def test_https_is_converted_to_wss(self):
        self.assertEqual(_to_ws_scheme("https://stream.tradier.com/v1/markets/events"),
                          "wss://stream.tradier.com/v1/markets/events")

    def test_http_is_converted_to_ws(self):
        self.assertEqual(_to_ws_scheme("http://stream.tradier.com/v1/markets/events"),
                          "ws://stream.tradier.com/v1/markets/events")

    def test_already_correct_wss_scheme_is_left_unchanged(self):
        self.assertEqual(_to_ws_scheme("wss://ws.tradier.com/v1/markets/events"),
                          "wss://ws.tradier.com/v1/markets/events")

    def test_get_session_normalizes_a_real_shaped_response(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            from trading_engine.tradier_client import TradierRestClient
            rest_client = TradierRestClient(session=MagicMock())
        post_response = MagicMock()
        post_response.status_code = 200
        post_response.raise_for_status.side_effect = None
        post_response.json.return_value = {
            "stream": {"url": "https://stream.tradier.com/v1/markets/events", "sessionid": "abc123"}
        }
        rest_client._session.post.return_value = post_response

        client = TradierStreamClient(symbols=["SPY"], rest_client=rest_client, on_tick=lambda *a: None)
        session = client._get_session()
        self.assertEqual(session["url"], "wss://stream.tradier.com/v1/markets/events")
        self.assertEqual(session["sessionid"], "abc123")

    def test_get_session_raises_auth_error_on_401(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            from trading_engine.tradier_client import TradierRestClient
            rest_client = TradierRestClient(session=MagicMock())
        post_response = MagicMock()
        post_response.status_code = 401
        rest_client._session.post.return_value = post_response

        client = TradierStreamClient(symbols=["SPY"], rest_client=rest_client, on_tick=lambda *a: None)
        from trading_engine.tradier_client import TradierAuthError
        with self.assertRaises(TradierAuthError):
            client._get_session()


class TestDynamicSubscription(unittest.TestCase):
    """add_symbol/remove_symbol -- what makes on_option_quote() actually
    fire for a real, specific open position once it's opened."""

    def _client(self, symbols=None):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            from trading_engine.tradier_client import TradierRestClient
            rest_client = TradierRestClient(session=MagicMock())
        return TradierStreamClient(symbols=symbols or [], rest_client=rest_client, on_tick=lambda *a: None)

    def test_add_symbol_before_any_connection_just_queues_it(self):
        client = self._client(["SPY"])
        client.add_symbol("AAPL260115C00150000")
        self.assertIn("AAPL260115C00150000", client.symbols)

    def test_add_symbol_is_idempotent(self):
        client = self._client(["SPY"])
        client.add_symbol("AAPL260115C00150000")
        client.add_symbol("AAPL260115C00150000")
        self.assertEqual(client.symbols.count("AAPL260115C00150000"), 1)

    def test_remove_symbol_not_present_is_a_safe_no_op(self):
        client = self._client(["SPY"])
        client.remove_symbol("NOT_THERE")  # must not raise
        self.assertEqual(client.symbols, ["SPY"])

    def test_add_symbol_with_an_open_connection_resends_the_full_updated_list(self):
        client = self._client(["SPY"])
        mock_ws = MagicMock()
        client._ws = mock_ws
        client._session_id = "sess123"

        client.add_symbol("AAPL260115C00150000")

        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        self.assertEqual(set(sent["symbols"]), {"SPY", "AAPL260115C00150000"})
        self.assertEqual(sent["sessionid"], "sess123")
        self.assertIn("quote", sent["filter"])

    def test_remove_symbol_with_an_open_connection_resends_the_list_without_it(self):
        client = self._client(["SPY", "AAPL260115C00150000"])
        mock_ws = MagicMock()
        client._ws = mock_ws
        client._session_id = "sess123"

        client.remove_symbol("AAPL260115C00150000")

        sent = json.loads(mock_ws.send.call_args[0][0])
        self.assertEqual(sent["symbols"], ["SPY"])

    def test_add_symbol_with_no_open_connection_does_not_try_to_send(self):
        client = self._client(["SPY"])
        client.add_symbol("AAPL260115C00150000")  # _ws is None -- must not raise or attempt a send
        self.assertIn("AAPL260115C00150000", client.symbols)


class TestFullPositionLifecycleWiring(unittest.TestCase):
    """End-to-end: opening a real position through TradingRuntime actually
    subscribes its OCC symbol on a real (mocked-transport) stream client,
    and closing it actually unsubscribes -- the whole point of this feature."""

    def test_position_open_and_close_drive_real_stream_subscriptions(self):
        from trading_engine.runtime import TradingRuntime
        from trading_engine.contract_selection import OptionEntryResult

        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            from trading_engine.tradier_client import TradierRestClient
            rest_client = TradierRestClient(session=MagicMock())
        stream = TradierStreamClient(symbols=["AAPL"], rest_client=rest_client, on_tick=lambda *a: None)
        mock_ws = MagicMock()
        stream._ws = mock_ws
        stream._session_id = "sess123"

        rt = TradingRuntime(
            option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.0, occ_symbol="AAPL260115C00150000"),
            on_trade_opened=lambda tid, pos: stream.add_symbol(pos.occ_symbol),
            on_trade_closed=lambda tid, pos: stream.remove_symbol(pos.occ_symbol),
        )

        from tests.test_runtime import uptrend_with_a_genuine_flip
        closes = uptrend_with_a_genuine_flip()
        t = 1768494600.0
        for c in closes:
            rt.on_underlying_tick('AAPL', t, c)
            t += 120.0

        self.assertIn("AAPL260115C00150000", stream.symbols)
        _, pos = rt.positions['AAPL']

        rt.on_option_quote(pos.occ_symbol, t, bid=1.30, ask=1.31)  # armed
        rt.on_option_quote(pos.occ_symbol, t + 10, bid=1.15, ask=1.16)  # giveback exit
        rt.on_underlying_tick('AAPL', t + 10, closes[-1])

        self.assertEqual(len(rt.positions), 0)
        self.assertNotIn("AAPL260115C00150000", stream.symbols)


if __name__ == '__main__':
    unittest.main()
