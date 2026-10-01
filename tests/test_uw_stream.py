import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
import json
from unittest.mock import patch, Mock

from trading_engine.uw_stream import (
    parse_net_flow_message, parse_interval_flow_message, parse_market_tide_message,
    parse_gex_message, UWAuthError, UWStreamClient, _load_uw_key, _authenticated_url, bind_net_flow,
)
from trading_engine.symbol_state import NetFlowSample, IntervalFlowSample, MarketTideSample, GexSample


class TestKeyLoading(unittest.TestCase):
    def test_websocket_token_query_preserves_other_parameters(self):
        url = _authenticated_url("wss://api.unusualwhales.com/socket?region=us&token=old", "new key")
        self.assertEqual(url, "wss://api.unusualwhales.com/socket?region=us&token=new+key")

    def test_client_uses_query_token_not_authorization_header(self):
        client = UWStreamClient(symbols=["AAPL"], api_key="private-key")
        observed = {}
        class Socket:
            def __init__(self, url, **kwargs):
                observed.update(url=url, kwargs=kwargs)
                client._stop.set()
            def run_forever(self):
                pass
        with patch("trading_engine.uw_stream.websocket.WebSocketApp", Socket):
            client._run()
        self.assertEqual(observed["url"], "wss://api.unusualwhales.com/socket?token=private-key")
        self.assertEqual(observed["kwargs"]["header"], ["User-Agent: TradingEngine/1.0"])

    def test_missing_key_raises_without_leaking_anything(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(UWAuthError) as ctx:
                _load_uw_key()
            self.assertIn("UW_API_KEY", str(ctx.exception))

    def test_explicit_key_used_over_environment(self):
        with patch.dict(os.environ, {"UW_API_KEY": "env-key"}):
            self.assertEqual(_load_uw_key("explicit-key"), "explicit-key")

    def test_connection_error_reports_type_and_http_status_without_credentials(self):
        diagnostics = []
        client = UWStreamClient(symbols=["AAPL"], api_key="private-key",
                                on_diagnostic=lambda reason, detail: diagnostics.append((reason, detail)))
        class FailedSocket:
            def __init__(self, *args, **kwargs):
                self.on_error = kwargs["on_error"]
                client._stop.set()
            def run_forever(self):
                error = RuntimeError("private-key must never appear")
                error.status_code = 403
                self.on_error(self, error)
        with patch("trading_engine.uw_stream.websocket.WebSocketApp", FailedSocket):
            client._run()
        self.assertEqual(diagnostics, [("CONNECTION_ERROR", "RuntimeError:HTTP_403")])


class TestNetFlowParsing(unittest.TestCase):
    def test_parses_a_plausible_net_flow_message(self):
        raw = {"ticker": "AAPL", "net_delta_flow": 1250.5, "timestamp": 1768494600}
        result = parse_net_flow_message(raw)
        self.assertIsNotNone(result)
        symbol, sample = result
        self.assertEqual(symbol, "AAPL")
        self.assertIsInstance(sample, NetFlowSample)
        self.assertEqual(sample.dir_delta_flow, 1250.5)

    def test_missing_ticker_returns_none(self):
        self.assertIsNone(parse_net_flow_message({"net_delta_flow": 100.0}))

    def test_malformed_delta_returns_none_not_a_crash(self):
        self.assertIsNone(parse_net_flow_message({"ticker": "AAPL", "net_delta_flow": "not-a-number"}))

    def test_millisecond_and_second_timestamps_both_normalize(self):
        _, sample_ms = parse_net_flow_message({"ticker": "AAPL", "net_delta_flow": 1.0, "timestamp": 1768494600000})
        _, sample_s = parse_net_flow_message({"ticker": "AAPL", "net_delta_flow": 1.0, "timestamp": 1768494600})
        self.assertAlmostEqual(sample_ms.ts, sample_s.ts, delta=1.0)


class TestIntervalFlowParsing(unittest.TestCase):
    def test_parses_a_plausible_interval_flow_message(self):
        raw = {
            "ticker": "AAPL", "timestamp": 1768494600,
            "call_volume_ask_side": 500, "call_volume_bid_side": 50,
            "put_volume_ask_side": 40, "put_volume_bid_side": 480,
            "net_delta_flow": 200.0, "net_vega_flow": -30.0, "avg_dte": 0.0,
        }
        result = parse_interval_flow_message(raw)
        self.assertIsNotNone(result)
        symbol, sample = result
        self.assertEqual(symbol, "AAPL")
        self.assertIsInstance(sample, IntervalFlowSample)
        self.assertEqual(sample.call_vol_ask_side, 500)
        self.assertEqual(sample.avg_dte, 0.0)

    def test_missing_optional_fields_default_sanely(self):
        raw = {"ticker": "AAPL", "timestamp": 1768494600}
        result = parse_interval_flow_message(raw)
        self.assertIsNotNone(result)
        _, sample = result
        self.assertEqual(sample.call_vol_ask_side, 0.0)
        self.assertIsNone(sample.avg_dte)

    def test_missing_ticker_returns_none(self):
        self.assertIsNone(parse_interval_flow_message({"timestamp": 1768494600}))


class TestMarketTideParsing(unittest.TestCase):
    def test_parses_a_plausible_market_tide_message(self):
        raw = {"timestamp": 1768494600, "net_call_premium": 5_000_000.0, "net_put_premium": 3_000_000.0}
        sample = parse_market_tide_message(raw)
        self.assertIsInstance(sample, MarketTideSample)
        self.assertEqual(sample.net_call_premium, 5_000_000.0)

    def test_malformed_premium_returns_none(self):
        self.assertIsNone(parse_market_tide_message({"net_call_premium": "bad"}))


class TestGexParsing(unittest.TestCase):
    def test_parses_a_plausible_gex_message(self):
        raw = {"ticker": "SPY", "timestamp": 1768494600, "call_wall": 590.0, "put_wall": 570.0,
               "gamma_path": "BRAKES AHEAD", "target_candidates": [585.0, 588.0]}
        result = parse_gex_message(raw)
        self.assertIsNotNone(result)
        symbol, sample = result
        self.assertIsInstance(sample, GexSample)
        self.assertEqual(sample.call_wall, 590.0)
        self.assertEqual(sample.target_candidates, (585.0, 588.0))

    def test_missing_walls_default_to_none_not_zero(self):
        raw = {"ticker": "SPY", "timestamp": 1768494600, "gamma_path": "CLEAR"}
        result = parse_gex_message(raw)
        _, sample = result
        self.assertIsNone(sample.call_wall)
        self.assertIsNone(sample.put_wall)

    def test_missing_ticker_returns_none(self):
        self.assertIsNone(parse_gex_message({"call_wall": 590.0}))


class TestClientConstruction(unittest.TestCase):
    def test_on_open_joins_each_documented_channel(self):
        sent = []
        client = UWStreamClient(["AAPL"], api_key="private-key")
        class Socket:
            def __init__(self, url, **kwargs):
                self.on_open = kwargs["on_open"]
                client._stop.set()
            def send(self, message):
                sent.append(json.loads(message))
            def run_forever(self):
                self.on_open(self)
        with patch("trading_engine.uw_stream.websocket.WebSocketApp", Socket):
            client._run()
        self.assertEqual(sent, [{"channel": c, "msg_type": "join"}
                                for c in client._subscribe_channels()])

    def test_net_flow_stream_dispatches_into_runtime_signature(self):
        runtime = Mock()
        client = UWStreamClient(["AAPL"], api_key="private-key",
                                on_net_flow=bind_net_flow(runtime))
        client._on_message(None, json.dumps(["net_flow:AAPL", {
            "ticker": "AAPL", "time": 1768494600000, "net_call_prem": "200", "net_put_prem": "100"}]))
        client._on_message(None, json.dumps(["net_flow:AAPL", {
            "ticker": "AAPL", "time": 1768494601000, "net_call_prem": "330", "net_put_prem": "110"}]))
        runtime.on_net_flow.assert_called_once_with("AAPL", 1768494601, 120.0)

    def test_documented_interval_and_market_tide_arrays(self):
        interval, tide = Mock(), Mock()
        client = UWStreamClient(["AAPL"], api_key="private-key",
                                on_interval_flow=interval, on_market_tide=tide)
        payload = {"ticker": "AAPL", "interval_type": "All",
                   "tape_time": "2026-09-30T16:30:01Z",
                   "call_vol_ask_side": 38, "call_vol_bid_side": 37,
                   "put_vol_ask_side": 28, "put_vol_bid_side": 107,
                   "dir_delta_flow": 8, "dir_vega_flow": -892}
        client._on_message(None, json.dumps(["interval_flow", payload]))
        self.assertEqual(interval.call_args.args[0], "AAPL")
        self.assertEqual(interval.call_args.args[1].call_vol_ask_side, 38)
        self.assertEqual(interval.call_args.args[1].ts, 1790785801)
        client._on_message(None, json.dumps(["interval_flow", {**payload, "interval_type": "OtmOnly"}]))
        interval.assert_called_once()
        client._on_message(None, json.dumps(["market_tide", {
            "timestamp": "2026-09-30T16:30:01Z", "net_call_premium": "488078.0",
            "net_put_premium": "-218102.0"}]))
        self.assertEqual(tide.call_args.args[0].net_call_premium, 488078.0)

    def test_subscription_response_is_not_market_data(self):
        market_tide = Mock()
        events = []
        client = UWStreamClient(["AAPL"], api_key="private-key",
                                on_market_tide=market_tide,
                                on_diagnostic=lambda reason, detail: events.append((reason, detail)))
        client._on_message(None, json.dumps(["market_tide", {"status": "ok", "response": {}}]))
        market_tide.assert_not_called()
        self.assertEqual(events[0][0], "SUBSCRIPTION_RESPONSE")

    def test_unrecognized_server_ack_logs_shape_without_secret(self):
        events = []
        client = UWStreamClient(["AAPL"], api_key="private-key",
                                on_diagnostic=lambda reason, detail: events.append((reason, detail)))
        client._on_message(None, json.dumps({"message": "private-key", "token": "private-key", "data": {"status": "ok"}}))
        self.assertEqual(events[0][0], "UNHANDLED_CHANNEL")
        self.assertIn("status", events[0][1])
        self.assertNotIn("private-key", events[0][1])

    def test_missing_key_raises_at_construction(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(UWAuthError):
                UWStreamClient(symbols=["AAPL"])

    def test_subscribe_channels_include_evidenced_channel_names_and_per_symbol_net_flow(self):
        with patch.dict(os.environ, {"UW_API_KEY": "k"}):
            client = UWStreamClient(symbols=["AAPL", "MSFT"])
        channels = client._subscribe_channels()
        for expected in ["interval_flow", "market_tide", "gex", "net_flow:AAPL", "net_flow:MSFT"]:
            self.assertIn(expected, channels)
        self.assertNotIn("flow-alerts", channels)

    def test_documented_aggregate_gex_sign_has_no_fabricated_wall(self):
        result = parse_gex_message({"ticker": "SPY", "timestamp": 1790785801000,
                                    "gamma_per_one_percent_move_oi": "-262444980.31"})
        self.assertEqual(result[1].gamma_path, "NEGATIVE_GEX")
        self.assertIsNone(result[1].call_wall)
        self.assertEqual(result[1].target_candidates, ())


class TestEndToEndWithRealStateStore(unittest.TestCase):
    """Confirms parsed UW messages actually flow into the real
    SymbolStateStore correctly, not just that parsing produces a
    plausible-looking dataclass in isolation."""

    def test_a_realistic_message_sequence_produces_a_correctly_freshness_gated_snapshot(self):
        from trading_engine.symbol_state import SymbolStateStore, FreshnessState

        store = SymbolStateStore()
        now = 1768494605.0

        _, net_flow_sample = parse_net_flow_message({"ticker": "AAPL", "net_delta_flow": 300.0, "timestamp": now})
        store.ingest_net_flow("AAPL", net_flow_sample)

        _, interval_sample = parse_interval_flow_message({
            "ticker": "AAPL", "timestamp": now,
            "call_volume_ask_side": 400, "call_volume_bid_side": 20,
            "put_volume_ask_side": 30, "put_volume_bid_side": 350,
        })
        store.ingest_interval_flow("AAPL", interval_sample)

        tide_sample = parse_market_tide_message({"timestamp": now, "net_call_premium": 2_000_000, "net_put_premium": 1_000_000})
        store.ingest_market_tide("AAPL", tide_sample)

        _, gex_sample = parse_gex_message({"ticker": "AAPL", "timestamp": now, "call_wall": 155.0, "put_wall": 145.0, "gamma_path": "CLEAR"})
        store.ingest_gex("AAPL", gex_sample)

        snap = store.snapshot("AAPL", now=now + 1.0)
        self.assertEqual(snap.net_flow_state, FreshnessState.FRESH)
        self.assertEqual(snap.flow_30s, "CALL")  # positive net_delta_flow
        self.assertEqual(snap.interval_flow_state, FreshnessState.FRESH)
        self.assertEqual(snap.aggressor_direction, "CALL")
        self.assertEqual(snap.market_tide_direction, "CALL")
        self.assertEqual(snap.gex_state, FreshnessState.FRESH)
        self.assertEqual(snap.call_wall, 155.0)


if __name__ == '__main__':
    unittest.main()
