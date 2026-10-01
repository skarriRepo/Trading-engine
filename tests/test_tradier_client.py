import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
from unittest.mock import MagicMock, patch

from trading_engine.tradier_client import (
    TradierRestClient, TradierConfig, TradierAuthError, _parse_option_row,
    make_chain_source, _expiration_to_epoch,
)


def mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.side_effect = None
    return resp


class TestTokenLoading(unittest.TestCase):
    def test_missing_token_raises_without_leaking_anything(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(TradierAuthError) as ctx:
                TradierRestClient()
            self.assertIn("TRADIER_ACCESS_TOKEN", str(ctx.exception))

    def test_explicit_token_is_used_over_environment(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "env-token"}):
            client = TradierRestClient(token="explicit-token")
            self.assertIn("explicit-token", client._headers()["Authorization"])

    def test_token_never_appears_in_repr_or_str_accidentally(self):
        # Loose but meaningful guard: the client object itself shouldn't
        # trivially leak the token through default repr.
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "super-secret-token"}):
            client = TradierRestClient()
            self.assertNotIn("super-secret-token", repr(client))


class TestQuotesNormalization(unittest.TestCase):
    def _client(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            return TradierRestClient(session=MagicMock())

    def test_single_symbol_quote_dict_is_normalized_like_a_list(self):
        # Tradier's real quirk: a single-symbol request returns "quote" as a
        # dict, not a one-element list. Documented behavior, not a guess.
        client = self._client()
        client._session.get.return_value = mock_response({
            "quotes": {"quote": {"symbol": "AAPL", "last": 150.25, "bid": 150.20, "ask": 150.30}}
        })
        result = client.quotes(["AAPL"])
        self.assertEqual(result["AAPL"]["last"], 150.25)

    def test_multi_symbol_quote_list(self):
        client = self._client()
        client._session.get.return_value = mock_response({
            "quotes": {"quote": [
                {"symbol": "AAPL", "last": 150.25},
                {"symbol": "MSFT", "last": 410.10},
            ]}
        })
        result = client.quotes(["AAPL", "MSFT"])
        self.assertEqual(set(result.keys()), {"AAPL", "MSFT"})

    def test_empty_symbol_list_makes_no_request(self):
        client = self._client()
        result = client.quotes([])
        self.assertEqual(result, {})
        client._session.get.assert_not_called()

    def test_401_raises_auth_error(self):
        client = self._client()
        client._session.get.return_value = mock_response({}, status_code=401)
        with self.assertRaises(TradierAuthError):
            client.quotes(["AAPL"])


class TestExpirationsParsing(unittest.TestCase):
    def test_single_and_multiple_dates_both_normalize_to_a_sorted_list(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            client = TradierRestClient(session=MagicMock())
        client._session.get.return_value = mock_response({
            "expirations": {"date": ["2026-01-17", "2026-01-15", "2026-01-16"]}
        })
        self.assertEqual(client.expirations("AAPL"), ["2026-01-15", "2026-01-16", "2026-01-17"])

        client._session.get.return_value = mock_response({"expirations": {"date": "2026-01-15"}})
        self.assertEqual(client.expirations("AAPL"), ["2026-01-15"])

    def test_no_expirations_returns_empty_list(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            client = TradierRestClient(session=MagicMock())
        client._session.get.return_value = mock_response({"expirations": None})
        self.assertEqual(client.expirations("ZZZZ"), [])


class TestOptionRowParsing(unittest.TestCase):
    def test_parses_a_realistic_call_row(self):
        row = {
            "symbol": "AAPL260115C00150000", "underlying": "AAPL",
            "strike": 150.0, "option_type": "call", "expiration_date": "2026-01-15",
            "bid": 1.20, "ask": 1.25, "open_interest": 500, "volume": 200,
        }
        c = _parse_option_row(row)
        self.assertIsNotNone(c)
        self.assertEqual(c.option_type, "CALL")
        self.assertEqual(c.strike, 150.0)
        self.assertEqual(c.bid, 1.20)
        self.assertEqual(c.ask, 1.25)

    def test_parses_a_realistic_put_row(self):
        row = {"underlying": "AAPL", "strike": 145.0, "option_type": "put",
               "expiration_date": "2026-01-15", "bid": 0.9, "ask": 0.95}
        c = _parse_option_row(row)
        self.assertEqual(c.option_type, "PUT")

    def test_malformed_row_returns_none_instead_of_raising(self):
        self.assertIsNone(_parse_option_row({"strike": "not-a-number", "option_type": "call",
                                              "expiration_date": "2026-01-15"}))
        self.assertIsNone(_parse_option_row({}))

    def test_expiration_date_converts_to_a_sane_epoch(self):
        ts = _expiration_to_epoch("2026-01-15")
        import datetime as dt
        from zoneinfo import ZoneInfo
        back = dt.datetime.fromtimestamp(ts, tz=ZoneInfo("America/New_York"))
        self.assertEqual((back.year, back.month, back.day), (2026, 1, 15))


class TestChainSourceIntegration(unittest.TestCase):
    def test_chain_source_combines_multiple_expirations_into_one_contract_list(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            client = TradierRestClient(session=MagicMock())

        def fake_get(url, params, headers, timeout):
            if "expirations" in url:
                return mock_response({"expirations": {"date": ["2026-01-15", "2026-01-16"]}})
            exp = params["expiration"]
            return mock_response({"options": {"option": [
                {"strike": 150.0, "option_type": "call", "expiration_date": exp, "bid": 1.0, "ask": 1.05},
            ]}})
        client._session.get.side_effect = fake_get

        chain_source = make_chain_source(client)
        contracts = chain_source("AAPL", now=1768494600.0)
        self.assertEqual(len(contracts), 2)  # one per expiration
        self.assertTrue(all(c.symbol == "AAPL" for c in contracts))

    def test_chain_source_returns_empty_list_on_transient_failure_not_a_crash(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            client = TradierRestClient(session=MagicMock())
        client._session.get.side_effect = Exception("network blip")
        chain_source = make_chain_source(client)
        self.assertEqual(chain_source("AAPL", now=1768494600.0), [])

    def test_chain_source_propagates_auth_errors_rather_than_swallowing_them(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            client = TradierRestClient(session=MagicMock())
        client._session.get.return_value = mock_response({}, status_code=401)
        chain_source = make_chain_source(client)
        with self.assertRaises(TradierAuthError):
            chain_source("AAPL", now=1768494600.0)


class TestMinuteHistoryParsing(unittest.TestCase):
    def test_singleton_and_empty_timesales(self):
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
            client = TradierRestClient(session=MagicMock())
        row = {"time": "2026-09-29 11:00:00", "open": 100}
        client._session.get.return_value = mock_response({"series": {"data": row}})
        self.assertEqual(client.minute_candles("SPY", "2026-09-29 10:00", "2026-09-29 12:00"), [row])
        client._session.get.return_value = mock_response({"series": "null"})
        self.assertEqual(client.minute_candles("SPY", "2026-09-29 10:00", "2026-09-29 12:00"), [])


if __name__ == '__main__':
    unittest.main()
