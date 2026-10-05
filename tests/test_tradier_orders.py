import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
from unittest.mock import MagicMock, patch

from trading_engine.tradier_orders import TradierOrderClient, TradierOrderError, OrderResult
from trading_engine.tradier_client import TradierAuthError


def rest_client():
    with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "t"}):
        from trading_engine.tradier_client import TradierRestClient
        return TradierRestClient(session=MagicMock())


def mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    return resp


class TestConstruction(unittest.TestCase):
    def test_missing_account_id_raises_immediately(self):
        with self.assertRaises(TradierOrderError) as ctx:
            TradierOrderClient(rest_client(), account_id="")
        self.assertIn("TRADIER_ACCOUNT_ID", str(ctx.exception))

    def test_valid_account_id_constructs_fine(self):
        client = TradierOrderClient(rest_client(), account_id="ACC123")
        self.assertEqual(client.account_id, "ACC123")


class TestAccountCollectionParsing(unittest.TestCase):
    def test_empty_sandbox_positions_and_orders_as_null_strings(self):
        rc = rest_client()
        rc._session.get.side_effect = [
            mock_response({"positions": "null"}),
            mock_response({"orders": "null"}),
        ]
        client = TradierOrderClient(rc, account_id="ACC123")
        self.assertEqual(client.positions(), [])
        self.assertEqual(client.orders(), [])

    def test_single_position_and_order_objects(self):
        rc = rest_client()
        rc._session.get.side_effect = [
            mock_response({"positions": {"position": {"symbol": "SPY260930C00600000", "quantity": 1}}}),
            mock_response({"orders": {"order": {"id": 123, "status": "open"}}}),
        ]
        client = TradierOrderClient(rc, account_id="ACC123")
        self.assertEqual(len(client.positions()), 1)
        self.assertEqual(client.orders()[0]["id"], 123)

    def test_unexpected_collection_cannot_be_mistaken_for_empty_account(self):
        rc = rest_client()
        rc._session.get.return_value = mock_response({"positions": "error"})
        with self.assertRaisesRegex(TradierOrderError, "Unexpected Tradier positions"):
            TradierOrderClient(rc, account_id="ACC123").positions()


class TestValidationBeforeAnyNetworkCall(unittest.TestCase):
    """Every one of these must raise WITHOUT ever calling _session.post --
    a caller should never get a network round-trip for a locally-detectable
    mistake, and a bad request must never even reach Tradier."""

    def _client(self):
        rc = rest_client()
        return TradierOrderClient(rc, account_id="ACC123"), rc

    def test_empty_occ_symbol_is_refused(self):
        client, rc = self._client()
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("", "AAPL", 1)
        rc._session.post.assert_not_called()

    def test_empty_underlying_symbol_is_refused(self):
        client, rc = self._client()
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "", 1)
        rc._session.post.assert_not_called()

    def test_zero_quantity_is_refused(self):
        client, rc = self._client()
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 0)
        rc._session.post.assert_not_called()

    def test_negative_quantity_is_refused(self):
        client, rc = self._client()
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", -1)
        rc._session.post.assert_not_called()

    def test_non_integer_quantity_is_refused(self):
        client, rc = self._client()
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1.5)
        rc._session.post.assert_not_called()

    def test_limit_order_without_a_price_is_refused(self):
        client, rc = self._client()
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1, order_type="limit")
        rc._session.post.assert_not_called()

    def test_limit_order_with_zero_price_is_refused(self):
        client, rc = self._client()
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1, order_type="limit", limit_price=0.0)
        rc._session.post.assert_not_called()


class TestSuccessfulOrders(unittest.TestCase):
    def test_buy_to_open_sends_correct_form_payload(self):
        rc = rest_client()
        rc._session.post.return_value = mock_response({"order": {"id": 12345, "status": "ok"}})
        client = TradierOrderClient(rc, account_id="ACC123")

        result = client.buy_to_open("AAPL260115C00150000", "AAPL", 2)

        self.assertIsInstance(result, OrderResult)
        self.assertEqual(result.order_id, "12345")
        self.assertEqual(result.status, "ok")
        call = rc._session.post.call_args
        self.assertIn("/accounts/ACC123/orders", call[0][0])
        payload = call.kwargs["data"]
        self.assertEqual(payload["side"], "buy_to_open")
        self.assertEqual(payload["option_symbol"], "AAPL260115C00150000")
        self.assertEqual(payload["symbol"], "AAPL")
        self.assertEqual(payload["quantity"], "2")
        self.assertEqual(payload["type"], "market")
        self.assertNotIn("price", payload)  # market order must not send a price field

    def test_sell_to_close_sends_correct_side(self):
        rc = rest_client()
        rc._session.post.return_value = mock_response({"order": {"id": 99, "status": "ok"}})
        client = TradierOrderClient(rc, account_id="ACC123")
        client.sell_to_close("AAPL260115C00150000", "AAPL", 1)
        payload = rc._session.post.call_args.kwargs["data"]
        self.assertEqual(payload["side"], "sell_to_close")

    def test_limit_order_includes_a_formatted_price(self):
        rc = rest_client()
        rc._session.post.return_value = mock_response({"order": {"id": 1, "status": "ok"}})
        client = TradierOrderClient(rc, account_id="ACC123")
        client.buy_to_open("AAPL260115C00150000", "AAPL", 1, order_type="limit", limit_price=1.256)
        payload = rc._session.post.call_args.kwargs["data"]
        self.assertEqual(payload["type"], "limit")
        self.assertEqual(payload["price"], "1.26")


class TestFailureHandling(unittest.TestCase):
    def test_401_raises_auth_error_not_order_error(self):
        rc = rest_client()
        rc._session.post.return_value = mock_response({}, status_code=401)
        client = TradierOrderClient(rc, account_id="ACC123")
        with self.assertRaises(TradierAuthError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1)

    def test_tradier_rejection_shape_raises_order_error_with_detail(self):
        rc = rest_client()
        rc._session.post.return_value = mock_response(
            {"errors": {"error": ["Insufficient buying power."]}}, status_code=400,
        )
        client = TradierOrderClient(rc, account_id="ACC123")
        with self.assertRaises(TradierOrderError) as ctx:
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1)
        self.assertIn("Insufficient buying power", str(ctx.exception))

    def test_non_json_response_raises_order_error_not_a_crash(self):
        rc = rest_client()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.side_effect = ValueError("not json")
        rc._session.post.return_value = resp
        client = TradierOrderClient(rc, account_id="ACC123")
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1)

    def test_network_exception_is_wrapped_not_propagated_raw(self):
        import requests
        rc = rest_client()
        rc._session.post.side_effect = requests.ConnectionError("boom")
        client = TradierOrderClient(rc, account_id="ACC123")
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1)

    def test_a_single_failure_never_triggers_an_automatic_retry(self):
        # Exactly one call must be made per buy_to_open() invocation, success
        # or failure -- silently retrying a request whose outcome is unknown
        # risks placing a duplicate order.
        rc = rest_client()
        rc._session.post.return_value = mock_response({}, status_code=500)
        client = TradierOrderClient(rc, account_id="ACC123")
        with self.assertRaises(TradierOrderError):
            client.buy_to_open("AAPL260115C00150000", "AAPL", 1)
        self.assertEqual(rc._session.post.call_count, 1)


class TestRuntimeWiring(unittest.TestCase):
    """Confirms a real TradingRuntime position lifecycle actually drives real
    (mocked-transport) order placement calls with the correct parameters."""

    def test_open_and_close_place_matching_buy_and_sell_orders(self):
        from trading_engine.runtime import TradingRuntime
        from trading_engine.contract_selection import OptionEntryResult

        rc = rest_client()
        rc._session.post.return_value = mock_response({"order": {"id": 1, "status": "ok"}})
        order_client = TradierOrderClient(rc, account_id="ACC123")
        placed = []

        rt = TradingRuntime(
            option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.0, occ_symbol="AAPL260115C00150000"),
            on_trade_opened=lambda tid, pos: (order_client.buy_to_open(pos.occ_symbol, pos.symbol, 1), placed.append("buy")),
            on_trade_closed=lambda tid, pos: (order_client.sell_to_close(pos.occ_symbol, pos.symbol, 1), placed.append("sell")),
        )

        down = [112 - i for i in range(14)]
        up = [99 + i + (i * i) * 0.03 for i in range(20)]
        t = 1768494600.0
        for c in down + up:
            rt.on_underlying_tick('AAPL', t, c)
            t += 120.0
            if rt.positions:
                break  # test lifecycle wiring at the first actual entry

        self.assertIn("buy", placed)
        _, pos = rt.positions['AAPL']

        rt.on_option_quote(pos.occ_symbol, t, bid=1.30, ask=1.31)
        rt.on_option_quote(pos.occ_symbol, t + 10, bid=1.15, ask=1.16)
        rt.on_underlying_tick('AAPL', t + 10, up[-1])

        self.assertIn("sell", placed)
        buy_call, sell_call = rc._session.post.call_args_list
        self.assertEqual(buy_call.kwargs["data"]["side"], "buy_to_open")
        self.assertEqual(sell_call.kwargs["data"]["side"], "sell_to_close")


if __name__ == '__main__':
    unittest.main()
