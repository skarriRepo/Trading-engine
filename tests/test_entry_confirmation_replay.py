import unittest
from datetime import datetime, timezone
from replay_entry_confirmation import analyze


def row(event, offset, **values):
    start = 1768494600
    return {'event': event, 'symbol': 'TEST',
            'recorded_at_et': datetime.fromtimestamp(start + offset, timezone.utc).isoformat(),
            **values}


def baseline(extra):
    start = 1768494600
    return sorted([
        row('SIGNAL_DECISION', 0, source='PSAR_FLIP', direction='CALL', bar_ts=start-120,
            signal_bar={'high': 101, 'low': 99}, price_state='FRESH', underlying_price=100),
        row('CONTRACT_SELECTED', .1, direction='CALL', ask=1),
        row('ORDER_INTENT', .2, side='buy_to_open', tag='trade'),
        row('BROKER_ENTRY_FILLED', 1, trade_id='trade'),
        row('BID_EXIT_SHADOW_AT_ACTIVE_EXIT', 300, trade_id='trade', entry_live_ask=1, option_bid=1.1),
        row('BROKER_EXIT_FILLED', 301, trade_id='trade', quantity=1, reasons=['TEST_EXIT']),
        *extra,
    ], key=lambda r: r['recorded_at_et'])


class EntryConfirmationReplayTests(unittest.TestCase):
    def test_future_quote_cannot_price_an_earlier_crossing(self):
        rows = baseline([
            row('POSITION_OBSERVATION', 30, price_state='FRESH', underlying_price=102),
            row('OPTION_QUOTE', 60, trade_id='trade', accepted=True,
                market_ts=1768494660, bid=1.15, ask=1.2),
        ])
        result = analyze(rows)
        self.assertEqual(result['details'][0]['outcome'], 'CROSSING_WITHOUT_FRESH_QUOTE')

    def test_confirmed_entry_uses_live_ask_and_fixed_exit_bid(self):
        rows = baseline([
            row('OPTION_QUOTE', 60, trade_id='trade', accepted=True,
                market_ts=1768494660, bid=1.15, ask=1.2),
            row('POSITION_OBSERVATION', 65, price_state='FRESH', underlying_price=102),
        ])
        result = analyze(rows)
        self.assertEqual(result['matched_baseline_pnl'], 10)
        self.assertEqual(result['matched_confirmation_pnl'], -10)

    def test_breakout_at_next_bar_close_is_expired(self):
        rows = baseline([
            row('OPTION_QUOTE', 120, trade_id='trade', accepted=True,
                market_ts=1768494720, bid=1.15, ask=1.2),
            row('POSITION_OBSERVATION', 120, price_state='FRESH', underlying_price=102),
        ])
        self.assertEqual(analyze(rows)['details'][0]['outcome'], 'NO_OBSERVED_CROSSING')


if __name__ == '__main__':
    unittest.main()
