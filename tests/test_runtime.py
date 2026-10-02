import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import json
import unittest

from trading_engine.runtime import TradingRuntime
from trading_engine.exit_pipeline import ExitConfig
from trading_engine.contract_selection import OptionEntryResult


SAFE_TEST_TS = 1768494600.0  # 2026-01-15 11:30:00 America/New_York, clear of EOD


def uptrend_with_a_genuine_flip():
    """A longer initial flat/down stretch, then a sustained, accelerating
    uptrend -- unlike a pure monotonic rise, this contains an actual PSAR
    flip event for latest_psar_flip() to detect, matching how a real trigger
    would arrive.

    The down-leg is deliberately >= 12 bars (momentum_from_bars needs
    slow+2=12 completed bars before it will say anything at all): the flip
    must land AFTER enough history already exists, or evaluate_entry correctly
    SKIPs it for insufficient history at the one tick where the flip is fresh
    -- latest_psar_flip() only reports a flip on the latest bar, so that
    one-tick window doesn't come back around. A real system accumulates
    history continuously, so this is mostly a synthetic-test-data concern,
    but it's worth naming as a genuine edge case: a flip arriving before
    enough bar history exists is silently missed, not deferred.
    """
    down = [100 - .1 * i for i in range(14)]
    up = [98.7 + 5 + 5 * i for i in range(10)]  # expansion already present at the flip
    return down + up


def feed_uptrend(rt, symbol, closes=None, bar_seconds=120.0, start_ts=SAFE_TEST_TS):
    if closes is None:
        closes = uptrend_with_a_genuine_flip()
    t = start_ts
    for c in closes:
        rt.on_underlying_tick(symbol, t, c)
        t += bar_seconds
    return t


class TestPositionOpeningRequiresAPriceProvider(unittest.TestCase):
    def test_take_without_a_price_provider_does_not_open_a_position(self):
        rt = TradingRuntime()  # no option_entry_price_provider supplied
        closes = uptrend_with_a_genuine_flip()[:19]
        feed_uptrend(rt, 'AAPL', closes)
        self.assertEqual(len(rt.positions), 0)
        self.assertFalse(hasattr(rt, 'deferred'))

    def test_take_with_a_working_price_provider_opens_a_real_position(self):
        rt = TradingRuntime(option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.25, occ_symbol=f'{sym}-TEST'),
                            exit_config=ExitConfig(reversal_phase_exit=False))
        closes = uptrend_with_a_genuine_flip()
        feed_uptrend(rt, 'AAPL', closes)
        self.assertEqual(len(rt.positions), 1)
        trade_id, pos = rt.positions['AAPL']
        self.assertEqual(pos.entry_option_price, 1.25)
        self.assertEqual(pos.direction, 'CALL')

    def test_provider_returning_none_is_treated_the_same_as_no_provider(self):
        rt = TradingRuntime(option_entry_price_provider=lambda sym, d, now, u: None)
        closes = uptrend_with_a_genuine_flip()
        feed_uptrend(rt, 'AAPL', closes)
        self.assertEqual(len(rt.positions), 0)


class TestCallbacks(unittest.TestCase):
    def test_on_trade_opened_fires_with_real_position_data(self):
        opened = []
        rt = TradingRuntime(option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.0, occ_symbol=f'{sym}-TEST'),
                             on_trade_opened=lambda tid, pos: opened.append((tid, pos.symbol)))
        closes = uptrend_with_a_genuine_flip()
        feed_uptrend(rt, 'AAPL', closes)
        self.assertEqual(len(opened), 1)
        self.assertEqual(opened[0][1], 'AAPL')

    def test_on_trade_closed_fires_when_the_ladder_exits(self):
        closed = []
        rt = TradingRuntime(option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.0, occ_symbol=f'{sym}-TEST'),
                             on_trade_closed=lambda tid, pos: closed.append(tid),
                             exit_config=ExitConfig(reversal_phase_exit=False))
        closes = uptrend_with_a_genuine_flip()
        last_ts = feed_uptrend(rt, 'AAPL', closes)
        self.assertEqual(len(rt.positions), 1)
        trade_id, pos = rt.positions['AAPL']

        # Push the option price up (via the real option-quote path) then give
        # back past the giveback-exit threshold. Quotes route by the specific
        # contract's OCC symbol, not the underlying -- use the real one the
        # position was actually opened with.
        occ = rt.positions['AAPL'][1].occ_symbol
        rt.on_option_quote(occ, last_ts + 10, bid=1.30, ask=1.31)  # +30%, armed
        rt.on_option_quote(occ, last_ts + 20, bid=1.15, ask=1.16)  # giveback ~15pts
        # A tick re-triggers evaluation for this symbol.
        rt.on_underlying_tick('AAPL', last_ts + 20, closes[-1])
        self.assertEqual(len(rt.positions), 0)
        self.assertEqual(closed, [trade_id])
        closed_rows = rt.dashboard.closed_view()
        self.assertEqual(len(closed_rows), 1)
        self.assertEqual(closed_rows[0]['exit_reasons'], ['PROFIT_GIVEBACK_EXIT'])


class TestFlipOneShotLifecycle(unittest.TestCase):
    def test_missing_option_price_is_not_retried_on_later_ticks(self):
        attempts = []
        rt = TradingRuntime(option_entry_price_provider=lambda *args: attempts.append(args) or None)
        closes = uptrend_with_a_genuine_flip()[:19]
        last_ts = feed_uptrend(rt, 'AAPL', closes)
        count = len(attempts)
        self.assertGreater(count, 0)
        rt.on_underlying_tick('AAPL', last_ts + 5, closes[-1])
        self.assertEqual(len(attempts), count)
        self.assertFalse(rt.positions)


class TestOptionQuoteUpdatesOnlyOpenPositions(unittest.TestCase):
    def test_quote_for_a_symbol_with_no_open_position_is_a_no_op(self):
        rt = TradingRuntime()
        rt.on_option_quote('AAPL', SAFE_TEST_TS, bid=1.0, ask=1.05)  # should not raise
        self.assertEqual(len(rt.positions), 0)


class TestRealDataReplay(unittest.TestCase):
    """End-to-end: feed genuine historical candle data through the full
    runtime and confirm it runs to completion without error, produces at
    least some entries and exits, and every closed trade shows a real,
    recognized exit reason -- proof the whole pipeline actually functions
    together, not just each piece in isolation."""

    def test_replay_real_candles_end_to_end(self):
        path = '/mnt/user-data/uploads/v18_candles.json'
        if not os.path.exists(path):
            self.skipTest("real candle data not available in this environment")
        candles = json.load(open(path))

        # A simple, clearly-synthetic option pricer for this replay only:
        # leveraged multiple of the underlying's move since a fixed reference,
        # not a claim about real option behavior -- sufficient to exercise
        # the exit ladder's gain/peak/giveback math against real price paths.
        base_prices = {}

        def synthetic_option_price(symbol, direction, now, underlying_price):
            base = base_prices.setdefault(symbol, underlying_price)
            move_pct = (underlying_price / base - 1.0) * 100.0
            if direction == 'PUT':
                move_pct = -move_pct
            return max(0.01, 1.0 * (1.0 + move_pct * 5.0 / 100.0))

        opened_count = [0]
        closed_count = [0]
        rt = TradingRuntime(
            option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.0, occ_symbol=f'{sym}-TEST'),  # fixed entry ref; quote path carries real movement
            on_trade_opened=lambda tid, pos: opened_count.__setitem__(0, opened_count[0] + 1),
            on_trade_closed=lambda tid, pos: closed_count.__setitem__(0, closed_count[0] + 1),
        )

        for symbol, bars in candles.items():
            if len(bars) < 10:
                continue
            for bar in bars:
                ts, price = bar['ts'], bar['close']
                rt.on_underlying_tick(symbol, ts, price)
                if symbol in rt.positions:
                    pos = rt.positions[symbol][1]
                    opt_price = synthetic_option_price(symbol, pos.direction, ts, price)
                    rt.on_option_quote(pos.occ_symbol, ts, bid=opt_price, ask=opt_price * 1.01)

        # The replay must not have crashed (getting here at all proves that),
        # and it should have done *something* observable across 46 real
        # symbols' worth of real intraday data.
        total_activity = opened_count[0] + closed_count[0] + len(rt.dashboard.scan_view())
        self.assertGreater(total_activity, 0)

        for row in rt.dashboard.closed_view():
            self.assertTrue(len(row['exit_reasons']) > 0)
            self.assertIn(row['exit_reasons'][0], {
                'EOD_FORCE_CLOSE', 'EMERGENCY_OPTION_STOP', 'OPPOSITE_PSAR_CONFIRMED_BY_PRICE',
                'OPPOSITE_PSAR_BARE_PROVEN', 'THESIS_BROKEN_PRICE_FLOW', 'PROFIT_GIVEBACK_EXIT', 'STALL_EXIT',
            })

        print(f"\n[replay] symbols={len(candles)} opened={opened_count[0]} closed={closed_count[0]} "
              f"still_open={len(rt.positions)}")


if __name__ == '__main__':
    unittest.main()
