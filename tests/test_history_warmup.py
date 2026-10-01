import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from trading_engine.history_warmup import two_minute_bars, warmup_symbols
from trading_engine.runtime import TradingRuntime
from trading_engine.entry_pipeline import evaluate_entry, latest_psar_flip

ET = ZoneInfo("America/New_York")


def minutes(start, count):
    return [{"time": (start + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
             "open": 100+i, "high": 101+i, "low": 99+i,
             "close": 100.5+i, "volume": 10} for i in range(count)]


class HistoryWarmupTests(unittest.TestCase):
    def test_complete_one_minute_pairs_become_real_two_minute_ohlcv(self):
        start = datetime(2026, 9, 29, 11, 0, tzinfo=ET)
        rows = minutes(start, 30)
        bars = two_minute_bars(rows, (start + timedelta(minutes=31)).timestamp())
        self.assertEqual(len(bars), 15)
        self.assertEqual((bars[0].open, bars[0].high, bars[0].low,
                          bars[0].close, bars[0].volume), (100, 102, 99, 101.5, 20))
        self.assertTrue(all(bars[i].ts - bars[i-1].ts == 120 for i in range(1, len(bars))))

    def test_missing_minute_discards_earlier_disconnected_trend(self):
        start = datetime(2026, 9, 29, 11, 0, tzinfo=ET)
        rows = minutes(start, 30)
        del rows[18]
        bars = two_minute_bars(rows, (start + timedelta(minutes=31)).timestamp())
        self.assertEqual(len(bars), 5)  # 11:20 through 11:28 only

    def test_warmup_seeds_history_without_replaying_historical_flip(self):
        start = datetime(2026, 9, 29, 11, 0, tzinfo=ET)
        rows = minutes(start, 30)
        rows[-2]["close"] = rows[-2]["low"] = rows[-2]["open"] = 109
        rows[-2]["high"] = 129
        rows[-1]["open"] = 109
        rows[-1]["high"] = 110
        rows[-1]["low"] = 100
        rows[-1]["close"] = 101
        now = (start + timedelta(minutes=31)).timestamp()
        class Client:
            def minute_candles(self, symbol, begin, end):
                return rows
        rt = TradingRuntime()
        counts = warmup_symbols(Client(), rt, ["SPY"], now=now)
        self.assertEqual(counts["SPY"], 15)
        snap = rt.store.snapshot("SPY", now=now)
        self.assertEqual(len(snap.bars), 15)
        flip = latest_psar_flip(snap.bars)
        self.assertIsNotNone(flip)
        self.assertEqual(rt._processed_flip["SPY"], (snap.bars[-1].ts, flip.direction))
        rt.on_underlying_tick("SPY", now, 105)
        self.assertEqual(rt.dashboard.signal_view(), [])  # historical flip is not reconsidered

    def test_early_flip_waits_for_history_instead_of_permanent_skip(self):
        start = datetime(2026, 9, 29, 11, 0, tzinfo=ET)
        rt = TradingRuntime()
        bars = two_minute_bars(minutes(start, 8), (start + timedelta(minutes=9)).timestamp())
        rt.seed_history("SPY", bars)
        now = (start + timedelta(minutes=9)).timestamp()
        rt.store.ingest_price("SPY", now, 107)
        decision = evaluate_entry(rt.store.snapshot("SPY", now=now), "CALL", now=now)
        self.assertEqual(decision.action, "SKIP")
        self.assertEqual(decision.reasons, ("INSUFFICIENT_BAR_HISTORY",))


if __name__ == "__main__":
    unittest.main()
