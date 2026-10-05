import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from trading_engine.symbol_state import Bar
from trading_engine.entry_pipeline import compute_psar, PSARParams
from trading_engine.history_warmup import warmup_symbols

class PineSARTests(unittest.TestCase):
    def test_initialization_and_first_trend_bar(self):
        bars = (Bar(0,10,11,9,10), Bar(120,10,12,10,11), Bar(240,11,13,11,12))
        points = compute_psar(bars, PSARParams(.02,.02,.2))
        self.assertEqual([p.sar for p in points], [9,9])
        self.assertFalse(any(p.is_flip for p in points))

    def test_outside_reversal_uses_current_extreme(self):
        bars = (Bar(0,10,11,9,10), Bar(120,10,12,10,11), Bar(240,11,15,8,9))
        point = compute_psar(bars, PSARParams(.02,.02,.2))[-1]
        self.assertEqual((point.sar,point.direction,point.is_flip),(15,'PUT',True))

    def test_equal_closes_initialize_bearish_and_equal_sar_is_put(self):
        points = compute_psar((Bar(0,10,10,10,10),Bar(120,10,10,10,10)))
        self.assertEqual((points[0].sar,points[0].direction,points[0].is_flip),(10,'PUT',False))

    def test_prefix_results_do_not_use_future_bars(self):
        bars=tuple(Bar(i*120,c,c+1,c-1,c) for i,c in enumerate([10,11,12,9,8,13,14]))
        full=compute_psar(bars)
        for n in range(2,len(bars)+1):
            self.assertEqual(compute_psar(bars[:n]),full[:n-1])

    def test_premarket_launch_requests_history_from_four_am(self):
        now=datetime(2026,10,5,8,56,tzinfo=ZoneInfo('America/New_York'))
        requests=[]
        class Client:
            def minute_candles(self,symbol,start,end):
                requests.append((start,end));return []
        class Runtime:
            def seed_history(self,*args):pass
        warmup_symbols(Client(),Runtime(),['AAPL'],now.timestamp())
        self.assertEqual(requests,[('2026-10-05 04:00','2026-10-05 08:56')])
