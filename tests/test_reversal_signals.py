import random
import unittest

from trading_engine.reversal_signals import ReversalSettings, ReversalSignals, replay_reversal
from trading_engine.runtime import TradingRuntime
from trading_engine.symbol_state import Bar


class ReversalSignalsTests(unittest.TestCase):
    def test_completed_green_and_red_p_are_independent_of_chart_alerts(self):
        for step, direction in ((-1, "CALL"), (1, "PUT")):
            bars = tuple(Bar(i*120, 100+i*step, 100+i*step+0.1,
                             100+i*step-0.1, 100+i*step) for i in range(13))
            frames = replay_reversal(bars, ReversalSettings(momentum_display="None"))
            assert all(not any(e.kind == "MOMENTUM_COMPLETE" for e in f.events) for f in frames[:-1])
            marker = next(e for e in frames[-1].events if e.kind == "MOMENTUM_COMPLETE")
            self.assertEqual((marker.direction, marker.count, marker.perfected), (direction, 9, True))
            self.assertFalse(marker.visible)
            self.assertFalse(marker.alert)

    def test_exhaustion_and_optional_trade_setup_modes(self):
        random.seed(1)
        price = 100.0
        bars = []
        for i in range(200):
            price += random.gauss(0, 0.8)
            wick = random.random()*1.5
            bars.append(Bar(i*120, price, price+wick, price-wick, price))
        none = replay_reversal(bars)
        self.assertTrue(any(e.kind == "EXHAUSTION_COMPLETE" and e.direction == "PUT"
                            for e in none[68].events))
        self.assertFalse(any(e.kind.startswith("SETUP_") for f in none for e in f.events))
        momentum = replay_reversal(bars, ReversalSettings(trade_setups="Momentum"))
        exhaustion = replay_reversal(bars, ReversalSettings(trade_setups="Exhaustion"))
        self.assertTrue(any(e.kind == "SETUP_SHORT" for e in momentum[24].events))
        self.assertTrue(any(e.kind == "SETUP_SHORT" for e in exhaustion[76].events))
        self.assertTrue(any(f.bearish_exhaustion_target > 0 for f in exhaustion))
        self.assertTrue(any(f.bearish_exhaustion_risk > 0 for f in exhaustion))

    def test_seeded_history_does_not_reemit_and_late_market_time_does_not_rewind(self):
        class Audit:
            def __init__(self): self.events = []
            def emit(self, name, **fields): self.events.append((name, fields))

        audit = Audit()
        rt = TradingRuntime(audit=audit)
        bars = [Bar(1768494600+i*120, 120-i, 120.1-i, 119.9-i, 120-i)
                for i in range(13)]
        rt.seed_history('TEST', bars[:12])
        self.assertFalse(any(name == 'REVERSAL_SIGNAL' for name, _ in audit.events))
        frame = rt._reversal_frame('TEST', bars)
        self.assertEqual(frame.perfected_momentum(), 'CALL')
        markers = [f for name, f in audit.events
                   if name == 'REVERSAL_SIGNAL' and f['trigger'] == 'MOMENTUM_COMPLETE']
        self.assertEqual(len(markers), 1)
        self.assertEqual(len([x for x in rt.dashboard.reversal_view()
                              if x['kind'] == 'MOMENTUM_COMPLETE']), 1)
        rt._reversal_frame('TEST', bars[:12])
        rt._reversal_frame('TEST', bars)
        self.assertEqual(len([1 for name, f in audit.events
                              if name == 'REVERSAL_SIGNAL' and f['trigger'] == 'MOMENTUM_COMPLETE']), 1)


if __name__ == '__main__':
    unittest.main()
