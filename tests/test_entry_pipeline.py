import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest

from trading_engine.symbol_state import Bar, GexSample, SymbolStream
from trading_engine.entry_pipeline import (
    PSARParams, compute_psar, latest_psar_flip,
    momentum_from_bars, momentum_ok, uw_confluence,
    approximate_target, evaluate_entry, EntryConfig, UWConfluenceRead,
)
from trading_engine.exit_pipeline import structure_from_bars


def make_bars(closes, ts_start=0.0, step=120.0, wick=0.1):
    """Simple synthetic OHLC series: each bar's high/low pad the close by a
    small fixed wick so structure/PSAR calculations have something to bite on."""
    bars = []
    prev_close = closes[0]
    for i, c in enumerate(closes):
        o = prev_close
        bars.append(Bar(ts=ts_start + i * step, open=o, high=max(o, c) + wick,
                         low=min(o, c) - wick, close=c))
        prev_close = c
    return tuple(bars)


class TestPSAR(unittest.TestCase):
    def test_uptrend_then_reversal_produces_exactly_one_flip(self):
        # Steadily rising, then a sharp reversal.
        closes = [100, 101, 102, 103, 104, 105, 106, 90, 89, 88]
        bars = make_bars(closes)
        points = compute_psar(bars, PSARParams(start=0.03, increment=0.02, maximum=0.20))
        flips = [p for p in points if p.is_flip]
        self.assertEqual(len(flips), 1)
        self.assertEqual(flips[0].direction, "PUT")

    def test_fewer_than_three_bars_returns_empty(self):
        bars = make_bars([100, 101])
        self.assertEqual(compute_psar(bars), ())

    def test_latest_psar_flip_returns_none_when_last_bar_did_not_flip(self):
        closes = [100, 101, 102, 103, 104, 105, 106, 107]
        bars = make_bars(closes)
        # A steady uptrend with no reversal at the very end.
        self.assertIsNone(latest_psar_flip(bars))

    def test_latest_psar_flip_returns_the_point_when_last_bar_flipped(self):
        closes = [100, 101, 102, 103, 104, 105, 106, 90]
        bars = make_bars(closes)
        flip = latest_psar_flip(bars)
        self.assertIsNotNone(flip)
        self.assertEqual(flip.direction, "PUT")


class TestStructure(unittest.TestCase):
    def test_rising_highs_and_lows_is_hh_hl(self):
        closes = [100, 101, 102, 103, 104, 105]
        bars = make_bars(closes)
        self.assertEqual(structure_from_bars(bars), "HH/HL")

    def test_falling_highs_and_lows_is_lh_ll(self):
        closes = [105, 104, 103, 102, 101, 100]
        bars = make_bars(closes)
        self.assertEqual(structure_from_bars(bars), "LH/LL")

    def test_choppy_series_is_mixed(self):
        closes = [100, 103, 99, 104, 98, 105]
        bars = make_bars(closes)
        self.assertEqual(structure_from_bars(bars), "MIXED")

    def test_fewer_than_three_bars_is_mixed_not_a_guess(self):
        bars = make_bars([100, 101])
        self.assertEqual(structure_from_bars(bars), "MIXED")


class TestMomentum(unittest.TestCase):
    def test_insufficient_history_is_not_ready(self):
        bars = make_bars([100, 101, 102])
        self.assertEqual(momentum_from_bars(bars, fast=5, slow=10), "NOT_READY")

    def test_accelerating_uptrend_is_bull_expanding(self):
        # Increasingly steep climb -> separation between fast/slow EMA grows.
        closes = [100 + i + (i * i) * 0.05 for i in range(20)]
        bars = make_bars(closes)
        result = momentum_from_bars(bars, fast=5, slow=10)
        self.assertEqual(result, "BULL_EXPANDING")

    def test_flattening_uptrend_is_bull_decelerating(self):
        # Sharp initial climb that flattens out -- separation shrinks at the end.
        closes = [100 + min(i, 6) * 3 for i in range(20)]
        bars = make_bars(closes)
        result = momentum_from_bars(bars, fast=5, slow=10)
        self.assertEqual(result, "BULL_DECELERATING")


class TestUWConfluence(unittest.TestCase):
    """The deliberately conservative rule -- must require strong, broad
    agreement, and must never guess when evidence is thin."""

    def test_full_five_of_five_agreement_supports(self):
        snap = _snap_with_flows("CALL", "CALL", "CALL", "CALL", "CALL")
        r = uw_confluence(snap, "CALL")
        self.assertTrue(r.supports)
        self.assertFalse(r.opposes)

    def test_four_of_five_with_zero_disagreement_still_supports(self):
        snap = _snap_with_flows("CALL", "CALL", "CALL", "CALL", "NEUTRAL")
        r = uw_confluence(snap, "CALL")
        self.assertTrue(r.supports)

    def test_four_agree_but_one_disagrees_does_not_support(self):
        # Stricter than a simple majority: any outright disagreement blocks
        # `supports`, even with 4 agreeing.
        snap = _snap_with_flows("CALL", "CALL", "CALL", "CALL", "PUT")
        r = uw_confluence(snap, "CALL")
        self.assertFalse(r.supports)
        self.assertFalse(r.opposes)

    def test_only_three_of_five_agree_is_no_clear_read_not_a_guess(self):
        snap = _snap_with_flows("CALL", "CALL", "CALL", "PUT", "NEUTRAL")
        r = uw_confluence(snap, "CALL")
        self.assertFalse(r.supports)
        self.assertFalse(r.opposes)
        self.assertTrue(r.ready)  # there IS evidence, it's just not decisive

    def test_fewer_than_three_usable_signals_is_not_ready(self):
        snap = _snap_with_flows("NOT_READY", "NOT_READY", "NOT_READY", "CALL", "CALL")
        r = uw_confluence(snap, "CALL")
        self.assertFalse(r.ready)
        self.assertFalse(r.supports)


def _snap_with_flows(f30, f1m, f3m, f5m, aggr):
    stream = SymbolStream('TEST')
    snap = stream.snapshot(now=1000.0)
    return snap.__class__(
        **{**snap.__dict__, 'flow_30s': f30, 'flow_1m': f1m, 'flow_3m': f3m,
           'flow_5m': f5m, 'aggressor_direction': aggr}
    )


class TestApproximateTarget(unittest.TestCase):
    def test_invalid_price_or_atr_returns_none(self):
        stream = SymbolStream('TEST')
        snap = stream.snapshot(now=1000.0)
        self.assertIsNone(approximate_target(snap, "CALL", price=0.0, atr=1.0))
        self.assertIsNone(approximate_target(snap, "CALL", price=100.0, atr=0.0))

    def test_uses_fresh_gex_wall_when_it_clears_min_rr(self):
        stream = SymbolStream('TEST')
        stream.ingest_gex(GexSample(ts=999.0, call_wall=110.0, gamma_path="CLEAR"))
        snap = stream.snapshot(now=1000.0)
        result = approximate_target(snap, "CALL", price=100.0, atr=1.0)
        self.assertEqual(result.source, "GEX_WALL")
        self.assertEqual(result.target, 110.0)

    def test_no_fabricated_target_when_gex_not_ready(self):
        stream = SymbolStream('TEST')
        snap = stream.snapshot(now=1000.0)  # no GEX ingested at all
        result = approximate_target(snap, "CALL", price=100.0, atr=1.0)
        self.assertIsNone(result)

    def test_nearby_gex_wall_retains_low_rr(self):
        stream = SymbolStream('TEST')
        # Wall far too close to price to clear a 1.25 min R:R against this risk.
        stream.ingest_gex(GexSample(ts=999.0, call_wall=100.05, gamma_path="CLEAR"))
        snap = stream.snapshot(now=1000.0)
        result = approximate_target(snap, "CALL", price=100.0, atr=1.0)
        self.assertEqual(result.source, "GEX_WALL")
        self.assertLess(result.rr, 1.25)


class TestEvaluateEntry(unittest.TestCase):
    def _healthy_uptrend_snapshot(self, extra_flows=None):
        stream = SymbolStream('TEST', bar_seconds=120.0)
        closes = [100 + i + (i * i) * 0.03 for i in range(20)]
        t = 0.0
        for c in closes:
            stream.ingest_tick(ts=t, price=c)
            t += 120.0
        now = t + 130.0  # safely past the last bar's close so it counts as completed
        stream.ingest_gex(GexSample(ts=now - 1, call_wall=closes[-1] + 20, gamma_path="CLEAR"))
        snap = stream.snapshot(now=now)
        if extra_flows:
            snap = snap.__class__(**{**snap.__dict__, **extra_flows})
        return snap, now

    def test_price_not_ready_skips(self):
        stream = SymbolStream('TEST')
        snap = stream.snapshot(now=1000.0)
        decision = evaluate_entry(snap, "CALL", now=1000.0)
        self.assertEqual(decision.action, "SKIP")
        self.assertIn("PRICE_NOT_READY", decision.reasons)

    def test_healthy_uptrend_with_target_takes_without_structure(self):
        snap, now = self._healthy_uptrend_snapshot()
        decision = evaluate_entry(snap, "CALL", now=now)
        self.assertEqual(decision.action, "TAKE")
        self.assertFalse(hasattr(decision, "structure"))
        self.assertIn("BULL", decision.momentum_state)
        self.assertIsNotNone(decision.target)

    def test_no_gex_target_does_not_claim_reward_risk(self):
        snap, now = self._healthy_uptrend_snapshot()
        snap = snap.__class__(**{**snap.__dict__, 'gex_state': 'NOT_READY',
                                 'call_wall': None, 'target_candidates': ()})
        decision = evaluate_entry(snap, 'CALL', now=now)
        self.assertEqual(decision.action, 'TAKE')
        self.assertIsNone(decision.target)
        self.assertIn('NO_VERIFIED_TARGET', decision.reasons)

    def test_nearby_real_gex_wall_rejects_entry(self):
        snap, now = self._healthy_uptrend_snapshot()
        snap = snap.__class__(**{**snap.__dict__, 'gex_state': 'FRESH',
                                 'call_wall': snap.price + .01, 'target_candidates': ()})
        decision = evaluate_entry(snap, 'CALL', now=now)
        self.assertEqual(decision.action, 'SKIP')
        self.assertIn('TARGET_RR_TOO_LOW', decision.reasons)
        self.assertEqual(decision.target.source, 'GEX_WALL')

    def test_mixed_structure_does_not_block_expanding_momentum(self):
        snap, now = self._healthy_uptrend_snapshot()
        bars = list(snap.bars)
        idx = -3
        old = bars[idx]
        bars[idx] = Bar(old.ts, old.open, old.high, old.low - 15, old.close, old.volume)
        snap = snap.__class__(**{**snap.__dict__, 'bars': tuple(bars)})
        decision = evaluate_entry(snap, "CALL", now=now)
        self.assertEqual(decision.action, "TAKE")
        self.assertFalse(hasattr(decision, "structure"))

    def test_uw_strong_opposition_skips_even_with_good_structure(self):
        snap, now = self._healthy_uptrend_snapshot(extra_flows={
            'flow_30s': 'PUT', 'flow_1m': 'PUT', 'flow_3m': 'PUT', 'flow_5m': 'PUT', 'aggressor_direction': 'PUT',
        })
        decision = evaluate_entry(snap, "CALL", now=now)
        self.assertEqual(decision.action, "TAKE")
        self.assertIn("UW_OPPOSING_OBSERVATION", decision.reasons)

    def test_momentum_not_yet_expanding_skips_this_flip(self):
        stream = SymbolStream('TEST', bar_seconds=120.0)
        # A fresh flip without directional expansion does not queue a later entry.
        closes = [100] * 12 + [100.1, 100.05, 100.1, 100.05]
        t = 0.0
        for c in closes:
            stream.ingest_tick(ts=t, price=c)
            t += 120.0
        now = t + 130.0
        snap = stream.snapshot(now=now)
        decision = evaluate_entry(snap, "CALL", now=now)
        self.assertEqual(decision.action, "TAKE")
        self.assertIn("MOMENTUM_NOT_EXPANDING_OBSERVATION", decision.reasons)


if __name__ == '__main__':
    unittest.main()
