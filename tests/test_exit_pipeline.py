import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest

from trading_engine.symbol_state import SymbolStream, GexSample
from trading_engine.exit_pipeline import (
    PositionState, ExitConfig, evaluate_exit,
)


# A fixed, safe mid-session ET timestamp (2026-01-15 11:30:00 America/New_York),
# well clear of EOD_FORCE_CLOSE_ET (15:50 default). Using epoch 0.0 as a base
# (as a first draft of this test file did) is a real, recurring trap: epoch 0
# converts to 1969-12-31 19:00 EST -- already past 15:50 ET -- which silently
# makes every test hit EOD_FORCE_CLOSE first regardless of what it's actually
# testing. This is the same class of bug fixed earlier tonight in the old
# system's test suite for the identical reason.
SAFE_TEST_TS = 1768494600.0


def make_snapshot(closes, direction='CALL', bar_seconds=120.0, now_offset=0.0, uw_flows=None):
    """Builds a SymbolSnapshot from a synthetic uptrend/downtrend close series."""
    stream = SymbolStream('TEST', bar_seconds=bar_seconds)
    t = SAFE_TEST_TS
    for c in closes:
        stream.ingest_tick(ts=t, price=c)
        t += bar_seconds
    now = t + now_offset
    stream.ingest_price(ts=now, price=closes[-1])  # fresh underlying quote after last closed bar
    snap = stream.snapshot(now=now)
    if uw_flows:
        snap = snap.__class__(**{**snap.__dict__, **uw_flows})
    return snap, now


class TestPriorityOrdering(unittest.TestCase):
    def test_eod_beats_everything_including_emergency_stop(self):
        closes = [100 - i for i in range(15)]  # a clear downtrend -> would also hit emergency
        snap, base_now = make_snapshot(closes, direction='CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=0.0, entry_option_price=1.0)
        pos.update_price(0.5, base_now)  # -50% -> would trigger EMERGENCY on its own
        # Force "now" to be past the EOD cutoff (15:50 ET) regardless of the
        # symbol's own bar timestamps -- EOD is a real wall-clock check.
        import datetime as dt
        from zoneinfo import ZoneInfo
        eod_moment = dt.datetime(2026, 1, 15, 15, 55, tzinfo=ZoneInfo("America/New_York")).timestamp()
        decision = evaluate_exit(snap, pos, now=eod_moment)
        self.assertEqual(decision.state, "EXIT_PENDING")
        self.assertEqual(decision.reasons, ("EOD_FORCE_CLOSE",))

    def test_emergency_stop_beats_opposite_psar_and_everything_below(self):
        closes = [100 + i for i in range(10)] + [100 - i * 3 for i in range(1, 8)]  # up then hard reversal
        snap, now = make_snapshot(closes, direction='CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=0.0, entry_option_price=1.0)
        pos.update_price(1.05, now - 600)  # small early peak
        pos.peak_option_price = 1.05
        pos.update_price(0.68, now)  # -32%, past the -30% floor
        decision = evaluate_exit(snap, pos, now=now)
        self.assertEqual(decision.state, "EXIT_PENDING")
        self.assertEqual(decision.reasons, ("EMERGENCY_OPTION_STOP",))


class TestEpsilonTolerance(unittest.TestCase):
    def test_stale_bid_cannot_trigger_giveback_but_fresh_bid_can(self):
        snap, now = make_snapshot([100] * 12, direction='CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=now-300,
                            entry_option_price=1.0, current_option_price=1.05,
                            peak_option_price=1.35, armed=True,
                            last_quote_spread=0.05, peak_quote_spread=0.05)
        stale = evaluate_exit(snap, pos, now=now, option_quote_fresh=False)
        self.assertNotEqual(stale.state, "EXIT_PENDING")
        fresh = evaluate_exit(snap, pos, now=now, option_quote_fresh=True)
        self.assertEqual(fresh.reasons, ("PROFIT_GIVEBACK_EXIT",))

    def test_peak_epsilon_at_spread_arm_boundary_arms_and_exits(self):
        closes = [100] * 12
        snap, now = make_snapshot(closes, direction='CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=0.0, entry_option_price=1.0)
        pos.update_price(1.40 - 1e-9, now - 300, ask=1.45 - 1e-9)
        pos.update_price(1.0, now, ask=1.05)
        decision = evaluate_exit(snap, pos, now=now)
        self.assertEqual(decision.state, "EXIT_PENDING")
        self.assertEqual(decision.reasons, ("PROFIT_GIVEBACK_EXIT",))
        self.assertTrue(pos.armed)


class TestBareFlipProvenVsUnproven(unittest.TestCase):
    """The new, explicitly-not-yet-validated trigger. Must fire only once
    real progress has been shown, and must never fire on an unproven position
    even with the identical bare (unconfirmed) opposite flip."""

    def _reversal_snapshot(self, direction):
        # A clean, extended trend in `direction` (20 bars, long enough for
        # PSAR's acceleration factor to ramp up and trail tightly), then a
        # MODEST pullback on the last bar -- just enough to trip the
        # now-tightly-trailing SAR, but calibrated (verified directly against
        # compute_psar/momentum_from_bars) to be too small to flip the EMA-based
        # momentum reading's sign. This isolates a genuinely bare, unconfirmed
        # flip: PSAR reverses, momentum and structure do not.
        if direction == 'CALL':
            closes = [100 + i for i in range(20)] + [119 - 6]
        else:
            closes = [100 - i for i in range(20)] + [81 + 6]
        return make_snapshot(closes, direction=direction)

    def test_proven_position_exits_on_bare_flip_alone(self):
        snap, now = self._reversal_snapshot('CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=0.0, entry_option_price=1.0)
        pos.update_price(1.20, now - 400, ask=1.22)
        pos.update_price(1.06, now, ask=1.08)
        config = ExitConfig(require_price_confirmation_after_proven=False)
        decision = evaluate_exit(snap, pos, now=now, config=config)
        self.assertEqual(decision.state, "EXIT_PENDING")
        self.assertEqual(decision.reasons, ("OPPOSITE_PSAR_BARE_PROVEN",))

    def test_unproven_position_does_not_exit_on_the_same_bare_flip(self):
        snap, now = self._reversal_snapshot('CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=now - 60, entry_option_price=1.0)
        pos.update_price(1.03, now - 40, ask=1.05)
        pos.update_price(1.01, now, ask=1.03)
        config = ExitConfig(require_price_confirmation_after_proven=False)
        decision = evaluate_exit(snap, pos, now=now, config=config)
        self.assertNotEqual(decision.reasons, ("OPPOSITE_PSAR_BARE_PROVEN",))
        self.assertNotEqual(decision.state, "EXIT_PENDING")

    def test_flag_disables_the_bare_trigger_entirely_regardless_of_proven_state(self):
        snap, now = self._reversal_snapshot('CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=0.0, entry_option_price=1.0)
        pos.update_price(1.10, now - 400, ask=1.12)
        pos.update_price(1.06, now, ask=1.08)
        config = ExitConfig(require_price_confirmation_after_proven=True)
        decision = evaluate_exit(snap, pos, now=now, config=config)
        self.assertNotIn("OPPOSITE_PSAR_BARE_PROVEN", decision.reasons)


class TestFreshPSARRecomputation(unittest.TestCase):
    def test_exit_ladder_uses_current_bars_not_any_cached_direction(self):
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=0.0, entry_option_price=1.0)

        uptrend_closes = [100 + i for i in range(14)]
        snap_up, now_up = make_snapshot(uptrend_closes, direction='CALL')
        pos.update_price(1.05, now_up)
        decision_up = evaluate_exit(snap_up, pos, now=now_up)
        self.assertNotEqual(decision_up.reasons, ("OPPOSITE_PSAR_CONFIRMED_BY_PRICE",))

        # Same position, but now given a genuinely reversed and CONFIRMED bar
        # series (a real multi-bar downtrend, not just one reversal bar) --
        # the ladder must react to THIS series, proving it recomputed fresh
        # rather than reusing whatever it saw the first call.
        reversed_closes = [100 + i for i in range(10)] + [90 - i * 2 for i in range(8)]
        snap_down, now_down = make_snapshot(reversed_closes, direction='CALL')
        pos.update_price(1.0, now_down)
        decision_down = evaluate_exit(snap_down, pos, now=now_down)
        self.assertEqual(decision_down.reasons, ("OPPOSITE_PSAR_CONFIRMED_BY_PRICE",))


class TestStallExit(unittest.TestCase):
    def test_unarmed_stall_after_completed_bar_and_no_bid_high_exits(self):
        closes = [100 + i for i in range(10)] + [109] * 5
        snap, now = make_snapshot(closes, direction='CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=now-600, entry_option_price=1.0)
        pos.update_price(1.01, now - 300, ask=1.05)
        decision = evaluate_exit(snap, pos, now=now)
        self.assertEqual(decision.reasons, ("STALL_EXIT",))

    def test_armed_position_is_never_evaluated_for_stall(self):
        # Once armed by its spread, only the giveback trail applies.
        closes = [100] * 15
        snap, now = make_snapshot(closes, direction='CALL')
        pos = PositionState(symbol='TEST', direction='CALL', opened_ts=0.0, entry_option_price=1.0)
        pos.update_price(1.40, now - 300, ask=1.44)
        pos.update_price(1.38, now, ask=1.42)
        decision = evaluate_exit(snap, pos, now=now)
        self.assertNotEqual(decision.reasons, ("STALL_EXIT",))
        self.assertTrue(pos.armed)


class TestAdaptivePremiumTrail(unittest.TestCase):
    def test_five_cent_spread_warns_at_130_and_exits_at_120_after_140_peak(self):
        snap, now = make_snapshot([100 + i for i in range(10)] + [109] * 5)
        pos = PositionState('TEST', 'CALL', now-200, 1.0)
        pos.update_price(1.40, now-100, ask=1.45)
        pos.update_price(1.30, now-40, ask=1.35)
        self.assertEqual(evaluate_exit(snap, pos, now).state, 'PROFIT_LOCK')
        pos.update_price(1.20, now, ask=1.25)
        self.assertEqual(evaluate_exit(snap, pos, now).reasons, ('PROFIT_GIVEBACK_EXIT',))

    def test_same_bid_giveback_has_different_result_with_different_spreads(self):
        snap, now = make_snapshot([100 + i for i in range(10)] + [109] * 5)
        def decision(spread):
            pos = PositionState('TEST', 'CALL', now-200, 1.0)
            pos.update_price(1.40, now-100, ask=1.40+spread)
            pos.update_price(1.19, now, ask=1.19+spread)
            return evaluate_exit(snap, pos, now)
        self.assertEqual(decision(.04).reasons, ('PROFIT_GIVEBACK_EXIT',))
        self.assertEqual(decision(.12).state, 'HOLD')

    def test_small_early_pullback_and_widening_spread_do_not_exit(self):
        snap, now = make_snapshot([100 + i for i in range(10)] + [109] * 5)
        pos = PositionState('TEST', 'CALL', now-60, 1.92)
        pos.update_price(1.98, now-20, ask=1.99)
        pos.update_price(1.95, now, ask=1.96)
        self.assertNotEqual(evaluate_exit(snap, pos, now).state, 'EXIT_PENDING')
        self.assertFalse(pos.armed)
        self.assertAlmostEqual(pos.peak_quote_spread, .01)
        pos.update_price(2.30, now+1, ask=2.31)
        pos.update_price(2.20, now+2, ask=2.40)  # widening must not loosen trail
        self.assertEqual(evaluate_exit(snap, pos, now+2).reasons, ('PROFIT_GIVEBACK_EXIT',))

    def test_invalid_ask_cannot_drive_adaptive_exit(self):
        snap, now = make_snapshot([100] * 12)
        pos = PositionState('TEST', 'CALL', now-60, 1.0)
        pos.update_price(1.40, now-30, ask=1.45)
        pos.update_price(1.01, now, ask=1.00)
        self.assertEqual(pos.last_quote_spread, 0.0)
        self.assertNotEqual(evaluate_exit(snap, pos, now).state, 'EXIT_PENDING')


if __name__ == '__main__':
    unittest.main()
