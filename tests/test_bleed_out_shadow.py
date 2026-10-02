import unittest

from trading_engine.bleed_out_shadow import read_bleed_out_exit
from trading_engine.exit_pipeline import ExitConfig, PositionState


def make_pos(entry, current, armed=False):
    pos = PositionState(symbol="TEST", direction="CALL", opened_ts=1000.0, entry_option_price=entry)
    pos.current_option_price = current
    pos.armed = armed
    return pos


class TestBleedOutRead(unittest.TestCase):
    def test_fires_when_never_armed_and_loss_exceeds_threshold(self):
        pos = make_pos(entry=1.00, current=0.88, armed=False)  # -12% loss
        read = read_bleed_out_exit(pos, quote_fresh=True, config=ExitConfig(bleed_out_loss_pct=-10.0))
        self.assertEqual(read.reason, "BLEED_OUT_EXIT")

    def test_does_not_fire_above_the_threshold(self):
        pos = make_pos(entry=1.00, current=0.95, armed=False)  # -5% loss, not past -10%
        read = read_bleed_out_exit(pos, quote_fresh=True, config=ExitConfig(bleed_out_loss_pct=-10.0))
        self.assertIsNone(read.reason)

    def test_does_not_fire_once_armed_even_at_a_large_loss(self):
        # Once armed, PROFIT_GIVEBACK_EXIT is the live mechanism's job -- this
        # backstop is specifically for the never-armed case.
        pos = make_pos(entry=1.00, current=0.50, armed=True)  # -50%, but armed
        read = read_bleed_out_exit(pos, quote_fresh=True, config=ExitConfig(bleed_out_loss_pct=-10.0))
        self.assertIsNone(read.reason)

    def test_stale_quote_never_produces_a_reading(self):
        pos = make_pos(entry=1.00, current=0.50, armed=False)  # would otherwise fire
        read = read_bleed_out_exit(pos, quote_fresh=False, config=ExitConfig(bleed_out_loss_pct=-10.0))
        self.assertIsNone(read.reason)

    def test_zero_current_price_never_produces_a_reading(self):
        pos = make_pos(entry=1.00, current=0.0, armed=False)
        read = read_bleed_out_exit(pos, quote_fresh=True, config=ExitConfig(bleed_out_loss_pct=-10.0))
        self.assertIsNone(read.reason)

    def test_never_mutates_the_position(self):
        pos = make_pos(entry=1.00, current=0.80, armed=False)
        before = (pos.current_option_price, pos.armed, pos.entry_option_price)
        read_bleed_out_exit(pos, quote_fresh=True, config=ExitConfig())
        after = (pos.current_option_price, pos.armed, pos.entry_option_price)
        self.assertEqual(before, after)

    def test_threshold_is_configurable(self):
        pos = make_pos(entry=1.00, current=0.94, armed=False)  # -6%
        self.assertIsNone(read_bleed_out_exit(pos, True, ExitConfig(bleed_out_loss_pct=-10.0)).reason)
        self.assertEqual(read_bleed_out_exit(pos, True, ExitConfig(bleed_out_loss_pct=-5.0)).reason, "BLEED_OUT_EXIT")


class TestRuntimeWiring(unittest.TestCase):
    def test_shadow_fires_before_a_real_exit_and_logs_first_trigger_once(self):
        from trading_engine.runtime import TradingRuntime
        from trading_engine.contract_selection import OptionEntryResult

        emitted = []

        class FakeAudit:
            def emit(self, event, **kw):
                emitted.append((event, kw))

        rt = TradingRuntime(
            option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.0, occ_symbol="TEST260101C00100000"),
            exit_config=ExitConfig(reversal_phase_exit=False),
        )
        rt.audit = FakeAudit()

        down = [112 - i for i in range(14)]
        up = [99 + i + (i * i) * 0.03 for i in range(20)]
        t = 1768494600.0
        for c in down + up:
            rt.on_underlying_tick('AAPL', t, c)
            t += 120.0

        trade_id, pos = rt.positions['AAPL']
        # Drive the option price down past the bleed-out threshold without arming.
        rt.on_option_quote(pos.occ_symbol, t, bid=0.85, ask=0.87)  # -15%, still never armed
        rt._evaluate_symbol('AAPL', now=t)

        triggers = [e for e in emitted if e[0] == "BLEED_OUT_SHADOW_TRIGGER"]
        self.assertEqual(len(triggers), 1)
        self.assertEqual(triggers[0][1]["reason"], "BLEED_OUT_EXIT")

        # A second evaluation at the same depressed price must not re-log the trigger.
        rt._evaluate_symbol('AAPL', now=t + 1.0)
        triggers_again = [e for e in emitted if e[0] == "BLEED_OUT_SHADOW_TRIGGER"]
        self.assertEqual(len(triggers_again), 1)

    def test_shadow_tracking_is_cleaned_up_on_real_close(self):
        from trading_engine.runtime import TradingRuntime
        from trading_engine.contract_selection import OptionEntryResult

        class FakeAudit:
            def emit(self, event, **kw):
                pass

        rt = TradingRuntime(option_entry_price_provider=lambda sym, d, now, u: OptionEntryResult(price=1.0, occ_symbol="TEST260101C00100000"),
                            exit_config=ExitConfig(reversal_phase_exit=False))
        rt.audit = FakeAudit()
        down = [112 - i for i in range(14)]
        up = [99 + i + (i * i) * 0.03 for i in range(20)]
        t = 1768494600.0
        for c in down + up:
            rt.on_underlying_tick('AAPL', t, c)
            t += 120.0
        trade_id, pos = rt.positions['AAPL']
        rt.on_option_quote(pos.occ_symbol, t, bid=0.85, ask=0.87)
        rt._evaluate_symbol('AAPL', now=t)
        self.assertIn(trade_id, rt._bleed_out_shadow_first)

        # positions is keyed by symbol, not trade_id -- drive a real close via
        # EMERGENCY_OPTION_STOP and confirm the shadow tracking dict is cleaned up.
        rt.on_option_quote(pos.occ_symbol, t + 1, bid=0.60, ask=0.62)  # -40%, past the -30% floor
        rt._evaluate_symbol('AAPL', now=t + 1)
        self.assertNotIn('AAPL', rt.positions)
        self.assertNotIn(trade_id, rt._bleed_out_shadow_first)


if __name__ == '__main__':
    unittest.main()
