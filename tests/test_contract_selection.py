import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
import threading
import time
from unittest.mock import patch

from trading_engine.contract_selection import (
    OptionContract, SelectionConfig, select_contract, make_option_entry_price_provider,
    PrefetchedChainSource,
)

NOW = 1768494600.0  # 2026-01-15 11:30 ET
DAY = 86400.0


def contract(option_type, strike, dte_days, bid, ask):
    occ = f"AAPL{option_type[0]}{int(strike)}_{dte_days}dte"  # a plausible stand-in, not real OCC formatting
    return OptionContract(symbol='AAPL', option_type=option_type, strike=strike,
                           expiration_ts=NOW + dte_days * DAY, bid=bid, ask=ask, occ_symbol=occ)


class TestBasicSelection(unittest.TestCase):
    def test_picks_the_strike_closest_to_at_the_money(self):
        chain = [
            contract('CALL', 95.0, 0, 5.0, 5.1),
            contract('CALL', 100.0, 0, 1.0, 1.05),
            contract('CALL', 105.0, 0, 0.2, 0.25),
        ]
        result = select_contract(chain, 'CALL', underlying_price=100.5, now=NOW)
        self.assertIsNotNone(result)
        self.assertEqual(result.contract.strike, 100.0)
        self.assertEqual(result.entry_price, 1.05)  # ask, not bid -- ask-first entry rule

    def test_only_considers_the_requested_option_type(self):
        chain = [
            contract('PUT', 100.0, 0, 1.0, 1.05),
            contract('CALL', 100.0, 0, 0.9, 0.95),
        ]
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertEqual(result.contract.option_type, 'CALL')

    def test_empty_chain_returns_none(self):
        self.assertIsNone(select_contract([], 'CALL', underlying_price=100.0, now=NOW))

    def test_invalid_underlying_price_returns_none(self):
        chain = [contract('CALL', 100.0, 0, 1.0, 1.05)]
        self.assertIsNone(select_contract(chain, 'CALL', underlying_price=0.0, now=NOW))


class TestDTEOrdering(unittest.TestCase):
    def test_prefers_the_nearest_expiration_when_multiple_are_liquid(self):
        chain = [
            contract('CALL', 100.0, 0, 1.0, 1.05),   # 0DTE
            contract('CALL', 100.0, 1, 1.5, 1.55),   # 1DTE, also perfectly liquid
        ]
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertEqual(result.dte_days, 0)

    def test_falls_through_to_next_expiration_when_nearest_is_illiquid(self):
        # 0DTE exists but its only strike is too wide/thin to trust; the
        # system should move to 1DTE rather than return None -- an ordering,
        # not a hard gate.
        chain = [
            contract('CALL', 100.0, 0, 0.01, 1.00),  # bid far below min_bid -- untrustworthy
            contract('CALL', 100.0, 1, 1.4, 1.5),    # clean, liquid 1DTE
        ]
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertEqual(result.dte_days, 1)

    def test_expirations_beyond_max_dte_are_excluded_entirely(self):
        chain = [contract('CALL', 100.0, 10, 3.0, 3.1)]  # 10DTE, beyond default max_dte_days=4
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertIsNone(result)

    def test_a_symbol_with_only_a_far_dated_chain_still_gets_a_contract_within_the_limit(self):
        # Directly reflects the real, confirmed finding: some symbols
        # (AFRM, RIVN, LLY, UNH, MRVL, SOFI) structurally lack 0-1DTE chains.
        # This is a real constraint, not a strategy choice -- DTE preference
        # is an ordering, so such a symbol still gets whatever it has within
        # max_dte_days, rather than being blocked outright.
        chain = [contract('CALL', 100.0, 3, 4.0, 4.2)]
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertIsNotNone(result)
        self.assertEqual(result.dte_days, 3)


class TestLiquidityFiltering(unittest.TestCase):
    def test_rejects_a_quote_with_too_wide_a_spread(self):
        chain = [contract('CALL', 100.0, 0, 0.5, 1.5)]  # spread = (1.5-0.5)/1.5 = 66.7%
        config = SelectionConfig(max_spread_pct=15.0)
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW, config=config)
        self.assertIsNone(result)

    def test_rejects_a_near_zero_bid(self):
        chain = [contract('CALL', 100.0, 0, 0.0, 1.0)]
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertIsNone(result)

    def test_skips_a_bad_strike_but_still_finds_a_good_one_in_the_same_expiration(self):
        chain = [
            contract('CALL', 100.0, 0, 0.0, 1.0),   # ATM but illiquid (bid=0)
            contract('CALL', 101.0, 0, 0.8, 0.9),   # slightly off ATM but liquid
        ]
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertIsNotNone(result)
        self.assertEqual(result.contract.strike, 101.0)


class TestPremiumPctRecorded(unittest.TestCase):
    def test_premium_pct_of_underlying_is_computed_and_attached(self):
        chain = [contract('CALL', 100.0, 0, 0.95, 1.00)]
        result = select_contract(chain, 'CALL', underlying_price=100.0, now=NOW)
        self.assertAlmostEqual(result.premium_pct_of_underlying, 1.0, places=2)


class TestRuntimeAdapter(unittest.TestCase):
    def test_adapter_matches_the_runtime_provider_signature_and_returns_ask(self):
        chain = [contract('CALL', 100.0, 0, 0.95, 1.00)]
        provider = make_option_entry_price_provider(chain_source=lambda sym, now: chain)
        result = provider('AAPL', 'CALL', NOW, 100.0)
        self.assertEqual(result.price, 1.00)

    def test_adapter_returns_none_when_chain_source_has_nothing(self):
        provider = make_option_entry_price_provider(chain_source=lambda sym, now: [])
        self.assertIsNone(provider('AAPL', 'CALL', NOW, 100.0))

    def test_on_selected_callback_fires_with_the_full_selection(self):
        chain = [contract('CALL', 100.0, 0, 0.95, 1.00)]
        captured = []
        provider = make_option_entry_price_provider(
            chain_source=lambda sym, now: chain,
            on_selected=lambda sym, selected: captured.append((sym, selected.dte_days, selected.premium_pct_of_underlying)),
        )
        provider('AAPL', 'CALL', NOW, 100.0)
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][0], 'AAPL')
        self.assertEqual(captured[0][1], 0)

    def test_wired_directly_into_a_real_runtime_opens_a_position(self):
        from trading_engine.runtime import TradingRuntime
        chain = [contract('CALL', 100.0, 0, 0.95, 1.00),
                 contract('CALL', 130.0, 0, 0.95, 1.00)]  # a strike near where price ends up too
        provider = make_option_entry_price_provider(chain_source=lambda sym, now: chain)
        rt = TradingRuntime(option_entry_price_provider=provider)

        down = [100 - .1 * i for i in range(14)]
        up = [98.7 + 5 + 5 * i for i in range(10)]
        closes = down + up
        t = NOW
        for c in closes:
            rt.on_underlying_tick('AAPL', t, c)
            t += 120.0
        self.assertEqual(len(rt.positions), 1)
        _, pos = rt.positions['AAPL']
        self.assertEqual(pos.entry_option_price, 1.00)
        # The specific contract's own symbol must be captured on the
        # position -- this is what lets a real stream client subscribe to
        # this exact contract's live quotes, not just the underlying's.
        self.assertTrue(pos.occ_symbol)
        self.assertEqual(rt._occ_to_underlying.get(pos.occ_symbol), 'AAPL')


class TestChainPrefetch(unittest.TestCase):
    def test_slow_chain_fetch_never_blocks_a_market_tick_and_missing_cache_fails_closed(self):
        started = threading.Event()
        release = threading.Event()
        calls = []
        def slow_fetch(symbol, now):
            calls.append(symbol)
            started.set()
            release.wait(2)
            return [contract('CALL', 100, 0, .95, 1.0)]
        cache = PrefetchedChainSource(slow_fetch, workers=1)
        try:
            began = time.monotonic()
            cache.schedule('AAPL')
            self.assertLess(time.monotonic() - began, .25)
            self.assertTrue(started.wait(1))
            cache.schedule('AAPL')
            self.assertEqual(calls, ['AAPL'])
            self.assertEqual(cache.get('AAPL', NOW), [])
            release.set()
            cache._pool.submit(lambda: None).result(timeout=2)
            provider = make_option_entry_price_provider(cache.get)
            self.assertEqual(provider('AAPL', 'CALL', NOW, 100).occ_symbol, 'AAPLC100_0dte')
            with cache._lock:
                _, rows = cache._cache['AAPL']
                cache._cache['AAPL'] = (time.time() - 200, rows)
            self.assertEqual(cache.get('AAPL', NOW), [])
        finally:
            release.set()
            cache.close()


if __name__ == '__main__':
    unittest.main()
