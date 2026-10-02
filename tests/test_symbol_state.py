import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import threading
import time
import unittest

from trading_engine.symbol_state import (
    Bar, FeedTTL, FreshnessState, GexSample, IntervalFlowSample,
    MarketTideSample, NetFlowSample, SymbolStateStore, SymbolStream,
    completed_bars,
)


class TestPerFeedFreshness(unittest.TestCase):
    """The core discipline this store exists to enforce: one fresh feed must
    never hide another stale one behind it. Every field gets its own,
    independent freshness check."""

    def test_one_fresh_feed_does_not_mask_another_stale_feed(self):
        # Directly reproduces the bug found in the earlier shadow-layer
        # review: net_flow 42s old, interval_flow 38s old, market_tide 1s old,
        # gex missing. A global min(fresh_ages) would have reported the whole
        # thing as "1 second old, LIVE" -- exactly wrong.
        ttl = FeedTTL(net_flow=5.0, interval_flow=10.0, market_tide=10.0, gex=15.0)
        stream = SymbolStream('TEST', ttl=ttl)
        now = 1_000_000.0
        stream.ingest_net_flow(NetFlowSample(ts=now - 42, dir_delta_flow=100.0))
        stream.ingest_interval_flow(IntervalFlowSample(ts=now - 38, call_vol_ask_side=10))
        stream.ingest_market_tide(MarketTideSample(ts=now - 1, net_call_premium=100, net_put_premium=50))
        snap = stream.snapshot(now=now)

        self.assertEqual(snap.net_flow_state, FreshnessState.STALE)
        self.assertEqual(snap.interval_flow_state, FreshnessState.STALE)
        self.assertEqual(snap.market_tide_state, FreshnessState.FRESH)
        self.assertEqual(snap.gex_state, FreshnessState.NOT_READY)
        # And critically: the stale feeds' directions must not be reported as
        # if they were current.
        self.assertEqual(snap.flow_30s, "NOT_READY")
        self.assertEqual(snap.interval_direction, "NOT_READY")
        # The one genuinely fresh feed is still usable on its own.
        self.assertEqual(snap.market_tide_direction, "CALL")

    def test_delayed_trade_does_not_rewind_price_or_psar_candles(self):
        stream = SymbolStream('MSFT', bar_seconds=120)
        stream.ingest_tick(1200, 515.0)
        stream.ingest_tick(1320, 516.0)
        stream.ingest_tick(1250, 514.0)
        snap = stream.snapshot(now=1320)
        self.assertEqual([b.ts for b in snap.bars], [1200, 1320])
        self.assertEqual(snap.bars[0].low, 515.0)
        self.assertEqual(snap.price, 516.0)

    def test_never_seen_feed_is_not_ready_not_neutral(self):
        stream = SymbolStream('TEST')
        snap = stream.snapshot(now=1_000_000.0)
        self.assertEqual(snap.net_flow_state, FreshnessState.NOT_READY)
        self.assertEqual(snap.flow_30s, "NOT_READY")
        self.assertNotEqual(snap.flow_30s, "NEUTRAL")

    def test_disconnect_makes_every_feed_not_ready_immediately(self):
        store = SymbolStateStore()
        now = 1_000_000.0
        store.ingest_net_flow('AAPL', NetFlowSample(ts=now, dir_delta_flow=100.0))
        store.ingest_market_tide('AAPL', MarketTideSample(ts=now, net_call_premium=10, net_put_premium=5))
        fresh = store.snapshot('AAPL', now=now)
        self.assertEqual(fresh.net_flow_state, FreshnessState.FRESH)

        store.set_connected(False)
        after_disconnect = store.snapshot('AAPL', now=now)  # same instant, no time has passed
        self.assertEqual(after_disconnect.net_flow_state, FreshnessState.NOT_READY)
        self.assertEqual(after_disconnect.market_tide_state, FreshnessState.NOT_READY)
        self.assertFalse(after_disconnect.connected)

    def test_reconnect_restores_freshness_for_new_data(self):
        store = SymbolStateStore()
        store.set_connected(False)
        store.set_connected(True)
        now = 1_000_000.0
        store.ingest_net_flow('AAPL', NetFlowSample(ts=now, dir_delta_flow=50.0))
        snap = store.snapshot('AAPL', now=now)
        self.assertEqual(snap.net_flow_state, FreshnessState.FRESH)


class TestNetFlowWindows(unittest.TestCase):
    def test_four_horizons_use_independent_windows(self):
        # The freshness TTL (is the stream alive right now) and each horizon's
        # window length (how far back to look) are different concepts -- the
        # newest sample must stay within the TTL for the feed to read FRESH
        # at all, while older samples can still fall inside the larger windows.
        stream = SymbolStream('TEST')
        now = 1_000_000.0
        stream.ingest_net_flow(NetFlowSample(ts=now - 3, dir_delta_flow=10.0))    # in 30s (and newest -> keeps feed FRESH)
        stream.ingest_net_flow(NetFlowSample(ts=now - 45, dir_delta_flow=20.0))   # in 1m only (not 30s)
        stream.ingest_net_flow(NetFlowSample(ts=now - 150, dir_delta_flow=30.0))  # in 3m only
        snap = stream.snapshot(now=now)
        self.assertEqual(snap.net_flow_state, FreshnessState.FRESH)
        self.assertEqual(snap.flow_30s, "CALL")  # only the +10 sample -> positive
        self.assertEqual(snap.flow_1m, "CALL")    # +10 +20 = +30 -> positive
        self.assertEqual(snap.flow_3m, "CALL")    # +10+20+30 = +60 -> positive
        self.assertEqual(snap.flow_5m, "CALL")

    def test_negative_net_flow_reports_put(self):
        stream = SymbolStream('TEST')
        now = 1_000_000.0
        stream.ingest_net_flow(NetFlowSample(ts=now - 5, dir_delta_flow=-75.0))
        snap = stream.snapshot(now=now)
        self.assertEqual(snap.flow_30s, "PUT")

    def test_exactly_zero_net_flow_is_neutral_not_not_ready(self):
        # A genuine zero reading is real evidence of "no net flow" -- distinct
        # from having no data at all.
        stream = SymbolStream('TEST')
        now = 1_000_000.0
        stream.ingest_net_flow(NetFlowSample(ts=now - 5, dir_delta_flow=0.0))
        snap = stream.snapshot(now=now)
        self.assertEqual(snap.flow_30s, "NEUTRAL")


class TestBarIngestion(unittest.TestCase):
    def test_same_timestamp_updates_in_progress_bar_in_place(self):
        stream = SymbolStream('TEST')
        stream.ingest_bar(Bar(ts=100.0, open=10, high=10.5, low=9.8, close=10.2))
        stream.ingest_bar(Bar(ts=100.0, open=10, high=10.8, low=9.8, close=10.6))  # same bar, price moved
        snap = stream.snapshot(now=200.0)
        self.assertEqual(len(snap.bars), 1)
        self.assertEqual(snap.bars[0].high, 10.8)
        self.assertEqual(snap.bars[0].close, 10.6)

    def test_new_timestamp_appends_a_new_bar(self):
        stream = SymbolStream('TEST')
        stream.ingest_bar(Bar(ts=100.0, open=10, high=10.5, low=9.8, close=10.2))
        stream.ingest_bar(Bar(ts=220.0, open=10.2, high=10.9, low=10.1, close=10.7))
        snap = stream.snapshot(now=300.0)
        self.assertEqual(len(snap.bars), 2)

    def test_stale_bars_are_flagged_but_still_returned(self):
        # Bars remain visible even when stale -- PSAR/structure computation
        # over a slightly-stale-but-present bar series is a different question
        # than "is there current data"; the caller decides what to do with a
        # STALE bar_state, the store doesn't discard history.
        stream = SymbolStream('TEST', ttl=FeedTTL(bar=60.0))
        stream.ingest_bar(Bar(ts=0.0, open=10, high=10.5, low=9.8, close=10.2))
        snap = stream.snapshot(now=200.0)
        self.assertEqual(snap.bar_state, FreshnessState.STALE)
        self.assertEqual(len(snap.bars), 1)


class TestAggressorAndIntervalDerivation(unittest.TestCase):
    def test_call_side_aggression_reports_call(self):
        stream = SymbolStream('TEST')
        now = 1_000_000.0
        stream.ingest_interval_flow(IntervalFlowSample(
            ts=now - 2, call_vol_ask_side=500, call_vol_bid_side=50,
            put_vol_ask_side=50, put_vol_bid_side=500,
            dir_delta_flow=200.0, dir_vega_flow=-50.0, avg_dte=0.0,
        ))
        snap = stream.snapshot(now=now)
        self.assertEqual(snap.aggressor_direction, "CALL")
        self.assertGreater(snap.aggressor_strength, 0)
        self.assertEqual(snap.delta_flow_direction, "CALL")
        self.assertEqual(snap.vega_flow_direction, "PUT")
        self.assertEqual(snap.avg_dte, 0.0)

    def test_stale_interval_flow_reports_not_ready_direction_with_zero_strength(self):
        stream = SymbolStream('TEST', ttl=FeedTTL(interval_flow=5.0))
        now = 1_000_000.0
        stream.ingest_interval_flow(IntervalFlowSample(ts=now - 30, call_vol_ask_side=999))
        snap = stream.snapshot(now=now)
        self.assertEqual(snap.interval_flow_state, FreshnessState.STALE)
        self.assertEqual(snap.aggressor_direction, "NOT_READY")
        self.assertEqual(snap.aggressor_strength, 0.0)


class TestGexAndWalls(unittest.TestCase):
    def test_fresh_gex_exposes_walls_and_targets(self):
        stream = SymbolStream('TEST')
        now = 1_000_000.0
        stream.ingest_gex(GexSample(ts=now - 3, call_wall=105.0, put_wall=95.0,
                                     gamma_path="BRAKES AHEAD", target_candidates=(100.0, 98.0)))
        snap = stream.snapshot(now=now)
        self.assertEqual(snap.gex_state, FreshnessState.FRESH)
        self.assertEqual(snap.gamma_path, "BRAKES AHEAD")
        self.assertEqual(snap.call_wall, 105.0)
        self.assertEqual(snap.target_candidates, (100.0, 98.0))

    def test_stale_gex_clears_walls_rather_than_serving_old_levels(self):
        stream = SymbolStream('TEST', ttl=FeedTTL(gex=10.0))
        now = 1_000_000.0
        stream.ingest_gex(GexSample(ts=now - 30, call_wall=105.0, put_wall=95.0, gamma_path="CLEAR"))
        snap = stream.snapshot(now=now)
        self.assertEqual(snap.gex_state, FreshnessState.STALE)
        self.assertIsNone(snap.call_wall)
        self.assertEqual(snap.gamma_path, "N/A")


class TestTickToBarAggregation(unittest.TestCase):
    """ingest_tick() is the real-time streaming entry point: it must build
    correct bars from raw ticks exactly the way the reference system's
    MinuteCandleStore did by hand-polling and bucketing REST quotes."""

    def test_multiple_ticks_in_the_same_bucket_aggregate_into_one_bar(self):
        stream = SymbolStream('TEST', bar_seconds=120.0)
        # bar_seconds=120 buckets 1000.0 into [960, 1080) -- all three ticks
        # below fall in that same window.
        stream.ingest_tick(ts=1000.0, price=100.0, volume=10)
        stream.ingest_tick(ts=1030.0, price=101.5, volume=5)  # same 120s bucket
        stream.ingest_tick(ts=1070.0, price=99.0, volume=8)   # same bucket, new low
        snap = stream.snapshot(now=1100.0)
        self.assertEqual(len(snap.bars), 1)
        bar = snap.bars[0]
        self.assertEqual(bar.open, 100.0)
        self.assertEqual(bar.high, 101.5)
        self.assertEqual(bar.low, 99.0)
        self.assertEqual(bar.close, 99.0)
        self.assertEqual(bar.volume, 23)

    def test_tick_in_a_new_bucket_closes_the_prior_bar_and_starts_a_new_one(self):
        stream = SymbolStream('TEST', bar_seconds=120.0)
        stream.ingest_tick(ts=1000.0, price=100.0, volume=10)  # bucket at 960 (1000//120*120)
        stream.ingest_tick(ts=1300.0, price=105.0, volume=7)   # a later bucket
        snap = stream.snapshot(now=1400.0)
        self.assertEqual(len(snap.bars), 2)
        self.assertEqual(snap.bars[0].close, 100.0)
        self.assertEqual(snap.bars[1].open, 105.0)
        self.assertEqual(snap.bars[1].volume, 7)

    def test_ingest_tick_also_updates_the_raw_price_read(self):
        # A separate ingest_price() call must not be required -- price and
        # bars can never legitimately disagree about the last-seen price.
        stream = SymbolStream('TEST')
        stream.ingest_tick(ts=1000.0, price=42.5)
        snap = stream.snapshot(now=1001.0)
        self.assertEqual(snap.price, 42.5)
        self.assertEqual(snap.price_state, FreshnessState.FRESH)

    def test_non_positive_price_tick_is_safely_ignored(self):
        stream = SymbolStream('TEST')
        stream.ingest_tick(ts=1000.0, price=0.0)
        stream.ingest_tick(ts=1000.0, price=-5.0)
        snap = stream.snapshot(now=1001.0)
        self.assertIsNone(snap.price)
        self.assertEqual(len(snap.bars), 0)


class TestCompletedBars(unittest.TestCase):
    def test_still_forming_bar_is_excluded(self):
        bars = (
            Bar(ts=0.0, open=1, high=1, low=1, close=1),
            Bar(ts=120.0, open=1, high=1, low=1, close=1),
        )
        # now is inside the second bar's interval -- it hasn't closed yet
        result = completed_bars(bars, bar_seconds=120.0, now=200.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].ts, 0.0)

    def test_bar_becomes_completed_once_its_interval_has_fully_elapsed(self):
        bars = (
            Bar(ts=0.0, open=1, high=1, low=1, close=1),
            Bar(ts=120.0, open=1, high=1, low=1, close=1),
        )
        result = completed_bars(bars, bar_seconds=120.0, now=240.0)
        self.assertEqual(len(result), 2)

    def test_empty_bars_returns_empty(self):
        self.assertEqual(completed_bars((), bar_seconds=120.0, now=1000.0), ())


class TestThreadSafety(unittest.TestCase):
    def test_concurrent_ingest_and_snapshot_do_not_crash_or_corrupt(self):
        store = SymbolStateStore()
        stop = threading.Event()
        errors = []

        def writer():
            i = 0
            while not stop.is_set():
                try:
                    now = time.time()
                    store.ingest_net_flow('AAPL', NetFlowSample(ts=now, dir_delta_flow=float(i % 7 - 3)))
                    store.ingest_bar('AAPL', Bar(ts=now, open=1, high=1, low=1, close=1))
                    i += 1
                except Exception as exc:  # pragma: no cover - failure path
                    errors.append(exc)

        def reader():
            while not stop.is_set():
                try:
                    store.snapshot('AAPL')
                except Exception as exc:  # pragma: no cover - failure path
                    errors.append(exc)

        threads = [threading.Thread(target=writer) for _ in range(3)] + \
                  [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        time.sleep(0.3)
        stop.set()
        for t in threads:
            t.join(timeout=2)

        self.assertEqual(errors, [])


if __name__ == '__main__':
    unittest.main()
