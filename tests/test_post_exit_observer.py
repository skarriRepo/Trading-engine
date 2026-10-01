import unittest
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from trading_engine.post_exit_observer import PostExitQuoteObserver
from trading_engine.exit_pipeline import PositionState


class Timer:
    def __init__(self, seconds, callback, args):
        self.callback, self.args = callback, args
        self.daemon = False
    def start(self):
        pass
    def fire(self):
        self.callback(*self.args)


class TestPostExitQuoteObserver(unittest.TestCase):
    def test_post_exit_bid_shadow_records_first_live_trigger_without_orders(self):
        events, timers = [], []
        wall = [datetime(2026, 9, 30, 15, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()]
        stream = SimpleNamespace(add_symbol=lambda occ: None, remove_symbol=lambda occ: None)
        audit = SimpleNamespace(emit=lambda event, **fields: events.append((event, fields)))
        observer = PostExitQuoteObserver(stream, audit, clock=lambda: wall[0],
                                         timer_factory=lambda seconds, callback, args:
                                         timers.append(Timer(seconds, callback, args)) or timers[-1])
        pos = PositionState("SPY", "CALL", wall[0]-100, .63, occ_symbol="SPY260930C00600000")
        pos.update_price(.70, wall[0]-1, ask=.72)
        observer.opened("t1", pos)
        observer.closed("t1", pos)
        observer.quote(pos.occ_symbol, wall[0], .80, .82)
        wall[0] += 1
        observer.quote(pos.occ_symbol, wall[0], .75, .77)
        wall[0] += 1
        observer.quote(pos.occ_symbol, wall[0], .70, .72)
        triggers = [row for event, row in events if event == "POST_EXIT_BID_SHADOW_TRIGGER"]
        self.assertEqual(len(triggers), 1)
        self.assertEqual(triggers[0]["bid"], .70)
        timers[0].fire()
        end = [row for event, row in events if event == "POST_EXIT_SHADOW_END"][0]
        self.assertEqual(end["accepted_quotes"], 3)
        self.assertEqual(end["first_bid_exit"]["bid"], .70)

    def test_live_quotes_observed_without_orders_and_subscription_released(self):
        events, subscriptions, timers = [], [], []
        stream = SimpleNamespace(add_symbol=lambda occ: subscriptions.append(("ADD", occ)),
                                 remove_symbol=lambda occ: subscriptions.append(("REMOVE", occ)))
        audit = SimpleNamespace(emit=lambda event, **fields: events.append((event, fields)))
        def timer_factory(seconds, callback, args):
            timer = Timer(seconds, callback, args)
            timers.append(timer)
            return timer
        observer = PostExitQuoteObserver(stream, audit, clock=lambda: 1000.,
                                         timer_factory=timer_factory)
        pos = SimpleNamespace(occ_symbol="SPY260930P00766000", symbol="SPY", direction="PUT",
                              entry_option_price=.63, current_option_price=.58)
        observer.opened("trade1", pos)
        observer.closed("trade1", pos)
        observer.quote(pos.occ_symbol, 999., .75, .77)
        observer.quote(pos.occ_symbol, 998., .78, .80)  # out of order
        self.assertEqual(subscriptions, [("ADD", pos.occ_symbol)])
        self.assertEqual([e for e, _ in events].count("POST_EXIT_OPTION_QUOTE"), 1)
        self.assertEqual([r for e, r in events if e == "POST_EXIT_OPTION_QUOTE"][0]
                         ["ask_to_bid_usd_per_contract"], 12.)
        timers[0].fire()
        self.assertEqual(subscriptions[-1], ("REMOVE", pos.occ_symbol))
        self.assertEqual([r for e, r in events if e == "POST_EXIT_SHADOW_END"][0]["peak_bid"], .75)

    def test_new_position_keeps_same_contract_subscribed_after_old_shadow_ends(self):
        removed = []
        stream = SimpleNamespace(add_symbol=lambda occ: None,
                                 remove_symbol=lambda occ: removed.append(occ))
        audit = SimpleNamespace(emit=lambda *args, **kwargs: None)
        timers = []
        def timer_factory(seconds, callback, args):
            timer = Timer(seconds, callback, args)
            timers.append(timer)
            return timer
        observer = PostExitQuoteObserver(stream, audit, clock=lambda: 1000.,
                                         timer_factory=timer_factory)
        pos = SimpleNamespace(occ_symbol="SPY260930P00766000", symbol="SPY", direction="PUT",
                              entry_option_price=.63, current_option_price=.58)
        observer.opened("old", pos)
        observer.closed("old", pos)
        observer.opened("new", pos)
        timers[0].fire()
        self.assertEqual(removed, [])
