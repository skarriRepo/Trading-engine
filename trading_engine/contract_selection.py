"""Turns a symbol + direction + option chain into a real, priced contract to
enter -- the piece that implements TradingRuntime's option_entry_price_provider.

Deliberately does NOT reintroduce the DTE/premium-ratio hard block that was
tried and reverted earlier: that block's "allowed" tier showed a 12.5% win
rate at scale once actually measured. DTE preference here is an ORDERING
(prefer the nearest available expiration), not a live gate -- a symbol that
structurally lacks 0-1DTE options (AFRM, RIVN, LLY, UNH, MRVL, SOFI, confirmed
earlier) still gets a contract from whatever it does have; it is not blocked.
The chosen contract's DTE and premium-as-%-of-underlying are recorded on the
result so this data can finally be analyzed properly before any blocking rule
is reconsidered -- collecting the evidence first this time, not after.

Pure selection logic, no network I/O: given a chain already fetched by
something else, pick a contract. fetch_and_select() adapts this to the
callable shape TradingRuntime expects, given a pluggable chain source.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import threading
import time
from typing import Callable, List, Optional, Tuple


@dataclass(frozen=True)
class OptionContract:
    symbol: str        # the UNDERLYING ticker, e.g. "AAPL"
    option_type: str   # "CALL" or "PUT"
    strike: float
    expiration_ts: float  # midnight ET of the expiration date, as epoch seconds
    bid: float
    ask: float
    occ_symbol: str = ""  # the option's OWN symbol, e.g. "AAPL260115C00150000" --
                           # required for subscribing to this specific contract's
                           # live quotes. Defaults to "" only for callers that
                           # construct a contract directly without one (e.g. older
                           # tests); real chain data must always set this.
    open_interest: int = 0
    volume: int = 0


@dataclass(frozen=True)
class SelectionConfig:
    max_dte_days: int = 4          # do not consider expirations further out than this
    max_spread_pct: float = 15.0   # (ask-bid)/ask, rejects too-illiquid quotes
    min_bid: float = 0.02          # a bid of ~0 signals no real market, not a cheap contract
    strike_preference: str = "ATM"  # "ATM" is the only strategy implemented; see module docstring


@dataclass(frozen=True)
class SelectedContract:
    contract: OptionContract
    entry_price: float  # ask -- the validated ask-first entry rule
    dte_days: int
    premium_pct_of_underlying: float


@dataclass(frozen=True)
class OptionEntryResult:
    """What TradingRuntime.option_entry_price_provider actually needs to
    return: not just a price, but the specific contract's own OCC symbol, so
    the runtime can track it on the position and (via a stream client's
    add_symbol/remove_symbol) subscribe to that contract's live quotes. A
    bare float here would silently make it impossible to know which contract
    a position's price should be tracked against once more than one contract
    for the same underlying could ever exist in play.
    """
    price: float
    occ_symbol: str


def _dte_days(now: float, expiration_ts: float) -> int:
    seconds_per_day = 86400.0
    return max(0, int((expiration_ts - now) // seconds_per_day))


def select_contract(chain: List[OptionContract], direction: str, underlying_price: float,
                     now: float, config: SelectionConfig = SelectionConfig()) -> Optional[SelectedContract]:
    """Picks a contract from `chain`. Returns None if nothing in the chain is
    usable -- never fabricates a contract or silently relaxes a filter.

    Selection order:
      1. Restrict to the requested option_type and expirations within
         max_dte_days.
      2. Group by expiration; try expirations nearest-dated first (0DTE
         before 1DTE before 2DTE, etc.) -- an ordering, not a gate: if the
         nearest expiration has nothing that clears the liquidity filter,
         the next expiration out is tried rather than returning None
         outright, since some symbols structurally lack near-dated chains.
      3. Within an expiration, pick the strike closest to at-the-money.
      4. Reject (move to the next expiration) if the spread or bid is too
         thin to trust the quote.
    """
    if underlying_price <= 0 or not chain:
        return None
    option_type = direction  # "CALL"/"PUT" direction maps directly to option_type
    candidates = [c for c in chain if c.option_type == option_type
                  and _dte_days(now, c.expiration_ts) <= config.max_dte_days]
    if not candidates:
        return None

    by_expiration: dict = {}
    for c in candidates:
        by_expiration.setdefault(c.expiration_ts, []).append(c)

    for expiration_ts in sorted(by_expiration.keys()):
        strikes = by_expiration[expiration_ts]
        strikes_sorted = sorted(strikes, key=lambda c: abs(c.strike - underlying_price))
        for c in strikes_sorted:
            if c.ask <= 0 or c.bid < config.min_bid:
                continue
            spread_pct = (c.ask - c.bid) / c.ask * 100.0
            if spread_pct > config.max_spread_pct:
                continue
            dte = _dte_days(now, expiration_ts)
            premium_pct = c.ask / underlying_price * 100.0
            return SelectedContract(contract=c, entry_price=c.ask, dte_days=dte,
                                     premium_pct_of_underlying=round(premium_pct, 3))
    return None


ChainSource = Callable[[str, float], List[OptionContract]]  # (symbol, now) -> chain


class PrefetchedChainSource:
    """Refresh production option chains off the market-data callback.

    A missing, empty, or expired snapshot returns no contracts. Selection uses
    the underlying price at the actual flip, followed by a new live OCC quote.
    """

    def __init__(self, fetch: ChainSource, max_age_sec: float = 150.0,
                 refresh_sec: float = 90.0, workers: int = 2):
        if max_age_sec <= 0 or refresh_sec <= 0 or workers <= 0:
            raise ValueError("Chain cache intervals and worker count must be positive")
        self.fetch = fetch
        self.max_age_sec = max_age_sec
        self.refresh_sec = refresh_sec
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="chain-prefetch")
        self._cache: dict[str, tuple[float, List[OptionContract]]] = {}
        self._last_attempt: dict[str, float] = {}
        self._inflight: set[str] = set()
        self._closed = False

    def schedule(self, symbol: str) -> None:
        wall = time.time()
        with self._lock:
            if (self._closed or symbol in self._inflight or
                    wall - self._last_attempt.get(symbol, 0) < self.refresh_sec):
                return
            self._inflight.add(symbol)
            self._last_attempt[symbol] = wall
            self._pool.submit(self._refresh, symbol)

    def _refresh(self, symbol: str) -> None:
        try:
            contracts = self.fetch(symbol, time.time())
            if contracts:
                with self._lock:
                    self._cache[symbol] = (time.time(), contracts)
        except Exception:
            pass  # no stale-cache extension; the next scheduled refresh may recover
        finally:
            with self._lock:
                self._inflight.discard(symbol)

    def get(self, symbol: str, now: float) -> List[OptionContract]:
        with self._lock:
            entry = self._cache.get(symbol)
            if not entry or not 0 <= time.time() - entry[0] <= self.max_age_sec:
                return []
            return list(entry[1])

    def ready(self, symbol: str) -> bool:
        with self._lock:
            entry = self._cache.get(symbol)
            return bool(entry and 0 <= time.time() - entry[0] <= self.max_age_sec)

    def close(self) -> None:
        with self._lock:
            self._closed = True
        self._pool.shutdown(wait=False, cancel_futures=True)


def make_option_entry_price_provider(chain_source: ChainSource,
                                      config: SelectionConfig = SelectionConfig(),
                                      on_selected: Optional[Callable[[str, SelectedContract], None]] = None,
                                      ) -> Callable[[str, str, float, float], Optional[OptionEntryResult]]:
    """Adapts select_contract() to the (symbol, direction, now, underlying_price)
    -> Optional[OptionEntryResult] shape TradingRuntime.option_entry_price_provider
    expects. `chain_source` is pluggable -- a real Tradier option-chain client, a
    cached snapshot, or (in tests) a fixed synthetic chain. `on_selected`, if
    given, is called with the full SelectedContract (not just the price)
    whenever a contract is actually chosen -- e.g. to log the DTE and
    premium_pct_of_underlying data this module exists to finally start
    collecting.
    """
    def provider(symbol: str, direction: str, now: float, underlying_price: float) -> Optional[OptionEntryResult]:
        chain = chain_source(symbol, now)
        if not chain:
            return None
        selected = select_contract(chain, direction, underlying_price, now, config=config)
        if selected is None:
            return None
        if on_selected is not None:
            on_selected(symbol, selected)
        return OptionEntryResult(price=selected.entry_price, occ_symbol=selected.contract.occ_symbol)
    return provider
