"""Tradier REST client: quotes, option expirations, option chains.

Built against Tradier's public, documented REST API
(https://documentation.tradier.com/brokerage-api) and the same endpoint
shapes this session's reference codebase already used successfully
(market_data.py: GET /v1/markets/quotes, /v1/markets/options/chains,
/v1/markets/options/expirations). This is DOCUMENTED-BUT-NOT-LIVE-TESTED:
no real Tradier credentials are available in this environment, so the
parsing logic here is verified against realistic fixture JSON matching
Tradier's documented response schema (see tests/test_tradier_client.py),
not against an actual live response. Smoke-test against Tradier's sandbox
before trusting this in anything real.

The access token is read ONLY from the TRADIER_ACCESS_TOKEN environment
variable (see .env.example at the package root). It is never hardcoded,
never logged, and never included in any error message this module raises.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from .contract_selection import OptionContract

PRODUCTION_BASE = "https://api.tradier.com/v1"
SANDBOX_BASE = "https://sandbox.tradier.com/v1"


class TradierAuthError(RuntimeError):
    """Raised when TRADIER_ACCESS_TOKEN is missing or Tradier rejects it.
    Never includes the token itself in the message."""


@dataclass(frozen=True)
class TradierConfig:
    base_url: str = PRODUCTION_BASE
    timeout_sec: float = 5.0


def _load_token(explicit_token: Optional[str] = None) -> str:
    token = explicit_token if explicit_token is not None else os.environ.get("TRADIER_ACCESS_TOKEN")
    if not token:
        raise TradierAuthError(
            "TRADIER_ACCESS_TOKEN is not set. Set it as an environment variable "
            "(see .env.example) -- this client never accepts a token as a literal "
            "in code."
        )
    return token


class TradierRestClient:
    def __init__(self, config: TradierConfig = TradierConfig(), token: Optional[str] = None,
                 session: Optional[requests.Session] = None):
        self.config = config
        self._token = _load_token(token)
        self._session = session or requests.Session()

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        try:
            resp = self._session.get(url, params=params, headers=self._headers(),
                                      timeout=self.config.timeout_sec)
        except requests.RequestException as exc:
            raise TradierAuthError(f"Tradier request to {path} failed: {exc}") from None
        if resp.status_code == 401:
            raise TradierAuthError(f"Tradier rejected the configured token (401) on {path}.")
        resp.raise_for_status()
        return resp.json()

    # -- quotes --

    def quotes(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Returns {symbol: quote_dict}. Tradier's own API returns a single
        dict (not a list) for the "quote" field when only one symbol is
        requested -- normalized here so callers never have to special-case it.
        """
        if not symbols:
            return {}
        data = self._get("/markets/quotes", {"symbols": ",".join(symbols)})
        raw = (data.get("quotes") or {}).get("quote")
        if raw is None:
            return {}
        rows = raw if isinstance(raw, list) else [raw]
        return {row["symbol"]: row for row in rows if "symbol" in row}

    # -- option chain discovery --

    def expirations(self, symbol: str) -> List[str]:
        """Returns expiration dates as 'YYYY-MM-DD' strings, nearest first."""
        data = self._get("/markets/options/expirations", {"symbol": symbol, "includeAllRoots": "true"})
        raw = (data.get("expirations") or {}).get("date")
        if raw is None:
            return []
        dates = raw if isinstance(raw, list) else [raw]
        return sorted(dates)

    def option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        """Raw chain rows for one expiration, as Tradier returns them."""
        data = self._get("/markets/options/chains", {"symbol": symbol, "expiration": expiration, "greeks": "false"})
        raw = (data.get("options") or {}).get("option")
        if raw is None:
            return []
        return raw if isinstance(raw, list) else [raw]

    def minute_candles(self, symbol: str, start_et: str, end_et: str) -> List[Dict[str, Any]]:
        """One-minute time-and-sales candles, with empty responses normalized."""
        body = self._get("/markets/timesales", {"symbol": symbol, "interval": "1min",
                    "start": start_et, "end": end_et, "session_filter": "open"})
        series = body.get("series") if isinstance(body, dict) else None
        if not isinstance(series, dict):
            return []
        raw = series.get("data")
        if isinstance(raw, dict):
            return [raw]
        return [row for row in raw if isinstance(row, dict)] if isinstance(raw, list) else []


def _expiration_to_epoch(date_str: str) -> float:
    import datetime as dt
    from zoneinfo import ZoneInfo
    d = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/New_York"))
    return d.timestamp()


def _parse_option_row(row: Dict[str, Any]) -> Optional[OptionContract]:
    try:
        option_type = "CALL" if str(row.get("option_type", "")).lower() == "call" else "PUT"
        return OptionContract(
            symbol=str(row.get("underlying") or row.get("root_symbol") or ""),
            option_type=option_type,
            strike=float(row["strike"]),
            expiration_ts=_expiration_to_epoch(str(row["expiration_date"])),
            bid=float(row.get("bid") or 0.0),
            ask=float(row.get("ask") or 0.0),
            occ_symbol=str(row.get("symbol") or ""),  # the option's OWN symbol, e.g. "AAPL260115C00150000"
            open_interest=int(row.get("open_interest") or 0),
            volume=int(row.get("volume") or 0),
        )
    except (KeyError, ValueError, TypeError):
        return None  # a malformed row is skipped, not allowed to crash the whole chain fetch


def make_chain_source(client: TradierRestClient, max_expirations: int = 5):
    """A contract_selection.ChainSource: (symbol, now) -> List[OptionContract],
    built from real Tradier expirations() + option_chain() calls. Pass this
    to contract_selection.make_option_entry_price_provider().
    """
    def chain_source(symbol: str, now: float) -> List[OptionContract]:
        try:
            dates = client.expirations(symbol)[:max_expirations]
        except TradierAuthError:
            raise
        except Exception:
            return []  # a transient lookup failure yields "no chain", not a crash
        contracts: List[OptionContract] = []
        for exp in dates:
            try:
                rows = client.option_chain(symbol, exp)
            except TradierAuthError:
                raise
            except Exception:
                continue
            for row in rows:
                row.setdefault("underlying", symbol)
                c = _parse_option_row(row)
                if c is not None:
                    contracts.append(c)
        return contracts
    return chain_source
