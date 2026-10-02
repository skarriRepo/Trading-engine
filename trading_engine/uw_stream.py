"""Unusual Whales WebSocket client using the documented join/array protocol.

Net-flow messages are cumulative session net premiums. This adapter passes
per-update premium changes to the existing NetFlowSample.dir_delta_flow field;
the legacy field name does not imply Greek delta. The first snapshot after
each connection establishes a baseline and is not treated as window flow.
UW remains observational and never blocks entry. Live server verification is
still required before treating its readings as validated market evidence.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    import websocket  # from the `websocket-client` package
except ImportError:  # pragma: no cover
    websocket = None

from .symbol_state import GexSample, IntervalFlowSample, MarketTideSample, NetFlowSample

UW_WS_URL = "wss://api.unusualwhales.com/socket"  # best-effort; confirm against UW's current docs before relying on it


def bind_net_flow(runtime):
    """Adapt stream sample objects to TradingRuntime's three-argument handler."""
    def ingest(symbol: str, sample: NetFlowSample) -> None:
        runtime.on_net_flow(symbol, sample.ts, sample.dir_delta_flow)
    return ingest


def _authenticated_url(url: str, key: str) -> str:
    """UW requires the API token in the WebSocket URL query string."""
    parts = urlsplit(url)
    if parts.scheme not in ("ws", "wss") or not parts.netloc:
        raise ValueError("UW_WS_URL must be a WebSocket URL")
    query = [(name, value) for name, value in parse_qsl(parts.query, keep_blank_values=True)
             if name.lower() != "token"]
    query.append(("token", key))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


class UWAuthError(RuntimeError):
    """Raised when UW_API_KEY is missing. Never includes the key itself."""


def _load_uw_key(explicit_key: Optional[str] = None) -> str:
    key = explicit_key if explicit_key is not None else os.environ.get("UW_API_KEY")
    if not key:
        raise UWAuthError(
            "UW_API_KEY is not set. Set it as an environment variable "
            "(see README.md for the private credentials file) -- this client never accepts a key as a "
            "literal in code."
        )
    return key


# ---------------------------------------------------------------------------
# Parsing -- pure functions, one per channel, each independently testable
# ---------------------------------------------------------------------------

def parse_net_flow_message(raw: Dict[str, Any]) -> Optional[tuple]:
    """Parse cumulative session call-minus-put net premium per ticker."""
    symbol = raw.get("ticker") or raw.get("symbol")
    if not symbol:
        return None
    try:
        if "net_call_prem" in raw and "net_put_prem" in raw:
            delta = float(raw["net_call_prem"]) - float(raw["net_put_prem"])
        else:
            delta = float(raw.get("net_delta_flow", raw.get("delta", 0.0)))
        ts = _epoch_sec(raw.get("timestamp") or raw.get("time"))
    except (TypeError, ValueError):
        return None
    return symbol, NetFlowSample(ts=ts, dir_delta_flow=delta)


def parse_interval_flow_message(raw: Dict[str, Any]) -> Optional[tuple]:
    """interval_flow channel -> (symbol, IntervalFlowSample) or None.
    Expected shape: per-symbol call/put ask/bid-side volume for the current
    interval, plus delta/vega flow and average DTE of the flow observed.
    """
    symbol = raw.get("ticker") or raw.get("symbol")
    if not symbol:
        return None
    try:
        sample = IntervalFlowSample(
            ts=_epoch_sec(raw.get("tape_time") or raw.get("timestamp") or raw.get("time")),
            call_vol_ask_side=float(raw.get("call_vol_ask_side", raw.get("call_volume_ask_side", 0.0))),
            call_vol_bid_side=float(raw.get("call_vol_bid_side", raw.get("call_volume_bid_side", 0.0))),
            put_vol_ask_side=float(raw.get("put_vol_ask_side", raw.get("put_volume_ask_side", 0.0))),
            put_vol_bid_side=float(raw.get("put_vol_bid_side", raw.get("put_volume_bid_side", 0.0))),
            dir_delta_flow=float(raw.get("dir_delta_flow", raw.get("net_delta_flow", 0.0))),
            dir_vega_flow=float(raw.get("dir_vega_flow", raw.get("net_vega_flow", 0.0))),
            avg_dte=(float(raw["avg_dte"]) if raw.get("avg_dte") is not None else None),
        )
    except (TypeError, ValueError):
        return None
    return symbol, sample


def parse_market_tide_message(raw: Dict[str, Any]) -> Optional[MarketTideSample]:
    """market_tide channel -> MarketTideSample or None. Market-wide, not
    per-symbol -- applied to every watched symbol by the caller, matching
    TradingRuntime.on_market_tide's existing broadcast behavior.
    """
    try:
        return MarketTideSample(
            ts=_epoch_sec(raw.get("timestamp") or raw.get("time")),
            net_call_premium=float(raw.get("net_call_premium", 0.0)),
            net_put_premium=float(raw.get("net_put_premium", 0.0)),
        )
    except (TypeError, ValueError):
        return None


def parse_gex_message(raw: Dict[str, Any]) -> Optional[tuple]:
    """Ticker aggregate GEX sign; this channel has no target/wall levels."""
    symbol = raw.get("ticker") or raw.get("symbol")
    if not symbol:
        return None
    try:
        if "gamma_per_one_percent_move_oi" in raw:
            gamma = float(raw["gamma_per_one_percent_move_oi"])
            path = "POSITIVE_GEX" if gamma > 0 else "NEGATIVE_GEX" if gamma < 0 else "FLAT_GEX"
            return symbol, GexSample(ts=_epoch_sec(raw.get("timestamp")), gamma_path=path)
        call_wall = float(raw["call_wall"]) if raw.get("call_wall") is not None else None
        put_wall = float(raw["put_wall"]) if raw.get("put_wall") is not None else None
        targets = tuple(float(x) for x in (raw.get("target_candidates") or []))
        sample = GexSample(
            ts=_epoch_sec(raw.get("timestamp") or raw.get("time")),
            call_wall=call_wall, put_wall=put_wall,
            gamma_path=str(raw.get("gamma_path") or "N/A"),
            target_candidates=targets,
        )
    except (TypeError, ValueError):
        return None
    return symbol, sample


def _epoch_sec(raw: Any) -> float:
    if raw is None:
        return time.time()
    try:
        v = float(raw)
        return v / 1000.0 if v > 1e12 else v  # ms vs sec heuristic, same as tradier_stream.py's
    except (TypeError, ValueError):
        if isinstance(raw, str):
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()
            except ValueError:
                pass
        return time.time()


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

class UWStreamClient:
    """Manages the WebSocket connection and channel subscriptions. Dispatches
    parsed messages to on_net_flow/on_interval_flow/on_market_tide/on_gex --
    wire these directly to the matching TradingRuntime.on_* methods.
    """

    def __init__(self, symbols: list, api_key: Optional[str] = None,
                 on_net_flow: Optional[Callable[[str, NetFlowSample], None]] = None,
                 on_interval_flow: Optional[Callable[[str, IntervalFlowSample], None]] = None,
                 on_market_tide: Optional[Callable[[MarketTideSample], None]] = None,
                 on_gex: Optional[Callable[[str, GexSample], None]] = None,
                 on_connected: Optional[Callable[[bool], None]] = None,
                 ws_url: str = UW_WS_URL,
                 reconnect_backoff_sec: float = 2.0, max_backoff_sec: float = 30.0,
                 on_diagnostic: Optional[Callable[[str, str], None]] = None):
        if websocket is None:  # pragma: no cover
            raise ImportError("The 'websocket-client' package is required: pip install websocket-client")
        self.symbols = list(symbols)
        self._api_key = _load_uw_key(api_key)
        self.on_net_flow = on_net_flow
        self.on_interval_flow = on_interval_flow
        self.on_market_tide = on_market_tide
        self.on_gex = on_gex
        self.on_connected = on_connected
        self.on_diagnostic = on_diagnostic
        self.ws_url = ws_url
        self.reconnect_backoff_sec = reconnect_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._connection_state: Optional[bool] = None
        self._net_baseline: Dict[str, NetFlowSample] = {}

    def _subscribe_channels(self) -> list:
        channels = ["interval_flow", "market_tide", "gex"]
        channels += [f"net_flow:{sym}" for sym in self.symbols]
        return channels

    def _on_message(self, ws, message: str) -> None:
        try:
            raw = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            if self.on_diagnostic:
                self.on_diagnostic("MALFORMED_JSON", "UNKNOWN")
            return
        # A WebSocket handshake says nothing about accepted subscriptions.
        # Record only field names and short protocol status strings, never raw
        # payloads, URLs, or token-bearing server error text.
        if isinstance(raw, list) and len(raw) == 2 and isinstance(raw[0], str):
            channel, payload = raw
        elif isinstance(raw, dict):
            channel = raw.get("channel") or raw.get("type")
            payload = raw.get("data", raw)
        else:
            if self.on_diagnostic:
                self.on_diagnostic("UNHANDLED_ENVELOPE", type(raw).__name__)
            return

        def shape(obj):
            if isinstance(obj, dict):
                return ",".join(sorted(str(k) for k in obj.keys() if
                                         isinstance(k, str) and k.lower() not in
                                         ("token", "authorization", "api_key")))[:180]
            return type(obj).__name__

        def diagnostic_detail():
            safe_channel = channel if isinstance(channel, str) and channel in self._subscribe_channels() else "UNKNOWN"
            status = payload.get("status") if isinstance(payload, dict) else None
            safe_status = status.lower() if isinstance(status, str) and status.lower() in (
                "ok", "error", "subscribed", "unsubscribed", "connected", "rejected", "unauthorized"
            ) else "UNKNOWN"
            return (f"channel={safe_channel};status={safe_status};"
                    f"envelope_keys={shape(raw)};data_keys={shape(payload)}")

        def dropped(reason):
            if self.on_diagnostic:
                self.on_diagnostic(reason, diagnostic_detail())

        if isinstance(payload, dict) and "status" in payload and "response" in payload:
            # Subscription acknowledgements are control frames, not market
            # samples. In particular, an empty market_tide acknowledgement
            # must not become a fresh zero-premium market-tide observation.
            dropped("SUBSCRIPTION_RESPONSE")
            return

        if channel == "market_tide" and self.on_market_tide:
            sample = parse_market_tide_message(payload)
            if sample is not None:
                self.on_market_tide(sample)
            else:
                dropped("UNPARSEABLE_SAMPLE")
        elif channel == "interval_flow" and self.on_interval_flow:
            if isinstance(payload, dict) and payload.get("interval_type") not in (None, "All"):
                return  # do not mix the OTM-only rollup with the all-contract rollup
            result = parse_interval_flow_message(payload)
            if result is not None:
                self.on_interval_flow(*result)
            else:
                dropped("UNPARSEABLE_SAMPLE")
        elif isinstance(channel, str) and channel.startswith("net_flow:") and self.on_net_flow:
            result = parse_net_flow_message(payload)
            if result is not None:
                symbol, cumulative = result
                previous = self._net_baseline.get(symbol)
                self._net_baseline[symbol] = cumulative
                if previous is not None and cumulative.ts > previous.ts:
                    self.on_net_flow(symbol, NetFlowSample(
                        ts=cumulative.ts,
                        dir_delta_flow=cumulative.dir_delta_flow - previous.dir_delta_flow))
            else:
                dropped("UNPARSEABLE_SAMPLE")
        elif channel in ("gex", "gex_strike") and self.on_gex:
            result = parse_gex_message(payload)
            if result is not None:
                self.on_gex(*result)
            else:
                dropped("UNPARSEABLE_SAMPLE")
        else:
            dropped("UNHANDLED_CHANNEL")
        # "flow-alerts" and any unrecognized channel are intentionally not
        # wired to a TradingRuntime ingest method yet -- there is no
        # equivalent SymbolStateStore feed for a discrete alert event in this
        # rebuild's current design.

    def _run(self) -> None:
        backoff = self.reconnect_backoff_sec
        while not self._stop.is_set():
            def notify_connection(connected):
                if self._connection_state != connected:
                    self._connection_state = connected
                    if self.on_connected:
                        self.on_connected(connected)

            def on_open(ws):
                self._net_baseline.clear()
                for channel in self._subscribe_channels():
                    ws.send(json.dumps({"channel": channel, "msg_type": "join"}))
                notify_connection(True)

            def on_close(ws, *args):
                notify_connection(False)

            def on_error(ws, error):
                if self.on_diagnostic:
                    status = getattr(error, "status_code", None)
                    detail = type(error).__name__ + (f":HTTP_{status}" if status else "")
                    self.on_diagnostic("CONNECTION_ERROR", detail)
                notify_connection(False)

            ws = websocket.WebSocketApp(
                _authenticated_url(self.ws_url, self._api_key),
                header=["User-Agent: TradingEngine/1.0"],
                on_open=on_open, on_message=self._on_message,
                on_close=on_close, on_error=on_error,
            )
            ws.run_forever()
            if not self._stop.is_set():
                time.sleep(backoff)
                backoff = min(self.max_backoff_sec, backoff * 2)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
