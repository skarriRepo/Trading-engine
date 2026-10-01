"""Tradier WebSocket streaming client for real-time underlying ticks and
option quotes.

Built against Tradier's documented streaming API
(https://documentation.tradier.com/brokerage-api/streaming/get-markets-events):
POST /v1/markets/events/session to obtain a session, connect the returned
WebSocket URL, send a JSON subscribe frame, then receive newline-delimited
JSON messages of type "trade", "quote", "summary", or "timesale".

DOCUMENTED-BUT-NOT-LIVE-TESTED, same caveat as tradier_client.py: no real
credentials are available in this environment. Message PARSING
(parse_stream_message) is deliberately a pure function, fully unit-tested
against realistic fixture messages matching the documented schema
(tests/test_tradier_stream.py) -- the connection/reconnection handling below
it cannot be tested the same way and needs a real smoke test against
Tradier's sandbox before being trusted live.

On connect/disconnect, this calls SymbolStateStore.set_connected() (via the
runtime it's wired to) so every live feed correctly reads NOT_READY the
moment the socket drops, rather than silently serving stale last-known
values -- the same discipline the state store itself was built around.
"""
from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

try:
    import websocket  # from the `websocket-client` package
except ImportError:  # pragma: no cover
    websocket = None

from .tradier_client import TradierRestClient, TradierAuthError

STREAM_ENDPOINT_PATH = "/markets/events/session"
OCC_OPTION_SYMBOL = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$")


@dataclass(frozen=True)
class ParsedTick:
    symbol: str
    ts: float
    price: float
    volume: float = 0.0


@dataclass(frozen=True)
class ParsedQuote:
    symbol: str
    ts: float
    bid: float
    ask: float


def _to_ws_scheme(url: str) -> str:
    """Normalizes a session URL to a ws:// or wss:// scheme before handing it
    to websocket-client, which rejects anything else outright (observed
    live: a real Tradier session response returned an https:// URL, which
    websocket.WebSocketApp's own parse_url() raises ValueError on --
    "scheme https is invalid" -- rather than silently failing). Tradier
    documents both HTTP and WebSocket streaming as distinct products; this
    client is a WebSocket client, so any http(s) scheme in a session
    response is translated to its ws(s) equivalent rather than trusted
    as-is.
    """
    if url.startswith("https://"):
        return "wss://" + url[len("https://"):]
    if url.startswith("http://"):
        return "ws://" + url[len("http://"):]
    return url


def _epoch_sec_from_ms(raw: Any) -> float:
    try:
        return float(raw) / 1000.0
    except (TypeError, ValueError):
        return 0.0  # unknown exchange time must never masquerade as a live quote


def parse_stream_message(raw: Dict[str, Any]):
    """Parses one decoded JSON message from Tradier's stream into a
    ParsedTick, a ParsedQuote, or None (an unrecognized/unsupported type --
    "summary" messages and anything malformed return None rather than
    raising, since a bad or new message type must not kill the stream).
    """
    msg_type = raw.get("type")
    symbol = raw.get("symbol")
    if not symbol:
        return None

    if msg_type in ("trade", "timesale"):
        # A subscribed option also emits trades. Its premium must never be
        # ingested as an underlying price/bar or evaluated for PSAR entries.
        if OCC_OPTION_SYMBOL.fullmatch(symbol):
            return None
        try:
            price = float(raw["price"] if "price" in raw else raw["last"])
        except (KeyError, TypeError, ValueError):
            return None
        volume = 0.0
        try:
            volume = float(raw.get("size") or 0.0)
        except (TypeError, ValueError):
            pass
        ts = _epoch_sec_from_ms(raw.get("date"))
        if ts <= 0:
            return None
        return ParsedTick(symbol=symbol, ts=ts, price=price, volume=volume)

    if msg_type == "quote":
        try:
            bid = float(raw.get("bid") or 0.0)
            ask = float(raw.get("ask") or 0.0)
        except (TypeError, ValueError):
            return None
        ts = _epoch_sec_from_ms(raw.get("biddate") or raw.get("askdate"))
        if ts <= 0:
            return None
        return ParsedQuote(symbol=symbol, ts=ts, bid=bid, ask=ask)

    return None  # "summary" and any other/unknown type -- not needed here


class TradierStreamClient:
    """Manages the session handshake, the WebSocket connection, and
    reconnection with backoff. Dispatches parsed messages to on_tick/on_quote
    callbacks -- wire these directly to TradingRuntime.on_underlying_tick and
    TradingRuntime.on_option_quote (option quotes only for symbols with an
    open position; equity ticks for the whole watched universe).
    """

    def __init__(self, symbols: list, rest_client: TradierRestClient,
                 on_tick: Callable[[str, float, float, float], None],
                 on_quote: Optional[Callable[[str, float, float, float], None]] = None,
                 on_connected: Optional[Callable[[bool], None]] = None,
                 reconnect_backoff_sec: float = 2.0, max_backoff_sec: float = 30.0,
                 on_diagnostic: Optional[Callable[[str, str], None]] = None):
        if websocket is None:  # pragma: no cover
            raise ImportError("The 'websocket-client' package is required: pip install websocket-client")
        self._symbols_lock = threading.Lock()
        self.symbols = list(symbols)
        self.rest_client = rest_client
        self.on_tick = on_tick
        self.on_quote = on_quote
        self.on_connected = on_connected
        self.on_diagnostic = on_diagnostic
        self.reconnect_backoff_sec = reconnect_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws: Optional["websocket.WebSocketApp"] = None  # the currently-open connection, if any
        self._session_id: Optional[str] = None

    def _get_session(self) -> Dict[str, str]:
        resp = self.rest_client._session.post(
            f"{self.rest_client.config.base_url}{STREAM_ENDPOINT_PATH}",
            headers=self.rest_client._headers(), timeout=self.rest_client.config.timeout_sec,
        )
        if resp.status_code == 401:
            raise TradierAuthError("Tradier rejected the configured token (401) opening a stream session.")
        resp.raise_for_status()
        stream = resp.json().get("stream") or {}
        return {"url": _to_ws_scheme(stream["url"]), "sessionid": stream["sessionid"]}

    def _on_message(self, ws, message: str) -> None:
        for line in message.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                if self.on_diagnostic:
                    self.on_diagnostic("MALFORMED_JSON", "UNKNOWN")
                continue
            parsed = parse_stream_message(raw)
            if isinstance(parsed, ParsedTick):
                self.on_tick(parsed.symbol, parsed.ts, parsed.price, parsed.volume)
            elif isinstance(parsed, ParsedQuote) and self.on_quote is not None:
                self.on_quote(parsed.symbol, parsed.ts, parsed.bid, parsed.ask)
            elif parsed is None and self.on_diagnostic and not (
                    isinstance(raw, dict) and (raw.get("type") == "summary" or
                    OCC_OPTION_SYMBOL.fullmatch(str(raw.get("symbol", "")))
                    and raw.get("type") in ("trade", "timesale"))):
                self.on_diagnostic("UNPARSEABLE_OR_UNHANDLED_EVENT",
                                   str(raw.get("type", "UNKNOWN")) if isinstance(raw, dict) else "UNKNOWN")

    def _run(self) -> None:
        backoff = self.reconnect_backoff_sec
        while not self._stop.is_set():
            try:
                session = self._get_session()
            except TradierAuthError:
                raise  # a bad token should not retry silently forever
            except Exception as exc:
                if self.on_diagnostic:
                    self.on_diagnostic("SESSION_OPEN_ERROR", type(exc).__name__)
                time.sleep(backoff)
                backoff = min(self.max_backoff_sec, backoff * 2)
                continue

            def on_open(ws):
                self._send_subscribe(ws, session["sessionid"])
                if self.on_connected:
                    self.on_connected(True)

            def on_close(ws, *args):
                self._ws = None
                if self.on_connected:
                    self.on_connected(False)

            ws = websocket.WebSocketApp(session["url"], on_open=on_open,
                                         on_message=self._on_message, on_close=on_close)
            self._ws = ws
            self._session_id = session["sessionid"]
            backoff = self.reconnect_backoff_sec  # reset after a session was successfully obtained
            ws.run_forever()  # blocks until the connection drops or self.close() is called
            self._ws = None
            if not self._stop.is_set():
                time.sleep(backoff)
                backoff = min(self.max_backoff_sec, backoff * 2)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()

    # -- dynamic subscription: called from any thread as positions open/close --

    def _send_subscribe(self, ws, session_id: str) -> None:
        with self._symbols_lock:
            symbols = list(self.symbols)
        ws.send(json.dumps({"symbols": symbols, "filter": ["trade", "timesale", "quote"],
                            "sessionid": session_id, "linebreak": True}))
        if self.on_diagnostic:
            self.on_diagnostic("SUBSCRIPTION_SENT", ",".join(symbols))

    def add_symbol(self, symbol: str) -> None:
        """Adds a symbol (equity ticker or a specific option's own OCC
        symbol) to the live subscription and re-sends the subscribe frame on
        the current connection, if one is open. If no connection is open
        yet, the symbol is queued into self.symbols and picked up by the
        next connect. Tradier's documented streaming protocol takes a full
        subscribe frame per message, not an incremental add -- so this
        resends the complete updated list rather than attempting a partial
        update, which is the behavior verified in tests/test_tradier_stream.py.
        """
        with self._symbols_lock:
            if symbol in self.symbols:
                return
            self.symbols.append(symbol)
        ws, session_id = self._ws, self._session_id
        if ws is not None and session_id is not None:
            self._send_subscribe(ws, session_id)

    def remove_symbol(self, symbol: str) -> None:
        with self._symbols_lock:
            if symbol not in self.symbols:
                return
            self.symbols.remove(symbol)
        ws, session_id = self._ws, self._session_id
        if ws is not None and session_id is not None:
            self._send_subscribe(ws, session_id)
