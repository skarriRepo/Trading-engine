"""TradingView transport. No broker calls or TradingView price calculations."""
from __future__ import annotations
import hmac
import queue
import threading
import time
from datetime import datetime
from collections import OrderedDict
from fastapi import APIRouter, HTTPException, Request


def source(value):
    value = value.strip().upper()
    if value not in {"ENGINE", "WEBHOOK"}:
        raise ValueError("Signal source must be ENGINE or WEBHOOK")
    return value


def timestamp(value):
    if isinstance(value, (float, int)):
        return float(value)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp needs timezone")
    return parsed.timestamp()


def normalize(payload, symbols, bar_seconds, max_age, now):
    symbol = str(payload.get("symbol", "")).upper()
    indicator = str(payload.get("indicator", "")).upper()
    event = str(payload.get("signal", "")).upper()
    mapping = {("PSAR", "LONG"): "CALL", ("PSAR", "SHORT"): "PUT",
               ("REVERSAL", "BEARISH_PERFECTED"): "PUT",
               ("REVERSAL", "BULLISH_PERFECTED"): "CALL"}
    if symbol not in symbols or (indicator, event) not in mapping:
        raise ValueError("Unsupported symbol or signal")
    if str(payload.get("timeframe")) != str(int(bar_seconds / 60)):
        raise ValueError("Wrong timeframe")
    bar = timestamp(payload["bar_time"])
    sent = timestamp(payload["sent_at"])
    close = bar + bar_seconds
    if abs(bar / bar_seconds - round(bar / bar_seconds)) > 1e-6:
        raise ValueError("Bar time must be the bar opening timestamp")
    if close > now + 2 or sent > now + 2 or sent < close - 2:
        raise ValueError("Unconfirmed bar or invalid sent time")
    if now - close > max_age or now - sent > max_age:
        raise ValueError("Expired signal")
    return dict(symbol=symbol, indicator=indicator, signal=event,
                direction=mapping[indicator, event], bar_time=bar,
                sent_at=sent, expires_at=close + max_age)


class WebhookReceiver:
    def __init__(self, runtime, symbols, secret, max_age=30):
        if not secret:
            raise ValueError("WEBHOOK_SECRET is required when webhook is enabled")
        if max_age <= 0 or max_age > runtime.bar_seconds:
            raise ValueError("Webhook maximum age must be positive and no longer than one bar")
        self.runtime, self.symbols, self.secret = runtime, set(symbols), secret
        self.max_age = max_age
        self.queue = queue.Queue(maxsize=100)
        self.seen = OrderedDict()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.worker = threading.Thread(target=self.run, daemon=True, name="tradingview-signals")
        self.worker.start()
        self.router = APIRouter()
        self.router.add_api_route("/webhook/tradingview", self.receive, methods=["POST"])

    def audit(self, event, **fields):
        if self.runtime.audit:
            self.runtime.audit.emit(event, **fields)

    async def receive(self, request: Request):
        raw = await request.body()
        if len(raw) > 8192:
            raise HTTPException(413, "Payload too large")
        try:
            import json
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("Expected JSON object")
        except (ValueError, TypeError):
            raise HTTPException(400, "Invalid JSON object")
        # TradingView does not offer custom request headers; body token is an
        # ingress-only secret, never a broker/API credential and never logged.
        supplied = request.headers.get("x-webhook-token") or str(payload.pop("token", ""))
        if not hmac.compare_digest(supplied, self.secret):
            self.audit("WEBHOOK_REJECTED", reason="UNAUTHORIZED")
            raise HTTPException(401, "Unauthorized")
        try:
            signal = normalize(payload, self.symbols, self.runtime.bar_seconds, self.max_age, time.time())
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            self.audit("WEBHOOK_REJECTED", reason=str(exc),
                       **{k: str(payload.get(k, ""))[:80] for k in
                          ("symbol", "indicator", "signal", "timeframe", "bar_time", "sent_at")})
            raise HTTPException(422, str(exc))
        key = (signal["symbol"], signal["indicator"], signal["bar_time"])
        with self.lock:
            if key in self.seen:
                self.audit("WEBHOOK_DUPLICATE", symbol=signal["symbol"], indicator=signal["indicator"])
                return {"status": "duplicate"}
            try:
                self.queue.put_nowait(signal)
            except queue.Full:
                raise HTTPException(503, "Signal queue full")
            self.seen[key] = True
            while len(self.seen) > 5000:
                self.seen.popitem(last=False)
        self.audit("WEBHOOK_ACCEPTED", **signal)
        return {"status": "queued"}

    def run(self):
        while not self.stop.is_set():
            try:
                signal = self.queue.get(timeout=.2)
            except queue.Empty:
                continue
            try:
                self.runtime.on_webhook_signal(signal)
            except Exception as exc:
                self.audit("WEBHOOK_PROCESSING_ERROR", symbol=signal["symbol"], error=type(exc).__name__)
            finally:
                self.queue.task_done()

    def close(self):
        self.stop.set()
        self.worker.join(timeout=2)
