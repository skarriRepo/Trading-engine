"""Seed complete two-minute bars from Tradier one-minute time-and-sales data.

Historical bars initialize PSAR and momentum but never execute a historical
signal. The first tradable flip must occur on a newly completed live bar.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time
from zoneinfo import ZoneInfo

from .symbol_state import Bar
from .tradier_client import TradierAuthError

ET = ZoneInfo("America/New_York")


def two_minute_bars(rows: list[dict], now: float) -> list[Bar]:
    current = datetime.fromtimestamp(now, ET)
    minutes = {}
    for row in rows:
        try:
            stamped = datetime.fromisoformat(str(row["time"]).replace("Z", "+00:00"))
            stamped = stamped.replace(tzinfo=ET) if stamped.tzinfo is None else stamped.astimezone(ET)
            if stamped.date() != current.date():
                continue
            minute_ts = int(stamped.timestamp()) // 60 * 60
            o, h, l, c = (float(row[field]) for field in ("open", "high", "low", "close"))
            if min(o, h, l, c) <= 0 or h < max(o, c) or l > min(o, c):
                continue
            if minute_ts + 60 > now - 10:
                continue  # do not seed a still-forming or just-closed minute
            minutes[minute_ts] = Bar(minute_ts, o, h, l, c, float(row.get("volume") or 0))
        except (KeyError, ValueError, TypeError, OverflowError):
            continue
    groups = defaultdict(dict)
    for minute_ts, bar in minutes.items():
        groups[minute_ts // 120 * 120][minute_ts] = bar
    out = []
    for bucket, pair in sorted(groups.items()):
        if bucket not in pair or bucket + 60 not in pair or bucket + 120 > now - 10:
            continue
        first, second = pair[bucket], pair[bucket + 60]
        out.append(Bar(bucket, first.open, max(first.high, second.high),
                       min(first.low, second.low), second.close, first.volume + second.volume))
    # Missing minutes are not fabricated. Preserve preceding completed bars:
    # Pine advances over available chart bars rather than resetting at a gap.
    return out


def warmup_symbols(client, runtime, symbols: list[str], now: float | None = None) -> dict[str, int]:
    now = datetime.now(ET).timestamp() if now is None else now
    current = datetime.fromtimestamp(now, ET)
    session_start = datetime.combine(current.date(), time(4, 0), tzinfo=ET)
    if current.weekday() >= 5 or now <= session_start.timestamp():
        return {symbol: 0 for symbol in symbols}
    start = session_start
    end = min(current, datetime.combine(current.date(), time(16, 0), tzinfo=ET))
    counts = {}
    for symbol in symbols:
        try:
            rows = client.minute_candles(symbol, start.strftime("%Y-%m-%d %H:%M"),
                                         end.strftime("%Y-%m-%d %H:%M"))
            bars = two_minute_bars(rows, now)
            runtime.seed_history(symbol, bars)
            counts[symbol] = len(bars)
        except TradierAuthError:
            raise
        except Exception as exc:
            counts[symbol] = 0
            print(f"[warmup] {symbol}: unavailable ({exc}); waiting for live bars", flush=True)
    return counts
