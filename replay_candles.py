"""Replay the current engine's bar-close decisions on archived one-minute candles.

Usage: python historical_candle_replay.py PATH [PATH ...] --out replay_results.json
No option quotes are synthesized; TAKE means entry filter authorization only.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT if (ROOT / "trading_engine").is_dir() else ROOT / "new_engine"))
from trading_engine.symbol_state import Bar, completed_bars
from trading_engine.entry_pipeline import compute_psar, momentum_from_bars, momentum_ok
from trading_engine.exit_pipeline import structure_from_bars, structure_ok
from trading_engine.runtime import TradingRuntime

ET = ZoneInfo("America/New_York")


def bars_2m(rows):
    minutes = {int(r["ts"]): r for r in rows if min(r["open"], r["high"], r["low"], r["close"]) > 0}
    groups = defaultdict(dict)
    for ts, row in minutes.items():
        groups[ts // 120 * 120][ts] = row
    result = []
    for bucket, pair in sorted(groups.items()):
        if bucket not in pair or bucket+60 not in pair:
            continue
        a, b = pair[bucket], pair[bucket+60]
        result.append(Bar(bucket, a["open"], max(a["high"], b["high"]),
                          min(a["low"], b["low"]), b["close"], a.get("volume", 0)+b.get("volume", 0)))
    return result


def replay(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    details = []
    rejected_minutes = 0
    per_symbol = {}
    first_takes = []
    for symbol, rows in sorted(data.items()):
        bars = bars_2m(rows)
        rejected_minutes += len(rows) - len(bars)*2
        runtime = TradingRuntime(option_entry_price_provider=lambda *args: None)
        segment_start = 0
        last = None
        seen_candidates = set()
        last_flip_ts = None
        segment_bars = []
        for bar in bars:
            if last is not None and bar.ts - last != 120:
                runtime = TradingRuntime(option_entry_price_provider=lambda *args: None)
                segment_start = bar.ts
                segment_bars = []
                seen_candidates = set()
                last_flip_ts = None
            elif last is None:
                segment_start = bar.ts
            last = bar.ts
            segment_bars.append(bar)
            now = bar.ts + 120
            runtime.store.ingest_bar(symbol, bar)
            runtime.store.ingest_price(symbol, now, bar.close)
            previous_count = len(runtime.dashboard.signal_view())
            runtime._evaluate_symbol(symbol, now)
            signals = runtime.dashboard.signal_view()
            for signal in reversed(signals[:len(signals)-previous_count]):
                hour = datetime.fromtimestamp(now, ET)
                if signal["source"] == "PSAR_FLIP":
                    last_flip_ts = signal["bar_ts"]
                item = {"dataset": Path(path).name, "symbol": symbol,
                                "time_et": hour.isoformat(), "bar_ts": signal.get("bar_ts"),
                                "direction": signal["direction"], "source": signal["source"],
                                "action": signal["action"], "reasons": signal["reasons"],
                                "momentum": signal["momentum"],
                                "underlying_close": bar.close,
                                "entry_session": hour.weekday()<5 and (9,30)<=(hour.hour,hour.minute)<(15,30)}
                details.append(item)
                if item["action"] == "TAKE":
                    candidate_key = (segment_start, last_flip_ts, item["direction"])
                    if candidate_key not in seen_candidates:
                        seen_candidates.add(candidate_key)
                        first_takes.append({**item, "candidate_flip_bar_ts": last_flip_ts,
                                            "segment_start_ts": segment_start})
        per_symbol[symbol] = {"minute_rows": len(rows), "complete_2m_bars": len(bars)}
    counts = Counter((d["source"], d["action"]) for d in details)
    qualified = first_takes
    # Forward underlying moves describe directional price action only; option
    # fills, greeks, spreads, and option-premium exits cannot be recovered.
    for item in qualified:
        rows = data[item["symbol"]]
        future = sorted((r for r in rows if r["ts"] >= item["bar_ts"]+120), key=lambda r:r["ts"])
        entry = item["underlying_close"]
        sign = 1 if item["direction"] == "CALL" else -1
        item["underlying_forward_pct"] = {}
        for minutes in (2, 5, 10):
            at = item["bar_ts"]+120+minutes*60
            next_rows = [r for r in future if r["ts"]+60 >= at]
            if next_rows and next_rows[0]["ts"]+60 <= at+60:
                item["underlying_forward_pct"][str(minutes)] = round(sign*(next_rows[0]["close"]/entry-1)*100, 4)
        # Independent price-action exit: confirmed opposite PSAR only. The
        # bare-proven and giveback exits require option peak/bid, thesis flow
        # requires UW; neither exists in this archive.
        replay_bars = bars_2m(rows)
        prior = [b for b in replay_bars if item["segment_start_ts"] <= b.ts <= item["bar_ts"]]
        for next_bar in (b for b in replay_bars if b.ts > item["bar_ts"]):
            et = datetime.fromtimestamp(next_bar.ts+120, ET)
            if (et.hour,et.minute)>=(15,50):
                item["price_exit"] = {"reason":"EOD_FORCE_CLOSE", "time_et":et.isoformat()}
                break
            if prior and next_bar.ts-prior[-1].ts!=120:
                break
            prior.append(next_bar)
            points = compute_psar(tuple(prior[-200:]))
            if not points or points[-1].direction == item["direction"]:
                continue
            other = "PUT" if item["direction"]=="CALL" else "CALL"
            if (structure_ok(other,structure_from_bars(tuple(prior[-6:]))) or
                momentum_ok(other,momentum_from_bars(tuple(prior[-200:])))):
                item["price_exit"]={"reason":"OPPOSITE_PSAR_CONFIRMED_BY_PRICE",
                                    "time_et":et.isoformat(),
                                    "directional_underlying_pct":round(sign*(next_bar.close/entry-1)*100,4)}
                break
    return {"path": str(path), "symbols": len(data), "minutes": sum(map(len,data.values())),
            "unpaired_minute_rows": rejected_minutes, "per_symbol": per_symbol,
            "decision_counts": {f"{source}:{action}": count for (source,action),count in sorted(counts.items())},
            "unique_take_candidates": len(qualified),
            "take_in_session": sum(d["entry_session"] for d in qualified),
            "take_outside_session": sum(not d["entry_session"] for d in qualified),
            "take_signals": qualified, "all_decisions": details}


def conflicts(path_a, path_b):
    a = json.loads(Path(path_a).read_text()); b = json.loads(Path(path_b).read_text())
    overlap = changed = 0
    by_symbol = {}
    for sym in sorted(set(a)&set(b)):
        x = {int(r["ts"]): r for r in a[sym]}; y = {int(r["ts"]): r for r in b[sym]}
        shared = x.keys()&y.keys()
        dif = sum(x[t] != y[t] for t in shared)
        overlap += len(shared); changed += dif
        by_symbol[sym] = dif
    return {"overlapping_minutes": overlap, "different_candles": changed,
            "different_by_symbol": by_symbol}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--out", default="historical_replay_results.json")
    args = parser.parse_args()
    result = {"method": "Current TradingRuntime at complete 2-minute bar close, no UW, no option quotes; option provider returns None. Separate captures are not merged.",
              "datasets": [replay(p) for p in args.paths]}
    if len(args.paths) >= 2:
        result["overlap_last_two"] = conflicts(args.paths[-2], args.paths[-1])
    Path(args.out).write_text(json.dumps(result, indent=2), encoding="utf-8")
    for d in result["datasets"]:
        print(Path(d["path"]).name,d["decision_counts"],"TAKE in session",d["take_in_session"],"outside",d["take_outside_session"])
    print(args.out)
