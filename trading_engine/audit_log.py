"""Append-only ET daily audit and reproducible sandbox EOD report."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import json
import os
from pathlib import Path
import threading
import time
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


class AuditLog:
    def __init__(self, directory: str = "logs"):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._last_report = 0.0
        self.observers = []

    def emit(self, event: str, *, durable: bool = True, **fields) -> None:
        now = time.time()
        day = datetime.fromtimestamp(now, ET).date().isoformat()
        row = {"recorded_at_et": datetime.fromtimestamp(now, ET).isoformat(),
               "day_et": day, "event": event, **fields}
        with self._lock:
            with (self.directory / f"engine_{day}.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, default=str, allow_nan=False) + "\n")
                f.flush()
                if durable:
                    os.fsync(f.fileno())

        for observer in self.observers:
            observer(row)

    def report(self, day: str, journal_path: str = "sandbox_orders.json") -> Path:
        # Derive the report afresh from durable records so it can be regenerated.
        datetime.strptime(day, "%Y-%m-%d")
        events = []
        source = self.directory / f"engine_{day}.jsonl"
        with self._lock:
            if source.exists():
                for line in source.read_text(encoding="utf-8").splitlines():
                    try:
                        events.append(json.loads(line))
                    except (ValueError, TypeError):
                        continue  # tolerate a truncated final line after a crash
            journal = Path(journal_path)
            data = json.loads(journal.read_text(encoding="utf-8")) if journal.exists() else {}
            closed = [r for r in data.get("closed", []) if r.get("day_et") == day]
            by_symbol = defaultdict(lambda: {"trades": 0, "known_live_quote_trades": 0,
                                            "unknown_live_quote_trades": 0,
                                            "live_quote_pnl_usd": 0.0})
            for trade in closed:
                bucket = by_symbol[trade["symbol"]]
                bucket["trades"] += 1
                if trade.get("live_pnl") is None:
                    bucket["unknown_live_quote_trades"] += 1
                else:
                    bucket["known_live_quote_trades"] += 1
                    bucket["live_quote_pnl_usd"] += float(trade["live_pnl"])
            signal_events = [r for r in events if r.get("event") == "SIGNAL_DECISION"]
            reason_counts = Counter(reason for r in signal_events for reason in r.get("reasons", []))
            signal_by_symbol = defaultdict(lambda: {"actions": Counter(), "reasons": Counter()})
            for signal in signal_events:
                bucket = signal_by_symbol[signal.get("symbol", "UNKNOWN")]
                bucket["actions"][signal.get("action", "UNKNOWN")] += 1
                bucket["reasons"].update(signal.get("reasons", []))
            trade_observations = []
            for trade in closed:
                quotes = [r for r in events if r.get("event") == "OPTION_QUOTE"
                          and r.get("trade_id") == trade.get("trade_id")]
                accepted_quotes = [r for r in quotes if r.get("accepted", True)]
                observations = [r for r in events if r.get("event") == "POSITION_OBSERVATION"
                                and r.get("trade_id") == trade.get("trade_id")]
                uw_samples = [r for r in events if r.get("event") == "UW_POSITION_SAMPLE"
                              and r.get("trade_id") == trade.get("trade_id")]
                exit_decisions = [r for r in events if r.get("event") == "EXIT_DECISION"
                                  and r.get("trade_id") == trade.get("trade_id")]
                uw_health = [r for r in events if r.get("event") == "UW_FEED_HEALTH"
                             and r.get("trade_id") == trade.get("trade_id")]
                post_exit_quotes = [r for r in events if r.get("event") == "POST_EXIT_OPTION_QUOTE"
                                    and r.get("trade_id") == trade.get("trade_id")]
                subscriptions = [r for r in events if r.get("event") == "OPTION_SUBSCRIPTION_REQUESTED"
                                 and r.get("trade_id") == trade.get("trade_id")]
                context_events = sorted(observations + uw_samples,
                                        key=lambda r: r.get("recorded_at_et", ""))
                opposite = next((r for r in context_events if r.get("uw_opposite_supports")), None)
                shadow = next((r for r in context_events if r.get("uw_shadow_exit")), None)
                warning = next((r for r in observations if r.get("decision_state") == "PROFIT_LOCK"), None)
                exit_observation = next((r for r in reversed(observations)
                                         if r.get("decision_state") == "EXIT_PENDING"), None)
                trigger = trade.get("exit_trigger") or {}
                fill_at = trade.get("closed_at_et")
                submitted_at = trade.get("exit_submitted_at_et")
                latency = None
                if submitted_at and fill_at:
                    latency = round((datetime.fromisoformat(fill_at) -
                                     datetime.fromisoformat(submitted_at)).total_seconds(), 3)
                trigger_bid = trigger.get("bid")
                peak_bid = trigger.get("peak_bid")
                fill_price = trade.get("exit_fill")
                trade_observations.append({
                    "symbol": trade.get("symbol"), "occ_symbol": trade.get("occ_symbol"),
                    "exit_order_id": trade.get("exit_order_id"),
                    "complete": bool(accepted_quotes) and trigger_bid is not None and peak_bid is not None and latency is not None,
                    "quote_history_available": bool(accepted_quotes),
                    "peak_bid": peak_bid, "trigger_bid": trigger_bid,
                    "trigger_ask": trigger.get("ask"), "trigger_quote_market_ts": trigger.get("market_ts"),
                    "trigger_quote_age_sec": trigger.get("quote_age_sec"),
                    "latest_quote_at_reconciliation": trade.get("fill_observation"),
                    "exit_fill": fill_price, "submission_to_confirmed_fill_sec": latency,
                    "option_quotes_recorded": len(quotes),
                    "option_quotes_accepted": len(accepted_quotes),
                    "post_exit_quotes_recorded": len(post_exit_quotes),
                    "post_exit_peak_bid": max((r["bid"] for r in post_exit_quotes), default=None),
                    "post_exit_last_quote": {k: post_exit_quotes[-1].get(k) for k in
                        ("recorded_at_et", "market_ts", "bid", "ask", "spread")}
                        if post_exit_quotes else None,
                    "post_exit_peak_ask_to_bid_usd_per_contract":
                        round((max(r["bid"] for r in post_exit_quotes) - float(trade["live_entry_ask"]))*100, 2)
                        if post_exit_quotes and trade.get("live_entry_ask") else None,
                    "position_observations_recorded": len(observations),
                    "uw_samples_recorded": len(uw_samples),
                    "exit_decisions_recorded": len(exit_decisions),
                    "uw_feed_health_transitions": uw_health,
                    "option_subscription_events": subscriptions,
                    "uw_sample_types": dict(Counter(r.get("sample_type") for r in uw_samples)),
                    "evidence_available": {
                        "live_quote_history": bool(accepted_quotes),
                        "per_quote_decisions": any(r.get("observation_source") == "OPTION_QUOTE"
                                                   for r in observations),
                        "uw_sample_history": bool(uw_samples),
                        "exit_context": exit_observation is not None,
                        "trigger_quote": trigger_bid is not None,
                        "broker_fill": fill_price is not None,
                    },
                    "first_opposing_uw": opposite,
                    "first_uw_shadow_exit": shadow,
                    "first_profit_lock_warning": warning,
                    "exit_decision_context": exit_observation,
                    "first_quote": {k: accepted_quotes[0].get(k) for k in
                        ("recorded_at_et", "market_ts", "bid", "ask", "quote_age_sec")}
                        if accepted_quotes else None,
                    "peak_quote": {k: max(accepted_quotes, key=lambda q: q.get("bid") or 0).get(k) for k in
                        ("recorded_at_et", "market_ts", "bid", "ask", "quote_age_sec")}
                        if accepted_quotes else None,
                    "last_quote": {k: accepted_quotes[-1].get(k) for k in
                        ("recorded_at_et", "market_ts", "bid", "ask", "quote_age_sec")}
                        if accepted_quotes else None,
                    "peak_to_trigger_usd_per_contract": round((peak_bid-trigger_bid)*100, 2)
                        if peak_bid is not None and trigger_bid is not None else None,
                    "trigger_bid_to_fill_usd_per_contract": round((trigger_bid-fill_price)*100, 2)
                        if trigger_bid is not None and fill_price is not None else None,
                    "note": "Confirmed fill time is reconciliation observation, not exchange execution time. Bid-to-fill change cannot alone distinguish spread, price movement, or sandbox simulation."
                })
            generated = datetime.now(ET)
            missing_by_trade = {
                r["symbol"] + ":" + str(r.get("exit_order_id")): [k for k, ok in r["evidence_available"].items() if not ok]
                for r in trade_observations if not all(r["evidence_available"].values())
            }
            open_context = {}
            for symbol, record in data.get("open", {}).items():
                trade_id = record.get("tag")
                matching = [r for r in events if r.get("trade_id") == trade_id
                            and r.get("event") in ("POSITION_OBSERVATION", "UW_POSITION_SAMPLE")]
                open_context[symbol] = {
                    "trade_id": trade_id, "observations_recorded": len(matching),
                    "last_observation": matching[-1] if matching else None,
                    "missing_observations": not bool(matching),
                }
            result = {
                "day_et": day,
                "generated_at_et": generated.isoformat(),
                "report_status": "INTRADAY_SNAPSHOT" if generated.date().isoformat() == day
                    and (generated.hour, generated.minute) < (16, 5) else "POST_SESSION_SNAPSHOT",
                "last_logged_event_at_et": events[-1].get("recorded_at_et") if events else None,
                "session_complete": (generated.date().isoformat() > day or
                                     (generated.date().isoformat() == day and
                                      (generated.hour, generated.minute) >= (16, 5))),
                "source": {"events": str(source), "journal": str(journal)},
                "event_counts": dict(Counter(r.get("event") for r in events)),
                "evidence_coverage": {"missing_by_closed_trade": missing_by_trade,
                                      "open_positions": open_context},
                "signals": {"decisions": dict(Counter(r.get("action") for r in signal_events)),
                            "sources": dict(Counter(r.get("source") for r in signal_events)),
                            "reasons": dict(reason_counts),
                            "by_symbol": {s: {"actions": dict(b["actions"]), "reasons": dict(b["reasons"])}
                                          for s, b in sorted(signal_by_symbol.items())}},
                "live_quotes": {"closed_trades": len(closed),
                           "known_outcomes": sum(r.get("live_pnl") is not None for r in closed),
                           "unknown_outcomes": sum(r.get("live_pnl") is None for r in closed),
                           "ask_to_exit_bid_usd": round(sum(float(r["live_pnl"]) for r in closed
                                                        if r.get("live_pnl") is not None), 2),
                           "by_symbol": {s: {**b, "live_quote_pnl_usd": round(b["live_quote_pnl_usd"], 2)}
                                         for s, b in sorted(by_symbol.items())}},
                "broker": {"closed_trades": len(closed),
                           "exit_reasons": dict(Counter(reason for r in closed
                               for reason in r.get("exit_reasons", []))),
                           "closed_trade_details": closed,
                           "trade_observations": trade_observations,
                           "pending_at_report": list(data.get("pending", {})),
                           "open_at_report": list(data.get("open", {})),
                           "blocked_at_report": data.get("blocked", {})},
                "notes": "Strategy outcome uses live entry ask minus live exit trigger bid for long options, reported as ask-to-bid quote change, not realized execution P&L. Unknown quotes are excluded and counted. Sandbox receipts exist only to confirm and reconcile order state; their prices never drive analysis or trading decisions. Missing evidence is explicit per trade."
            }
            target = self.directory / f"eod_{day}.json"
            with target.open("w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            return target

    def maybe_report(self, journal_path: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        eastern = datetime.fromtimestamp(now, ET)
        if eastern.weekday() >= 5 or not ((9, 30) <= (eastern.hour, eastern.minute) < (17, 0)):
            return
        if now - self._last_report < 300:
            return
        self.report(eastern.date().isoformat(), journal_path)
        self._last_report = now
