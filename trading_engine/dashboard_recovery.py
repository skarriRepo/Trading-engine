"""Display-only ET-day recovery. Never replay signals into the runtime."""
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from .audit_log import ET
from .dashboard_store import ClosedTrade
from .entry_pipeline import EntryDecision, UWConfluenceRead


def restore_today(store, log_path, closed_records=(), now=None):
    now = datetime.now(ET).timestamp() if now is None else now
    day = datetime.fromtimestamp(now, ET).date().isoformat()
    entries, observations, latest = {}, {}, {}
    malformed = 0
    path = Path(log_path)
    if path.exists():
        with path.open(encoding='utf-8') as stream:
            for line in stream:
                try:
                    r = json.loads(line)
                    stamp = datetime.fromisoformat(r['recorded_at_et']).timestamp()
                    if datetime.fromtimestamp(stamp, ET).date().isoformat() != day:
                        continue
                    event, symbol = r.get('event'), r.get('symbol')
                    store.record_order_event(r)
                    if event == 'SIGNAL_DECISION':
                        decision = EntryDecision(r['action'], symbol, r['direction'],
                            tuple(r.get('reasons', [])), r.get('momentum', 'NOT_READY'),
                            UWConfluenceRead(r.get('uw_supports', False), False,
                                             r.get('uw_ready', False), 0, 2), None)
                        store.record_signal(symbol, r['direction'], decision, stamp,
                                            r.get('source', 'PSAR_FLIP'), r.get('bar_ts'))
                        latest[symbol] = (decision, stamp)
                    elif event == 'REVERSAL_SIGNAL':
                        store.record_reversal(symbol, SimpleNamespace(bar_ts=r.get('market_ts'),
                            kind=r.get('trigger'), direction=r.get('direction'),
                            perfected=r.get('perfected'), count=r.get('count'), value=r.get('level'),
                            visible=r.get('visible'), alert=r.get('pine_alert'), detail=r.get('detail')))
                    elif event == 'BROKER_ENTRY_FILLED':
                        entries[r['trade_id']] = stamp
                    elif event in ('POSITION_OBSERVATION', 'EXIT_DECISION') and r.get('trade_id'):
                        observations[r['trade_id']] = r
                except (ValueError, TypeError, KeyError):
                    malformed += 1
    # Restore only decisions; cached market data must never appear fresh.
    for symbol, (decision, stamp) in latest.items():
        snap = SimpleNamespace(symbol=symbol, price=None, price_state='NOT_READY',
            connected=False, bar_state='NOT_READY', flow_30s='NOT_READY', flow_1m='NOT_READY',
            flow_3m='NOT_READY', flow_5m='NOT_READY', net_flow_state='NOT_READY',
            gamma_path='N/A', gex_state='NOT_READY')
        store.record_scan(snap, decision, stamp)
    with store._lock:
        known = {t.trade_id for t in store._closed}
        for r in closed_records:
            if r.get('day_et') != day or r.get('trade_id') in known:
                continue
            tid = r.get('trade_id')
            if not tid or not r.get('closed_at_et'):
                continue
            at = datetime.fromisoformat(r['closed_at_et']).timestamp()
            obs = observations.get(tid, {})
            ask, bid = float(r.get('live_entry_ask') or 0), float(r.get('live_exit_bid') or 0)
            gain = (bid / ask - 1) * 100 if ask > 0 and bid > 0 else None
            opened = entries.get(tid) or obs.get('opened_ts')
            store._closed.append(ClosedTrade(tid, r['symbol'], r['direction'], ask, bid,
                opened, at, obs.get('peak_gain_pct'), gain, tuple(r.get('exit_reasons', [])),
                int(r.get('quantity', 1)), True, float(r.get('entry_fill') or 0),
                float(r.get('exit_fill') or 0)))
            known.add(tid)
    return dict(closed=len(store.closed_view()), signals=len(store.signal_view()),
                reversals=len(store.reversal_view()), orders=len(store.order_history_view()),
                malformed_records=malformed)
