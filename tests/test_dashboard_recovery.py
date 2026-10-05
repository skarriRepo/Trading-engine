import json,tempfile,unittest
from pathlib import Path
from datetime import datetime
from trading_engine.audit_log import ET
from trading_engine.dashboard_store import DashboardStore
from trading_engine.dashboard_recovery import restore_today

class RecoveryTests(unittest.TestCase):
    def test_today_history_restores_without_positions_or_fresh_market_data(self):
        today='2026-10-05';now=datetime(2026,10,5,12,tzinfo=ET).timestamp()
        rows=[dict(event='SIGNAL_DECISION',symbol='AAPL',direction='CALL',action='TAKE',source='PSAR_FLIP',bar_ts=1,reasons=['ENTRY_AUTHORIZED']),
              dict(event='BROKER_ENTRY_FILLED',symbol='AAPL',trade_id='t'),
              dict(event='POSITION_OBSERVATION',symbol='AAPL',trade_id='t',peak_gain_pct=20),
              dict(event='ORDER_SUBMITTED',symbol='AAPL',side='buy_to_open',order_id='1'),
              dict(event='ORDER_STATUS',symbol='AAPL',order_id='1',status='FILLED')]
        for r in rows:r['recorded_at_et']=today+'T10:00:00-04:00'
        rows.append({**rows[0],'symbol':'OLD','recorded_at_et':'2026-10-02T10:00:00-04:00'})
        closed=[dict(day_et=today,trade_id='t',symbol='AAPL',direction='CALL',live_entry_ask=1,live_exit_bid=1.1,entry_fill=5,exit_fill=4,quantity=1,closed_at_et=today+'T10:05:00-04:00',exit_reasons=['EXIT'])]
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'log';p.write_text(''.join(json.dumps(r)+'\n' for r in rows)+'broken\n')
            store=DashboardStore();restore_today(store,p,closed,now)
            self.assertEqual(len(store.signal_view()),1)
            self.assertEqual(store.active_view(),[])
            self.assertEqual(store.scan_view()[0]['price_state'],'NOT_READY')
            self.assertEqual(store.closed_view()[0]['live_quote_pnl_usd'],10)
            self.assertEqual(store.order_history_view()[0]['status'],'FILLED')
            restore_today(store,p,closed,now)
            self.assertEqual(len(store.closed_view()),1)
            self.assertEqual(len(store.signal_view()),1)
    def test_missing_quotes_and_entry_time_remain_unknown(self):
        now=datetime(2026,10,5,12,tzinfo=ET).timestamp();store=DashboardStore()
        r=dict(day_et='2026-10-05',trade_id='t',symbol='AAPL',direction='CALL',entry_fill=2,exit_fill=3,closed_at_et='2026-10-05T10:05:00-04:00')
        restore_today(store,'/nonexistent',[r],now)
        row=store.closed_view()[0]
        self.assertIsNone(row['live_quote_pnl_usd']);self.assertIsNone(row['duration_sec']);self.assertIsNone(row['final_gain_pct'])
