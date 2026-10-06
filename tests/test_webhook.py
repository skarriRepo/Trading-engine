import asyncio
import time
import unittest
from types import SimpleNamespace
from fastapi import HTTPException
from starlette.requests import Request
from trading_engine.webhook import normalize, source, WebhookReceiver

class WebhookTests(unittest.TestCase):
    def payload(self, now):
        bar = int(now // 120) * 120 - 120
        return dict(symbol='NVDA', indicator='REVERSAL', signal='BULLISH_PERFECTED',
                    timeframe='2', bar_time=bar, sent_at=bar+120)

    def test_validation(self):
        now=120005
        p=self.payload(now)
        self.assertEqual(normalize(p, {'NVDA'},120,30,now)['direction'],'CALL')
        for patch in [dict(timeframe='1'),dict(symbol='BAD'),dict(signal='SMALL_TEAL'),
                      dict(bar_time=120000),dict(sent_at=120100)]:
            with self.assertRaises(ValueError):normalize(p|patch,{'NVDA'},120,30,now)
        with self.assertRaises(ValueError):normalize(p,{'NVDA'},120,30,now+40)
        with self.assertRaises(ValueError):source('both')

    def test_authentication_duplicate_and_queue(self):
        from unittest.mock import patch
        import json
        events=[];signals=[]
        rt=SimpleNamespace(bar_seconds=120,audit=SimpleNamespace(emit=lambda e,**k:events.append(e)),
                           on_webhook_signal=signals.append)
        receiver=WebhookReceiver(rt,{'NVDA'},'local-secret')
        def request(payload):
            body=json.dumps(payload).encode()
            async def receive():return {'type':'http.request','body':body,'more_body':False}
            return Request({'type':'http','method':'POST','path':'/webhook/tradingview','headers':[]},receive)
        try:
            with patch('trading_engine.webhook.time.time',return_value=120005):
                with self.assertRaises(HTTPException) as cm:asyncio.run(receiver.receive(request(self.payload(120005))))
                self.assertEqual(cm.exception.status_code,401)
                payload=self.payload(120005)|{'token':'local-secret'}
                self.assertEqual(asyncio.run(receiver.receive(request(payload)))['status'],'queued')
                self.assertEqual(asyncio.run(receiver.receive(request(payload)))['status'],'duplicate')
            receiver.queue.join()
            self.assertEqual(len(signals),1)
            self.assertNotIn('token',signals[0])
        finally:receiver.close()

    def test_runtime_source_isolation_and_expiry(self):
        from trading_engine.runtime import TradingRuntime
        rt=TradingRuntime()
        rt.psar_signal_source='ENGINE'
        rt.on_webhook_signal(dict(symbol='NVDA',indicator='PSAR',signal='LONG',
                              direction='CALL',bar_time=time.time()-120,expires_at=time.time()+10))
        self.assertFalse(rt.positions)
        rt.psar_signal_source='WEBHOOK'
        rt.on_webhook_signal(dict(symbol='NVDA',indicator='PSAR',signal='LONG',
                              direction='CALL',bar_time=time.time()-120,expires_at=time.time()-1))
        self.assertFalse(rt.positions)

class RuntimeRoutingTests(unittest.TestCase):
    def test_opposite_perfected_routes_exit_only(self):
        from unittest.mock import Mock
        from trading_engine.runtime import TradingRuntime
        from trading_engine.exit_pipeline import PositionState
        rt=TradingRuntime()
        rt.reversal_signal_source='WEBHOOK'
        now=time.time()
        pos=PositionState('NVDA','PUT',now-200,1.0)
        rt.positions['NVDA']=('trade',pos)
        rt._evaluate_symbol=Mock()
        p=dict(symbol='NVDA',indicator='REVERSAL',signal='BULLISH_PERFECTED',
               direction='CALL',bar_time=now-120,expires_at=now+10)
        rt.on_webhook_signal(p)
        self.assertEqual(rt._webhook_exit['NVDA'][1: ],('trade','WEBHOOK_OPPOSITE_PERFECT_REVERSAL'))
        rt._evaluate_symbol.assert_called_once()
        rt._webhook_exit.clear()
        rt.on_webhook_signal(p|{'direction':'PUT'})
        self.assertFalse(rt._webhook_exit)
        rt.on_webhook_signal(p|{'bar_time':now-300})
        self.assertFalse(rt._webhook_exit)

    def test_psar_webhook_uses_existing_live_provider_path(self):
        from unittest.mock import Mock,patch
        from trading_engine.runtime import TradingRuntime
        from trading_engine.symbol_state import Bar
        rt=TradingRuntime()
        rt.psar_signal_source='WEBHOOK'
        now=time.time()
        rt.store.ingest_tick('NVDA',now,100)
        rt._entry_session=Mock(return_value=True)
        rt._open_position=Mock(return_value=True)
        rt._audit_signal=Mock()
        with patch('trading_engine.runtime.evaluate_entry',return_value=SimpleNamespace(action='TAKE')):
            rt.dashboard.record_scan=Mock()
            rt.on_webhook_signal(dict(symbol='NVDA',indicator='PSAR',signal='LONG',
                direction='CALL',bar_time=now-120,expires_at=now+10))
        rt._open_position.assert_called_once()
        self.assertEqual(rt._open_position.call_args.args[:2],('NVDA','CALL'))
        self.assertEqual(rt._open_position.call_args.args[2].price,100)

class StartupWiringTests(unittest.TestCase):
    def test_main_uses_runtime_webhook_configuration(self):
        from unittest.mock import Mock, patch
        import main
        import dashboard_app
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                receiver=Mock() if enabled else None
                rt=SimpleNamespace(psar_signal_source='WEBHOOK' if enabled else 'ENGINE',
                    reversal_signal_source='ENGINE',webhook_enabled=enabled,
                    webhook_receiver=receiver,chain_cache=Mock(),audit=Mock())
                stream=Mock()
                with patch.object(main,'build_runtime',return_value=(rt,Mock(),stream,None,['NVDA'],None)),\
                     patch('uvicorn.run') as serve,\
                     patch.object(dashboard_app.app,'include_router') as router:
                    main.main()
                    serve.assert_called_once()
                    self.assertEqual(dashboard_app.signal_sources['webhook_enabled'],enabled)
                    if enabled:
                        router.assert_called_once_with(receiver.router)
                        receiver.close.assert_called_once()
                    else:router.assert_not_called()
                    stream.close.assert_called_once()
