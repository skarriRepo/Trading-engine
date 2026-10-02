"""Real entry point: loads TRADIER_ACCESS_TOKEN from the environment, wires
a live Tradier connection into TradingRuntime and the dashboard, and runs
both in one process.

Run:
    # Save credentials once in ~/.trading_engine/credentials.env or set
    # ENGINE_CREDENTIALS_FILE to an existing private .env path.
    pip install -r requirements.txt
    python main.py

This has NOT been tested against a credentialed Tradier sandbox connection.
See README.md for verification status. Sandbox data is never used for signals.

What this wires up:
  - TradierRestClient          (tradier_client.py)  -- quotes, chains, expirations
  - make_chain_source + make_option_entry_price_provider (contract_selection.py)
  - TradierStreamClient        (tradier_stream.py)  -- live equity ticks
  - TradingRuntime             (runtime.py)          -- the pipeline itself
  - DashboardStore + dashboard_app                   -- Scan/Active/Closed over HTTP

UW is wired the same way as Tradier below, from UW_API_KEY -- but read
uw_stream.py's own module docstring before trusting it: its channel names
are evidenced from this session's real reference-system exploration, but its
raw per-message field parsing is the least-verified piece of this whole
bundle and needs a live message sample to confirm before going live.

Dynamic option-quote subscription is wired for each held OCC contract.
After a confirmed close, a bounded observer continues recording that
contract's live bids and asks for exit research; it never changes orders.

When TRADIER_ACCOUNT_ID is set, orders are sandbox-only. A broker position
appears on the dashboard only after a confirmed full buy fill and disappears
only after a confirmed full sell fill. Pending/unknown orders are journaled
and never retried automatically.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import math

from trading_engine.credentials import load_engine_environment

from trading_engine.tradier_client import TradierRestClient, TradierConfig, PRODUCTION_BASE, SANDBOX_BASE
from trading_engine.tradier_stream import TradierStreamClient
from trading_engine.tradier_poll import TradierPollClient
from trading_engine.contract_selection import PrefetchedChainSource, make_option_entry_price_provider
from trading_engine.tradier_client import make_chain_source
from trading_engine.uw_stream import UWStreamClient, bind_net_flow
from trading_engine.tradier_orders import TradierOrderClient, TradierOrderError
from trading_engine.sandbox_execution import SandboxExecution
from trading_engine.tradier_client import TradierAuthError
from trading_engine.runtime import TradingRuntime
from trading_engine.exit_pipeline import ExitConfig
from trading_engine.reversal_signals import ReversalSettings
from trading_engine.dashboard_store import DashboardStore
from trading_engine.history_warmup import warmup_symbols
from trading_engine.audit_log import AuditLog, ET
from trading_engine.post_exit_observer import PostExitQuoteObserver
from datetime import datetime

DEFAULT_WATCH_SYMBOLS = (
    "SPY", "QQQ", "IWM", "TSLA", "NVDA", "AAPL", "AMD", "AMZN",
    "META", "MSFT", "GOOGL", "NFLX",
)


def configured_symbols(value: str = "") -> list[str]:
    """Always monitor the requested universe; WATCH_SYMBOLS adds extras."""
    return list(dict.fromkeys((*DEFAULT_WATCH_SYMBOLS,
                               *(part.strip().upper() for part in value.split(",") if part.strip()))))


def build_runtime() -> tuple:
    load_engine_environment()

    env = os.environ.get("TRADIER_ENV", "sandbox").strip().lower()
    if env not in {"production", "sandbox"}:
        raise ValueError("TRADIER_ENV must be sandbox or production")
    base_url = PRODUCTION_BASE if env == "production" else SANDBOX_BASE
    symbols = configured_symbols(os.environ.get("WATCH_SYMBOLS", ""))
    order_quantity = int(os.environ.get("ORDER_QUANTITY", "1"))
    reversal_exit_enabled = os.environ.get("REVERSAL_PHASE_EXIT", "true").strip().lower() in {"1", "true", "yes"}
    stall_exit_enabled = os.environ.get("STALL_EXIT_ENABLED", "false").strip().lower() in {"1", "true", "yes"}
    reversal_settings = ReversalSettings(
        momentum_display=os.environ.get("REVERSAL_MOMENTUM_DISPLAY", "Completed"),
        support_resistance=os.environ.get("REVERSAL_SUPPORT_RESISTANCE", "true").lower() in {"1", "true", "yes"},
        level_style=os.environ.get("REVERSAL_LEVEL_STYLE", "Step Line w/ Diamonds"),
        momentum_risk=os.environ.get("REVERSAL_MOMENTUM_RISK", "false").lower() in {"1", "true", "yes"},
        exhaustion_display=os.environ.get("REVERSAL_EXHAUSTION_DISPLAY", "Completed"),
        exhaustion_risk=os.environ.get("REVERSAL_EXHAUSTION_RISK", "false").lower() in {"1", "true", "yes"},
        exhaustion_target=os.environ.get("REVERSAL_EXHAUSTION_TARGET", "false").lower() in {"1", "true", "yes"},
        trade_setups=os.environ.get("REVERSAL_TRADE_SETUPS", "None"),
        setup_warnings=os.environ.get("REVERSAL_SETUP_WARNINGS", "false").lower() in {"1", "true", "yes"},
    )
    audit = AuditLog(os.environ.get("ENGINE_LOG_DIR", "logs"))
    audit.emit("ENGINE_START", symbols=symbols, environment=env,
               orders_enabled=bool(os.environ.get("TRADIER_ACCOUNT_ID", "").strip()),
               max_order_debit=float(os.environ.get("MAX_ORDER_DEBIT", "300")),
               max_total_debit=float(os.environ.get("MAX_TOTAL_DEBIT", "600")),
               order_quantity=order_quantity,
               stall_exit_enabled=stall_exit_enabled,
               reversal_phase_exit_enabled=reversal_exit_enabled)
    audit.emit("REVERSAL_CONFIG", **vars(reversal_settings))

    rest_client = TradierRestClient(config=TradierConfig(base_url=base_url))
    live_data_token = os.environ.get("TRADIER_LIVE_DATA_TOKEN", "").strip()
    if env == "sandbox" and not live_data_token:
        raise ValueError("TRADIER_LIVE_DATA_TOKEN is required: sandbox quotes cannot drive analysis or decisions.")
    data_client = (TradierRestClient(config=TradierConfig(base_url=PRODUCTION_BASE), token=live_data_token)
                   if env == "sandbox" else rest_client)
    # Keep the chain fetcher's HTTP session separate from the live quote and
    # stream session. Requests sessions are not shared across worker threads.
    chain_client = TradierRestClient(config=TradierConfig(base_url=PRODUCTION_BASE),
                                     token=live_data_token or os.environ.get("TRADIER_ACCESS_TOKEN", "").strip())
    chain_cache = PrefetchedChainSource(make_chain_source(chain_client))
    def _selected_contract(symbol, selected):
        print(f"[contract] {symbol} dte={selected.dte_days} "
              f"premium_pct={selected.premium_pct_of_underlying}% ask={selected.entry_price}")
        audit.emit("CONTRACT_DETAILS", symbol=symbol,
                   occ_symbol=selected.contract.occ_symbol,
                   dte_days=selected.dte_days,
                   premium_pct_of_underlying=selected.premium_pct_of_underlying,
                   bid=selected.contract.bid, ask=selected.entry_price,
                   strike=selected.contract.strike)
    selected_price_provider = make_option_entry_price_provider(
        chain_source=chain_cache.get,
        on_selected=_selected_contract,
    )
    def option_price_provider(symbol, direction, now, underlying_price):
        selected = selected_price_provider(symbol, direction, now, underlying_price)
        if not selected or not selected.occ_symbol:
            if not chain_cache.ready(symbol):
                audit.emit("CHAIN_PREFETCH_UNAVAILABLE", symbol=symbol, direction=direction,
                           market_ts=now)
            return None
        row = data_client.quotes([selected.occ_symbol]).get(selected.occ_symbol) or {}
        ask_ts = TradierPollClient._event_time(row.get("ask_date"))
        bid_ts = TradierPollClient._event_time(row.get("bid_date"))
        ask = float(row.get("ask") or 0)
        bid = float(row.get("bid") or 0)
        wall = time.time()
        if (not ask_ts or not bid_ts or not math.isfinite(ask) or not math.isfinite(bid)
                or not 0 < bid <= ask or ask - bid > ask * .15
                or not 0 <= wall - ask_ts <= 10 or not 0 <= wall - bid_ts <= 10):
            audit.emit("LIVE_OPTION_QUOTE_UNAVAILABLE", symbol=symbol,
                       occ_symbol=selected.occ_symbol, ask_ts=ask_ts, bid_ts=bid_ts)
            return None
        return type(selected)(price=ask, occ_symbol=selected.occ_symbol)

    account_id = os.environ.get("TRADIER_ACCOUNT_ID", "").strip()
    if account_id and env != "sandbox":
        raise ValueError("Automated orders are sandbox-only; remove TRADIER_ACCOUNT_ID for production read-only mode.")
    executor = (SandboxExecution(TradierOrderClient(rest_client, account_id),
                quantity=order_quantity, journal_path=os.environ.get("SANDBOX_ORDER_JOURNAL", "sandbox_orders.json"),
                max_order_debit=float(os.environ.get("MAX_ORDER_DEBIT", "300")),
                max_total_debit=float(os.environ.get("MAX_TOTAL_DEBIT", "600")),
                max_daily_loss=float(os.environ.get("MAX_DAILY_LOSS", "500")),
                max_slippage_pct=float(os.environ.get("MAX_ENTRY_SLIPPAGE_PCT", "5")), audit=audit)
                if account_id else None)

    dashboard = DashboardStore()
    def _recover_option_quote(occ_symbol):
        row = data_client.quotes([occ_symbol]).get(occ_symbol) or {}
        ts = TradierPollClient._event_time(row.get("bid_date"))
        bid = float(row.get("bid") or 0)
        ask = float(row.get("ask") or 0)
        return (ts, bid, ask) if ts and bid > 0 else None
    rt = TradingRuntime(dashboard=dashboard, option_entry_price_provider=option_price_provider,
                        order_executor=executor,
                        audit=audit,
                        exit_config=ExitConfig(reversal_phase_exit=reversal_exit_enabled,
                                               stall_exit_enabled=stall_exit_enabled),
                        reversal_settings=reversal_settings,
                        option_quote_recovery=_recover_option_quote if executor else None,
                        option_quote_timeout_sec=float(os.environ.get("OPTION_QUOTE_TIMEOUT_SEC", "20")),
                        max_market_age_sec=10)
    rt.chain_cache = chain_cache

    def _on_underlying_tick(symbol, ts, price, volume):
        if symbol not in symbols:
            return  # contract trades and unrecognized symbols cannot form equity bars
        # Scheduling is lock-only; option-chain REST runs on a bounded worker.
        if abs(time.time() - ts) <= 10:
            chain_cache.schedule(symbol)
        rt.on_underlying_tick(symbol, ts, price, volume)

    # Sandbox credentials are never passed to a market-data transport.
    post_exit_observer = None
    def _on_option_quote(occ, ts, bid, ask):
        rt.on_option_quote(occ, ts, bid, ask)
        if post_exit_observer is not None:
            post_exit_observer.quote(occ, ts, bid, ask)

    tradier_stream = TradierStreamClient(
        symbols=symbols, rest_client=data_client,
        on_tick=_on_underlying_tick, on_quote=_on_option_quote, on_connected=rt.set_connected,
        on_diagnostic=lambda reason, detail: audit.emit("TRADIER_STREAM_DIAGNOSTIC",
                                                        durable=reason != "SUBSCRIPTION_SENT",
                                                        reason=reason, detail=detail),
    )
    post_exit_observer = PostExitQuoteObserver(
        tradier_stream, audit, seconds=float(os.environ.get("POST_EXIT_OBSERVE_SEC", "600")),
        exit_config=rt.exit_config)

    # Set after both objects exist (on_trade_opened/closed reference
    # tradier_stream, which itself needed rt.on_underlying_tick to
    # construct) -- these are plain instance attributes, not required at
    # TradingRuntime's own __init__ time. This is the actual fix for the
    # "positions never get live price updates" gap: add_symbol/remove_symbol
    # make the stream subscribe to each position's own OCC contract exactly
    # while it's open, so on_option_quote() (wired above) actually fires.
    if executor is None:
        print("TRADIER_ACCOUNT_ID not set -- running without order placement; "
              "positions are simulated internally only, no real orders will be sent.")

    def _on_trade_opened(trade_id, pos):
        print(f"[opened] {trade_id} {pos.direction} {pos.quantity} @ {pos.entry_option_price}")
        if pos.occ_symbol:
            audit.emit("OPTION_SUBSCRIPTION_REQUESTED", trade_id=trade_id,
                       symbol=pos.symbol, occ_symbol=pos.occ_symbol, action="ADD")
            post_exit_observer.opened(trade_id, pos)

    def _on_trade_closed(trade_id, pos):
        print(f"[closed] {trade_id} final_gain={pos.gain_pct():.1f}%")
        if pos.occ_symbol:
            audit.emit("OPTION_SUBSCRIPTION_REQUESTED", trade_id=trade_id,
                       symbol=pos.symbol, occ_symbol=pos.occ_symbol, action="SHADOW")
            post_exit_observer.closed(trade_id, pos)

    rt.on_trade_opened = _on_trade_opened
    rt.on_trade_closed = _on_trade_closed
    if executor is not None:
        executor.attach(rt)  # validates broker/journal state before any scanner signal

    warmup = warmup_symbols(data_client, rt, symbols)
    audit.emit("HISTORY_WARMUP", bar_counts=warmup,
               minimum_bars=rt.entry_config.momentum_slow + 2)
    print("[warmup] complete 2-minute bars: " + ", ".join(
        f"{symbol}={count}" for symbol, count in warmup.items()) +
        f" (entry needs {rt.entry_config.momentum_slow + 2})", flush=True)
    for symbol in symbols:
        chain_cache.schedule(symbol)

    # UW is optional: if UW_API_KEY isn't set, run with Tradier alone rather
    # than fail the whole startup -- every UW confluence read will simply
    # show NOT_READY, which entry_pipeline.py already treats as "no opinion,"
    # not as a crash-worthy condition.
    uw_stream = None
    if os.environ.get("UW_API_KEY"):
        uw_stream = UWStreamClient(
            symbols=symbols,
            on_net_flow=bind_net_flow(rt),
            on_interval_flow=rt.on_interval_flow,
            on_market_tide=lambda sample: rt.on_market_tide(sample.ts, sample.net_call_premium, sample.net_put_premium),
            on_gex=rt.on_gex,
            on_connected=lambda connected: audit.emit("UW_FEED_CONNECTION", connected=connected),
            on_diagnostic=lambda reason, channel: audit.emit("UW_STREAM_DIAGNOSTIC",
                                                         durable=reason != "UNHANDLED_CHANNEL",
                                                         reason=reason, channel=channel),
            ws_url=os.environ.get("UW_WS_URL", "wss://api.unusualwhales.com/socket"),
        )
    else:
        audit.emit("UW_FEED_DISABLED", reason="NO_API_KEY")
        print("UW_API_KEY not set -- running with Tradier only; UW confluence reads will show NOT_READY.")

    return rt, dashboard, tradier_stream, uw_stream, symbols, executor


def main() -> None:
    try:
        rt, dashboard, tradier_stream, uw_stream, symbols, executor = build_runtime()
    except Exception as exc:
        print(f"Failed to start ({type(exc).__name__}): {exc}", file=sys.stderr)
        if isinstance(exc, TradierAuthError):
            print("Check the Tradier tokens in the private credentials file or environment.", file=sys.stderr)
        if os.environ.get("ENGINE_DEBUG_STARTUP", "").lower() == "true":
            import traceback
            traceback.print_exc()
        sys.exit(1)

    print(f"Watching: {', '.join(symbols)}")
    if executor is not None:
        print(f"Sandbox debit caps: ${executor.max_order_debit:,.2f} per order; "
              f"${executor.max_total_debit:,.2f} total", flush=True)
    if executor is not None:
        executor.start()
    tradier_stream.start()
    if uw_stream is not None:
        uw_stream.start()

    import dashboard_app
    dashboard_app.store = dashboard
    dashboard_app.order_executor = executor
    dashboard_app.data_mode = "LIVE PRODUCTION MARKET DATA"

    import uvicorn
    try:
        uvicorn.run(dashboard_app.app, host="0.0.0.0", port=8000)
    finally:
        tradier_stream.close()
        rt.chain_cache.close()
        if executor is not None:
            executor.close()
        if uw_stream is not None:
            uw_stream.close()
        try:
            rt.audit.emit("ENGINE_STOP")
            journal = str(executor.path) if executor else os.environ.get("SANDBOX_ORDER_JOURNAL", "sandbox_orders.json")
            rt.audit.report(datetime.now(ET).date().isoformat(), journal)
        except Exception as exc:
            print(f"[audit] EOD report failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
