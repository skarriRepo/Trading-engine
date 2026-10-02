"""Operator dashboard with broker status and diagnostic drilldown.

Reads only from a DashboardStore -- never touches SymbolStateStore,
PositionState, or the pipeline modules directly. Whatever feeds the store
(live runtime, replay harness, or a test) is invisible to this app.

Run standalone for local viewing:
    uvicorn dashboard_app:app --reload --port 8000
"""
from __future__ import annotations

from pathlib import Path
import time

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from trading_engine.dashboard_store import DashboardStore

app = FastAPI(title="Trading Dashboard")
store = DashboardStore()
order_executor = None
data_mode = "Market data status unknown"


@app.get("/api/scan")
def api_scan() -> JSONResponse:
    return JSONResponse(store.scan_view())


@app.get("/api/active")
def api_active() -> JSONResponse:
    return JSONResponse(store.active_view())


@app.get("/api/closed")
def api_closed() -> JSONResponse:
    return JSONResponse(store.closed_view())


@app.get("/api/signals")
def api_signals() -> JSONResponse:
    return JSONResponse(store.signal_view())


@app.get("/api/reversals")
def api_reversals() -> JSONResponse:
    return JSONResponse(store.reversal_view())


@app.get("/api/orders")
def api_orders() -> JSONResponse:
    return JSONResponse(order_executor.order_view() if order_executor else [])


@app.get("/api/overview")
def api_overview() -> JSONResponse:
    active = store.active_view()
    closed = store.closed_view()
    orders = order_executor.order_view() if order_executor else []
    scan = store.scan_view()
    return JSONResponse({
        "generated_ts": time.time(),
        "execution_mode": "SANDBOX ORDERS" if order_executor else "SIGNALS ONLY",
        "data_mode": data_mode,
        "active": active, "closed": closed, "orders": orders,
        "scan": scan, "signals": store.signal_view(),
        "reversals": store.reversal_view(),
        "live_quote_pnl_usd": round(sum(
            row["live_quote_pnl_usd"] for row in closed
            if row["live_quote_pnl_usd"] is not None), 2),
        "realized_scope": "Current engine run; live entry ask to live exit trigger bid, before fees",
    })


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")
