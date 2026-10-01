"""Sandbox-only broker lifecycle. A submission is never treated as a fill.

The atomic journal prevents a lost HTTP response or process restart from
silently issuing a duplicate order. An unknown submission stays blocked until
it is found by its Tradier tag or resolved manually in the sandbox account.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import threading
import time
import uuid
import math
from datetime import datetime, time as clock_time
from zoneinfo import ZoneInfo

from .tradier_client import SANDBOX_BASE
from .tradier_orders import TradierOrderClient, TradierOrderError


class SandboxExecution:
    def __init__(self, client: TradierOrderClient, quantity: int = 1,
                 journal_path: str = "sandbox_orders.json", poll_seconds: float = 15.0,
                 max_order_debit: float = 300.0, max_total_debit: float = 600.0,
                 max_daily_loss: float = 500.0, max_slippage_pct: float = 5.0,
                 entry_timeout_sec: float = 45.0, audit=None):
        if client.rest_client.config.base_url.rstrip("/") != SANDBOX_BASE:
            raise TradierOrderError("This order coordinator accepts sandbox.tradier.com only.")
        if not isinstance(quantity, int) or quantity < 1:
            raise TradierOrderError("ORDER_QUANTITY must be a positive integer.")
        if min(max_order_debit, max_total_debit, max_daily_loss, entry_timeout_sec) <= 0 or not 0 <= max_slippage_pct <= 20:
            raise TradierOrderError("Sandbox risk caps and entry timeout must be positive; slippage cap must be 0–20%.")
        self.client, self.quantity = client, quantity
        self.max_order_debit = max_order_debit
        self.max_total_debit = max_total_debit
        self.max_daily_loss = max_daily_loss
        self.max_slippage_pct = max_slippage_pct
        self.entry_timeout_sec = entry_timeout_sec
        self.path = Path(journal_path)
        self.poll_seconds = max(15., poll_seconds)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self.runtime = None
        self.audit = audit
        data = json.loads(self.path.read_text()) if self.path.exists() else {}
        if not isinstance(data, dict) or any(not isinstance(data.get(key, default), typ)
                for key, default, typ in (("pending", {}, dict), ("open", {}, dict),
                                          ("blocked", {}, dict), ("closed", [], list))):
            raise TradierOrderError("Sandbox order journal has an unexpected shape; inspect it before starting.")
        if data and data.get("account_id") != client.account_id:
            raise TradierOrderError("Journal account differs from TRADIER_ACCOUNT_ID.")
        self.pending: dict = data.get("pending", {})
        self.open: dict = data.get("open", {})
        self.blocked: dict = data.get("blocked", {})
        self.closed: list = data.get("closed", [])

    def _save(self) -> None:
        data = {"account_id": self.client.account_id, "pending": self.pending,
                "open": self.open, "blocked": self.blocked, "closed": self.closed}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(self.path.name + ".tmp")
        with temp.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.flush(); os.fsync(f.fileno())
        # OneDrive/antivirus can briefly hold the destination on Windows.
        # Retain the fsynced temp file and retry the atomic replacement;
        # never continue to an order POST if journal persistence fails.
        for attempt in range(8):
            try:
                os.replace(temp, self.path)
                return
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(0.05 * (attempt + 1))

    @staticmethod
    def _broker_qty(positions: list, occ: str) -> float:
        return sum(float(p.get("quantity") or 0) for p in positions
                   if str(p.get("symbol") or "").upper() == occ.upper())

    def _recover_manual_close(self, symbol: str, record: dict) -> bool:
        """Journal an independently filled close, using only a unique broker receipt."""
        if symbol in self.pending:
            return False
        candidates = [o for o in self.client.orders()
                      if str(o.get("side") or "").lower() == "sell_to_close"
                      and str(o.get("option_symbol") or "").upper() == record["occ_symbol"].upper()
                      and str(o.get("status") or "").lower() == "filled"
                      and str(o.get("id") or "").isdigit()
                      and str(record.get("entry_order_id") or "").isdigit()
                      and int(o["id"]) > int(record["entry_order_id"])]
        if len(candidates) != 1:
            return False
        receipt = self.client.get_order(str(candidates[0]["id"]))
        qty = record["quantity"]
        if (str(receipt.get("status") or "").lower() != "filled"
                or str(receipt.get("side") or "").lower() != "sell_to_close"
                or str(receipt.get("option_symbol") or "").upper() != record["occ_symbol"].upper()
                or float(receipt.get("quantity") or 0) != qty
                or float(receipt.get("exec_quantity") or 0) != qty
                or float(receipt.get("avg_fill_price") or 0) <= 0):
            return False
        if self._broker_qty(self.client.positions(), record["occ_symbol"]) != 0:
            return False
        price = float(receipt["avg_fill_price"])
        et_now = datetime.now(ZoneInfo("America/New_York"))
        closed = {"symbol": symbol, "day_et": et_now.date().isoformat(),
                  "entry_fill": record["entry_fill"], "exit_fill": price,
                  "live_entry_ask": record.get("live_entry_ask"),
                  "live_entry_limit": record.get("limit_price"), "live_exit_bid": None,
                  "live_pnl": None, "quantity": qty, "direction": record["direction"],
                  "occ_symbol": record["occ_symbol"], "trade_id": record["tag"],
                  "entry_order_id": record["entry_order_id"],
                  "exit_order_id": str(receipt["id"]),
                  "exit_reasons": ["MANUAL_BROKER_CLOSE"], "exit_trigger": None,
                  "fill_observation": None, "exit_submitted_at_et": None,
                  "closed_at_et": et_now.isoformat(),
                  "pnl": round((price - record["entry_fill"]) * qty * 100, 2)}
        self.closed.append(closed)
        self.open.pop(symbol)
        self.blocked.pop(symbol, None)
        self._save()
        if self.audit:
            self.audit.emit("MANUAL_BROKER_CLOSE_RECONCILED", symbol=symbol,
                            trade_id=record["tag"], occ_symbol=record["occ_symbol"],
                            exit_order_id=str(receipt["id"]), quantity=qty,
                            exit_fill=price, realized_pnl_usd_before_fees=closed["pnl"])
        return True

    def attach(self, runtime) -> None:
        """Check the account before signals can submit; recover only journaled positions."""
        with self._lock:
            self.runtime = runtime
            positions = self.client.positions()  # authentication failure is fatal at startup
            journal_occ = {x["occ_symbol"].upper() for x in self.open.values()}
            journal_occ.update(x["occ_symbol"].upper() for x in self.pending.values())
            unmanaged = [p.get("symbol") for p in positions
                         if float(p.get("quantity") or 0) > 0
                         and str(p.get("symbol") or "").upper() not in journal_occ]
            if unmanaged:
                raise TradierOrderError(f"Unmanaged sandbox positions {unmanaged}; reconcile them before starting automated orders.")
            for symbol, record in list(self.open.items()):
                broker_qty = self._broker_qty(positions, record["occ_symbol"])
                if broker_qty != record["quantity"]:
                    if broker_qty == 0 and self._recover_manual_close(symbol, record):
                        continue
                    pending_exit = self.pending.get(symbol)
                    if (broker_qty == 0 and pending_exit
                            and pending_exit.get("side") == "sell_to_close"
                            and pending_exit.get("occ_symbol", "").upper() == record["occ_symbol"].upper()
                            and pending_exit.get("quantity") == record["quantity"]
                            and pending_exit.get("order_id")):
                        # Only a broker-confirmed, fully filled journaled exit
                        # may account for a missing position on restart.
                        order = self.client.get_order(pending_exit["order_id"])
                        if (str(order.get("status") or "").lower() == "filled"
                                and float(order.get("exec_quantity") or 0) == record["quantity"]
                                and float(order.get("avg_fill_price") or 0) > 0
                                and (not order.get("side") or order["side"] == "sell_to_close")
                                and (not order.get("option_symbol") or
                                     order["option_symbol"].upper() == record["occ_symbol"].upper())):
                            runtime.restore_broker_position(record)
                            continue  # reconcile() writes the confirmed close to the journal
                    raise TradierOrderError(
                        f"Journaled {symbol} {record['occ_symbol']} quantity {record['quantity']} "
                        f"differs from broker quantity {broker_qty}; inspect broker orders and the journal before startup.")
                runtime.restore_broker_position(record)
            for symbol in self.pending:
                runtime.pending_symbols.add(symbol)
            for symbol in self.blocked:
                runtime.blocked_symbols.add(symbol)
            self.reconcile()
            for symbol, record in self.open.items():
                if self._broker_qty(self.client.positions(), record["occ_symbol"]) != record["quantity"]:
                    raise TradierOrderError(f"Journaled {symbol} exit did not reconcile; automated orders remain stopped.")

    def _check_available(self, symbol: str, occ: str, side: str, quantity: int) -> None:
        if symbol in self.pending or symbol in self.blocked:
            raise TradierOrderError(f"{symbol} has an unresolved broker order; no duplicate will be submitted.")
        positions = self.client.positions()
        qty = self._broker_qty(positions, occ)
        if side == "buy_to_open":
            held = [p.get("symbol") for p in positions if
                    float(p.get("quantity") or 0) > 0 and
                    str(p.get("symbol") or "").upper().startswith(symbol.upper())]
            if held:
                raise TradierOrderError(f"Broker already holds {symbol} contracts: {held}")
        if side == "buy_to_open" and qty != 0:
            raise TradierOrderError(f"Broker already holds {occ}; new entry blocked.")
        if side == "sell_to_close" and qty != quantity:
            raise TradierOrderError(f"Broker holds {qty} of {occ}, expected {quantity}; close blocked.")
        for order in self.client.orders():
            if (str(order.get("status") or "").lower() in
                    {"pending", "open", "partially_filled", "pending_cancel",
                     "accepted_for_bidding", "held", "calculated"}
                    and (str(order.get("option_symbol") or "").upper().startswith(symbol.upper())
                         or (str(order.get("class") or "").lower() == "option"
                             and str(order.get("symbol") or "").upper() == symbol.upper()))):
                raise TradierOrderError(f"Broker has a working order for {occ}; no duplicate submitted.")

    def block(self, symbol: str, reason: str) -> None:
        with self._lock:
            self.blocked[symbol] = reason
            self._save()
            if self.audit:
                self.audit.emit("BROKER_BLOCKED", symbol=symbol, reason=reason)

    def order_view(self) -> list[dict]:
        with self._lock:
            rows = [{"symbol": s, "side": r["side"], "option": r["occ_symbol"],
                     "status": r["status"], "order_id": r.get("order_id"),
                     "detail": r.get("error", "")}
                    for s, r in self.pending.items()]
            rows += [{"symbol": s, "side": "", "option": "", "status": "BLOCKED",
                      "order_id": None, "detail": why} for s, why in self.blocked.items()]
        if self.runtime is not None:
            rows += self.runtime.option_feed_view()
        return rows

    def _submit(self, symbol: str, direction: str, occ: str, side: str,
                trigger_ts: float, exit_reasons: tuple = (), entry_ask: float = 0.,
                quantity: int = 0, observation: dict | None = None) -> None:
        with self._lock:
            quantity = quantity or self.quantity
            limit_price = None
            if side == "buy_to_open":
                eastern = datetime.now(ZoneInfo("America/New_York"))
                if eastern.weekday() >= 5 or not (clock_time(9, 30) <= eastern.time() < clock_time(15, 30)):
                    raise TradierOrderError("New sandbox entries require a weekday 09:30–15:30 ET session.")
                if not math.isfinite(entry_ask) or entry_ask <= 0:
                    raise TradierOrderError("A valid option ask is required for a bounded entry.")
                limit_price = math.ceil(entry_ask * (1 + self.max_slippage_pct / 100) * 100) / 100
                debit = limit_price * quantity * 100
                if debit > self.max_order_debit:
                    raise TradierOrderError(f"Order debit ${debit:.2f} exceeds ${self.max_order_debit:.2f} cap.")
                if any(not x.get("live_entry_ask") for x in self.open.values()):
                    raise TradierOrderError("Open position lacks live entry ask; new entries blocked.")
                exposure = sum(x["live_entry_ask"] * x["quantity"] * 100 for x in self.open.values())
                exposure += sum(x.get("limit_price", 0) * x["quantity"] * 100
                                for x in self.pending.values() if x["side"] == "buy_to_open")
                if exposure + debit > self.max_total_debit:
                    raise TradierOrderError("Total sandbox debit cap reached.")
                today = eastern.date().isoformat()
                today_closed = [x for x in self.closed if x["day_et"] == today]
                pnl = 0.0
                for closed in today_closed:
                    if closed.get("live_pnl") is not None:
                        pnl += float(closed["live_pnl"])
                        continue
                    ask = closed.get("live_entry_ask")
                    qty = closed.get("quantity")
                    if (ask is None or qty is None or not math.isfinite(float(ask))
                            or float(ask) <= 0 or int(qty) <= 0):
                        raise TradierOrderError("Closed position lacks a valid live entry quote; new entries blocked.")
                    # With no live exit quote, assume zero recovery. Older journal
                    # records lack the quote-derived entry limit: use the maximum
                    # permitted entry slippage (20%) as their conservative bound.
                    entry_limit = closed.get("live_entry_limit")
                    bound = (float(entry_limit) if entry_limit is not None
                             else math.ceil(float(ask) * 1.20 * 100) / 100)
                    if not math.isfinite(bound) or bound < float(ask):
                        raise TradierOrderError("Closed position has an invalid live entry limit; new entries blocked.")
                    pnl -= bound * int(qty) * 100
                    if self.audit:
                        self.audit.emit("DAILY_LOSS_CONSERVATIVE_BOUND", symbol=closed.get("symbol"),
                                        trade_id=closed.get("trade_id"),
                                        quote_derived_entry_limit=bound, quantity=int(qty),
                                        assumed_exit_bid=0.0)
                if pnl <= -self.max_daily_loss:
                    raise TradierOrderError("Daily sandbox loss limit reached.")
            self._check_available(symbol, occ, side, quantity)
            tag = "sx-" + uuid.uuid4().hex[:20]
            order_type = "limit" if side == "buy_to_open" else "market"
            try:
                preview = self.client.preview(occ, symbol, side, quantity, tag=tag,
                                              order_type=order_type, limit_price=limit_price)
                if preview.status.lower() != "ok":
                    raise TradierOrderError(f"Sandbox order preview failed: {preview.status}")
            except TradierOrderError as exc:
                # Preview is not an order. An internal preview failure must
                # not prevent an urgent close when the exact held quantity
                # was just verified. The actual POST remains single-shot and
                # durably journaled before transmission.
                if side != "sell_to_close" or "Unexpected server error" not in str(exc):
                    raise
                if self.audit:
                    self.audit.emit("EXIT_PREVIEW_SERVER_ERROR", symbol=symbol,
                                    occ_symbol=occ, quantity=quantity,
                                    reason="TRADIER_UNEXPECTED_SERVER_ERROR")
                preview = None
            if self.audit:
                self.audit.emit("ORDER_PREVIEW_OK" if preview else "ORDER_PREVIEW_BYPASSED", symbol=symbol, direction=direction,
                                occ_symbol=occ, side=side, quantity=quantity,
                                ask=entry_ask if side == "buy_to_open" else None,
                                limit_price=limit_price, trigger_ts=trigger_ts,
                                exit_reasons=list(exit_reasons))
            record = {"symbol": symbol, "direction": direction, "occ_symbol": occ,
                      "side": side, "quantity": quantity, "tag": tag,
                      "live_entry_ask": entry_ask if side == "buy_to_open" else None,
                      "trigger_ts": trigger_ts, "exit_reasons": list(exit_reasons),
                      "submitted_at_wall": time.time(), "limit_price": limit_price,
                      "exit_trigger": observation if side == "sell_to_close" else None,
                      "order_id": None, "status": "SUBMITTING"}
            self.pending[symbol] = record
            self._save()  # durable intent before the single POST
            if self.audit:
                self.audit.emit("ORDER_INTENT", symbol=symbol, side=side, occ_symbol=occ,
                                quantity=quantity, tag=tag, limit_price=limit_price)
            try:
                result = (self.client.buy_to_open(occ, symbol, quantity,
                          order_type="limit", limit_price=limit_price, tag=tag)
                          if side == "buy_to_open" else
                          self.client.sell_to_close(occ, symbol, quantity, tag=tag))
                if not result.order_id or not result.order_id.isdigit():
                    raise TradierOrderError("Tradier did not return an order ID; submission outcome is unknown.")
                record["order_id"] = result.order_id
                record["status"] = "PENDING"
                self._save()
                if self.audit:
                    self.audit.emit("ORDER_SUBMITTED", symbol=symbol, side=side,
                                    order_id=result.order_id, tag=tag,
                                    trigger_observation=observation if side == "sell_to_close" else None)
                print(f"[sandbox] {side} submitted {symbol} {occ} id={result.order_id}", flush=True)
            except Exception as exc:
                # Even a timeout can mean the order was accepted. Never re-POST.
                record["status"] = "UNKNOWN" if not record["order_id"] else record["status"]
                record["error"] = str(exc)
                self._save()
                if self.audit:
                    self.audit.emit("ORDER_UNKNOWN", symbol=symbol, side=side, tag=tag,
                                    order_id=record.get("order_id"), error=str(exc))
                print(f"[sandbox] {side} unresolved for {symbol}: {exc}", flush=True)

    def submit_entry(self, symbol: str, direction: str, occ: str, now: float, ask: float) -> None:
        self._submit(symbol, direction, occ, "buy_to_open", now, entry_ask=ask)

    def submit_exit(self, symbol: str, pos, reasons: tuple, now: float,
                    observation: dict | None = None) -> None:
        self._submit(symbol, pos.direction, pos.occ_symbol, "sell_to_close", now, reasons,
                     quantity=pos.quantity, observation=observation)

    def reconcile(self) -> None:
        if self.runtime is None:
            return
        with self._lock:
            symbols = list(self.pending)
        for symbol in symbols:
            try:
                with self._lock:
                    r = self.pending.get(symbol)
                    if r is None:
                        continue
                    if not r.get("order_id"):
                        matches = [o for o in self.client.orders()
                                   if o.get("tag") == r["tag"]]
                        if len(matches) == 1 and matches[0].get("id"):
                            r["order_id"] = str(matches[0]["id"])
                            r["status"] = "PENDING"; self._save()
                            if self.audit:
                                self.audit.emit("ORDER_RECOVERED", symbol=symbol, order_id=r["order_id"], tag=r["tag"])
                        else:
                            continue  # unknown, held for manual inspection
                    order = self.client.get_order(r["order_id"])
                    status = str(order.get("status") or "").lower()
                    previous_status = r["status"]
                    r["status"] = status.upper(); self._save()
                    if self.audit and previous_status != r["status"]:
                        self.audit.emit("ORDER_STATUS", symbol=symbol, side=r["side"],
                                        order_id=r["order_id"], status=r["status"],
                                        exec_quantity=order.get("exec_quantity"),
                                        avg_fill_price=order.get("avg_fill_price"))
                    if (r["side"] == "buy_to_open" and status in {"open", "pending"}
                            and r.get("submitted_at_wall")
                            and time.time() - r["submitted_at_wall"] >= self.entry_timeout_sec
                            and not r.get("cancel_requested")
                            and float(order.get("exec_quantity") or 0) == 0):
                        r["cancel_requested"] = True
                        self._save()  # never repeat an ambiguous cancel
                        if self.audit:
                            self.audit.emit("ORDER_CANCEL_REQUESTED", symbol=symbol, order_id=r["order_id"])
                        self.client.cancel_order(r["order_id"])
                        print(f"[sandbox] entry cancel requested {symbol} id={r['order_id']}", flush=True)
                        continue  # cancellation itself is not confirmation
                    if status in {"rejected", "canceled", "expired", "error"}:
                        if float(order.get("exec_quantity") or 0) > 0:
                            raise TradierOrderError("Terminal order has a partial execution; manual broker reconciliation required.")
                        broker_qty = self._broker_qty(self.client.positions(), r["occ_symbol"])
                        if (r["side"] == "buy_to_open" and broker_qty > 0) or (
                                r["side"] == "sell_to_close" and broker_qty < r["quantity"]):
                            raise TradierOrderError("Terminal order and broker position differ; manual reconciliation required.")
                        # Apply the runtime event without holding the broker lock.
                        event = ("rejected", r.copy(), None)
                    elif status != "filled":
                        continue
                    else:
                        filled = float(order.get("exec_quantity") or 0)
                        price = float(order.get("avg_fill_price") or 0)
                        if filled < r["quantity"] or price <= 0:
                            continue
                        if order.get("side") and order["side"] != r["side"]:
                            raise TradierOrderError("Order side differs from journal; no position mutation.")
                        if order.get("option_symbol") and order["option_symbol"].upper() != r["occ_symbol"].upper():
                            raise TradierOrderError("Order contract differs from journal; no position mutation.")
                        broker_qty = self._broker_qty(self.client.positions(), r["occ_symbol"])
                        if r["side"] == "buy_to_open":
                            if broker_qty != r["quantity"]:
                                continue
                            opened = {**r, "entry_fill": price, "opened_ts": time.time(),
                                      "current_bid": 0., "peak_bid": 0., "entry_order_id": r["order_id"]}
                            event = ("entry", opened, price)
                        else:
                            if broker_qty != 0:
                                continue
                            event = ("exit", r.copy(), price)
                kind, record, price = event
                if kind == "entry":
                    self.runtime.broker_entry_filled(record)
                elif kind == "exit":
                    self.runtime.broker_exit_filled(symbol, record, price)
                else:
                    self.runtime.broker_order_rejected(symbol, record)
                with self._lock:
                    if kind == "rejected":
                        self.pending.pop(symbol)
                        self.blocked[symbol] = f"Broker {r['side']} {status} ({r['order_id']})"
                    elif kind == "entry":
                        self.open[symbol] = opened
                        self.pending.pop(symbol)
                    else:
                        opened = self.open[symbol]
                        et_day = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
                        live_ask = opened.get("live_entry_ask")
                        live_bid = (r.get("exit_trigger") or {}).get("bid")
                        live_pnl = (round((float(live_bid) - float(live_ask)) * r["quantity"] * 100, 2)
                                    if live_ask and live_bid and float(live_bid) > 0 else None)
                        self.closed.append({"symbol": symbol, "day_et": et_day,
                            "entry_fill": opened["entry_fill"], "exit_fill": price,
                            "live_entry_ask": live_ask, "live_exit_bid": live_bid,
                            "live_pnl": live_pnl,
                            "quantity": r["quantity"],
                            "direction": r["direction"], "occ_symbol": r["occ_symbol"],
                            "trade_id": opened["tag"],
                            "entry_order_id": opened["entry_order_id"],
                            "exit_order_id": r["order_id"], "exit_reasons": r["exit_reasons"],
                            "exit_trigger": r.get("exit_trigger"),
                            "fill_observation": record.get("fill_observation"),
                            "exit_submitted_at_et": datetime.fromtimestamp(
                                r["submitted_at_wall"], ZoneInfo("America/New_York")).isoformat(),
                            "closed_at_et": datetime.now(ZoneInfo("America/New_York")).isoformat(),
                            "pnl": round((price - opened["entry_fill"]) * r["quantity"] * 100, 2)})
                        self.pending.pop(symbol)
                        self.open.pop(symbol, None)
                    self._save()
                    if self.audit:
                        if kind == "entry":
                            self.audit.emit("BROKER_ENTRY_FILLED", symbol=symbol, trade_id=r["tag"],
                                            direction=r["direction"],
                                            occ_symbol=r["occ_symbol"], order_id=r["order_id"],
                                            quantity=r["quantity"], fill_price=price)
                        elif kind == "exit":
                            self.audit.emit("BROKER_EXIT_FILLED", symbol=symbol,
                                            trade_id=opened["tag"], direction=r["direction"],
                                            occ_symbol=r["occ_symbol"], order_id=r["order_id"],
                                            quantity=r["quantity"], entry_fill=opened["entry_fill"],
                                            exit_fill=price, realized_pnl_usd_before_fees=self.closed[-1]["pnl"],
                                            reasons=r["exit_reasons"],
                                            fill_observation=record.get("fill_observation"))
                        else:
                            self.audit.emit("BROKER_ORDER_REJECTED", symbol=symbol, side=r["side"],
                                            order_id=r["order_id"], status=status)
                print(f"[sandbox] {r['side']} {status.upper()} {symbol} id={r['order_id']}", flush=True)
            except Exception as exc:
                with self._lock:
                    if symbol in self.pending:
                        self.pending[symbol]["error"] = str(exc); self._save()
                print(f"[sandbox] reconcile {symbol}: {exc}", flush=True)
                if self.audit:
                    self.audit.emit("RECONCILE_ERROR", symbol=symbol, error=str(exc))

    def remember_position(self, pos) -> None:
        with self._lock:
            record = self.open.get(pos.symbol)
            if record:
                record.update(current_bid=pos.current_option_price,
                              peak_bid=pos.peak_option_price,
                              armed=pos.armed, last_new_peak_ts=pos.last_new_peak_ts,
                              last_quote_spread=pos.last_quote_spread,
                              peak_quote_spread=pos.peak_quote_spread)
                self._save()

    def start(self) -> None:
        def worker():
            next_reconcile = time.monotonic()
            while not self._stop.wait(2.):
                try:
                    if self.runtime:
                        self.runtime.on_clock()
                    if self.audit:
                        self.audit.maybe_report(str(self.path))
                    if time.monotonic() >= next_reconcile:
                        self.reconcile()
                        with self._lock:
                            count = len(self.pending)
                        next_reconcile = time.monotonic() + max(self.poll_seconds, 2.5 * count)
                except Exception as exc:
                    print(f"[sandbox] watchdog error: {exc}", flush=True)
        self._thread = threading.Thread(target=worker, daemon=True, name="sandbox-order-reconcile")
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3.)
