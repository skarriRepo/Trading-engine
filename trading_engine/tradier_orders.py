"""Tradier order placement: POST /v1/accounts/{account_id}/orders.

Built against Tradier's public, documented brokerage order API. Same
DOCUMENTED-BUT-NOT-LIVE-TESTED status as tradier_client.py/tradier_stream.py
-- no real account exists in this environment to place a test order against.
Smoke-test this against TRADIER_ENV=sandbox with a small, known quantity
before trusting it with anything real, the same as everything else here.

This is held to a stricter standard than the read-only clients, because a
wrong order has real consequences even in sandbox:
  - Every parameter is validated before a request is ever sent -- a bad
    quantity, symbol, or side is refused locally, not discovered from a
    confusing broker error.
  - A failed or ambiguous order is NEVER silently retried. Retrying a
    request whose actual outcome is unknown risks a duplicate order --
    worse than doing nothing. Callers decide what to do with a failure;
    this module never resends on their behalf.
  - The sandbox coordinator journals intent and reconciles broker receipts;
    see sandbox_execution.py.

Tradier's order endpoint is form-encoded (application/x-www-form-urlencoded),
not JSON -- a real, easy mistake to make by copying the JSON pattern from
tradier_client.py, so this is deliberately its own request-building code
rather than reusing TradierRestClient's _get().
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

import requests

from .tradier_client import TradierRestClient, TradierAuthError


class TradierOrderError(RuntimeError):
    """Raised when Tradier rejects an order or the request itself fails.
    Never includes the access token; includes Tradier's own error detail
    when available, since that's what a caller needs to fix the problem.
    """


@dataclass(frozen=True)
class OrderResult:
    order_id: Optional[str]
    status: str  # Tradier's own status string, e.g. "ok"
    raw: Dict[str, Any]


VALID_SIDES = {"buy_to_open", "sell_to_close", "buy_to_close", "sell_to_open"}


def _validate(account_id: str, occ_symbol: str, underlying_symbol: str, side: str, quantity: int) -> None:
    if not account_id:
        raise TradierOrderError("account_id is required and was empty.")
    if not occ_symbol:
        raise TradierOrderError("occ_symbol is required and was empty -- refusing to place an order with no contract.")
    if not underlying_symbol:
        raise TradierOrderError("underlying_symbol is required and was empty.")
    if side not in VALID_SIDES:
        raise TradierOrderError(f"side must be one of {sorted(VALID_SIDES)}, got {side!r}.")
    if not isinstance(quantity, int) or quantity <= 0:
        raise TradierOrderError(f"quantity must be a positive integer, got {quantity!r}.")


class TradierOrderClient:
    def __init__(self, rest_client: TradierRestClient, account_id: str):
        if not account_id:
            raise TradierOrderError(
                "TRADIER_ACCOUNT_ID is not set. Set it as an environment variable "
                "(see README.md for the private credentials file) -- order placement refuses to guess an account."
            )
        self.rest_client = rest_client
        self.account_id = account_id

    def _place(self, occ_symbol: str, underlying_symbol: str, side: str, quantity: int,
               order_type: str = "market", limit_price: Optional[float] = None,
               duration: str = "day", preview: bool = False,
               tag: Optional[str] = None) -> OrderResult:
        _validate(self.account_id, occ_symbol, underlying_symbol, side, quantity)
        if order_type not in {"market", "limit"} or duration != "day":
            raise TradierOrderError("Only market/limit day option orders are supported.")
        if order_type == "limit" and (limit_price is None or limit_price <= 0):
            raise TradierOrderError("order_type='limit' requires a positive limit_price.")

        payload = {
            "class": "option",
            "symbol": underlying_symbol,
            "option_symbol": occ_symbol,
            "side": side,
            "quantity": str(quantity),
            "type": order_type,
            "duration": duration,
        }
        if order_type == "limit":
            payload["price"] = f"{limit_price:.2f}"
        if preview:
            payload["preview"] = "true"
        if tag:
            payload["tag"] = tag

        url = f"{self.rest_client.config.base_url}/accounts/{self.account_id}/orders"
        try:
            resp = self.rest_client._session.post(
                url, data=payload, headers={**self.rest_client._headers(),
                    "Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.rest_client.config.timeout_sec,
            )
        except requests.RequestException as exc:
            raise TradierOrderError(f"Order request failed before reaching Tradier: {exc}") from None

        if resp.status_code == 401:
            raise TradierAuthError("Tradier rejected the configured token (401) placing an order.")

        try:
            body = resp.json()
        except ValueError:
            raise TradierOrderError(f"Tradier returned a non-JSON response (HTTP {resp.status_code}).") from None

        if resp.status_code >= 400 or "errors" in body:
            detail = body.get("errors", {}).get("error", body)
            raise TradierOrderError(f"Tradier rejected the order (HTTP {resp.status_code}): {detail}")

        order = body.get("order", {})
        if not isinstance(order, dict) or order.get("result") is False:
            raise TradierOrderError(f"Tradier did not accept the {'preview' if preview else 'order'}: {body}")
        return OrderResult(order_id=str(order.get("id")) if order.get("id") is not None else None,
                            status=str(order.get("status", "unknown")), raw=body)

    def preview(self, occ_symbol: str, underlying_symbol: str, side: str,
                quantity: int, *, tag: Optional[str] = None,
                order_type: str = "market", limit_price: Optional[float] = None) -> OrderResult:
        return self._place(occ_symbol, underlying_symbol, side, quantity,
                           order_type=order_type, limit_price=limit_price,
                           preview=True, tag=tag)

    def cancel_order(self, order_id: str) -> None:
        if not str(order_id).isdigit():
            raise TradierOrderError("A numeric broker order ID is required to cancel.")
        try:
            response = self.rest_client._session.delete(
                f"{self.rest_client.config.base_url}/accounts/{self.account_id}/orders/{order_id}",
                headers=self.rest_client._headers(), timeout=self.rest_client.config.timeout_sec)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise TradierOrderError(f"Cancel request outcome unknown: {exc}") from None

    def _get_account(self, path: str, params: Optional[dict] = None) -> dict:
        body = self.rest_client._get(f"/accounts/{self.account_id}/{path}", params or {})
        if not isinstance(body, dict):
            raise TradierOrderError(f"Unexpected Tradier {path} response: expected a JSON object.")
        return body

    @staticmethod
    def _rows(body: dict, collection: str, item: str) -> List[dict]:
        value = body.get(collection)
        # Tradier can represent an empty collection as JSON null or the
        # literal string "null" rather than an object with an empty array.
        if value is None or value == "null" or value == "":
            return []
        if not isinstance(value, dict):
            raise TradierOrderError(f"Unexpected Tradier {collection} response type: {type(value).__name__}.")
        rows = value.get(item)
        if rows is None or rows == "null" or rows == "":
            return []
        rows = [rows] if isinstance(rows, dict) else rows
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise TradierOrderError(f"Unexpected Tradier {collection}.{item} response type.")
        return rows

    def get_order(self, order_id: str) -> dict:
        if not str(order_id).isdigit():
            raise TradierOrderError("A numeric broker order ID is required.")
        body = self._get_account(f"orders/{order_id}")
        order = body.get("order")
        if not isinstance(order, dict) or str(order.get("id")) != str(order_id):
            raise TradierOrderError(f"Order {order_id} was not returned by Tradier.")
        return order

    def positions(self) -> List[dict]:
        return self._rows(self._get_account("positions"), "positions", "position")

    def orders(self) -> List[dict]:
        return self._rows(self._get_account("orders", {"limit": 1000, "includeTags": "true"}),
                          "orders", "order")

    def buy_to_open(self, occ_symbol: str, underlying_symbol: str, quantity: int,
                     order_type: str = "market", limit_price: Optional[float] = None,
                     *, tag: Optional[str] = None) -> OrderResult:
        return self._place(occ_symbol, underlying_symbol, "buy_to_open", quantity, order_type, limit_price, tag=tag)

    def sell_to_close(self, occ_symbol: str, underlying_symbol: str, quantity: int,
                       order_type: str = "market", limit_price: Optional[float] = None,
                       *, tag: Optional[str] = None) -> OrderResult:
        return self._place(occ_symbol, underlying_symbol, "sell_to_close", quantity, order_type, limit_price, tag=tag)
