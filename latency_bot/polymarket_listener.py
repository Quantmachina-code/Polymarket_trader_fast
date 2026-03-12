from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

import websockets

from latency_bot.utils import log_json


@dataclass(slots=True)
class MarketQuote:
    token_id: str
    best_bid: float
    best_ask: float
    mid_price: float
    ts: float


class PolymarketListener:
    def __init__(self, ws_url: str, logger) -> None:
        self.ws_url = ws_url
        self.logger = logger
        self._token_ids: set[str] = set()
        self._quotes: dict[str, MarketQuote] = {}
        self._quote_events: dict[str, asyncio.Event] = {}

    async def update_subscriptions(self, token_ids: list[str]) -> None:
        self._token_ids = {str(t) for t in token_ids if t}
        for token_id in self._token_ids:
            self._quote_events.setdefault(token_id, asyncio.Event())
        log_json(self.logger, "polymarket_subscriptions_updated", token_count=len(self._token_ids))

    def get_quote(self, token_id: str) -> MarketQuote | None:
        return self._quotes.get(token_id)

    async def wait_for_quote(self, token_id: str, timeout: float = 5.0) -> MarketQuote | None:
        existing = self._quotes.get(token_id)
        if existing:
            return existing

        event = self._quote_events.setdefault(token_id, asyncio.Event())
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError:
            return None
        return self._quotes.get(token_id)

    async def run(self) -> None:
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    await self._send_subscriptions(ws)
                    log_json(self.logger, "polymarket_connected", token_count=len(self._token_ids))
                    async for raw in ws:
                        self._handle_message(raw)
            except Exception as exc:  # noqa: BLE001
                log_json(self.logger, "polymarket_error", error=str(exc))
                await asyncio.sleep(2)

    async def _send_subscriptions(self, ws: websockets.WebSocketClientProtocol) -> None:
        if not self._token_ids:
            return
        payloads = [
            {"type": "subscribe", "markets": list(self._token_ids)},
            {"event": "subscribe", "assets_ids": list(self._token_ids)},
        ]
        for payload in payloads:
            try:
                await ws.send(json.dumps(payload))
            except Exception:  # noqa: BLE001
                continue

    def _handle_message(self, raw: str) -> None:
        msg = json.loads(raw)
        token_id = str(msg.get("market") or msg.get("asset_id") or msg.get("token_id") or "")
        if not token_id:
            return

        bid, ask = self._extract_bid_ask(msg)
        if bid is None and ask is None:
            return

        if bid is None:
            bid = ask
        if ask is None:
            ask = bid
        mid = (bid + ask) / 2

        quote = MarketQuote(token_id=token_id, best_bid=bid, best_ask=ask, mid_price=mid, ts=time.time())
        self._quotes[token_id] = quote
        self._quote_events.setdefault(token_id, asyncio.Event()).set()

    @staticmethod
    def _extract_bid_ask(msg: dict[str, Any]) -> tuple[float | None, float | None]:
        if "best_bid" in msg or "best_ask" in msg:
            bid = msg.get("best_bid")
            ask = msg.get("best_ask")
            return (float(bid) if bid is not None else None, float(ask) if ask is not None else None)

        bids = msg.get("bids") or msg.get("buys") or []
        asks = msg.get("asks") or msg.get("sells") or []

        bid = float(bids[0][0] if bids and isinstance(bids[0], (list, tuple)) else bids[0].get("price")) if bids else None
        ask = float(asks[0][0] if asks and isinstance(asks[0], (list, tuple)) else asks[0].get("price")) if asks else None
        return bid, ask
