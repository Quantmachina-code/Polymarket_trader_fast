from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass

import websockets

from latency_bot.config import Settings
from latency_bot.utils import log_json


@dataclass(slots=True)
class BinanceMoveEvent:
    symbol: str
    move: float
    last_price: float
    reference_price: float
    timestamp: float


class BinanceListener:
    def __init__(self, settings: Settings, logger, queue: asyncio.Queue[BinanceMoveEvent]) -> None:
        self.settings = settings
        self.logger = logger
        self.queue = queue
        self._last_price: dict[str, float] = {}
        self._snapshot_price: dict[str, float] = {}
        self._last_snapshot_ts: dict[str, float] = {}

    async def run(self) -> None:
        stream_names = "/".join(f"{s.lower()}usdt@trade" for s in self.settings.tracked_symbols)
        url = f"{self.settings.binance_ws_url}/{stream_names}"

        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    log_json(self.logger, "binance_connected", url=url)
                    async for raw in ws:
                        await self._handle_message(raw)
            except Exception as exc:  # noqa: BLE001
                log_json(self.logger, "binance_error", error=str(exc))
                await asyncio.sleep(2)

    async def _handle_message(self, raw: str) -> None:
        msg = json.loads(raw)
        data = msg.get("data", msg)

        symbol_raw = str(data.get("s", ""))
        if not symbol_raw.endswith("USDT"):
            return
        symbol = symbol_raw.replace("USDT", "")

        price = float(data.get("p") or data.get("price"))
        ts = float(data.get("T") or data.get("E") or time.time() * 1000) / 1000.0

        self._last_price[symbol] = price

        snap = self._snapshot_price.get(symbol)
        last_snap_ts = self._last_snapshot_ts.get(symbol, 0.0)

        if snap is None or (ts - last_snap_ts) >= self.settings.binance_snapshot_seconds:
            self._snapshot_price[symbol] = price
            self._last_snapshot_ts[symbol] = ts
            return

        move = (price - snap) / snap

        for threshold in self.settings.thresholds:
            if abs(move) >= threshold:
                event = BinanceMoveEvent(
                    symbol=symbol,
                    move=move,
                    last_price=price,
                    reference_price=snap,
                    timestamp=ts,
                )
                if self.queue.full():
                    log_json(self.logger, "binance_queue_full", symbol=symbol)
                    return
                await self.queue.put(event)
                log_json(
                    self.logger,
                    "binance_move_detected",
                    symbol=symbol,
                    move=move,
                    price=price,
                    reference_price=snap,
                )
                self._snapshot_price[symbol] = price
                self._last_snapshot_ts[symbol] = ts
                return
