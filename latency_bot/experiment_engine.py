from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass

from latency_bot.binance_listener import BinanceMoveEvent
from latency_bot.config import Settings, classify_phase
from latency_bot.edge_model import edge_from_prices
from latency_bot.market_discovery import MarketInfo
from latency_bot.performance_tracker import ExperimentResult, PerformanceTracker
from latency_bot.polymarket_listener import PolymarketListener
from latency_bot.utils import log_json


@dataclass(slots=True)
class ExperimentContext:
    symbol: str
    threshold: float
    wait_time: int
    phase: str
    binance_move: float
    trigger_time: float
    market: MarketInfo


class ExperimentEngine:
    def __init__(
        self,
        settings: Settings,
        logger,
        moves_queue: asyncio.Queue[BinanceMoveEvent],
        polymarket_listener: PolymarketListener,
        performance: PerformanceTracker,
    ) -> None:
        self.settings = settings
        self.logger = logger
        self.moves_queue = moves_queue
        self.polymarket_listener = polymarket_listener
        self.performance = performance
        self._markets_by_symbol: dict[str, list[MarketInfo]] = {}

    async def update_markets(self, markets: list[MarketInfo]) -> None:
        by_symbol: dict[str, list[MarketInfo]] = {}
        for market in markets:
            by_symbol.setdefault(market.symbol, []).append(market)
        self._markets_by_symbol = by_symbol
        token_ids = [m.token_id for m in markets]
        await self.polymarket_listener.update_subscriptions(token_ids)

    async def run(self) -> None:
        while True:
            event = await self.moves_queue.get()
            await self._schedule_experiments(event)

    async def _schedule_experiments(self, event: BinanceMoveEvent) -> None:
        markets = self._markets_by_symbol.get(event.symbol, [])
        if not markets:
            log_json(self.logger, "no_polymarket_market", symbol=event.symbol)
            return

        for threshold in self.settings.thresholds:
            if abs(event.move) < threshold:
                continue
            for wait_time in self.settings.wait_times_seconds:
                for market in markets:
                    context = ExperimentContext(
                        symbol=event.symbol,
                        threshold=threshold,
                        wait_time=wait_time,
                        phase=classify_phase(wait_time),
                        binance_move=event.move,
                        trigger_time=event.timestamp,
                        market=market,
                    )
                    asyncio.create_task(self._run_experiment(context))
                    log_json(
                        self.logger,
                        "experiment_created",
                        symbol=context.symbol,
                        threshold=context.threshold,
                        wait_time=context.wait_time,
                        market_id=context.market.market_id,
                    )

    async def _run_experiment(self, context: ExperimentContext) -> None:
        await asyncio.sleep(context.wait_time)
        quote = await self.polymarket_listener.wait_for_quote(context.market.token_id, timeout=3)
        if quote is None:
            log_json(self.logger, "experiment_skipped_no_quote", token_id=context.market.token_id)
            return

        entry_time = time.time()
        entry_price = quote.mid_price
        edge = edge_from_prices(context.binance_move, entry_price, self.settings.sensitivity)

        immediate_exit_price = await self._simulate_immediate_exit(context.market.token_id, entry_price)
        immediate_pnl = immediate_exit_price - entry_price

        resolution_exit_price = self._simulate_resolution_exit(context.binance_move)
        resolution_pnl = resolution_exit_price - entry_price

        result = ExperimentResult(
            id=str(uuid.uuid4()),
            symbol=context.symbol,
            market_id=context.market.market_id,
            token_id=context.market.token_id,
            threshold=context.threshold,
            wait_time=context.wait_time,
            phase=context.phase,
            binance_move=context.binance_move,
            trigger_time=context.trigger_time,
            entry_time=entry_time,
            entry_price=entry_price,
            immediate_exit_price=immediate_exit_price,
            resolution_exit_price=resolution_exit_price,
            edge=edge,
            immediate_pnl=immediate_pnl,
            resolution_pnl=resolution_pnl,
            created_ts=time.time(),
        )

        await self.performance.record(result)
        log_json(
            self.logger,
            "experiment_completed",
            symbol=context.symbol,
            market_id=context.market.market_id,
            entry_price=entry_price,
            immediate_exit_price=immediate_exit_price,
            edge=edge,
            immediate_pnl=immediate_pnl,
            resolution_pnl=resolution_pnl,
        )

    async def _simulate_immediate_exit(self, token_id: str, entry_price: float) -> float:
        deadline = time.time() + self.settings.immediate_exit_timeout_seconds
        while time.time() < deadline:
            await asyncio.sleep(1)
            quote = self.polymarket_listener.get_quote(token_id)
            if quote is None:
                continue
            if abs(quote.mid_price - entry_price) > 0.002:
                return quote.mid_price
        return entry_price

    @staticmethod
    def _simulate_resolution_exit(binance_move: float) -> float:
        return 1.0 if binance_move > 0 else 0.0
