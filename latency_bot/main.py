from __future__ import annotations

import asyncio

from latency_bot.binance_listener import BinanceListener
from latency_bot.config import SETTINGS
from latency_bot.experiment_engine import ExperimentEngine
from latency_bot.market_discovery import MarketDiscovery
from latency_bot.performance_tracker import PerformanceTracker
from latency_bot.polymarket_listener import PolymarketListener
from latency_bot.storage import Storage
from latency_bot.utils import setup_logger


async def run() -> None:
    logger = setup_logger()

    move_queue = asyncio.Queue(maxsize=SETTINGS.max_queue_size)
    storage = Storage(SETTINGS.sqlite_path, SETTINGS.csv_path)
    performance = PerformanceTracker(storage, logger)

    binance = BinanceListener(SETTINGS, logger, move_queue)
    polymarket = PolymarketListener(SETTINGS.polymarket_ws_url, logger)
    discovery = MarketDiscovery(SETTINGS, logger)
    engine = ExperimentEngine(SETTINGS, logger, move_queue, polymarket, performance)

    markets = await discovery.discover()
    await engine.update_markets(markets)

    await asyncio.gather(
        binance.run(),
        polymarket.run(),
        engine.run(),
        performance.report_loop(SETTINGS.report_interval_seconds),
    )


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
