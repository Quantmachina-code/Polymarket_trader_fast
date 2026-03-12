from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass

import pandas as pd

from latency_bot.storage import Storage


@dataclass(slots=True)
class ExperimentResult:
    id: str
    symbol: str
    market_id: str
    token_id: str
    threshold: float
    wait_time: int
    phase: str
    binance_move: float
    trigger_time: float
    entry_time: float
    entry_price: float
    immediate_exit_price: float
    resolution_exit_price: float
    edge: float
    immediate_pnl: float
    resolution_pnl: float
    created_ts: float


class PerformanceTracker:
    def __init__(self, storage: Storage, logger) -> None:
        self.storage = storage
        self.logger = logger
        self._rows: list[ExperimentResult] = []
        self._lock = asyncio.Lock()

    async def record(self, result: ExperimentResult) -> None:
        async with self._lock:
            self._rows.append(result)
            self.storage.insert_rows([asdict(result)])

    async def report_loop(self, interval_seconds: int) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            await self.print_report()

    async def print_report(self) -> None:
        async with self._lock:
            if not self._rows:
                print("===== 5 MIN REPORT =====\nNo experiments recorded yet.")
                return
            df = pd.DataFrame([asdict(r) for r in self._rows])

        grouped = (
            df.groupby(["symbol", "threshold", "wait_time", "phase"], as_index=False)
            .agg(
                trade_count=("id", "count"),
                avg_edge=("edge", "mean"),
                cumulative_edge=("edge", "sum"),
                avg_pnl=("immediate_pnl", "mean"),
                cumulative_pnl=("immediate_pnl", "sum"),
                avg_resolution_pnl=("resolution_pnl", "mean"),
                cumulative_resolution_pnl=("resolution_pnl", "sum"),
            )
            .sort_values(["symbol", "threshold", "wait_time"])
        )

        print("\n===== 5 MIN REPORT =====")
        print(grouped.to_string(index=False, float_format=lambda x: f"{x:0.6f}"))
