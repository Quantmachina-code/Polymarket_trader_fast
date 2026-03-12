from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    dry_run: bool = True

    tracked_symbols: tuple[str, ...] = ("BTC", "ETH", "SOL", "XRP")
    horizons_minutes: tuple[int, ...] = (5, 15)

    thresholds: tuple[float, ...] = (0.0005, 0.0008, 0.0010, 0.0012, 0.0015)
    wait_times_seconds: tuple[int, ...] = (2, 5, 8, 11, 14, 17, 20, 23)
    sensitivity: float = 5.0

    report_interval_seconds: int = 300
    binance_snapshot_seconds: int = 1
    immediate_exit_timeout_seconds: int = 90

    binance_ws_url: str = "wss://stream.binance.com:9443/ws"
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    polymarket_gamma_url: str = "https://gamma-api.polymarket.com/markets"

    sqlite_path: Path = Path("latency_bot/results.sqlite3")
    csv_path: Path = Path("latency_bot/results.csv")

    max_queue_size: int = 10_000
    stale_market_seconds: int = 45


SETTINGS = Settings()


def classify_phase(wait_seconds: int) -> str:
    if wait_seconds <= 7:
        return "EARLY"
    if wait_seconds <= 14:
        return "MID"
    return "LATE"
