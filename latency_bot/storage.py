from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Iterable


class Storage:
    def __init__(self, sqlite_path: Path, csv_path: Path) -> None:
        self.sqlite_path = sqlite_path
        self.csv_path = csv_path
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS experiments (
                    id TEXT PRIMARY KEY,
                    symbol TEXT,
                    market_id TEXT,
                    token_id TEXT,
                    threshold REAL,
                    wait_time INTEGER,
                    phase TEXT,
                    binance_move REAL,
                    trigger_time REAL,
                    entry_time REAL,
                    entry_price REAL,
                    immediate_exit_price REAL,
                    resolution_exit_price REAL,
                    edge REAL,
                    immediate_pnl REAL,
                    resolution_pnl REAL,
                    created_ts REAL
                )
                """
            )

    def insert_rows(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows:
            return

        with sqlite3.connect(self.sqlite_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO experiments (
                    id, symbol, market_id, token_id, threshold, wait_time, phase, binance_move,
                    trigger_time, entry_time, entry_price, immediate_exit_price,
                    resolution_exit_price, edge, immediate_pnl, resolution_pnl, created_ts
                ) VALUES (
                    :id, :symbol, :market_id, :token_id, :threshold, :wait_time, :phase, :binance_move,
                    :trigger_time, :entry_time, :entry_price, :immediate_exit_price,
                    :resolution_exit_price, :edge, :immediate_pnl, :resolution_pnl, :created_ts
                )
                """,
                rows,
            )

        write_header = not self.csv_path.exists()
        with self.csv_path.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            if write_header:
                writer.writeheader()
            writer.writerows(rows)
