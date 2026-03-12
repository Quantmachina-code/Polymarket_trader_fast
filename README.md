# Polymarket–Binance Latency Discovery Bot

Async paper-trading research bot that listens to **Binance spot trades** and **Polymarket orderbook feeds**, detects lag opportunities, and simulates entries/exits across multiple parameter sets.

## Features

- Live Binance trade stream monitoring for BTC/ETH/SOL/XRP.
- Polymarket market discovery from Gamma API (active 5m / 15m markets).
- Live Polymarket websocket quote ingestion (best bid/ask + mid).
- Multi-parameter experiment generation:
  - thresholds: `0.05%, 0.08%, 0.10%, 0.12%, 0.15%`
  - wait windows: `[2,5,8,11,14,17,20,23]`
  - phase classification: EARLY / MID / LATE
- Edge model:
  - `implied_probability = 0.5 + move * sensitivity`
  - `edge = implied_probability - polymarket_price`
- Two exit simulations:
  - Immediate arbitrage-style exit (price converges)
  - Hold-to-resolution proxy outcome
- Persistent storage in SQLite + CSV.
- JSON logs for all major events.
- 5-minute summary aggregation table.
- **Dry-run only** (`dry_run=True`), no real orders.

## Project Structure

```text
latency_bot/
  config.py
  main.py
  binance_listener.py
  polymarket_listener.py
  market_discovery.py
  experiment_engine.py
  performance_tracker.py
  edge_model.py
  storage.py
  utils.py
main.py
```

## Requirements

- Python 3.11+
- `aiohttp`
- `websockets`
- `pandas`
- `numpy`

Install dependencies:

```bash
python -m pip install aiohttp websockets pandas numpy
```

## Run

```bash
python main.py
```

## Output

- JSON logs to stdout, including move detection and experiment lifecycle events.
- 5-minute report table grouped by:
  - symbol
  - threshold
  - wait_time
  - phase
- Storage files:
  - `latency_bot/results.sqlite3`
  - `latency_bot/results.csv`

## Notes

- Polymarket websocket payload formats vary by channel/market type; parser includes flexible extraction for common best bid/ask and orderbook shapes.
- This implementation is designed for **research/paper simulation**, not execution.
