from __future__ import annotations


def implied_probability(move: float, sensitivity: float) -> float:
    value = 0.5 + (move * sensitivity)
    return min(0.999, max(0.001, value))


def edge_from_prices(binance_move: float, polymarket_price: float, sensitivity: float) -> float:
    implied = implied_probability(binance_move, sensitivity)
    return implied - polymarket_price
