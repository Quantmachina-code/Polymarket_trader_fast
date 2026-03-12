from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aiohttp

from latency_bot.config import Settings
from latency_bot.utils import log_json


@dataclass(slots=True)
class MarketInfo:
    market_id: str
    token_id: str
    symbol: str
    horizon_minutes: int
    question: str
    resolution_time: datetime | None


_SYMBOL_PATTERNS = {
    "BTC": re.compile(r"\bBTC\b|bitcoin", re.IGNORECASE),
    "ETH": re.compile(r"\bETH\b|ethereum", re.IGNORECASE),
    "SOL": re.compile(r"\bSOL\b|solana", re.IGNORECASE),
    "XRP": re.compile(r"\bXRP\b|ripple", re.IGNORECASE),
}


class MarketDiscovery:
    def __init__(self, settings: Settings, logger) -> None:
        self.settings = settings
        self.logger = logger

    async def discover(self) -> list[MarketInfo]:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            params = {"active": "true", "closed": "false", "limit": 500}
            async with session.get(self.settings.polymarket_gamma_url, params=params) as response:
                response.raise_for_status()
                payload = await response.json()

        records = payload if isinstance(payload, list) else payload.get("data", [])
        discovered: list[MarketInfo] = []

        for item in records:
            market = self._parse_market(item)
            if market:
                discovered.append(market)

        log_json(self.logger, "market_discovery", discovered=len(discovered))
        return discovered

    def _parse_market(self, item: dict[str, Any]) -> MarketInfo | None:
        question = str(item.get("question") or item.get("title") or "")
        if not question:
            return None

        symbol = next((sym for sym, pattern in _SYMBOL_PATTERNS.items() if pattern.search(question)), None)
        if symbol not in self.settings.tracked_symbols:
            return None

        horizon = self._extract_horizon_minutes(question)
        if horizon not in self.settings.horizons_minutes:
            return None

        token_id = self._extract_token_id(item)
        if not token_id:
            return None

        resolution_time = self._extract_resolution_time(item)
        market_id = str(item.get("id") or item.get("marketId") or token_id)

        return MarketInfo(
            market_id=market_id,
            token_id=token_id,
            symbol=symbol,
            horizon_minutes=horizon,
            question=question,
            resolution_time=resolution_time,
        )

    @staticmethod
    def _extract_horizon_minutes(question: str) -> int | None:
        lowered = question.lower()
        if "5 minute" in lowered or "5 min" in lowered:
            return 5
        if "15 minute" in lowered or "15 min" in lowered:
            return 15
        return None

    @staticmethod
    def _extract_token_id(item: dict[str, Any]) -> str | None:
        for key in ("token_id", "tokenId", "clobTokenId", "outcomeTokenId"):
            if item.get(key):
                return str(item[key])

        tokens = item.get("tokens") or item.get("outcomes")
        if isinstance(tokens, list) and tokens:
            first = tokens[0]
            if isinstance(first, dict):
                for key in ("token_id", "tokenId", "clobTokenId"):
                    if first.get(key):
                        return str(first[key])
            elif isinstance(first, str):
                return first
        return None

    @staticmethod
    def _extract_resolution_time(item: dict[str, Any]) -> datetime | None:
        for key in ("endDate", "resolveDate", "resolution_time", "end_time"):
            value = item.get(key)
            if not value:
                continue
            try:
                return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
            except ValueError:
                continue
        return None


async def periodic_market_refresh(discovery: MarketDiscovery, interval_seconds: int, sink) -> None:
    while True:
        markets = await discovery.discover()
        await sink(markets)
        await asyncio.sleep(interval_seconds)
