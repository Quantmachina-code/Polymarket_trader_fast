from __future__ import annotations

import json
import logging
import time
from typing import Any


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("latency_bot")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def log_json(logger: logging.Logger, event: str, **payload: Any) -> None:
    record = {"event": event, "ts": time.time(), **payload}
    logger.info(json.dumps(record, default=str))
