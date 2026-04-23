"""Offline batch downloader rate limiter.

Delegates to the canonical implementation in
akshare_data.ingestion.router.DomainRateLimiter.
"""
# ruff: noqa: E402

from __future__ import annotations

import logging
from typing import Dict, Optional

from akshare_data.ingestion.router import (
    DomainRateLimiter as _RouterDomainRateLimiter,
)

logger = logging.getLogger("akshare_data")


class DomainRateLimiter:
    """Offline batch domain rate limiter.

    Delegates to ``akshare_data.ingestion.router.DomainRateLimiter``.
    Accepts a plain ``intervals`` dict for backward compatibility.
    """

    def __init__(self, intervals: Optional[Dict[str, float]] = None):
        self._limiter = _RouterDomainRateLimiter(
            intervals=intervals or {},
            domain_map={},
        )

    def wait(self, key: str = "default") -> None:
        self._limiter.wait_if_needed(key)

    def set_interval(self, rate_key: str, interval: float) -> None:
        self._limiter.set_interval(rate_key, interval)

    def get_interval(self, key: str) -> float:
        return self._limiter.get_interval(key)

    def reset(self) -> None:
        self._limiter.reset()
