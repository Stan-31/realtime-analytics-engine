"""Per-symbol sliding-window aggregator.

Single-asyncio-thread, lock-free. Pure in-memory deque per symbol. Snapshots
return moving averages; symbols with zero samples in the window are skipped
so the chart never receives null points.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque


@dataclass(frozen=True)
class Aggregate:
    ts: float
    symbol: str
    avg_price: float
    sample_count: int


class Aggregator:
    def __init__(self, window_seconds: float) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._window = window_seconds
        self._series: dict[str, Deque[tuple[float, float]]] = defaultdict(deque)

    def add(self, symbol: str, price: float, ts: float) -> None:
        self._series[symbol].append((ts, price))

    def _evict(self, now: float) -> None:
        cutoff = now - self._window
        for buf in self._series.values():
            while buf and buf[0][0] < cutoff:
                buf.popleft()

    def snapshot(self, now: float | None = None) -> list[Aggregate]:
        if now is None:
            now = time.time()
        self._evict(now)
        out: list[Aggregate] = []
        for symbol, buf in self._series.items():
            n = len(buf)
            if n == 0:
                continue
            total = 0.0
            for _, price in buf:
                total += price
            out.append(
                Aggregate(
                    ts=now,
                    symbol=symbol,
                    avg_price=total / n,
                    sample_count=n,
                )
            )
        # Stable ordering helps tests and gives the dashboard a consistent
        # iteration order across snapshots.
        out.sort(key=lambda a: a.symbol)
        return out
