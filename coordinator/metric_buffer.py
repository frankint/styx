from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
import threading
import time


@dataclass
class MetricSample:
    timestamp: float
    value: float


@dataclass
class MetricBuffer:
    """Bounded, thread-safe ring buffer for multiple named time series."""

    max_len: int = 512
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _series: dict[str, deque[MetricSample]] = field(default_factory=dict, repr=False)

    def add(self, name: str, value: float, ts: float | None = None) -> None:
        ts = ts or time.time()
        with self._lock:
            if name not in self._series:
                self._series[name] = deque(maxlen=self.max_len)
            self._series[name].append(MetricSample(ts, value))

    def snapshot(self) -> dict[str, list[float]]:
        """Return a {name: [values]} snapshot suitable for Chronos context."""
        with self._lock:
            return {name: [s.value for s in buf] for name, buf in self._series.items() if buf}

    def snapshot_with_timestamps(self) -> dict[str, list[tuple[float, float]]]:
        """Return {name: [(ts, value), ...]} for covariate-aware models."""
        with self._lock:
            return {name: [(s.timestamp, s.value) for s in buf] for name, buf in self._series.items() if buf}

    def length(self, name: str) -> int:
        with self._lock:
            return len(self._series.get(name, []))


@dataclass
class AggregatingMetricBuffer:
    """
    Time-bucketed metric buffer that aggregates values into fixed intervals.
    Raw counts are summed within each time bucket (e.g., 1 second),
    producing per-second rates for input rate forecasting.
    """

    bucket_interval: float = 1.0  # seconds
    max_buckets: int = 512
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    # {metric_name: {bucket_timestamp: accumulated_value}}
    _pending: dict[str, dict[int, float]] = field(default_factory=lambda: defaultdict(dict), repr=False)
    # {metric_name: deque of (bucket_ts, aggregated_value)}
    _finalized: dict[str, deque[tuple[int, float]]] = field(default_factory=dict, repr=False)

    def _bucket_ts(self, ts: float) -> int:
        # Floor timestamp to bucket boundary
        return int(ts // self.bucket_interval)

    def add(self, name: str, value: float, ts: float | None = None) -> None:
        ts = ts or time.time()
        bucket = self._bucket_ts(ts)
        current_bucket = self._bucket_ts(time.time())
        with self._lock:
            if name not in self._finalized:
                self._finalized[name] = deque(maxlen=self.max_buckets)
            if bucket not in self._pending[name]:
                self._pending[name][bucket] = 0.0
            self._pending[name][bucket] += value

            # Finalize any buckets that are complete (older than current)
            buckets_to_finalize = [b for b in self._pending[name] if b < current_bucket]
            for b in sorted(buckets_to_finalize):
                self._finalized[name].append((b, self._pending[name].pop(b)))

    def snapshot(self) -> dict[str, list[float]]:
        """Return {name: [aggregated_values]} for Chronos context (values only)."""
        with self._lock:
            return {name: [v for _, v in buf] for name, buf in self._finalized.items() if buf}

    def snapshot_with_timestamps(self) -> dict[str, list[tuple[float, float]]]:
        """Return {name: [(bucket_ts, aggregated_value), ...]}."""
        with self._lock:
            return {
                name: [(float(ts) * self.bucket_interval, v) for ts, v in buf]
                for name, buf in self._finalized.items()
                if buf
            }

    def length(self, name: str) -> int:
        with self._lock:
            return len(self._finalized.get(name, []))
