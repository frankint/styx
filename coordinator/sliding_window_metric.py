from collections import deque
from dataclasses import dataclass
import time


@dataclass
class TimestampedSample:
    timestamp: float
    value: float


class SlidingWindowMetric:
    """Time-based sliding window for metrics aggregation."""

    def __init__(self, window_seconds: float) -> None:
        self.window_seconds = window_seconds
        self.samples: deque[TimestampedSample] = deque()

    def add(self, value: float) -> None:
        """Add a sample with current timestamp."""
        self.samples.append(TimestampedSample(time.time(), value))
        self._prune_old_samples()

    def _prune_old_samples(self) -> None:
        """Remove samples older than the window."""
        cutoff = time.time() - self.window_seconds
        while self.samples and self.samples[0].timestamp < cutoff:
            self.samples.popleft()

    def average(self) -> float | None:
        """Get average of all samples in window."""
        self._prune_old_samples()
        if not self.samples:
            return None
        return sum(s.value for s in self.samples) / len(self.samples)
