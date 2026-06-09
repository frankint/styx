from __future__ import annotations


class WorkerCapacityModel:
    """Estimates per-worker max throughput via batch-weighted EWMA of per-txn cost."""

    def __init__(
        self,
        epoch_max_size: int,
        min_batch_threshold: int = 50,
        base_alpha: float = 0.15,
    ) -> None:
        self.epoch_max_size = epoch_max_size
        self.min_batch_threshold = min_batch_threshold
        self.base_alpha = base_alpha
        self.min_weight_threshold = 0.5

        self._per_txn_cost_ewma: float | None = None
        self._max_observed_batch: int = 0

    def record(self, batch_size: int, epoch_latency_ms: float) -> None:
        """Record an epoch's metrics and update the per-txn cost EWMA.
        Args:
            batch_size: Number of transactions processed in this epoch (total_txns)
            epoch_latency_ms: Total epoch latency in milliseconds
        """
        if batch_size < self.min_batch_threshold or epoch_latency_ms <= 0:
            return

        per_txn_cost = epoch_latency_ms / batch_size

        # Track max observed batch for confidence calculation
        self._max_observed_batch = max(self._max_observed_batch, batch_size)

        # Weight the EWMA alpha by batch size -- larger batches (more accurate)
        # should dominate the estimate
        weight = min(batch_size / self.epoch_max_size, 1.0)
        effective_alpha = self.base_alpha * weight

        if self._per_txn_cost_ewma is None:
            self._per_txn_cost_ewma = per_txn_cost
        else:
            # Only allow cost to increase (capacity decrease) with high-confidence observations
            if per_txn_cost > self._per_txn_cost_ewma and weight < self.min_weight_threshold:
                return
            self._per_txn_cost_ewma += effective_alpha * (per_txn_cost - self._per_txn_cost_ewma)

    def estimate_max_tps(self) -> float | None:
        """Estimate maximum sustainable TPS for this worker.
        Returns:
            Estimated max TPS, or None if no data recorded yet.
        """
        if self._per_txn_cost_ewma is None or self._per_txn_cost_ewma <= 0:
            return None
        return 1000.0 / self._per_txn_cost_ewma  # ms to second conversion

    @property
    def confidence(self) -> float:
        """Confidence score based on max observed batch size.
        Returns a value in [0, 1] indicating how reliable the estimate is.
        At confidence=0.25 (batch=250/1000), predictive scaling can kick in.
        """
        if self._max_observed_batch == 0:
            return 0.0
        return min(self._max_observed_batch / self.epoch_max_size, 1.0)

    def reset(self) -> None:
        """Reset the model state."""
        self._per_txn_cost_ewma = None
        self._max_observed_batch = 0


class SystemCapacityEstimator:
    """Aggregates per-worker models into a system-level capacity estimate."""

    def __init__(
        self,
        sequence_max_size: int = 1000,
        min_batch_threshold: int = 50,
        base_alpha: float = 0.15,
    ) -> None:
        self.sequence_max_size = sequence_max_size
        self.min_batch_threshold = min_batch_threshold
        self.base_alpha = base_alpha
        self._models: dict[int, WorkerCapacityModel] = {}

    def get_model(self, worker_id: int) -> WorkerCapacityModel:
        if worker_id not in self._models:
            self._models[worker_id] = WorkerCapacityModel(
                self.sequence_max_size,
                self.sequence_max_size // 10,
                self.base_alpha,
            )
        return self._models[worker_id]

    def record(
        self,
        worker_id: int,
        total_txns: int,
        epoch_latency_ms: float,
    ) -> None:
        """Record epoch metrics for a worker.
        Args:
            worker_id: The worker ID
            total_txns: Total transactions processed in this epoch
            epoch_latency_ms: Total epoch latency in milliseconds
        """
        self.get_model(worker_id).record(total_txns, epoch_latency_ms)

    def estimate_system_capacity(self) -> float | None:
        """Return estimated total system TPS across all workers.
        Uses the minimum per-worker capacity (the bottleneck) multiplied
        by the number of workers.
        """
        if not self._models:
            return None

        per_worker: list[float] = []
        for model in self._models.values():
            est = model.estimate_max_tps()
            if est is not None:
                per_worker.append(est)

        if not per_worker:
            return None

        bottleneck = min(per_worker)
        return bottleneck * len(self._models)

    @property
    def confidence(self) -> float:
        """System-level confidence as the minimum worker confidence."""
        if not self._models:
            return 0.0

        confidences = [model.confidence for model in self._models.values()]
        if not confidences:
            return 0.0

        return min(confidences)

    def remove_worker(self, worker_id: int) -> None:
        self._models.pop(worker_id, None)
