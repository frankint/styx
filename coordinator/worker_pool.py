import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
import heapq
import os
from typing import TYPE_CHECKING

from styx.common.logging import logging

if TYPE_CHECKING:
    from styx.common.base_operator import BaseOperator
    from styx.common.operator import Operator
    from styx.common.types import OperatorPartition

HEARTBEAT_LIMIT: int = int(os.getenv("HEARTBEAT_LIMIT", "5000"))  # 5000ms
MAX_WAIT_FOR_RESTARTS_SEC: int = int(os.getenv("MAX_WAIT_FOR_RESTARTS_SEC", "0"))  # 0s


@dataclass
class Worker:
    worker_id: int
    worker_ip: str
    worker_port: int
    protocol_port: int
    assigned_operators: dict[OperatorPartition, Operator]
    previous_heartbeat: float = 1_000_000.0

    @property
    def priority(self) -> int:
        return len(self.assigned_operators)

    @property
    def participating(self) -> int:
        return len(self.assigned_operators) != 0

    def __hash__(self) -> int:
        return hash(self.worker_id)

    def to_tuple(self) -> tuple[str, int, int]:
        return self.worker_ip, self.worker_port, self.protocol_port


class WorkerPool:
    def __init__(self) -> None:
        # in the case of worker failure
        self._tombstone = "<removed-worker>"
        # priority queue to be used for roundrobin scheduling
        self._queue: list[list[int | Worker | str]] = []
        self._standby_queue: deque[Worker] = deque()
        self._standby_worker_ids: set[int] = set()
        # index is used so that we have deterministic selection when priority is the same
        self._index: int = 0
        # Worker ids start from 1
        self.worker_counter: int = 1
        self.dead_worker_ids: list[int] = []
        self._worker_queue_idx: dict[int, list[int | Worker | str]] = {}
        self.operator_partition_to_worker: dict[OperatorPartition, int] = {}
        self.orphaned_operator_assignments: dict[OperatorPartition, Operator] = {}

    def get_live_workers(self) -> list[Worker]:
        """Returns all currently known (non-tombstoned) workers"""
        live: list[Worker] = []
        for _, _, worker in self._queue:
            if worker == self._tombstone:
                continue
            live.append(worker)
        return live

    def reset_all_assignments(self) -> None:
        """
        Clears all operator assignments and rebuilds the priority queue.

        This is useful for manual rebalance (e.g., after scaling up), where we want to
        redistribute partitions across *all* live workers.
        """
        # Make the live workers list deterministic by sorting by worker_id
        live_workers = sorted(self.get_live_workers(), key=lambda w: w.worker_id)
        for w in live_workers:
            w.assigned_operators = {}
        self.operator_partition_to_worker.clear()
        self.orphaned_operator_assignments.clear()

        # Rebuild heap and index maps
        self._queue = []
        self._worker_queue_idx = {}
        self._index = 0
        for w in live_workers:
            self.put(w)

    def register_worker(self, worker_ip: str, worker_port: int, protocol_port: int, standby: bool) -> int:
        if self.dead_worker_ids:
            worker_id: int = self.dead_worker_ids.pop()
        else:
            worker_id: int = self.worker_counter
            self.worker_counter += 1
        worker = Worker(
            worker_id=worker_id,
            worker_ip=worker_ip,
            worker_port=worker_port,
            protocol_port=protocol_port,
            assigned_operators={},
        )
        if standby:
            self._standby_queue.append(worker)
            self._standby_worker_ids.add(worker_id)
        else:
            self.put(worker)
        return worker_id

    def register_worker_heartbeat(self, worker_id: int, heartbeat_time: float) -> None:
        if not self.is_worker_active(worker_id):
            return
        try:
            self.peek(worker_id).previous_heartbeat = heartbeat_time
        except KeyError:
            logging.warning(
                f"Tried to register heartbeat for worker {worker_id} that does not exist {self._worker_queue_idx}",
            )

    def check_heartbeats(
        self,
        heartbeat_check_time: float,
    ) -> tuple[set[Worker], dict[int, float]]:
        """Checks active workers whether one failed"""
        failed_workers: set[Worker] = set()
        heartbeats_per_worker: dict[int, float] = {}
        for _, _, worker in self._queue:
            if worker == self._tombstone:
                # If it is a dead worker continue
                continue
            time_since_last_heartbeat_ms = (heartbeat_check_time - worker.previous_heartbeat) * 1000
            heartbeats_per_worker[worker.worker_id] = time_since_last_heartbeat_ms
            if time_since_last_heartbeat_ms > HEARTBEAT_LIMIT:
                logging.error(
                    f"Worker: {worker.worker_id} failed to register a heartbeat",
                )
                # Worker is considered dead
                dead_worker = self.remove_worker(worker.worker_id)
                if dead_worker.participating:
                    # If the worker was participating in the deployment
                    failed_workers.add(dead_worker)
                    self.dead_worker_ids.append(dead_worker.worker_id)
                    self.orphaned_operator_assignments |= dead_worker.assigned_operators
        return failed_workers, heartbeats_per_worker

    async def initiate_recovery(self, failed_workers: set[Worker]) -> None:
        logging.warning(
            f"Waiting for {MAX_WAIT_FOR_RESTARTS_SEC} seconds for workers {failed_workers} to reboot",
        )
        await asyncio.sleep(MAX_WAIT_FOR_RESTARTS_SEC)
        logging.warning("Rescheduling operators")
        for operator_partition, operator in self.orphaned_operator_assignments.items():
            self.schedule_operator_partition(operator_partition, operator)

    def put(self, worker: Worker) -> None:
        # O(log(n)) heappush
        entry: list = [worker.priority, self._index, worker]
        for operator_partition in worker.assigned_operators:
            self.operator_partition_to_worker[operator_partition] = worker.worker_id
        heapq.heappush(self._queue, entry)
        self._worker_queue_idx[worker.worker_id] = entry
        self._index += 1

    def schedule_operator_partition(
        self,
        operator_partition: OperatorPartition,
        operator: Operator | BaseOperator,
    ) -> None:
        """Add an operator partition using RoundRobin"""
        worker: Worker = self.pop()
        self.operator_partition_to_worker[operator_partition] = worker.worker_id
        worker.assigned_operators[operator_partition] = operator
        self.put(worker)

    def remove_operator_partition(self, operator_partition: OperatorPartition) -> None:
        """Downscale an operator by removing partitions"""
        worker_id = self.operator_partition_to_worker[operator_partition]
        # need to remove and put again because the priority changes
        worker: Worker = self.remove_worker(worker_id)
        del worker.assigned_operators[operator_partition]
        del self.operator_partition_to_worker[operator_partition]
        self.put(worker)

    def update_operator(
        self,
        operator_partition: OperatorPartition,
        operator: Operator | BaseOperator,
    ) -> None:
        worker_id = self.operator_partition_to_worker[operator_partition]
        worker = self.peek(worker_id)
        worker.assigned_operators[operator_partition] = operator

    def pop(self) -> Worker | None:
        # O(log(n)) heappop
        while self._queue:
            worker = heapq.heappop(self._queue)[-1]
            if worker is not self._tombstone:
                del self._worker_queue_idx[worker.worker_id]
                return worker
        return None

    def peek(self, worker_id: int) -> Worker:
        # O(1) peek
        return self._worker_queue_idx[worker_id][-1]

    def remove_worker(self, worker_id: int) -> Worker:
        # O(1) remove
        entry = self._worker_queue_idx.pop(worker_id)
        worker = entry[-1]
        entry[-1] = self._tombstone
        return worker

    def activate_standby_worker(self) -> Worker:
        if not self._standby_queue:
            return None
        worker: Worker = self._standby_queue.popleft()
        self._standby_worker_ids.remove(worker.worker_id)
        self.put(worker)
        return worker

    def is_worker_active(self, worker_id: int) -> bool:
        return (
            worker_id not in self._standby_worker_ids
            and worker_id not in self.dead_worker_ids
            and worker_id in self._worker_queue_idx
        )

    def number_of_workers(self) -> int:
        return len(self._queue)

    def get_standby_workers(self) -> list[Worker]:
        return [worker for _, _, worker in self._queue if worker != self._tombstone and not worker.participating]

    def get_participating_workers(self) -> list[Worker]:
        return [worker for _, _, worker in self._queue if worker != self._tombstone and worker.participating]

    def get_workers(self) -> dict[int, tuple[str, int, int]]:
        return {
            worker.worker_id: (
                worker.worker_ip,
                worker.worker_port,
                worker.protocol_port,
            )
            for _, _, worker in self._queue
            if worker != self._tombstone
        }

    def get_worker_assignments(
        self,
    ) -> dict[tuple[str, int, int], dict[OperatorPartition, Operator]]:
        return {
            (
                worker.worker_ip,
                worker.worker_port,
                worker.protocol_port,
            ): worker.assigned_operators
            for _, _, worker in self._queue
            if worker != self._tombstone and worker.participating
        }

    def get_operator_partition_locations(
        self,
    ) -> dict[str, dict[int, tuple[str, int, int]]]:
        operator_partition_locations = defaultdict(dict)
        for _, _, worker in self._queue:
            if worker == self._tombstone:
                continue
            for operator, partition in worker.assigned_operators:
                operator_partition_locations[operator][partition] = (
                    worker.worker_ip,
                    worker.worker_port,
                    worker.protocol_port,
                )
        return operator_partition_locations
