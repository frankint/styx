import asyncio
from collections import defaultdict
import contextlib
from dataclasses import astuple
import os
import time
from timeit import default_timer as timer
from traceback import format_exc
from typing import TYPE_CHECKING

from setuptools._distutils.util import strtobool
from styx.common.base_protocol import BaseTransactionalProtocol
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.metrics import WorkerEpochStats
from styx.common.run_func_payload import RunFuncPayload, SequencedItem
from styx.common.serialization import Serializer, msgpack_serialization
from styx.common.tcp_networking import NetworkingManager
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress
from worker.ingress.styx_kafka_ingress import StyxKafkaIngress
from worker.operator_state.aria.conflict_detection_types import (
    AriaConflictDetectionType,
)
from worker.sequencer.sequencer import Sequencer
from worker.util.phase_resource_tracker import PhaseResourceTracker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from aiokafka import TopicPartition
    from styx.common.operator import Operator
    from styx.common.types import OperatorPartition

    from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
    from worker.operator_state.stateless import Stateless

DISCOVERY_HOST: str = os.environ["DISCOVERY_HOST"]
DISCOVERY_PORT: int = int(os.environ["DISCOVERY_PORT"])

CONFLICT_DETECTION_METHOD: AriaConflictDetectionType = AriaConflictDetectionType(
    int(os.getenv("CONFLICT_DETECTION_METHOD", "0")),
)
# if more than 10% aborts use fallback strategy
FALLBACK_STRATEGY_PERCENTAGE: float = float(
    os.getenv("FALLBACK_STRATEGY_PERCENTAGE", "0.1"),
)
SNAPSHOTTING_THREADS: int = int(os.getenv("SNAPSHOTTING_THREADS", "4"))
SEQUENCE_MAX_SIZE: int = int(os.getenv("SEQUENCE_MAX_SIZE", "1_000"))
KAFKA_URL: str = os.environ["KAFKA_URL"]
USE_ASYNC_MIGRATION: bool = bool(strtobool(os.getenv("USE_ASYNC_MIGRATION", "true")))
ASYNC_MIGRATION_BATCH_SIZE: int = int(os.getenv("ASYNC_MIGRATION_BATCH_SIZE", "2000"))
EPOCH_INTERVAL_MS: int = int(os.getenv("EPOCH_INTERVAL_MS", "10"))


class AriaProtocol(BaseTransactionalProtocol):
    def __init__(
        self,
        worker_id: int,
        peers: dict[int, tuple[str, int, int]],
        dns: dict[str, dict[int, tuple[str, int, int]]],
        networking: NetworkingManager,
        registered_operators: dict[OperatorPartition, Operator],
        topic_partitions: list[TopicPartition],
        state: InMemoryOperatorState | Stateless,
        snapshotting_port: int,
        topic_partition_offsets: dict[OperatorPartition, int] | None = None,
        output_offsets: dict[OperatorPartition, int] | None = None,
        epoch_counter: int = 0,
        t_counter: int = 0,
        request_id_to_t_id_map: dict[bytes, int] | None = None,
        restart_after_recovery: bool = False,
        restart_after_migration: bool = False,
    ) -> None:
        if topic_partition_offsets is None:
            topic_partition_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}
        if output_offsets is None:
            output_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}

        self.id: int = worker_id

        self.topic_partitions = topic_partitions
        self.networking = networking
        self.snapshotting_networking_manager = NetworkingManager(None, size=1)

        self.local_state: InMemoryOperatorState | Stateless = state
        self.aio_task_scheduler: AIOTaskScheduler = AIOTaskScheduler()
        self.background_functions: AIOTaskScheduler = AIOTaskScheduler()

        # worker_id: host, port
        self.peers: dict[int, tuple[str, int, int]] = peers
        self.dns: dict[str, dict[int, tuple[str, int, int]]] = dns
        self.topic_partition_offsets: dict[OperatorPartition, int] = topic_partition_offsets
        # worker_id: set of aborted t_ids
        self.concurrency_aborts_everywhere: set[int] = set()
        self.t_ids_to_reschedule: set[int] = set()
        self.fallback_rescheduled_t_ids: set[int] = set()

        # ready_to_commit_events -> worker_id: Event that appears if the peer is ready to commit
        self.ready_to_reorder_events: dict[int, asyncio.Event] = {peer_id: asyncio.Event() for peer_id in self.peers}

        # FALLBACK LOCKING
        # t_id: its lock
        self.fallback_locking_event_map: dict[int, asyncio.Event] = {}
        self.fallback_locking_event_map_lock: asyncio.Lock = asyncio.Lock()
        # t_id: the t_ids it depends on
        self.waiting_on_transactions: dict[int, set[int]] = {}

        self.registered_operators: dict[OperatorPartition, Operator] = registered_operators

        self.sequencer = Sequencer(
            SEQUENCE_MAX_SIZE,
            t_counter=t_counter,
            epoch_counter=epoch_counter,
        )
        self.sequencer.set_sequencer_id(list(self.peers.keys()), self.id)
        self.sequencer.set_wal_values_after_recovery(request_id_to_t_id_map)

        self.ingress: StyxKafkaIngress = StyxKafkaIngress(
            networking=self.networking,
            sequencer=self.sequencer,
            state=self.local_state,
            registered_operators=self.registered_operators,
            worker_id=self.id,
            kafka_url=KAFKA_URL,
            sequence_max_size=SEQUENCE_MAX_SIZE,
            epoch_interval_ms=100,
        )

        self.egress: StyxKafkaBatchEgress = StyxKafkaBatchEgress(
            output_offsets,
            restart_after_recovery or restart_after_migration,
        )
        # Primary task used for processing
        self.function_scheduler_task: asyncio.Task | None = None
        self.communication_task: asyncio.Task | None = None
        self.migration_sender_task: asyncio.Task | None = None

        self.max_t_counter: int = -1
        self.total_processed_seq_size: int = -1

        self.sync_workers_event: dict[MessageType, asyncio.Event] = {
            MessageType.AriaProcessingDone: asyncio.Event(),
            MessageType.SyncCleanup: asyncio.Event(),
            MessageType.AriaFallbackStart: asyncio.Event(),
            MessageType.AriaFallbackDone: asyncio.Event(),
            MessageType.AriaCommit: asyncio.Event(),
            MessageType.DeterministicReordering: asyncio.Event(),
        }

        self.remote_wants_to_proceed: bool = False
        self.currently_processing: bool = False

        self.started = asyncio.Event()
        self.wait_responses_to_be_sent = asyncio.Event()

        self.running: bool = True
        self.stopped: asyncio.Event = asyncio.Event()
        self.snapshot_marker_received: bool = False
        self.snapshotting_port: int = snapshotting_port

        self.migrating_state: bool = restart_after_migration

        self.protocol_handlers_map: dict[MessageType, Callable[[bytes], Awaitable[None]]] = {
            MessageType.RunFunRemote: self._handle_run_fun_remote,
            MessageType.RunFunRemoteBatch: self._handle_run_fun_remote_batch,
            MessageType.WrongPartitionRequest: self._handle_wrong_partition_request,
            MessageType.RunFunRqRsRemote: self._handle_deprecated_rqrs,
            MessageType.AriaCommit: self._handle_aria_commit,
            MessageType.AriaFallbackDone: self._handle_sync_event_only,
            MessageType.AriaFallbackStart: self._handle_sync_event_only,
            MessageType.SyncCleanup: self._handle_sync_cleanup,
            MessageType.AriaProcessingDone: self._handle_aria_processing_done,
            MessageType.Ack: self._handle_ack,
            MessageType.AckBatch: self._handle_ack_batch,
            MessageType.ChainAbort: self._handle_chain_abort,
            MessageType.ResponseToRoot: self._handle_response_to_root,
            MessageType.Unlock: self._handle_unlock,
            MessageType.DeterministicReordering: self._handle_deterministic_reordering,
            MessageType.RemoteWantsToProceed: self._handle_remote_wants_to_proceed,
            MessageType.AsyncMigration: self._handle_async_migration,
        }

        # Idle time tracking for downscaling policies
        # Tracks time between epochs (waiting for work)
        self._last_epoch_end_time: float = 0.0  # Timestamp when last epoch ended
        self._idle_time_ms: float = 0.0  # Idle time in ms for current epoch
        # Track epochs with no local work (forced to sync by coordinator)
        self._empty_epoch: bool = False  # True if current epoch had no local sequence
        self.cpu_work_ms: float = 0.0  # Time spent in actual function execution

        self.operator_metrics = {}
        # Per-phase resource attribution (CPU/RSS/RX/TX deltas), aggregated per epoch.
        self.phase_resource_tracker = PhaseResourceTracker()

    def record_operator_call(
        self, operator_name: str, partition: int, function_name: str, duration_ms: float, success: bool
    ) -> None:
        """
        Record an operator call for metrics tracking.
        """
        key = (operator_name, partition, function_name)
        m = self.operator_metrics.setdefault(key, {"count": 0, "failures": 0, "sum_ms": 0.0})
        m["count"] += 1
        m["sum_ms"] += duration_ms
        if not success:
            m["failures"] += 1

    async def wait_stopped(self) -> None:
        await self.stopped.wait()

    async def stop(self) -> None:
        await self.ingress.stop()
        await self.egress.stop()
        await self.aio_task_scheduler.close()
        await self.background_functions.close()
        await self.snapshotting_networking_manager.close_all_connections()
        # Guard against the standby code-path
        if self.function_scheduler_task is not None:
            self.function_scheduler_task.cancel()
        if self.communication_task is not None:
            self.communication_task.cancel()
        if self.migration_sender_task is not None:
            self.migration_sender_task.cancel()
        try:
            if self.function_scheduler_task is not None:
                await self.function_scheduler_task
            if self.communication_task is not None:
                await self.communication_task
            if self.migration_sender_task is not None:
                await self.migration_sender_task
        except asyncio.CancelledError:
            logging.warning("Protocol coroutines stopped")
        logging.info(f"Active tasks: {asyncio.all_tasks()}")
        self.stopped.set()
        logging.warning(f"Aria protocol stopped at: {self.topic_partition_offsets}")

    def _task_exception_handler(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Task {task.get_name()} crashed: {e}\n{format_exc()}")

    def start(self) -> None:
        if self.registered_operators:
            logging.warning(f"Aria protocol started with operator partitions: {list(self.registered_operators.keys())}")
            self.function_scheduler_task = asyncio.create_task(self.function_scheduler())
            self.function_scheduler_task.add_done_callback(self._task_exception_handler)
        else:
            logging.warning("Aria protocol started with no registered operators")
            self.running = False
            self.stopped.set()
        self.communication_task = asyncio.create_task(self.communication_protocol())
        self.communication_task.add_done_callback(self._task_exception_handler)
        if self.migrating_state and USE_ASYNC_MIGRATION:
            self.migration_sender_task = asyncio.create_task(self._continuous_migration_sender())

    async def run_function(
        self,
        t_id: int,
        payload: RunFuncPayload,
        fallback_mode: bool = False,
    ) -> bool:
        # logging.info(f"Running function: {payload.function_name} with T_ID {t_id} with params {payload.params}"
        #              f" and ack payload {payload.ack_payload}")

        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]

        success: bool = await operator_partition.run_function(
            payload.key,
            t_id,
            payload.request_id,
            payload.function_name,
            payload.partition,
            payload.ack_payload,
            fallback_mode,
            payload.params,
            self,
        )
        return success

    async def take_snapshot(self) -> None:
        if self.snapshot_marker_received:
            logging.warning(
                f"ARIA | Snapshot marker received @epoch: {self.sequencer.epoch_counter}",
            )
            await self.snapshotting_networking_manager.send_message(
                self.networking.host_name,
                self.snapshotting_port,
                msg=(
                    self.topic_partition_offsets,
                    self.egress.topic_partition_output_offsets,
                    self.sequencer.epoch_counter,
                    self.sequencer.t_counter,
                ),
                msg_type=MessageType.SnapTakeSnapshot,
                serializer=Serializer.MSGPACK,
            )
            self.snapshot_marker_received = False

    async def communication_protocol(self) -> None:
        await self.ingress.start(self.topic_partitions, self.topic_partition_offsets)
        logging.warning("Ingress started")
        await self.egress.start(self.id)
        logging.warning("Egress started")
        await self.started.wait()

    async def protocol_tcp_controller(self, data: bytes) -> None:
        """Legacy entry point retained for symmetry with the rest of the codebase.

        The hot path now dispatches inline in `worker_service.protocol_queue_worker`
        (one fewer coroutine frame per message). Kept here so direct callers
        (tests, recovery, snapshotting) still work without changes.
        """
        message_type: MessageType = self.networking.get_msg_type(data)
        handler = self.protocol_handlers_map.get(message_type)
        if handler is None:
            logging.error(
                f"Aria protocol: Non supported command message type: {message_type}",
            )
            return
        await handler(data)

    # -------------------
    # Handlers
    # -------------------
    async def _handle_run_fun_remote(self, data: bytes) -> None:
        # Lock-free: no awaits in this handler; single-threaded asyncio gives atomicity.
        logging.debug("CALLED RUN FUN FROM PEER")
        (
            t_id,
            request_id,
            operator_name,
            function_name,
            key,
            partition,
            fallback_enabled,
            params,
            ack,
        ) = self.networking.decode_message(data)

        payload = RunFuncPayload(
            request_id=request_id,
            key=key,
            operator_name=operator_name,
            partition=partition,
            function_name=function_name,
            params=params,
            ack_payload=ack,
        )

        if fallback_enabled:
            # Bypass the semaphore for fallback chain participants: their
            # dominant time is `await fallback_locking_event_map[d].wait()`,
            # and those events fire only when other participants (queued
            # behind the very same semaphore) get to run. Holding a slot
            # while sleeping causes a cluster-wide deadlock under load.
            self.background_functions.create_unbounded_task(
                self.run_fallback_function(t_id, payload, internal=True),
            )
            return

        self.background_functions.create_task(self.run_function(t_id, payload))

    async def _handle_run_fun_remote_batch(self, data: bytes) -> None:
        # Lock-free: no awaits. Decodes a batch of remote-call payloads and
        # schedules each as a task, mirroring _handle_run_fun_remote per entry.
        batch = self.networking.decode_message(data)
        for entry in batch:
            (
                t_id,
                request_id,
                operator_name,
                function_name,
                key,
                partition,
                fallback_enabled,
                params,
                ack,
            ) = entry
            payload = RunFuncPayload(
                request_id=request_id,
                key=key,
                operator_name=operator_name,
                partition=partition,
                function_name=function_name,
                params=params,
                ack_payload=ack,
            )
            if fallback_enabled:
                self.background_functions.create_unbounded_task(
                    self.run_fallback_function(t_id, payload, internal=True),
                )
            else:
                self.background_functions.create_task(self.run_function(t_id, payload))

    async def _handle_wrong_partition_request(self, data: bytes) -> None:
        # Lock-free: no awaits.
        (
            request_id,
            operator_name,
            function_name,
            key,
            partition,
            kafka_ingress_partition,
            kafka_offset,
            params,
        ) = self.networking.decode_message(data)

        logging.debug(
            f"Aria WrongPartitionRequest: {request_id}:{operator_name}:{kafka_ingress_partition}",
        )

        payload = RunFuncPayload(
            request_id=request_id,
            key=key,
            operator_name=operator_name,
            partition=partition,
            function_name=function_name,
            params=params,
            kafka_ingress_partition=kafka_ingress_partition,
            kafka_offset=kafka_offset,
        )
        self.sequencer.sequence(payload)

    async def _handle_deprecated_rqrs(self, _: bytes) -> None:
        logging.error("REQUEST RESPONSE HAS BEEN DEPRECATED")

    async def _handle_aria_commit(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.AriaCommit
        (
            self.concurrency_aborts_everywhere,
            self.total_processed_seq_size,
            self.max_t_counter,
            self.snapshot_marker_received,
        ) = self.networking.decode_message(data)
        self.sync_workers_event[mt].set()

    async def _handle_sync_event_only(self, data: bytes) -> None:
        # Used for AriaFallbackDone / AriaFallbackStart. Lock-free: no awaits.
        mt: MessageType = self.networking.get_msg_type(data)
        self.sync_workers_event[mt].set()

    async def _handle_sync_cleanup(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.SyncCleanup
        (stop_gracefully,) = self.networking.decode_message(data)
        if stop_gracefully:
            self.running = False
        self.sync_workers_event[mt].set()

    async def _handle_aria_processing_done(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.AriaProcessingDone
        (self.networking.logic_aborts_everywhere,) = self.networking.decode_message(data)
        self.sync_workers_event[mt].set()

    async def _handle_ack(self, data: bytes) -> None:
        # Lock-free: no awaits.
        ack_id, fraction_str, chain_participants = self.networking.decode_message(data)
        self.networking.add_ack_fraction_str(
            ack_id,
            fraction_str,
            chain_participants,
        )

    async def _handle_ack_batch(self, data: bytes) -> None:
        # Lock-free: no awaits. Decodes a coalesced batch from a peer and
        # applies each entry through the same single-entry codepath.
        batch = self.networking.decode_message(data)
        add = self.networking.add_ack_fraction_str
        for ack_id, fraction_str, chain_participants in batch:
            add(ack_id, fraction_str, chain_participants)

    async def _handle_chain_abort(self, data: bytes) -> None:
        # Lock-free: no awaits.
        ack_id, exception_str = self.networking.decode_message(data)
        self.networking.abort_chain(ack_id, exception_str)

    async def _handle_response_to_root(self, data: bytes) -> None:
        # Lock-free: no awaits.
        ack_id, resp = self.networking.decode_message(data)
        self.networking.add_response(ack_id, resp)

    async def _handle_unlock(self, data: bytes) -> None:
        # fallback phase
        # here we handle the logic to unlock locks held by the provided distributed transaction
        # The outer lock is unnecessary: commit_fallback_transaction is sync, and
        # unlock_tid acquires its own fallback_locking_event_map_lock around the await.
        t_id, success = self.networking.decode_message(data)
        if success:
            # commit changes
            self.local_state.commit_fallback_transaction(t_id)
        # unlock
        await self.unlock_tid(t_id)

    async def _handle_deterministic_reordering(self, data: bytes) -> None:
        # Lock-free: no awaits.
        mt = MessageType.DeterministicReordering
        _, global_read_reservations, global_write_set, global_read_set = self.networking.decode_message(data)
        self.local_state.set_global_read_write_sets(
            global_read_reservations,
            global_write_set,
            global_read_set,
        )
        self.sync_workers_event[mt].set()

    async def _handle_remote_wants_to_proceed(self, _: bytes) -> None:
        if not self.currently_processing:
            self.remote_wants_to_proceed = True
            self.ingress.messages_available.set()

    async def _handle_async_migration(self, data: bytes) -> None:
        # Lock-free: no awaits.
        operator_partition, batch = self.networking.decode_message(data)
        logging.warning(
            f"ASYNC_MIGRATION | Worker {self.id} | Received batch for {operator_partition} | {len(batch)} keys"
        )

        self.local_state.set_batch_data_from_migration(operator_partition, batch)
        # Unblock any transactions waiting for these keys via RequestRemoteKey
        op = tuple(operator_partition)
        for key in batch:
            self.networking.key_received(op, key)

    async def _write_to_wal(self, sequence: list[SequencedItem]) -> tuple[float, float]:
        start_wal = timer()
        sequence_to_log = msgpack_serialization(
            {seq_item.payload.request_id: seq_item.t_id for seq_item in sequence},
        )
        await self.egress.send_message_to_topic(
            key=msgpack_serialization(self.sequencer.epoch_counter),
            message=sequence_to_log,
            topic="sequencer-wal",
        )
        end_wal = timer()
        logging.debug(
            f"Write to WAL successful at epoch: {self.sequencer.epoch_counter}",
        )
        return start_wal, end_wal

    async def _send_migration_batch(
        self,
        batch: dict,
    ) -> None:
        """Send a migration batch to destination workers in parallel."""
        send_tasks = []
        logging.info("MIGRATION | Sending batch for migration")
        for operator_partition, k_v_pairs in batch.items():
            operator_name, partition = operator_partition
            worker = self.dns[operator_name][partition]
            if self.networking.in_the_same_network(worker[0], worker[2]):
                self.local_state.set_batch_data_from_migration(
                    operator_partition,
                    k_v_pairs,
                )
                op = tuple(operator_partition)
                # For scenarios where a key moves between partitions on the same worker,
                # we need to signal the event to unblock the transactions waiting for this key
                for key in k_v_pairs:
                    self.networking.key_received(op, key)
            else:
                send_tasks.append(
                    self.networking.send_message(
                        worker[0],
                        worker[2],
                        msg=(operator_partition, k_v_pairs),
                        msg_type=MessageType.AsyncMigration,
                        serializer=Serializer.MSGPACK,
                    ),
                )
        if send_tasks:
            await asyncio.gather(*send_tasks)

    async def _continuous_migration_sender(self) -> None:
        """Background coroutine that continuously sends migration batches
        independently of epoch barriers, completing migration faster.

        Completion is based only on keys_remaining_to_send() — the coordinator's
        global barrier (all workers report MigrationDone) ensures all data is
        fully transferred.  We do NOT check keys_remaining_to_remote() because
        hash metadata from other workers may not have arrived yet.
        """
        logging.warning("MIGRATION | Continuous migration sender started")
        try:
            while self.migrating_state:
                if not self.local_state.has_keys_to_send():
                    if self.local_state.keys_remaining_to_send() == 0:
                        self.migrating_state = False
                        await self.networking.send_message(
                            DISCOVERY_HOST,
                            DISCOVERY_PORT + 1,
                            msg=b"",
                            msg_type=MessageType.MigrationDone,
                            serializer=Serializer.NONE,
                        )
                        logging.warning(
                            f"MIGRATION_FINISHED at epoch: {self.sequencer.epoch_counter}"
                            f" at time: {time.time_ns() // 1_000_000}",
                        )
                        self.local_state.log_state_summary(self.id, context="MIGRATION COMPLETE (async transfer done)")
                        return
                    # keys_to_send dict is empty but keys_remaining_to_send > 0
                    # shouldn't normally happen, but yield and retry
                    await asyncio.sleep(0.01)
                    continue

                batch = self.local_state.get_async_migrate_batch(ASYNC_MIGRATION_BATCH_SIZE)
                await self._send_migration_batch(batch)
                # Yield to event loop between batches
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            logging.warning("MIGRATION | Continuous migration sender cancelled")
            raise

    async def function_scheduler(self) -> None:
        await self.started.wait()
        logging.warning("STARTED function scheduler")

        while self.running:
            # Wait until the ingress signals that messages are available,
            # or a remote peer wants to proceed, instead of busy-spinning.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self.ingress.messages_available.wait(),
                    timeout=0.1,
                )
            self.ingress.messages_available.clear()

            async with self.sequencer.lock:
                sequence: list[SequencedItem] = self.sequencer.get_epoch()

                if not sequence and not self.remote_wants_to_proceed:
                    continue

                self.currently_processing = True
                await self._process_epoch(sequence)

        await self.stop()

    # TODO: refactor this function to be more readable
    async def _process_epoch(self, sequence: list[SequencedItem]) -> None:  # noqa: PLR0915 temporary ignore
        epoch_start = timer()

        sequence = await self._redirect_migration_backlog_transactions(sequence)

        # Calculate idle time: time spent waiting since last epoch ended
        idle_end = timer()
        idle_start = self._last_epoch_end_time or idle_end
        self._idle_time_ms = (idle_end - idle_start) * 1000  # Convert to ms
        # Track if this is an empty epoch (no local work, just sync)
        self._empty_epoch = not bool(sequence)

        self.currently_processing = True
        logging.warning(f"{self.id} ||| Epoch: {self.sequencer.epoch_counter} running {len(sequence)} functions...")
        self.phase_resource_tracker.reset_epoch()

        timings = await self._run_epoch_functions_and_chain(sequence)
        logging.debug(f"Finished running {len(sequence)} functions")

        sync_time = 0.0
        # Capture local logic aborts before sync overwrites with global
        logic_aborts_count = len(self.networking.logic_aborts_everywhere)
        sync_time += await self._sync_processing_done()
        logging.debug("Finished syncing processing")

        conflict_resolution_start = timer()
        self.phase_resource_tracker.begin("Conflict Resolution")
        # HERE WE KNOW ALL THE LOGIC ABORTS
        self.local_state.remove_aborted_from_rw_sets(
            self.networking.logic_aborts_everywhere,
        )
        concurrency_aborts = await self._compute_concurrency_aborts()
        self.phase_resource_tracker.end("Conflict Resolution")
        conflict_resolution_end = timer()
        logging.debug("Finished conflict resolution")
        local_abort_rate = (len(concurrency_aborts) / len(sequence)) if sequence else 0.0

        # Notify peers that we are ready to commit
        sync_time += await self._sync_commit(sequence, concurrency_aborts)
        logging.debug("Finished syncing commit")
        # HERE WE KNOW ALL THE CONCURRENCY ABORTS
        commit_start = timer()
        self.phase_resource_tracker.begin("Commit time")
        self._commit_and_prepare_responses(sequence)
        await self.send_delta_to_snapshotting_proc()
        self.phase_resource_tracker.end("Commit time")
        commit_end = timer()
        logging.debug("Finished commit")
        # Track lock-free vs fallback commits
        local_concurrency_aborted = {
            seq_i.t_id for seq_i in sequence if seq_i.t_id in self.concurrency_aborts_everywhere
        }
        local_aborted_t_ids = {seq_i.t_id for seq_i in sequence if seq_i.t_id in self.concurrency_aborts_everywhere}
        committed_lock_free = len(sequence) - len(local_aborted_t_ids)

        fallback_start = timer()
        self.phase_resource_tracker.begin("Fallback")
        _, committed_fallback = await self._maybe_run_fallback()
        self.phase_resource_tracker.end("Fallback")
        fallback_end = timer()
        logging.debug("Finished fallback")
        self._advance_offsets(sequence)

        self.sequencer.increment_epoch(self.max_t_counter, self.t_ids_to_reschedule)
        await self.wait_responses_to_be_sent.wait()
        self.cleanup_after_epoch()

        snap_start = timer()
        self.phase_resource_tracker.begin("Async Snapshot")
        await self.take_snapshot()
        self.phase_resource_tracker.end("Async Snapshot")
        snap_end = timer()

        epoch_end = timer()

        # Transaction count metrics for this epoch
        total_txns = len(sequence)
        committed_txns = committed_lock_free + committed_fallback
        concurrency_aborts_count = len(local_concurrency_aborted)

        epoch_latency = max(round((epoch_end - epoch_start) * 1000, 4), 1)
        epoch_throughput = (committed_txns * 1000) // epoch_latency  # TPS
        input_rate = self.ingress.epoch_stats["consumed"]

        operator_agg: dict[tuple[str, int], dict[str, float | int]] = {}
        for (op_name, partition, _func_name), m in self.operator_metrics.items():
            key = (op_name, partition)
            agg = operator_agg.setdefault(key, {"calls": 0, "sum_ms": 0.0})
            agg["calls"] += m["count"]
            agg["sum_ms"] += m["sum_ms"]

        operator_epoch_stats: list[tuple[str, int, float, float, int]] = []
        epoch_seconds = max(epoch_latency / 1000.0, 1e-6)
        for (op_name, partition), agg in operator_agg.items():
            call_count = int(agg["calls"])
            if call_count == 0:
                continue
            total_latency_ms = float(agg["sum_ms"])
            avg_latency_ms = total_latency_ms / call_count
            tps = call_count / epoch_seconds
            operator_epoch_stats.append((op_name, partition, tps, avg_latency_ms, call_count))

        # Reset per-epoch operator metrics after epoch
        self.operator_metrics.clear()
        phase_resources = self.phase_resource_tracker.export()

        func_time = round(timings["func_ms"], 4)
        chain_time = round(timings["chain_ms"], 4)
        fallback_time = (fallback_end - fallback_start) * 1000
        conflict_resolution_time = (conflict_resolution_end - conflict_resolution_start) * 1000
        commit_time = (commit_end - commit_start) * 1000
        wal_time = round(timings["wal_ms"], 4)
        snap_time = (snap_end - snap_start) * 1000

        cpu_work_ms = func_time + conflict_resolution_time + commit_time + fallback_time
        io_wait_time_ms = chain_time + wal_time + snap_time + sync_time
        # ratio of processing time to total time
        cpu_utilization = (cpu_work_ms / epoch_latency) if epoch_latency > 0 else 0.0
        # ratio of IO wait time to total time
        io_wait_utilization = (io_wait_time_ms / epoch_latency) if epoch_latency > 0 else 0.0
        key_counts = sum(len(data) for data in self.local_state.data.values())

        logging.warning(f"Epoch throughput: {epoch_throughput} | Latency: {epoch_latency} ms")
        logging.warning(f"Queue backlog: {len(self.sequencer.distributed_log)}")
        if total_txns > 0:
            logging.warning(f"Per txn cost: {epoch_latency / total_txns} ms")

        worker_epoch_stats = WorkerEpochStats(
            worker_id=self.id,
            epoch_throughput=epoch_throughput,
            epoch_latency=epoch_latency,
            local_abort_rate=local_abort_rate,
            wal_time=wal_time,
            func_time=func_time,
            chain_ack_time=chain_time,
            sync_time=sync_time,
            conflict_res_time=conflict_resolution_time,
            commit_time=commit_time,
            fallback_time=fallback_time,
            snap_time=snap_time,
            input_rate=input_rate,
            queue_backlog=len(self.sequencer.distributed_log),
            idle_time_ms=round(self._idle_time_ms, 4),
            total_txns=total_txns,
            committed_txns=committed_txns,
            logic_aborts=logic_aborts_count,
            concurrency_aborts=concurrency_aborts_count,
            committed_lock_free=committed_lock_free,
            committed_fallback=committed_fallback,
            empty_epoch=self._empty_epoch,
            cpu_utilization=cpu_utilization,
            io_wait_utilization=io_wait_utilization,
            operator_epoch_stats=operator_epoch_stats,
            phase_resources=phase_resources,
            key_counts=key_counts,
        )
        logging.debug(
            f"{self.id} ||| Epoch: {self.sequencer.epoch_counter - 1} done in "
            f"{epoch_latency}ms "
            f"global logic aborts: {len(self.networking.logic_aborts_everywhere)} "
            f"concurrency aborts for next epoch: {len(self.concurrency_aborts_everywhere)} "
            f"commited transactions: {committed_txns} "
            f"total transactions: {total_txns} "
            f"sequencer backlog: {len(self.sequencer.distributed_log)} "
        )

        await self._sync_cleanup(worker_epoch_stats)
        self._last_epoch_end_time = timer()

    """
    When migration happens, the transactions that are in the backlog of the migration
    need to be redirected to the new partition. If the key is in the set_keys_to_send
    and the partition is not the same as the new partition, the transaction needs to be redirected
    to the new partition decided by the previous migration.
    """

    async def _redirect_migration_backlog_transactions(self, sequence: list[SequencedItem]) -> list[SequencedItem]:
        sequence_to_process = []
        for seq_item in sequence:
            if (
                seq_item.payload.key in self.local_state.set_keys_to_send
                and seq_item.payload.partition != self.local_state.keys_to_workers[seq_item.payload.key]
            ):
                new_partition = self.local_state.keys_to_workers[seq_item.payload.key]
                logging.debug(
                    f"Key {seq_item.payload.key} needs redirect: "
                    f"partition {seq_item.payload.partition} -> {new_partition}"
                )
                operator_name = seq_item.payload.operator_name
                operator_host = self.dns[operator_name][new_partition][0]
                operator_port = self.dns[operator_name][new_partition][2]

                if self.networking.in_the_same_network(operator_host, operator_port):
                    # Internal transfer within the same worker: re-sequence with corrected partition
                    corrected_payload = RunFuncPayload(
                        request_id=seq_item.payload.request_id,
                        key=seq_item.payload.key,
                        operator_name=operator_name,
                        partition=new_partition,
                        function_name=seq_item.payload.function_name,
                        params=seq_item.payload.params,
                        kafka_ingress_partition=seq_item.payload.kafka_ingress_partition,
                        kafka_offset=seq_item.payload.kafka_offset,
                    )
                    self.sequencer.sequence(corrected_payload)
                else:
                    # Forward to remote worker
                    # Handler expects tuple: (request_id, operator_name, function_name, key,
                    # partition, kafka_ingress_partition, kafka_offset, params)
                    payload = (
                        seq_item.payload.request_id,
                        operator_name,
                        seq_item.payload.function_name,
                        seq_item.payload.key,
                        new_partition,
                        seq_item.payload.kafka_ingress_partition,
                        seq_item.payload.kafka_offset,
                        seq_item.payload.params,
                    )
                    await self.networking.send_message(
                        operator_host,
                        operator_port,
                        msg=payload,
                        msg_type=MessageType.WrongPartitionRequest,
                        serializer=Serializer.MSGPACK,
                    )
            else:
                sequence_to_process.append(seq_item)
        return sequence_to_process

    async def _run_epoch_functions_and_chain(
        self,
        sequence: list[SequencedItem],
    ) -> dict[str, float]:
        if not sequence:
            return {"wal_ms": 0.0, "func_ms": 0.0, "chain_ms": 0.0}

        self.phase_resource_tracker.begin("WAL")
        start_wal, end_wal = await self._write_to_wal(sequence)
        self.phase_resource_tracker.end("WAL")

        self.phase_resource_tracker.begin("1st Run")
        start_func = timer()
        await asyncio.gather(
            *[self.run_function(item.t_id, item.payload) for item in sequence],
        )
        end_func = timer()
        self.phase_resource_tracker.end("1st Run")

        # Wait for chains to finish
        logging.debug(f"{self.id} ||| Waiting on chained {len(self.networking.waited_ack_events)} functions...")
        self.phase_resource_tracker.begin("Chain Acks")
        start_chain = timer()
        await asyncio.gather(
            *[ack.wait() for ack in self.networking.waited_ack_events.values()],
        )
        end_chain = timer()
        self.phase_resource_tracker.end("Chain Acks")
        logging.debug("Finished waiting on chained functions")

        return {
            "wal_ms": (end_wal - start_wal) * 1000,
            "func_ms": (end_func - start_func) * 1000,
            "chain_ms": (end_chain - start_chain) * 1000,
        }

    async def _sync_processing_done(self) -> float:
        self.phase_resource_tracker.begin("SYNC")
        start = timer()
        await self.sync_workers(
            msg_type=MessageType.AriaProcessingDone,
            message=(self.id, self.networking.logic_aborts_everywhere),
            serializer=Serializer.PICKLE,
        )
        end = timer()
        self.phase_resource_tracker.end("SYNC")

        logging.debug(
            f"{self.id} ||| logic_aborts_everywhere: {self.networking.logic_aborts_everywhere}",
        )
        return end - start

    async def _compute_concurrency_aborts(self) -> set[int]:
        logging.debug(f"{self.id} ||| Checking conflicts...")

        handlers = {
            AriaConflictDetectionType.DEFAULT_SERIALIZABLE: self._conflicts_default,
            AriaConflictDetectionType.DETERMINISTIC_REORDERING: self._conflicts_deterministic_reordering,
            AriaConflictDetectionType.SNAPSHOT_ISOLATION: self._conflicts_snapshot_isolation,
        }

        try:
            return await handlers[CONFLICT_DETECTION_METHOD]()
        except KeyError as e:
            logging.error(
                "Invalid conflict detection method: %r",
                CONFLICT_DETECTION_METHOD,
            )
            raise e

    async def _conflicts_default(self) -> set[int]:
        return self.local_state.check_conflicts()

    async def _conflicts_deterministic_reordering(self) -> set[int]:
        await self.sync_workers(
            msg_type=MessageType.DeterministicReordering,
            message=(
                self.id,
                self.local_state.reads,
                self.local_state.write_sets,
                self.local_state.read_sets,
            ),
            serializer=Serializer.PICKLE,
        )
        return self.local_state.check_conflicts_deterministic_reordering()

    async def _conflicts_snapshot_isolation(self) -> set[int]:
        return self.local_state.check_conflicts_snapshot_isolation()

    async def _sync_commit(
        self,
        sequence: list[SequencedItem],
        concurrency_aborts: set[int],
    ) -> float:
        start = timer()
        self.phase_resource_tracker.begin("SYNC")
        await self.sync_workers(
            msg_type=MessageType.AriaCommit,
            message=(
                self.id,
                concurrency_aborts,
                self.sequencer.t_counter,
                len(sequence),
            ),
            serializer=Serializer.PICKLE,
        )
        self.phase_resource_tracker.end("SYNC")
        end = timer()

        return end - start

    def _commit_and_prepare_responses(self, sequence: list[SequencedItem]) -> None:
        logging.debug(
            f"{self.id} ||| Starting commit! {self.concurrency_aborts_everywhere}",
        )

        self.local_state.commit(self.concurrency_aborts_everywhere)

        self.t_ids_to_reschedule = self.concurrency_aborts_everywhere - self.networking.logic_aborts_everywhere

        current_completed_t_ids: list[SequencedItem] = [
            item for item in sequence if item.t_id not in self.concurrency_aborts_everywhere
        ]

        # Shallow copies — values (response bytes / exception strings) are
        # immutable, and cleanup_after_epoch will rebind the originals rather
        # than mutating these dicts. Cheaper than the msgpack round-trip we
        # used to do here.
        self.aio_task_scheduler.create_task(
            self.send_responses(
                current_completed_t_ids,
                dict(self.networking.client_responses),
                dict(self.networking.aborted_events),
            ),
        )

        logging.debug(
            f"{self.id} ||| Sequence committed! | "
            f"{len(self.concurrency_aborts_everywhere)} / {self.total_processed_seq_size}",
        )

    async def _maybe_run_fallback(self) -> tuple[float, int]:
        abort_rate: float = (
            len(self.concurrency_aborts_everywhere) / self.total_processed_seq_size
            if self.total_processed_seq_size
            else 0.0
        )
        committed_fallback = 0
        if abort_rate > FALLBACK_STRATEGY_PERCENTAGE:
            logging.warning(
                f"{self.id} ||| Epoch: {self.sequencer.epoch_counter} "
                f"Abort percentage: {int(abort_rate * 100)}% initiating fallback strategy...",
            )
            # Transactions to commit in fallback = concurrency aborts minus logic aborts
            local_aborted_t_ids = self.sequencer.get_aborted_sequence(self.t_ids_to_reschedule)
            committed_fallback = len(local_aborted_t_ids)

            logging.debug(
                f"FALLBACK_ENTER to_reschedule={len(self.t_ids_to_reschedule)}",
            )
            await self.run_fallback_strategy()
            logging.debug("FALLBACK_AFTER_STRATEGY")
            await self.send_delta_to_snapshotting_proc()
            logging.debug("FALLBACK_AFTER_DELTA")
            self.concurrency_aborts_everywhere = set()
            # Keep rescheduled t_ids (rw-set changed during fallback) for the next epoch
            self.t_ids_to_reschedule = self.fallback_rescheduled_t_ids.copy()
            self.fallback_rescheduled_t_ids.clear()
        return abort_rate, committed_fallback

    def _advance_offsets(self, sequence: list[SequencedItem]) -> None:
        partition_reqs = defaultdict(int)
        for item in sequence:
            if item.t_id in self.concurrency_aborts_everywhere:
                continue

            payload = item.payload
            partition_reqs[f"{payload.operator_name} {payload.partition}"] += 1
            tpo_key = (payload.operator_name, payload.kafka_ingress_partition)

            prev = self.topic_partition_offsets.get(tpo_key)
            if prev is None:
                self.topic_partition_offsets[tpo_key] = payload.kafka_offset
            else:
                self.topic_partition_offsets[tpo_key] = max(payload.kafka_offset, prev)
        logging.debug(f"Partition requests: {partition_reqs}")

    async def _sync_cleanup(
        self,
        worker_epoch_stats: WorkerEpochStats,
    ) -> None:
        await self.sync_workers(
            msg_type=MessageType.SyncCleanup,
            message=astuple(worker_epoch_stats),
            serializer=Serializer.MSGPACK,
        )

    async def send_delta_to_snapshotting_proc(self) -> None:
        delta_to_send = self.local_state.get_data_for_snapshot()

        await self.snapshotting_networking_manager.send_message(
            self.networking.host_name,
            self.snapshotting_port,
            msg=(delta_to_send,),
            msg_type=MessageType.SnapProcDelta,
            serializer=Serializer.MSGPACK,
        )
        self.local_state.clear_delta_map()

    def cleanup_after_epoch(self) -> None:
        self.concurrency_aborts_everywhere.clear()
        self.t_ids_to_reschedule.clear()
        self.fallback_rescheduled_t_ids.clear()
        self.wait_responses_to_be_sent.clear()
        self.networking.cleanup_after_epoch()
        self.local_state.cleanup()
        self.waiting_on_transactions.clear()
        self.fallback_locking_event_map.clear()
        self.remote_wants_to_proceed = False
        self.currently_processing = False

    async def run_fallback_function(
        self,
        t_id: int,
        payload: RunFuncPayload,
        internal: bool = False,
    ) -> None:
        # Wait for all transactions that this transaction depends on to finish
        if self.waiting_on_transactions.get(t_id):
            tasks = [
                self.fallback_locking_event_map[dependency_t_id].wait()
                for dependency_t_id in self.waiting_on_transactions[t_id]
                if dependency_t_id in self.fallback_locking_event_map
            ]
            await asyncio.gather(*tasks)

        # Run transaction
        success = await self.run_function(t_id, payload, fallback_mode=True)
        if not internal:
            if t_id in self.networking.waited_ack_events:
                # wait on ack of parts
                await self.networking.waited_ack_events[t_id].wait()
            transaction_failed: bool = t_id in self.networking.aborted_events or not success

            # Per the Aria paper: if the rw-set changed during fallback
            # re-execution, the dependency graph is invalid — reschedule
            # the transaction to the next epoch instead of committing.
            rw_changed = self.local_state.has_fallback_rw_set_changed(t_id)
            if not transaction_failed and not rw_changed:
                self.local_state.commit_fallback_transaction(t_id)
            elif rw_changed:
                self.fallback_rescheduled_t_ids.add(t_id)
                transaction_failed = True

            await self.fallback_unlock(t_id, success=not transaction_failed)

            # If the txn was rescheduled because the fallback rw-set drifted
            # from the optimistic one, the commit hasn't happened and the
            # response isn't durable yet. Defer egress to the next epoch's
            # run; otherwise we'd send the response now AND again when the
            # txn finally commits, producing duplicates.
            # Batched send: each `egress.send` enqueues a Kafka future and
            # returns immediately; the strategy flushes all of them with a
            # single `send_batch()` at the end of fallback. Previously we
            # used `send_immediate` here, which did a synchronous round-trip
            # per txn — fine with 1-2 fallback commits per epoch, but a major
            # bottleneck when fallback commits ~100 txns at once.
            if rw_changed:
                pass
            elif t_id in self.networking.aborted_events:
                await self.egress.send(
                    key=payload.request_id,
                    value=msgpack_serialization(self.networking.aborted_events[t_id]),
                    operator_name=payload.operator_name,
                    partition=payload.partition,
                )
            elif t_id in self.networking.client_responses:
                await self.egress.send(
                    key=payload.request_id,
                    value=msgpack_serialization(self.networking.client_responses[t_id]),
                    operator_name=payload.operator_name,
                    partition=payload.partition,
                )

    async def run_fallback_strategy(self) -> None:
        logging.debug("Starting fallback strategy...")
        (self.waiting_on_transactions, self.fallback_locking_event_map) = self.local_state.get_dep_transactions(
            self.t_ids_to_reschedule
        )

        fallback_tasks = []
        aborted_sequence: list[SequencedItem] = self.sequencer.get_aborted_sequence(
            self.t_ids_to_reschedule,
        )
        self.committed_fallback = len(aborted_sequence)

        self.networking.clear_aborted_events_for_fallback()
        for sequenced_item in aborted_sequence:
            # current worker is the root of the chain
            self.networking.reset_ack_for_fallback(sequenced_item.t_id)
            fallback_tasks.append(
                self.run_fallback_function(
                    sequenced_item.t_id,
                    sequenced_item.payload,
                ),
            )
        # logging.warning(f"Remote function calls: {self.networking.remote_function_calls}")

        await self.sync_workers(
            msg_type=MessageType.AriaFallbackStart,
            message=(self.id,),
            serializer=Serializer.MSGPACK,
        )
        logging.debug("FALLBACK_SYNC_START_DONE")
        if fallback_tasks:
            await asyncio.gather(*fallback_tasks)
            # Flush all fallback egress sends in one batch (each root above
            # used `egress.send`, which enqueues; `send_batch` awaits the
            # producer futures together).
            await self.egress.send_batch()

        logging.debug(
            f"Epoch: {self.sequencer.epoch_counter} Fallback strategy done waiting for peers",
        )

        await self.sync_workers(
            msg_type=MessageType.AriaFallbackDone,
            message=(self.id,),
            serializer=Serializer.MSGPACK,
        )
        logging.debug("FALLBACK_SYNC_DONE_DONE")

    async def unlock_tid(self, t_id_to_unlock: int) -> None:
        if t_id_to_unlock in self.fallback_locking_event_map:
            async with self.fallback_locking_event_map_lock:
                self.fallback_locking_event_map[t_id_to_unlock].set()
        else:
            logging.error(
                f"{self.sequencer.epoch_counter} Unlock tid {t_id_to_unlock} not found. But should exist!",
            )

    async def send_responses(
        self,
        current_sequence_t_ids: list[SequencedItem],
        client_responses: dict[int, str],
        aborted_events: dict[int, str],
    ) -> None:
        for sequenced_item in current_sequence_t_ids:
            t_id = sequenced_item.t_id
            request_id = sequenced_item.payload.request_id
            operator_name = sequenced_item.payload.operator_name
            partition = sequenced_item.payload.partition
            if t_id in aborted_events:
                await self.egress.send(
                    key=request_id,
                    value=msgpack_serialization(aborted_events[t_id]),
                    operator_name=operator_name,
                    partition=partition,
                )
            elif t_id in client_responses:
                await self.egress.send(
                    key=request_id,
                    value=msgpack_serialization(client_responses[t_id]),
                    operator_name=operator_name,
                    partition=partition,
                )
        await self.egress.send_batch()
        self.wait_responses_to_be_sent.set()

    async def fallback_unlock(self, t_id: int, success: bool) -> None:
        # Release the locks for local
        await self.unlock_tid(t_id)
        # Release the locks for remote participants
        if self.networking.chain_participants.get(t_id):
            async with asyncio.TaskGroup() as tg:
                for participant in self.networking.chain_participants[t_id]:
                    tg.create_task(
                        self.networking.send_message(
                            self.peers[participant][0],
                            self.peers[participant][2],
                            msg=(t_id, success),
                            msg_type=MessageType.Unlock,
                            serializer=Serializer.MSGPACK,
                        ),
                    )

    async def sync_workers(
        self,
        msg_type: MessageType,
        message: tuple | bytes,
        serializer: Serializer = Serializer.MSGPACK,
    ) -> None:
        await self.networking.send_message(
            DISCOVERY_HOST,
            DISCOVERY_PORT + 1,
            msg=message,
            msg_type=msg_type,
            serializer=serializer,
        )
        await self.sync_workers_event[msg_type].wait()
        self.sync_workers_event[msg_type].clear()
