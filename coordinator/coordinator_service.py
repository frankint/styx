#!/usr/bin/env python3

import asyncio
from asyncio import StreamReader, StreamWriter
from collections.abc import Awaitable, Callable
import concurrent.futures
import contextlib
from copy import deepcopy
from enum import Enum, auto
from math import ceil
import os
import socket
import struct
import time
from timeit import default_timer as timer
from typing import TYPE_CHECKING

from aria_sync_metadata import AriaSyncMetadata
import boto3
import botocore
from coordinator_metadata import Coordinator
from prometheus_client import Counter, Gauge, start_http_server
from setuptools._distutils.util import strtobool
from sliding_window_metric import SlidingWindowMetric
from styx.common.base_networking import SOCKET_RCV_BUF, SOCKET_SND_BUF
from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.metrics import WorkerEpochStats
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.tcp_networking import MessagingMode, NetworkingManager
from styx.common.util.aio_task_scheduler import AIOTaskScheduler
import uvloop

from coordinator.capacity_model import SystemCapacityEstimator
try:
    from coordinator.forecaster_factory import create_forecaster
except ImportError:
    from forecaster_factory import create_forecaster
from coordinator.metric_buffer import AggregatingMetricBuffer
from coordinator.migration_metadata import MigrationMetadata
from coordinator.pid_controller import BacklogPIDController

if TYPE_CHECKING:
    from styx.common.stateflow_graph import StateflowGraph

    from coordinator.worker_pool import Worker

import csv
import json 
import time
import os

SUPER_VERBOSE = True

SERVER_PORT = 8888
PROTOCOL_PORT = 8889

S3_ENDPOINT: str = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY: str = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY: str = os.environ["S3_SECRET_KEY"]
S3_REGION: str = os.getenv("S3_REGION", "us-east-1")

PROTOCOL = Protocols.Aria

SNAPSHOT_BUCKET_NAME: str = os.getenv("SNAPSHOT_BUCKET_NAME", "styx-snapshots")
SNAPSHOT_FREQUENCY_SEC = int(os.getenv("SNAPSHOT_FREQUENCY_SEC", "30"))
HEARTBEAT_CHECK_INTERVAL: int = int(
    os.getenv("HEARTBEAT_CHECK_INTERVAL", "1000"),
)  # 1000ms
S3_INIT_RETRY_SEC: float = float(os.getenv("S3_INIT_RETRY_SEC", "2"))
S3_INIT_MAX_RETRIES: int = int(os.getenv("S3_INIT_MAX_RETRIES", "30"))

SEQUENCE_MAX_SIZE: int = int(os.getenv("SEQUENCE_MAX_SIZE", "1_000"))
ASYNC_MIGRATION_BATCH_SIZE: int = int(os.getenv("ASYNC_MIGRATION_BATCH_SIZE", "2000"))
FORECASTER_FORECAST_INTERVAL: float = float(os.getenv("FORECASTER_FORECAST_INTERVAL", "2.0"))

CoordHandler = Callable[[StreamWriter, bytes, concurrent.futures.ProcessPoolExecutor], Awaitable[None]]


class RecoveryState(Enum):
    IDLE = auto()
    RECOVERING = auto()


class CoordinatorService:
    def __init__(self) -> None:
        self._init_networking_stack()
        self._init_listen_sockets()
        self._init_recovery_fields()
        self._init_prometheus_metrics()
        self._init_concurrency_and_handlers()
        self._init_scaling_capacity_and_chronos()

    @staticmethod
    def _bind_listen_tcp_socket(port: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )  # Enable LINGER, timeout 0
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        sock.bind(("0.0.0.0", port))  # noqa: S104
        sock.setblocking(False)
        return sock

    def _init_networking_stack(self) -> None:
        self.networking = NetworkingManager(SERVER_PORT)
        self.protocol_networking = NetworkingManager(
            PROTOCOL_PORT,
            size=4,
            mode=MessagingMode.PROTOCOL_PROTOCOL,
        )
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
        )
        self.coordinator = Coordinator(self.networking, self.s3_client)
        self.aio_task_scheduler = AIOTaskScheduler()
        self.aio_task_scheduler_coord = AIOTaskScheduler()
        self.puller_task: asyncio.Task | None = None

    def _init_listen_sockets(self) -> None:
        self.coor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coor_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )  # Enable LINGER, timeout 0
        self.coor_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.coor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_SND_BUF)
        self.coor_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_RCV_BUF)
        self.coor_socket.bind(("0.0.0.0", SERVER_PORT))  # noqa: S104
        self.coor_socket.setblocking(False)

        self.protocol_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )  # Enable LINGER, timeout 0
        self.protocol_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_SND_BUF)
        self.protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_RCV_BUF)
        self.protocol_socket.bind(("0.0.0.0", SERVER_PORT + 1))  # noqa: S104
        self.protocol_socket.setblocking(False)

    def _init_recovery_fields(self) -> None:
        self.aria_metadata: AriaSyncMetadata | None = None
        self.migration_metadata: MigrationMetadata | None = None
        self.workers_that_re_registered: list[Worker] = []
        self.recovery_lock: asyncio.Lock = asyncio.Lock()
        self.recovery_state: RecoveryState = RecoveryState.IDLE

    def _init_prometheus_metrics(self) -> None:
        self.metrics_server = start_http_server(8000)
        self.live_worker_count_gauge = Gauge("live_worker_count", "Number of live workers registered with coordinator")
        self.cpu_usage_gauge = Gauge("worker_cpu_usage_percent", "CPU usage percentage", ["instance"])
        self.memory_usage_gauge = Gauge("worker_memory_usage_mb", "Memory usage in MB", ["instance"])
        self.network_rx_gauge = Gauge("worker_network_rx_kb", "Network received KB", ["instance"])
        self.network_tx_gauge = Gauge("worker_network_tx_kb", "Network transmitted KB", ["instance"])
        self.epoch_latency_gauge = Gauge("worker_epoch_latency_ms", "Epoch Latency (ms)", ["instance"])
        self.epoch_throughput_gauge = Gauge(
            "worker_epoch_throughput_tps", "Epoch Throughput (transactions per second)", ["instance"]
        )
        self.epoch_abort_gauge = Gauge("worker_abort_percent", "Epoch Concurrency Abort percentage", ["instance"])
        self.latency_breakdown_gauge = Gauge(
            "latency_breakdown",
            "Time Spent in different phases within the transactional protocol",
            ["instance", "component"],
        )
        self.snapshotting_gauge = Gauge("worker_total_snapshotting_time_ms", "Snapshotting time (ms)", ["instance"])
        self.heartbeat_gauge = Gauge("time_since_last_heartbeat", "Time Since Last Heartbeat", ["instance"])
        self.queue_backlog_gauge = Gauge("queue_backlog", "Backlog in the worker queue", ["instance"])
        self.idle_time_ms_gauge = Gauge("idle_time_ms_per_second", "Idle time ms per second", ["instance"])

        self.input_rate_counter = Counter("input_rate_counter", "Input rate", ["instance"])
        # Transaction count metrics
        self.epoch_total_txns_counter = Counter(
            "epoch_total_transactions", "Total transactions processed (cumulative)", ["instance"]
        )
        self.epoch_committed_txns_counter = Counter(
            "epoch_committed_transactions", "Committed transactions (cumulative)", ["instance"]
        )
        self.epoch_logic_aborts_counter = Counter(
            "epoch_logic_aborts", "Logic/global aborts (cumulative)", ["instance"]
        )
        self.epoch_concurrency_aborts_counter = Counter(
            "epoch_concurrency_aborts", "Concurrency aborts (cumulative)", ["instance"]
        )
        self.epoch_committed_lock_free_counter = Counter(
            "epoch_committed_lock_free", "Transactions committed in lock-free phase (cumulative)", ["instance"]
        )
        self.epoch_committed_fallback_counter = Counter(
            "epoch_committed_fallback", "Transactions committed in fallback phase (cumulative)", ["instance"]
        )
        # Metrics for downscaling policies
        self.empty_epoch_gauge = Gauge(
            "worker_empty_epoch", "1 if epoch had no local work (just sync), 0 otherwise", ["instance"]
        )

        self.cpu_utilization_ratio_gauge = Gauge(
            "worker_cpu_utilization", "Ratio of CPU work in the epoch", ["instance"]
        )
        self.io_utilization_ratio_gauge = Gauge(
            "worker_io_utilization", "Ratio of IO wait time in the epoch", ["instance"]
        )
        # Operator-level performance metrics
        self.operator_tps_counter = Counter(
            "operator_tps",
            "Transactions per second per operator partition (cumulative)",
            ["instance", "operator", "partition"],
        )
        self.operator_call_count_counter = Counter(
            "operator_call_count",
            "Number of calls to an operator partition (cumulative)",
            ["instance", "operator", "partition"],
        )
        self.operator_latency_gauge = Gauge(
            "operator_latency_ms",
            "Average operator call latency in ms for this epoch",
            ["instance", "operator", "partition"],
        )

        self.migration_start_time_gauge = Gauge("migration_start_time_ms", "Timestamp when the migration started", [])
        self.migration_end_time_gauge = Gauge("migration_end_time_ms", "Timestamp when the migration completed", [])

        self.migration_start_time: float = 0.0
        self.migration_end_time: float = 0.0

        # Used for annotations in the grafana dashboard
        self.migration_start_count = Counter("migration_start_total", "Number of migrations started", [])
        self.migration_end_count = Counter("migration_end_total", "Number of migrations completed", [])

        # Phase-attributed resource metrics (aggregated per epoch in the worker, scraped at coordinator).
        self.phase_cpu_ms_total = Counter(
            "phase_cpu_ms_total",
            "Process CPU time attributed to a transactional protocol phase (ms, cumulative)",
            ["instance", "phase"],
        )
        self.phase_net_rx_bytes_total = Counter(
            "phase_net_rx_bytes_total",
            "Network RX bytes attributed to a transactional protocol phase (bytes, cumulative)",
            ["instance", "phase"],
        )
        self.phase_net_tx_bytes_total = Counter(
            "phase_net_tx_bytes_total",
            "Network TX bytes attributed to a transactional protocol phase (bytes, cumulative)",
            ["instance", "phase"],
        )
        self.phase_rss_max_mb = Gauge(
            "phase_rss_max_mb",
            "Max RSS observed during a transactional protocol phase within the last reported epoch (MB)",
            ["instance", "phase"],
        )

        self.migration_in_progress: bool = False

    def _init_concurrency_and_handlers(self) -> None:
        self.networking_locks: dict[MessageType, asyncio.Lock] = {
            MessageType.SendExecutionGraph: asyncio.Lock(),
            MessageType.UpdateExecutionGraph: asyncio.Lock(),
            MessageType.MigrationRepartitioningDone: asyncio.Lock(),
            MessageType.MigrationDone: asyncio.Lock(),
            MessageType.MigrationInitDone: asyncio.Lock(),
            MessageType.MigrationReadyToStart: asyncio.Lock(),
            MessageType.RegisterWorker: asyncio.Lock(),
            MessageType.SnapID: asyncio.Lock(),
            MessageType.Heartbeat: asyncio.Lock(),
            MessageType.AriaProcessingDone: asyncio.Lock(),
            MessageType.AriaCommit: asyncio.Lock(),
            MessageType.AriaFallbackStart: asyncio.Lock(),
            MessageType.AriaFallbackDone: asyncio.Lock(),
            MessageType.SyncCleanup: asyncio.Lock(),
            MessageType.DeterministicReordering: asyncio.Lock(),
            MessageType.ReadyAfterRecovery: asyncio.Lock(),
        }

        self.snapshotting_task: asyncio.Task | None = None

        self._protocol_controller_handlers_map: dict[MessageType, Callable[[bytes], Awaitable[None]]] = {
            MessageType.AriaProcessingDone: self._handle_aria_processing_done,
            MessageType.AriaCommit: self._handle_aria_commit,
            MessageType.AriaFallbackStart: self._handle_aria_fallback_sync,
            MessageType.AriaFallbackDone: self._handle_aria_fallback_sync,
            MessageType.SyncCleanup: self._handle_sync_cleanup,
            MessageType.DeterministicReordering: self._handle_deterministic_reordering,
            MessageType.MigrationDone: self._handle_migration_done,
        }

        self._coordinator_handlers_map: dict[MessageType, CoordHandler] = {
            MessageType.SendExecutionGraph: self._handle_send_execution_graph,
            MessageType.UpdateExecutionGraph: self._handle_update_execution_graph,
            MessageType.MigrationRepartitioningDone: self._handle_migration_repartitioning_done,
            MessageType.MigrationInitDone: self._handle_migration_init_done,
            MessageType.MigrationReadyToStart: self._handle_migration_ready_to_start,
            MessageType.RegisterWorker: self._handle_register_worker,
            MessageType.SnapID: self._handle_snap_id,
            MessageType.Heartbeat: self._handle_heartbeat,
            MessageType.ReadyAfterRecovery: self._handle_ready_after_recovery,
            MessageType.InitDataComplete: self._handle_init_data_complete,
        }

    def _init_scaling_capacity_and_chronos(self) -> None:
        self.enable_autoscale: bool = bool(strtobool(os.getenv("ENABLE_AUTOSCALE", "true")))
        self.scale_cooldown_period: float = 30.0
        self.capacity_confidence_threshold: float = 0.25
        self.downscale_safety_factor: float = 0.90
        self._pending_downscale: bool = False
        self._downscale_victim_ids: set[int] = set()

        self.last_scale_action_time: float = time.time()
        self.scale_window_seconds: int = 10
        self.migration_time_window: SlidingWindowMetric = SlidingWindowMetric(self.scale_window_seconds)
        self.epoch_duration_window = SlidingWindowMetric(self.scale_window_seconds)
        self.tps_sliding_window: SlidingWindowMetric = SlidingWindowMetric(self.scale_window_seconds)

        self.sec_per_moved_key_ewma: float | None = None
        self.sec_per_moved_key_ewma_alpha: float = 0.5
        self._migration_keys_to_move: float = 0.0

        self._last_action_was_downscale: bool = False
        self._downscale_suppressed_until: float = 0.0
        self._downscale_strikes: int = 0
        self._downscale_penalty_base: float = self.scale_cooldown_period * 2
        self._downscale_penalty_max: float = self.scale_cooldown_period * 30

        self.pid_controller = BacklogPIDController()
        # Per-epoch accumulators: populated as each worker reports, consumed when all have synced
        self.epoch_backlog_accum: dict[int, float] = {}
        self.epoch_rate_accum: dict[int, float] = {}
        self.total_tps_accum: dict[int, float] = {}
        self.num_keys_accum: dict[int, int] = {}

        self.system_capacity_estimator: SystemCapacityEstimator = SystemCapacityEstimator(
            sequence_max_size=SEQUENCE_MAX_SIZE,
        )
        self._capacity_ewma: float | None = None
        self._capacity_ewma_alpha: float = 0.3  # smoothing factor (0→slow, 1→no smoothing)

        # Total live state keys across the cluster (refreshed each epoch sync).
        self.total_keys: int = 0

        # Chronos forecaster (background process)
        self.total_keys_accum: int = 0
        max_context_len = int(os.getenv("FORECASTER_MAX_CONTEXT_LENGTH", "512"))
        self.metric_buffer: AggregatingMetricBuffer = AggregatingMetricBuffer(
            bucket_interval=1.0, max_buckets=max_context_len
        )
        self.chronos_forecaster: Any | None = None
        self.forecaster_task: asyncio.Task | None = None

        os.makedirs("raw_predictions", exist_ok=True)
        start_timestamp = time.strftime("%Y%m%d_%H%M%S")
        self.prediction_log_file = f"raw_predictions/predictions_vs_actual_{start_timestamp}.csv"

        if not os.path.exists(self.prediction_log_file):
            with open(self.prediction_log_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "actual_tps", "predicted_tps_horizon"])

    def _migration_workers(self) -> list:
        """Workers that must participate in the current migration.
        During a downscale, victims have zero assignments (not 'participating')
        but still hold state and must hit every migration barrier.
        """
        if self._pending_downscale:
            return self.coordinator.worker_pool.get_live_workers()
        return self.coordinator.worker_pool.get_participating_workers()

    async def scale_up(self, new_n_partitions: int, new_worker_num: int) -> None:
        if self.migration_in_progress:
            logging.warning("Scale Up requested but a migration is already in progress.")
            return
        if not self.coordinator.graph_submitted or self.coordinator.submitted_graph is None:
            logging.warning("Scale Up requested but no graph is currently submitted.")
            return

        if new_n_partitions <= 0:
            logging.warning(f"Scale Up requested with invalid new_n_partitions={new_n_partitions}")
            return

        # 1) Build an updated graph (same topology, updated partition count)
        # Ensure we never reduce partitions below the current count (Kafka can't shrink partitions)
        current_partitions = self.coordinator.submitted_graph.max_operator_parallelism
        effective_n_partitions = max(new_n_partitions, current_partitions)
        # Actual live operator partition count (basis for how many keys rehash);
        # captured before the graph update mutates it.
        old_operator_partitions = self._graph_operator_partitions(self.coordinator.submitted_graph)

        new_graph = deepcopy(self.coordinator.submitted_graph)
        new_graph.max_operator_parallelism = effective_n_partitions
        for _op_name, op in iter(new_graph):
            # Scale all operators uniformly for now (fits YCSB demo; can be extended per-operator later)
            if hasattr(op, "set_n_partitions"):
                op.set_n_partitions(effective_n_partitions)
            else:
                op.n_partitions = effective_n_partitions

        # 2) Mark migration in progress and stop snapshotting.
        #    Protocol stop is deferred to finalize_migration_repartition().
        self.migration_in_progress = True
        await self.stop_snapshotting()

        # 3) Activate the new workers
        activation_time = timer()
        for _ in range(new_worker_num):
            worker = self.coordinator.worker_pool.activate_standby_worker(current_time=activation_time)
            if worker is None:
                logging.error("No standby workers available to activate")
                break
            logging.warning(f"Activated standby worker_id: {worker.worker_id}")

        # 4) Expand Kafka topic partitions to match the new partition count
        if effective_n_partitions > current_partitions:
            await self.coordinator.expand_kafka_topic_partitions(new_graph, effective_n_partitions)
            # Allow time for Kafka metadata to propagate to brokers and workers
            await asyncio.sleep(2)

        # 5) Recompute assignments so standby/new workers become participating
        self.coordinator.reschedule_all_partitions_round_robin(new_graph)

        # 6) Start the data migration
        self.migration_metadata = MigrationMetadata(len(self.coordinator.worker_pool.get_participating_workers()))
        logging.warning(
            f"SCALE_UP | starting migration | new_n_partitions={effective_n_partitions} "
            f"workers_live={len(self.coordinator.worker_pool.get_live_workers())} "
            f"workers_participating={len(self.coordinator.worker_pool.get_participating_workers())} "
            f"new graph={new_graph}"
        )
        await self.coordinator.update_stateflow_graph(new_graph)
        # Keys actually rehashed to a new partition by this scale-up; used to
        # normalize the measured duration into a per-moved-key rate on completion.
        self._migration_keys_to_move = self.total_keys * self._f_migrate(
            old_operator_partitions,
            effective_n_partitions,
        )
        start_time = time.time_ns()
        self.migration_start_time_gauge.set(start_time / 1_000_000)
        self.migration_start_time = start_time
        self.migration_start_count.inc()
        self.live_worker_count_gauge.set(len(self.coordinator.worker_pool.get_live_workers()))
        self._note_scale_action(is_downscale=False)

    def _note_scale_action(self, is_downscale: bool) -> None:
        now = time.time()
        if not is_downscale and self._last_action_was_downscale:
            # scale-up is the first action after a scale-down => the down was wrong
            self._downscale_strikes += 1
            penalty = min(
                self._downscale_penalty_base * (2 ** (self._downscale_strikes - 1)),
                self._downscale_penalty_max,
            )
            self._downscale_suppressed_until = now + penalty
            logging.warning(
                f"DOWNSCALE | correction detected (strike #{self._downscale_strikes}); "
                f"suppressing downscale for {penalty:.0f}s"
            )
        elif is_downscale and now > self._downscale_suppressed_until and self._last_action_was_downscale:
            # long stretch with no correction -> forgive past strikes
            self._downscale_strikes = 0
        self._last_action_was_downscale = is_downscale

    async def scale_down(self, workers_to_remove: int) -> None:
        """Scale down by removing *workers_to_remove* workers without changing
        the partition count. The existing partitions are redistributed across
        the surviving workers; victims keep zero assignments but stay live so
        they can participate in migration (send their state). After migration
        completes, victims are deactivated to standby.
        """
        if self.migration_in_progress:
            logging.warning("Scale Down requested but a migration is already in progress.")
            return
        if not self.coordinator.graph_submitted or self.coordinator.submitted_graph is None:
            logging.warning("Scale Down requested but no graph is currently submitted.")
            return

        live_workers = self.coordinator.worker_pool.get_live_workers()
        if workers_to_remove <= 0 or workers_to_remove >= len(live_workers):
            logging.warning(
                f"Scale Down requested with invalid workers_to_remove={workers_to_remove} (live={len(live_workers)})"
            )
            return

        # 1) Pick victims witg smallest backlog
        sorted_workers = sorted(
            live_workers,
            key=lambda w: self.epoch_backlog_accum.get(w.worker_id, 0),
        )
        self._downscale_victim_ids = {w.worker_id for w in sorted_workers[:workers_to_remove]}

        # 2) Mark migration in progress and stop snapshotting.
        self.migration_in_progress = True
        self._pending_downscale = True
        await self.stop_snapshotting()

        graph = deepcopy(self.coordinator.submitted_graph)
        # 3) Reschedule all partitions onto survivors only.
        self.coordinator.worker_pool.reschedule_excluding(self._downscale_victim_ids, graph)

        # 4) Start the migration pipeline. Barrier counts include ALL live workers (including victims) because
        # every worker must hit every migration barrier, include_all_live=True ensures this
        n_live = len(self.coordinator.worker_pool.get_live_workers())
        self.migration_metadata = MigrationMetadata(n_live)
        logging.warning(
            f"SCALE_DOWN | starting migration | workers_to_remove={workers_to_remove} "
            f"victims={self._downscale_victim_ids} "
            f"workers_live={n_live}"
            f"workers_participating={len(self.coordinator.worker_pool.get_participating_workers())}"
        )
        await self.coordinator.update_stateflow_graph(graph, include_all_live=True)

        self._migration_keys_to_move = 0.0
        start_time = time.time_ns()
        self.migration_start_time_gauge.set(start_time / 1_000_000)
        self.migration_start_time = start_time
        self.migration_start_count.inc()
        self.live_worker_count_gauge.set(n_live)
        self._note_scale_action(is_downscale=True)

    async def coordinator_controller(
        self,
        transport: StreamWriter,
        data: bytes,
        pool: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        try:
            mt: MessageType = self.networking.get_msg_type(data)
            if SUPER_VERBOSE:
                # logging.warning(f"COORDINATOR SERVER: Received message of type {mt}")
                pass
            handler = self._coordinator_handlers_map.get(mt)
            if handler is None:
                logging.error(f"COORDINATOR SERVER: Non supported message type: {mt}")
                return
            await handler(transport, data, pool)
            if SUPER_VERBOSE:
                # logging.warning(f"COORDINATOR SERVER: Finished handling message of type {mt}")
                pass
        except Exception as e:
            if SUPER_VERBOSE:
                logging.warning(f"COORDINATOR SERVER: Exception in coordinator_controller handling message: {e}", exc_info=True)
            raise

    # ------------------------
    # Handlers
    # ------------------------
    async def _handle_send_execution_graph(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.SendExecutionGraph
        async with self.networking_locks[mt]:
            (graph,) = self.networking.decode_message(data)

            if not self.coordinator.graph_submitted:
                await self._submit_initial_graph(graph)
            else:
                logging.warning(
                    "Another graph is deployed! You have to use the update API! "
                    "(Graph multitenancy is currently not supported)"
                )
                return

            logging.info("Submitted Stateflow Graph to Workers")

    async def _handle_update_execution_graph(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.SendExecutionGraph
        async with self.networking_locks[mt]:
            (graph,) = self.networking.decode_message(data)
            if not self.coordinator.graph_submitted:
                logging.warning("No graph exists in the cluster, cannot initiate an update!")
                return
            logging.warning("compatible, migration_required = graph.compare_with(self.coordinator.submitted_graph)")
            compatible, migration_required = graph.compare_with(self.coordinator.submitted_graph)
            logging.warning("compatible, migration_required = graph.compare_with(self.coordinator.submitted_graph) done")
            if not compatible:
                logging.warning("Graph is incompatible!")
                return
            if not self.migration_in_progress:
                if migration_required:
                    await self._start_migration(graph)
                else:
                    await self._update_the_deployed_graph_code(graph)
            else:
                logging.warning("A migration is currently in progress! Cannot update the cluster at the moment...")
                return

            logging.info("Submitted Stateflow Graph Update to Workers")

    async def _start_migration(self, graph: StateflowGraph) -> None:
        # Phase A: do NOT stop the protocol yet — workers will rehash in the background
        self.migration_in_progress = True
        await self.stop_snapshotting()

        old_partitions = self._graph_operator_partitions(self.coordinator.submitted_graph)
        new_partitions = self._graph_operator_partitions(graph)
        self._migration_keys_to_move = self.total_keys * self._f_migrate(old_partitions, new_partitions)

        logging.warning(f"MIGRATION | START {graph}")
        await self.coordinator.update_stateflow_graph(graph)

        n_workers = len(self.coordinator.worker_pool.get_participating_workers())
        self.migration_metadata = MigrationMetadata(n_workers)

    async def _update_the_deployed_graph_code(self, graph: StateflowGraph) -> None:
        # TODO add the functionality to update the code in the next epoch
        logging.warning("Graph code updates not implemented yet! %s", graph)

    async def _submit_initial_graph(self, graph: StateflowGraph) -> None:
        await self.coordinator.submit_stateflow_graph(graph)
        n_workers = len(self.coordinator.worker_pool.get_participating_workers())
        logging.info(f"Submitting graph with {n_workers} live worker(s)")
        self.aria_metadata = AriaSyncMetadata(n_workers)

    async def _handle_migration_repartitioning_done(
        self,
        _: StreamWriter,
        __: bytes,
        ___: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.MigrationRepartitioningDone
        logging.warning("DEBUG_MIGRATION | Migration repartitioning done received!")

        async with self.networking_locks[mt]:
            sync_complete: bool = await self.migration_metadata.repartitioning_done()

            logging.warning(f"DEBUG_MIGRATION | Migration repartitioning is complete: {sync_complete}")

            if not sync_complete:
                return

            logging.warning("DEBUG_MIGRATION | Calling finalize_migration_repartition")
            await self.finalize_migration_repartition()
            logging.warning("DEBUG_MIGRATION | Calling migration_metadata.cleanup(mt)")
            await self.migration_metadata.cleanup(mt)
            logging.warning("DEBUG_MIGRATION | _handle_migration_repartitioning_done finished")

    async def _handle_migration_init_done(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.MigrationInitDone
        async with self.networking_locks[mt]:
            epoch_counter, t_counter, input_offsets, output_offsets = self.networking.decode_message(data)

            sync_complete: bool = await self.migration_metadata.init_done(
                epoch_counter,
                t_counter,
                input_offsets,
                output_offsets,
            )
            logging.warning(f"MIGRATION | MigrationInitDone | {self.migration_metadata.sync_sum}")

            if not sync_complete:
                return

            logging.warning("MIGRATION | MigrationInitDone | sync_complete")
            n_workers = len(self.coordinator.worker_pool.get_participating_workers())

            self.aria_metadata = AriaSyncMetadata(n_workers)
            await self.protocol_networking.close_all_connections()
            await self.finalize_migration()
            await self.migration_metadata.cleanup(mt)

    async def _handle_migration_ready_to_start(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        """
        Each participating worker reports here once it has finished Phase B
        (runtime rebuilt, protocol relaunched). Only when every worker has
        reported do we broadcast the go-ahead, so no worker starts the
        post-migration epoch while a peer is still rebuilding the
        transactional protocol and dropping important messages.
        """
        mt = MessageType.MigrationReadyToStart
        async with self.networking_locks[mt]:
            (worker_id,) = self.networking.decode_message(data)
            sync_complete: bool = await self.migration_metadata.set_empty_sync_done(mt)
            logging.warning(
                f"MIGRATION | MigrationReadyToStart | worker {worker_id} | "
                f"{self.migration_metadata.sync_sum[mt]}/{self.migration_metadata.n_workers}",
            )
            if not sync_complete:
                return

            logging.warning("MIGRATION | MigrationReadyToStart | all workers ready, releasing")
            await self.finalize_migration_ready_to_start()
            await self.migration_metadata.cleanup(mt)

    async def _handle_register_worker(
        self,
        transport: StreamWriter,
        data: bytes,
        _: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.RegisterWorker
        async with self.networking_locks[mt]:
            worker_ip, worker_port, protocol_port, standby = self.networking.decode_message(data)

            worker_id, init_recovery = self.coordinator.register_worker(
                worker_ip,
                worker_port,
                protocol_port,
                standby,
            )

            transport.write(
                self.networking.encode_message(
                    msg=worker_id,
                    msg_type=MessageType.RegisterWorker,
                    serializer=Serializer.MSGPACK,
                ),
            )

            if init_recovery:
                await self._track_reregistered_worker(worker_id)

            logging.warning(
                f"Worker registered {worker_ip}:{worker_port} with id {worker_id}",
            )
            self.live_worker_count_gauge.set(len(self.coordinator.worker_pool.get_live_workers()))

    async def _track_reregistered_worker(self, worker_id: int) -> None:
        async with self.recovery_lock:
            self.workers_that_re_registered.append(
                self.coordinator.get_worker_with_id(worker_id),
            )

    async def _handle_snap_id(
        self,
        _: StreamWriter,
        data: bytes,
        pool: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.SnapID
        async with self.networking_locks[mt]:
            (
                worker_id,
                snapshot_id,
                start,
                end,
                partial_input_offsets,
                partial_output_offsets,
                epoch_counter,
                t_counter,
                sn_size,
            ) = self.networking.decode_message(data)

            snapshot_time = end - start
            self.snapshotting_gauge.labels(instance=worker_id).set(snapshot_time)

            logging.warning(
                f"Worker: {worker_id} | "
                f"@Epoch: {epoch_counter} | "
                f"Completed snapshot: {snapshot_id} | "
                f"started at: {start} | "
                f"ended at: {end} | "
                f"took: {snapshot_time}ms | "
                f"size: {sn_size} Bytes"
            )

            self.coordinator.register_snapshot(
                worker_id,
                snapshot_id,
                partial_input_offsets,
                partial_output_offsets,
                epoch_counter,
                t_counter,
                pool,
            )

    async def _handle_heartbeat(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.Heartbeat
        async with self.networking_locks[mt]:
            worker_id, cpu_perc, mem_util, rx_net, tx_net = self.networking.decode_message(data)

            self.cpu_usage_gauge.labels(instance=worker_id).set(cpu_perc)  # %
            self.memory_usage_gauge.labels(instance=worker_id).set(mem_util)  # MB
            self.network_rx_gauge.labels(instance=worker_id).set(rx_net)  # KB
            self.network_tx_gauge.labels(instance=worker_id).set(tx_net)  # KB

            heartbeat_rcv_time = timer()
            # logging.info(
            #    f"Heartbeat received from: {worker_id} at time: {heartbeat_rcv_time}",
            # )

            self.coordinator.register_worker_heartbeat(worker_id, heartbeat_rcv_time)

    async def _handle_ready_after_recovery(
        self,
        _: StreamWriter,
        data: bytes,
        __: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        mt = MessageType.ReadyAfterRecovery
        async with self.networking_locks[mt]:
            (worker_id,) = self.networking.decode_message(data)
            self.coordinator.worker_is_ready_after_recovery(worker_id)
            logging.info(f"ready after recovery received from: {worker_id}")

    async def _handle_init_data_complete(
        self,
        _: StreamWriter,
        __: bytes,
        ___: concurrent.futures.ProcessPoolExecutor,
    ) -> None:
        self.coordinator.init_data_complete()
        await asyncio.sleep(0)

    async def protocol_controller(self, data: bytes) -> None:
        try:
            mt: MessageType = self.protocol_networking.get_msg_type(data)
            if SUPER_VERBOSE:
                # logging.warning(f"COORDINATOR PROTOCOL SERVER: Received message of type {mt}")
                pass
            handler = self._protocol_controller_handlers_map.get(mt)
            if handler is None:
                logging.error(
                    f"COORDINATOR PROTOCOL SERVER: Non supported message type: {mt}",
                )
                return
            await handler(data)
            if SUPER_VERBOSE:
                # logging.warning(f"COORDINATOR PROTOCOL SERVER: Finished handling message of type {mt}")
                pass
        except Exception as e:
            if SUPER_VERBOSE:
                logging.warning(f"COORDINATOR PROTOCOL SERVER: Exception handling message: {e}", exc_info=True)
            raise

    # ------------------------
    # Handlers
    # ------------------------
    async def _handle_aria_processing_done(self, data: bytes) -> None:
        mt = MessageType.AriaProcessingDone
        async with self.networking_locks[mt]:
            if not self.aria_metadata.sent_proceed_msg:
                self.aria_metadata.sent_proceed_msg = True
                await self.worker_wants_to_proceed()

            worker_id, remote_logic_aborts = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_aria_processing_done(
                worker_id,
                remote_logic_aborts,
            )
            if not sync_complete:
                return

            self.aria_metadata.reset(mt)
            await self.finalize_worker_sync(
                mt,
                (self.aria_metadata.logic_aborts_everywhere,),
                Serializer.PICKLE,
            )

    async def _handle_aria_commit(self, data: bytes) -> None:
        mt = MessageType.AriaCommit
        async with self.networking_locks[mt]:
            worker_id, aborted, remote_t_counter, processed_seq_size = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_aria_commit_done(
                worker_id,
                aborted,
                remote_t_counter,
                processed_seq_size,
            )
            if not sync_complete:
                return

            commit_payload = (
                self.aria_metadata.concurrency_aborts_everywhere,
                self.aria_metadata.processed_seq_size,
                self.aria_metadata.max_t_counter,
                self.aria_metadata.take_snapshot,
            )
            self.aria_metadata.reset(mt)
            await self.finalize_worker_sync(
                mt,
                commit_payload,
                Serializer.PICKLE,
            )

    async def _handle_aria_fallback_sync(self, data: bytes) -> None:
        # Handles both AriaFallbackStart and AriaFallbackDone
        mt: MessageType = self.protocol_networking.get_msg_type(data)
        async with self.networking_locks[mt]:
            (worker_id,) = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_empty_sync_done(mt, worker_id)
            if not sync_complete:
                return

            self.aria_metadata.reset(mt)
            await self.finalize_worker_sync(
                mt,
                b"",
                Serializer.NONE,
            )

    async def _handle_sync_cleanup(self, data: bytes) -> None:
        mt = MessageType.SyncCleanup
        async with self.networking_locks[mt]:
            (
                worker_id,
                epoch_throughput,
                epoch_latency,
                local_abort_rate,
                wal_time,
                func_time,
                chain_ack_time,
                sync_time,
                conflict_res_time,
                commit_time,
                fallback_time,
                snap_time,
                input_rate,
                queue_backlog,
                idle_time_ms,
                total_txns,
                committed_txns,
                logic_aborts,
                concurrency_aborts,
                committed_lock_free,
                committed_fallback,
                empty_epoch,
                cpu_utilization,
                io_wait_utilization,
                operator_epoch_stats,
                phase_resources,
                key_counts,
            ) = self.protocol_networking.decode_message(data)

            worker_epoch_stats = WorkerEpochStats(
                worker_id=worker_id,
                epoch_throughput=epoch_throughput,
                epoch_latency=epoch_latency,
                local_abort_rate=local_abort_rate,
                wal_time=wal_time,
                func_time=func_time,
                chain_ack_time=chain_ack_time,
                sync_time=sync_time,
                conflict_res_time=conflict_res_time,
                commit_time=commit_time,
                fallback_time=fallback_time,
                snap_time=snap_time,
                input_rate=input_rate,
                queue_backlog=queue_backlog,
                idle_time_ms=idle_time_ms,
                total_txns=total_txns,
                committed_txns=committed_txns,
                logic_aborts=logic_aborts,
                concurrency_aborts=concurrency_aborts,
                committed_lock_free=committed_lock_free,
                committed_fallback=committed_fallback,
                empty_epoch=empty_epoch,
                cpu_utilization=cpu_utilization,
                io_wait_utilization=io_wait_utilization,
                operator_epoch_stats=operator_epoch_stats,
                phase_resources=phase_resources,
                key_counts=key_counts,
            )
            self.epoch_backlog_accum[worker_id] = worker_epoch_stats.queue_backlog
            self.total_tps_accum[worker_id] = worker_epoch_stats.epoch_throughput
            self.epoch_rate_accum[worker_id] = worker_epoch_stats.input_rate
            self.num_keys_accum[worker_id] = worker_epoch_stats.key_counts

            self._record_epoch_metrics(
                worker_epoch_stats,
            )
            self.system_capacity_estimator.record(
                worker_id,
                worker_epoch_stats.total_txns,
                worker_epoch_stats.epoch_latency,
            )

            # Record metric only when system is stabilized after migration
            if not self.migration_in_progress:
                now = time.time()
                self.metric_buffer.add("input_rate", worker_epoch_stats.input_rate, now)
                self.epoch_duration_window.add(worker_epoch_stats.epoch_latency)

            sync_complete: bool = self.aria_metadata.set_empty_sync_done(mt, worker_id)
            if not sync_complete:
                return

            stop_next_epoch = self.aria_metadata.stop_next_epoch
            self.aria_metadata.reset(mt)
            await self.finalize_worker_sync(
                mt,
                (stop_next_epoch,),
                Serializer.MSGPACK,
            )

            # All workers have synced -- aggregate epoch-level metrics
            total_backlog = sum(self.epoch_backlog_accum.values())
            total_tps = sum(self.total_tps_accum.values())
            self.total_keys = sum(self.num_keys_accum.values())
            self.tps_sliding_window.add(total_tps)

            # Clear accumulators for the next epoch
            self.epoch_backlog_accum.clear()
            self.epoch_rate_accum.clear()
            self.num_keys_accum.clear()
            # logging.warning(f"Epoch duration: {self.epoch_duration_window.average()}")

            if self.enable_autoscale and not self.migration_in_progress:
                smoothed_tps = self.tps_sliding_window.average() or 0.0
                pid_output = self.pid_controller.compute(total_backlog, smoothed_tps)
                if pid_output >= self.pid_controller.scale_up_threshold and not (
                    time.time() - self.last_scale_action_time < self.scale_cooldown_period
                ):
                    to_add = round(pid_output / self.pid_controller.scale_up_threshold)
                    to_add = self._resolve_scale_up_workers(to_add)
                    if to_add == 0:
                        self.last_scale_action_time = time.time()
                        logging.warning("PID | no standby workers available, skipping scale up")
                        return
                    new_partition_num = len(self.coordinator.worker_pool.get_participating_workers()) + to_add
                    await self.scale_up(new_partition_num, to_add)

    @staticmethod
    def _f_migrate(n_partitions_old: int, n_partitions_new: int) -> float:
        """Fraction of keys that change partition under hash repartitioning."""
        if n_partitions_new > n_partitions_old:
            return 1.0 - n_partitions_old / n_partitions_new
        if n_partitions_new < n_partitions_old:
            return 1.0 - n_partitions_new / n_partitions_old
        return 0.0

    @staticmethod
    def _graph_operator_partitions(graph) -> int:
        if graph is None:
            return 1
        return max((op.n_partitions for op in graph.nodes.values()), default=1)

    def _planned_partition_counts(self) -> tuple[int, int]:
        """Pessimistic partition counts for the next possible scale-up."""
        n_workers = len(self.coordinator.worker_pool.get_participating_workers())
        n_standby = self.coordinator.worker_pool.pending_standby_worker_count()
        max_to_add = min(n_workers, n_standby) if n_standby > 0 else 0
        if self.coordinator.graph_submitted and self.coordinator.submitted_graph is not None:
            n_partitions_old = self._graph_operator_partitions(self.coordinator.submitted_graph)
        else:
            n_partitions_old = max(n_workers, 1)
        n_partitions_new = n_workers + max_to_add
        logging.warning(
            f"PLANNED PARTITION COUNTS | n_partitions_old={n_partitions_old} | n_partitions_new={n_partitions_new}"
        )
        return n_partitions_old, n_partitions_new

    def _expected_keys_to_move(self, total_keys: int) -> float:
        """Keys expected to change partition for the next planned scale-up."""
        n_partitions_old, n_partitions_new = self._planned_partition_counts()
        return total_keys * self._f_migrate(n_partitions_old, n_partitions_new)

    def _note_migration_duration(self, duration_sec: float) -> None:
        """Fold a completed migration into the learned per-moved-key rate.

        Normalizing by the number of keys actually moved lets a single learned
        rate generalize across migration sizes and directions, so we no longer
        depend on hand-tuned hashing/transfer constants.
        """
        keys_moved = self._migration_keys_to_move
        if keys_moved <= 0:
            # Nothing moved (e.g. same partition count) -> no rate signal.
            return
        sample = duration_sec / keys_moved
        if self.sec_per_moved_key_ewma is None:
            self.sec_per_moved_key_ewma = sample
        else:
            self.sec_per_moved_key_ewma += self.sec_per_moved_key_ewma_alpha * (sample - self.sec_per_moved_key_ewma)
        logging.warning(
            f"MIGRATION RATE | duration={duration_sec:.2f}s | keys_moved={keys_moved:.0f} | "
            f"sample={sample * 1e6:.2f}us/key | ewma={self.sec_per_moved_key_ewma * 1e6:.2f}us/key",
        )

    def _estimate_migration_time(self, total_keys: int) -> float:
            avg_epoch_duration = self.epoch_duration_window.average()

            # Guard against an empty window returning None
            if avg_epoch_duration is None:
                logging.warning("avg_epoch_duration IS None!")
                # Return 0.0, or substitute a sensible DEFAULT_EPOCH_DURATION
                return 0.0 

            estimated_migration_time = (
                total_keys * avg_epoch_duration / ASYNC_MIGRATION_BATCH_SIZE
            ) / 1000.0
            
            self.migration_time_window.add(estimated_migration_time)
            
            # Ensure we always return a float, just in case migration_time_window 
            # acts identically and returns None on its first evaluation
            final_avg = self.migration_time_window.average()
            return final_avg if final_avg is not None else estimated_migration_time

    def _resolve_scale_up_workers(self, to_add: int) -> int:
        """Scale up only activates workers from _standby_queue; clamp and skip if none left.
        Returns the number of workers to add.
        """
        n_standby = self.coordinator.worker_pool.pending_standby_worker_count()
        if n_standby == 0:
            self.last_scale_action_time = time.time()
            logging.warning("SCALE UP | no standby workers available")
            return 0
        if to_add > n_standby:
            logging.warning(f"SCALE UP | not enough standby workers clamping to {n_standby} workers")
            return n_standby
        return to_add

    def _compute_predictive_upscaling(self, predictions: dict[str, list[float]]) -> tuple[bool, int]:
        """Compare the Chronos forecast against the capacity model.
        Returns (should_scale, workers_to_add).
        """
        # Check confidence before using capacity estimate
        confidence = self.system_capacity_estimator.confidence
        if confidence < self.capacity_confidence_threshold:
            logging.warning(f"PREDICTIVE | low confidence={confidence:.2f}")
            return False, 0

        n_workers = len(self.coordinator.worker_pool.get_live_workers())
        raw_capacity = self.system_capacity_estimator.estimate_system_capacity()
        if raw_capacity is None:
            return False, 0
            
        # Update EWMA *before* checking cooldown so the moving average stays accurate
        if self._capacity_ewma is None:
            self._capacity_ewma = raw_capacity
        else:
            self._capacity_ewma += self._capacity_ewma_alpha * (raw_capacity - self._capacity_ewma)
        system_capacity = self._capacity_ewma

        # Determine peak predicted value based on model type
        if "truth" in predictions:
            # Treat point forecast as absolute truth
            peak_predicted = max(predictions["truth"])
            logging.warning(f"PREDICTIVE (Point Forecast) | peak_predicted={peak_predicted:.0f}")
        else:
            # Standard Chronos confidence policy
            peak_predicted = max(predictions.get("0.75", [0.0]))
            logging.warning(f"PREDICTIVE (Probabilistic) | peak_predicted={peak_predicted:.0f}")

        headroom_factor = 1
        effective_capacity = system_capacity * headroom_factor
        
        logging.warning(
            f"PREDICTIVE | confidence={confidence:.2f} | peak_predicted={peak_predicted:.0f} | "
            f"raw_capacity={raw_capacity:.0f} | effective={effective_capacity:.0f}"
        )
        
        if peak_predicted <= effective_capacity:
            return False, 0

        # Point forecast scaling policy: Only scale if predictions represent a significant spike (e.g. > 15% increase)
        if "truth" in predictions:
            current_rate = self.tps_sliding_window.average() or 0.0
            if peak_predicted < current_rate * 1.15:
                logging.warning("PREDICTIVE | Predicted spike is too minor, skipping scaling action")
                return False, 0

        # Enforce cooldown *after* all logging and EWMA math, but *before* taking action
        if time.time() - self.last_scale_action_time < self.scale_cooldown_period:
            return False, 0

        per_worker_capacity = system_capacity / max(n_workers, 1)
        n_needed = max(
            n_workers + 1,
            int(peak_predicted / (per_worker_capacity * headroom_factor)) + 1,
        )
        
        # Don't more than double the cluster in one step
        to_add = min(n_needed - n_workers, n_workers)
        # scale_up only activates workers from _standby_queue; clamp and skip if none left
        to_add = self._resolve_scale_up_workers(to_add)
        
        if to_add <= 0:
            return False, 0

        logging.warning(f"PREDICTIVE | SCALE UP: need {n_needed} workers (currently {n_workers}, adding {to_add})")
        return True, to_add

    def _compute_predictive_downscaling(self, predictions: dict[str, list[float]] | None) -> tuple[bool, int]:
        """Check if the system can serve predicted demand with fewer workers.
        Returns (should_downscale, workers_to_remove).
        """
        if (
            time.time() - self.last_scale_action_time < self.scale_cooldown_period
            or self.migration_in_progress
            or time.time() < self._downscale_suppressed_until
        ):
            return False, 0
            
        n_workers = len(self.coordinator.worker_pool.get_live_workers())
        # Backlog must be essentially zero before considering downscale,
        # use value a little above zero to account for timing jitter on the worker side
        total_backlog = sum(self.epoch_backlog_accum.values())
        if n_workers <= 1 or total_backlog >= self.pid_controller.backlog_threshold or self._capacity_ewma is None:
            return False, 0

        per_worker_cap = self._capacity_ewma / n_workers
        
        # Determine peak expected demand
        peak_demand = self.tps_sliding_window.average() or 0.0
        if predictions:
            if "truth" in predictions:
                peak_predicted = max(predictions["truth"])
            else:
                # Assuming you want to fall back to p75 based on the updated snippet, or p90 from the old snippet. 
                # p75 is used here to match your updated code.
                peak_predicted = max(predictions.get("0.75", [0.0]))
            peak_demand = max(peak_demand, peak_predicted)

        if peak_demand <= 0:
            return False, 0

        # Can n workers handle peak demand with headroom?
        to_remove = 0
        for n in range(n_workers - 1, 1, -1):
            estimated_capacity = per_worker_cap * n * self.downscale_safety_factor
            logging.warning(f"SCALE DOWN: estimated_capacity={estimated_capacity:.0f} | peak_demand={peak_demand:.0f}")
            if peak_demand < estimated_capacity:
                logging.warning(f"SCALE DOWN: removing {n_workers - n} workers")
                to_remove = n_workers - n
            else:
                break

        perform_downscale = to_remove > 0
        return perform_downscale, to_remove

    async def chronos_forecast_loop(self) -> None:
        """Periodically submit metric snapshots to the Chronos forecaster
        process and poll for results.  Runs as a long-lived coroutine."""
        logging.warning("DEBUG_FORECASTER | chronos_forecast_loop started")
        while True:
            await asyncio.sleep(FORECASTER_FORECAST_INTERVAL)
            try:
                logging.warning("DEBUG_FORECASTER | Loop iteration started")
                if self.chronos_forecaster is None:
                    logging.warning("DEBUG_FORECASTER | chronos_forecaster is None, continuing")
                    continue
                if not self.chronos_forecaster.is_alive:
                    logging.warning("DEBUG_FORECASTER | chronos_forecaster is not alive, continuing")
                    continue
                if self.migration_in_progress:
                    logging.warning("DEBUG_FORECASTER | migration_in_progress is True, continuing")
                    continue

                context = self.metric_buffer.snapshot()
                if not context:
                    logging.warning("DEBUG_FORECASTER | metric_buffer.snapshot() returned empty context, continuing")
                    continue

                estimated_migration_time = self._estimate_migration_time(self.total_keys)
                logging.warning(f"DEBUG_FORECASTER | Estimated migration time: {estimated_migration_time} seconds")

                min_prediction_horizon = 10
                logging.warning("DEBUG_FORECASTER | submitting to forecaster")
                self.chronos_forecaster.submit(
                    context, prediction_length=max(min_prediction_horizon, ceil(estimated_migration_time))
                )
                logging.warning("DEBUG_FORECASTER | polling forecaster")
                predictions = self.chronos_forecaster.poll()
                logging.warning(f"DEBUG_FORECASTER | predictions: {predictions}")

                if predictions:
                    current_actual_rate = context.get("input_rate", [0.0])[-1]
                    forecast_key = "truth" if "truth" in predictions else "0.75"
                    forecast_array = predictions.get(forecast_key, [])
                    with open(self.prediction_log_file, "a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            time.time(),
                            current_actual_rate,
                            json.dumps(forecast_array)
                        ])

                logging.warning(f"DEBUG_FORECASTER | Predictions ready? {bool(predictions)} | Enable autoscale: {self.enable_autoscale} | Migration: {self.migration_in_progress}")
                if predictions and self.enable_autoscale and not self.migration_in_progress:
                    should_scale, to_add = self._compute_predictive_upscaling(predictions)
                    logging.warning(f"DEBUG_FORECASTER | should_scale: {should_scale}, to_add: {to_add}")
                    if should_scale:
                        time_since_scale = time.time() - self.last_scale_action_time
                        logging.warning(f"DEBUG_FORECASTER | time_since_scale: {time_since_scale}, cooldown: {self.scale_cooldown_period}")
                        if not (time_since_scale < self.scale_cooldown_period):
                            n_workers = len(self.coordinator.worker_pool.get_participating_workers())
                            new_partition_num = n_workers + to_add
                            logging.warning(f"DEBUG_FORECASTER | Calling scale_up({new_partition_num}, {to_add})")
                            await self.scale_up(new_partition_num, to_add)
                            logging.warning("DEBUG_FORECASTER | scale_up finished")
                        else:
                            logging.warning("DEBUG_FORECASTER | Scaling skipped due to cooldown")
                    elif not should_scale:
                        should_downscale, to_remove = self._compute_predictive_downscaling(predictions)
                        logging.warning(f"DEBUG_FORECASTER | should_downscale: {should_downscale}, to_remove: {to_remove}")
                        if should_downscale:
                            logging.warning(f"DEBUG_FORECASTER | Calling scale_down({to_remove})")
                            await self.scale_down(to_remove)
                            logging.warning("DEBUG_FORECASTER | scale_down finished")
                logging.warning("DEBUG_FORECASTER | Loop iteration finished")
            except asyncio.CancelledError:
                # Re-raise so the task can be cancelled gracefully during shutdown
                raise
            except Exception as e:
                # Catch any errors in scaling, forecasting logic, or kafka expansion
                logging.exception("DEBUG_FORECASTER | Forecaster loop crashed", exc_info=e)

    def _record_epoch_metrics(
        self,
        worker_epoch_stats: WorkerEpochStats,
    ) -> None:
        worker_id = worker_epoch_stats.worker_id
        self.epoch_throughput_gauge.labels(instance=worker_id).set(worker_epoch_stats.epoch_throughput)
        self.epoch_latency_gauge.labels(instance=worker_id).set(worker_epoch_stats.epoch_latency)
        self.epoch_abort_gauge.labels(instance=worker_id).set(worker_epoch_stats.local_abort_rate)

        self.latency_breakdown_gauge.labels(instance=worker_id, component="WAL").set(worker_epoch_stats.wal_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="1st Run").set(worker_epoch_stats.func_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Chain Acks").set(
            worker_epoch_stats.chain_ack_time
        )
        self.latency_breakdown_gauge.labels(instance=worker_id, component="SYNC").set(worker_epoch_stats.sync_time)
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Conflict Resolution").set(
            worker_epoch_stats.conflict_res_time
        )
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Commit time").set(
            worker_epoch_stats.commit_time
        )
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Fallback").set(
            worker_epoch_stats.fallback_time
        )
        self.latency_breakdown_gauge.labels(instance=worker_id, component="Async Snapshot").set(
            worker_epoch_stats.snap_time
        )

        self.input_rate_counter.labels(instance=worker_id).inc(worker_epoch_stats.input_rate)
        self.queue_backlog_gauge.labels(instance=worker_id).set(worker_epoch_stats.queue_backlog)
        self.idle_time_ms_gauge.labels(instance=worker_id).set(worker_epoch_stats.idle_time_ms)

        # Transaction count metrics
        self.epoch_total_txns_counter.labels(instance=worker_id).inc(worker_epoch_stats.total_txns)
        self.epoch_committed_txns_counter.labels(instance=worker_id).inc(worker_epoch_stats.committed_txns)
        self.epoch_logic_aborts_counter.labels(instance=worker_id).inc(worker_epoch_stats.logic_aborts)
        self.epoch_concurrency_aborts_counter.labels(instance=worker_id).inc(worker_epoch_stats.concurrency_aborts)
        self.epoch_committed_lock_free_counter.labels(instance=worker_id).inc(worker_epoch_stats.committed_lock_free)
        self.epoch_committed_fallback_counter.labels(instance=worker_id).inc(worker_epoch_stats.committed_fallback)

        # Downscaling metrics
        self.empty_epoch_gauge.labels(instance=worker_id).set(1 if worker_epoch_stats.empty_epoch else 0)
        self.cpu_utilization_ratio_gauge.labels(instance=worker_id).set(worker_epoch_stats.cpu_utilization)
        self.io_utilization_ratio_gauge.labels(instance=worker_id).set(worker_epoch_stats.io_wait_utilization)

        # Operator-level metrics for this worker and epoch
        for op_name, partition, tps, avg_latency_ms, call_count in worker_epoch_stats.operator_epoch_stats:
            labels = {"instance": worker_id, "operator": op_name, "partition": str(partition)}
            self.operator_tps_counter.labels(**labels).inc(tps)
            self.operator_call_count_counter.labels(**labels).inc(call_count)
            self.operator_latency_gauge.labels(**labels).set(avg_latency_ms)

        for phase, v in worker_epoch_stats.phase_resources.get("cpu_ns", {}).items():
            self.phase_cpu_ms_total.labels(instance=worker_id, phase=phase).inc(float(v) / 1e6)
        for phase, v in worker_epoch_stats.phase_resources.get("rx_bytes", {}).items():
            self.phase_net_rx_bytes_total.labels(instance=worker_id, phase=phase).inc(float(v))
        for phase, v in worker_epoch_stats.phase_resources.get("tx_bytes", {}).items():
            self.phase_net_tx_bytes_total.labels(instance=worker_id, phase=phase).inc(float(v))
        for phase, v in worker_epoch_stats.phase_resources.get("rss_max_bytes", {}).items():
            self.phase_rss_max_mb.labels(instance=worker_id, phase=phase).set(float(v) / (1024 * 1024))

    async def _handle_deterministic_reordering(self, data: bytes) -> None:
        mt = MessageType.DeterministicReordering
        async with self.networking_locks[mt]:
            (
                worker_id,
                remote_read_reservation,
                remote_write_set,
                remote_read_set,
            ) = self.protocol_networking.decode_message(data)

            sync_complete: bool = self.aria_metadata.set_deterministic_reordering_done(
                worker_id,
                remote_read_reservation,
                remote_write_set,
                remote_read_set,
            )
            if not sync_complete:
                return

            reordering_payload = (
                self.aria_metadata.global_read_reservations,
                self.aria_metadata.global_write_set,
                self.aria_metadata.global_read_set,
            )
            self.aria_metadata.reset(mt)
            await self.finalize_worker_sync(
                mt,
                reordering_payload,
                Serializer.PICKLE,
            )

    async def _handle_migration_done(self, _: bytes) -> None:
        mt = MessageType.MigrationDone
        logging.warning("DEBUG_MIGRATION | Coordinator received MigrationDone from a worker")
        async with self.networking_locks[mt]:
            sync_complete: bool = await self.migration_metadata.set_empty_sync_done(mt)
            logging.warning(
                f"DEBUG_MIGRATION | MIGRATION | MigrationDone | sync_sum={self.migration_metadata.sync_sum} | complete={sync_complete}"
            )
            if not sync_complete:
                return

            logging.warning("DEBUG_MIGRATION | Coordinator completing migration")
            end_time = time.time_ns()
            self.migration_end_time_gauge.set(end_time / 1_000_000)
            self.migration_end_time = end_time
            self.migration_end_count.inc()
            migration_duration = (self.migration_end_time - self.migration_start_time) / 1_000_000_000
            logging.warning(f"DEBUG_MIGRATION | MIGRATION_DURATION: {migration_duration:.2f} s")
            await self.migration_metadata.cleanup(mt)
            self.migration_in_progress = False
            logging.warning("DEBUG_MIGRATION | Set migration_in_progress = False")

            # Deactivate victim workers after scale-down migration completes
            if self._pending_downscale:
                non_participating_workers = self.coordinator.worker_pool.get_non_participating_workers()
                logging.warning(
                    f"DEBUG_MIGRATION | SCALE_DOWN | deactivating {len(non_participating_workers)} idle workers after migration"
                )
                for worker in non_participating_workers:
                    deactivated = self.coordinator.worker_pool.deactivate_to_standby(worker.worker_id)
                    if deactivated:
                        self.system_capacity_estimator.remove_worker(worker.worker_id)
                self._pending_downscale = False
                self._downscale_victim_ids.clear()
                self.live_worker_count_gauge.set(len(self.coordinator.worker_pool.get_live_workers()))

            self.last_scale_action_time = time.time()
            logging.warning(f"DEBUG_MIGRATION | Set last_scale_action_time to {self.last_scale_action_time}")
            logging.warning("DEBUG_MIGRATION | Restarting the snapshotting mechanism")
            self.snapshotting_task = asyncio.create_task(self.send_snapshot_marker())
            logging.warning("self.snapshotting_task = asyncio.create_task(self.send_snapshot_marker())")

    async def start_puller(self) -> None:
        async def request_handler(reader: StreamReader, writer: StreamWriter) -> None:
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = struct.unpack(">Q", data)
                    self.aio_task_scheduler.create_task(
                        self.protocol_controller(await reader.readexactly(size)),
                    )
            except asyncio.IncompleteReadError as e:
                logging.info(f"Client disconnected unexpectedly: {e}")
                if SUPER_VERBOSE:
                    logging.warning(f"COORDINATOR PULLER: Client disconnected unexpectedly: {e}", exc_info=True)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                if SUPER_VERBOSE:
                    logging.warning(f"COORDINATOR PULLER: Uncaught exception: {e}", exc_info=True)
            finally:
                logging.info("Closing the connection")
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(
            request_handler,
            sock=self.protocol_socket,
            limit=2**32,
        )
        async with server:
            await server.serve_forever()

    async def tcp_service(self) -> None:
        self.puller_task = asyncio.create_task(self.start_puller())
        logging.warning(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        with concurrent.futures.ProcessPoolExecutor(1) as pool:

            async def request_handler(
                reader: StreamReader,
                writer: StreamWriter,
            ) -> None:
                try:
                    while True:
                        data = await reader.readexactly(8)
                        (size,) = struct.unpack(">Q", data)
                        message = await reader.readexactly(size)
                        # Unbounded: this scheduler also runs
                        # `heartbeat_monitor_coroutine` (a perpetual loop) which
                        # drives recovery via `wait_cluster_healthy()` — that
                        # await is unblocked by `_handle_ready_after_recovery`
                        # running through this same scheduler. A bounded slot
                        # held by a suspended awaiter could starve the setter
                        # under a large cluster.
                        self.aio_task_scheduler_coord.create_unbounded_task(
                            self.coordinator_controller(writer, message, pool),
                        )
                except asyncio.IncompleteReadError as e:
                    logging.info(f"Client disconnected unexpectedly: {e}")
                    if SUPER_VERBOSE:
                        logging.warning(f"COORDINATOR TCP SERVICE: Client disconnected unexpectedly: {e}", exc_info=True)
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    if SUPER_VERBOSE:
                        logging.warning(f"COORDINATOR TCP SERVICE: Uncaught exception: {e}", exc_info=True)
                finally:
                    logging.info("Closing the connection")
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(
                request_handler,
                sock=self.coor_socket,
                limit=2**32,
            )
            async with server:
                await server.serve_forever()

    async def finalize_migration_repartition(self) -> None:
        logging.warning("DEBUG_MIGRATION | finalize_migration_repartition started")
        async with self.networking_locks[MessageType.SyncCleanup]:
            if self.aria_metadata is not None:
                logging.warning("DEBUG_MIGRATION | Setting aria_metadata.stop_in_next_epoch()")
                self.aria_metadata.stop_in_next_epoch()

        logging.warning("DEBUG_MIGRATION | Sending MigrationRepartitioningDone to all workers (protocol will stop)")
        async with asyncio.TaskGroup() as tg:
            for worker in self._migration_workers():
                logging.warning(f"DEBUG_MIGRATION | Sending MigrationRepartitioningDone to : {worker}")
                tg.create_task(
                    self.networking.send_message(
                        worker.worker_ip,
                        worker.worker_port,
                        msg=b"",
                        msg_type=MessageType.MigrationRepartitioningDone,
                        serializer=Serializer.NONE,
                    ),
                )
        logging.warning("DEBUG_MIGRATION | finalize_migration_repartition finished")

    async def finalize_migration(self) -> None:
        logging.warning("DEBUG_MIGRATION | finalize_migration started")
        async with asyncio.TaskGroup() as tg:
            for worker in self._migration_workers():
                logging.warning(f"DEBUG_MIGRATION | Sending MigrationDone to : {worker}")
                tg.create_task(
                    self.networking.send_message(
                        worker.worker_ip,
                        worker.worker_port,
                        msg=(
                            self.migration_metadata.epoch_counter,
                            self.migration_metadata.t_counter,
                            self.migration_metadata.input_offsets,
                            self.migration_metadata.output_offsets,
                        ),
                        msg_type=MessageType.MigrationDone,
                        serializer=Serializer.MSGPACK,
                    ),
                )
        logging.warning("DEBUG_MIGRATION | finalize_migration finished")

    async def finalize_migration_ready_to_start(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for worker in self._migration_workers():
                logging.warning(f"Sending MigrationReadyToStart to : {worker}")
                tg.create_task(
                    self.networking.send_message(
                        worker.worker_ip,
                        worker.worker_port,
                        msg=b"",
                        msg_type=MessageType.MigrationReadyToStart,
                        serializer=Serializer.NONE,
                    ),
                )

    async def finalize_worker_sync(
        self,
        msg_type: MessageType,
        message: tuple | bytes,
        serializer: Serializer = Serializer.MSGPACK,
    ) -> None:
        async with asyncio.TaskGroup() as tg:
            for worker in self._migration_workers():
                tg.create_task(
                    self.protocol_networking.send_message(
                        worker.worker_ip,
                        worker.protocol_port,
                        msg=message,
                        msg_type=msg_type,
                        serializer=serializer,
                    ),
                )

    async def worker_wants_to_proceed(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for worker in self._migration_workers():
                tg.create_task(
                    self.protocol_networking.send_message(
                        worker.worker_ip,
                        worker.protocol_port,
                        msg=b"",
                        msg_type=MessageType.RemoteWantsToProceed,
                        serializer=Serializer.NONE,
                    ),
                )

    async def _reset_after_recovery(self) -> None:
        """
        Reset all coordinator-side protocol metadata after a successful recovery.
        This is the core of the 'robust recovery state machine'.
        """
        participating_workers = self.coordinator.worker_pool.get_participating_workers()
        n_workers = len(participating_workers)

        logging.warning("Resetting protocol metadata after recovery")

        # 1) Reset Aria metadata (only if a graph is submitted)
        if self.coordinator.graph_submitted:
            self.aria_metadata = AriaSyncMetadata(n_workers)
        else:
            self.aria_metadata = None

        # 2) Reset migration metadata
        self.migration_metadata = MigrationMetadata(n_workers)
        self.migration_in_progress = False

        # 3) Reset snapshot completion metadata
        self.coordinator.completed_input_offsets.clear()
        self.coordinator.completed_out_offsets.clear()
        self.coordinator.completed_epoch_counter = 0
        self.coordinator.completed_t_counter = 0
        self.coordinator.prev_completed_snapshot_id = -1
        # All workers will effectively need to rebuild their snapshot IDs
        self.coordinator.worker_snapshot_ids = {worker.worker_id: -1 for worker in participating_workers}

        # 4) Reset worker heartbeat gauges and baseline times
        for worker in participating_workers:
            # Next heartbeat from worker defines fresh baseline
            worker.previous_heartbeat = 1_000_000.0
            self.heartbeat_gauge.labels(instance=worker.worker_id).set(0)

        # 5) Reset epoch-related metrics
        for worker in participating_workers:
            wid = worker.worker_id
            self.epoch_throughput_gauge.labels(instance=wid).set(0)
            self.epoch_latency_gauge.labels(instance=wid).set(0)
            self.epoch_abort_gauge.labels(instance=wid).set(0)
        # Reset latency breakdown (all labels)
        self.latency_breakdown_gauge._metrics.clear()  # noqa: SLF001

        logging.warning("Protocol metadata reset complete")

    async def _perform_recovery(self, workers_to_remove: set[Worker]) -> None:
        """
        Full recovery state machine:
        - close dead worker connections
        - start recovery
        - wait cluster healthy
        - reset protocol metadata
        - close protocol connections
        - notify workers that everyone is healthy
        """
        if not workers_to_remove:
            return

        logging.warning(f"Starting recovery process for workers: {workers_to_remove}")

        # 1) Clean up dead worker channels and buffered tasks
        logging.warning(f"Closing connections to dead workers: {workers_to_remove}")
        for worker in workers_to_remove:
            await self.networking.close_worker_connections(
                worker.worker_ip,
                worker.worker_port,
            )
            await self.protocol_networking.close_worker_connections(
                worker.worker_ip,
                worker.protocol_port,
            )
        await self.aio_task_scheduler.close()
        self.aio_task_scheduler = AIOTaskScheduler()

        # 2) Start recovery
        logging.warning(
            "Starting recovery process (reassign operators, send InitRecovery)",
        )
        await self.coordinator.start_recovery_process(workers_to_remove)

        # 3) Wait for the cluster to become healthy
        logging.warning("Waiting on the cluster to become healthy")
        await self.coordinator.wait_cluster_healthy()

        # 4) Reset protocol metadata (& snapshot/metrics state)
        logging.warning("Cleaning up protocol after everyone is healthy")
        await self._reset_after_recovery()

        # 5) Close all protocol connections (workers will reconnect clean)
        await self.protocol_networking.close_all_connections()

        # 6) Notify Cluster that everyone is ready
        logging.warning("Notify workers that cluster is healthy")
        await self.coordinator.notify_cluster_healthy()

        logging.warning("Recovery process completed")

    async def heartbeat_monitor_coroutine(self) -> None:
        interval_time = HEARTBEAT_CHECK_INTERVAL / 1000
        while True:
            await asyncio.sleep(interval_time)
            heartbeat_check_time = timer()
            workers_to_remove, heartbeats_per_worker = self.coordinator.check_heartbeats(heartbeat_check_time)
            for (
                worker_id,
                time_since_last_heartbeat_ms,
            ) in heartbeats_per_worker.items():
                self.heartbeat_gauge.labels(instance=worker_id).set(
                    time_since_last_heartbeat_ms,
                )

            # Add workers that re-registered (same IP/ports) to the failed set
            if (workers_to_remove or self.workers_that_re_registered) and self.recovery_state == RecoveryState.IDLE:
                async with self.recovery_lock:
                    if self.recovery_state != RecoveryState.IDLE:
                        # Another recovery started while we were waiting for the lock
                        continue
                    self.recovery_state = RecoveryState.RECOVERING
                    try:
                        # Merge "dead" workers and workers that re-registered with init_recovery=True
                        re_registered_set = set(self.workers_that_re_registered)
                        workers_to_remove.update(re_registered_set)
                        self.workers_that_re_registered = []

                        await self._perform_recovery(workers_to_remove)
                    except Exception as e:
                        logging.error(f"Error during recovery: {e}")
                    finally:
                        self.recovery_state = RecoveryState.IDLE

    async def send_snapshot_marker(self) -> None:
        while True:
            await asyncio.sleep(SNAPSHOT_FREQUENCY_SEC)
            if self.aria_metadata is not None:
                self.aria_metadata.take_snapshot_at_next_epoch()

    async def stop_snapshotting(self) -> None:
        if self.snapshotting_task:
            self.snapshotting_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.snapshotting_task
            self.snapshotting_task = None

    def init_snapshot_bucket(self) -> None:
        attempts = 0
        while True:
            try:
                self.s3_client.create_bucket(Bucket=SNAPSHOT_BUCKET_NAME)
            except botocore.exceptions.EndpointConnectionError as err:
                attempts += 1
                if S3_INIT_MAX_RETRIES and attempts >= S3_INIT_MAX_RETRIES:
                    msg = f"Could not connect to S3 after {attempts} attempts (endpoint={S3_ENDPOINT})"
                    raise RuntimeError(msg) from err
                logging.warning(
                    f"Could not establish connection to S3 (endpoint={S3_ENDPOINT}). "
                    f"Sleeping for {S3_INIT_RETRY_SEC:.1f} seconds and retrying..."
                )
                time.sleep(S3_INIT_RETRY_SEC)
                continue
            except botocore.exceptions.ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    return  # Bucket is already there
                # Other client errors should not be retried
                raise
            else:
                return

    async def main(self) -> None:
        logging.warning("Coordinator Booted Successfully")
        self.init_snapshot_bucket()
        logging.warning("Coordinator Connected to S3")
        # Unbounded: heartbeat_monitor is a long-lived loop that also drives
        # recovery (awaits `wait_cluster_healthy`). It must not consume a
        # semaphore slot while suspended, or it would permanently reduce the
        # scheduler's capacity and could starve `_handle_ready_after_recovery`.
        self.aio_task_scheduler_coord.create_unbounded_task(self.heartbeat_monitor_coroutine())
        logging.warning("Coordinator Heartbeat Sentinel online")
        self.snapshotting_task = asyncio.create_task(self.send_snapshot_marker())
        logging.warning("Coordinator Snapshotting online")

        if self.enable_autoscale:
            self.chronos_forecaster = create_forecaster()
            self.chronos_forecaster.start()
            self.forecaster_task = asyncio.create_task(self.chronos_forecast_loop())
            logging.warning("FORECASTING forecaster online (interval=%.1fs)", FORECASTER_FORECAST_INTERVAL)

        await self.tcp_service()


if __name__ == "__main__":
    coordinator_service = CoordinatorService()
    uvloop.run(coordinator_service.main())
