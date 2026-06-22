"""
Deadlock debugging utility for Styx distributed system.

This module provides a background monitoring task that periodically logs
the state of all synchronization primitives to help identify deadlocks.

Usage:
    from styx.common.deadlock_debugger import DeadlockDebugger

    # In your worker/aria initialization:
    debugger = DeadlockDebugger(
        networking_manager=self.networking,
        sync_workers_event=self.sync_workers_event,
        worker_id=self.id,
    )
    await debugger.start()

    # When shutting down:
    await debugger.stop()
"""

import asyncio
import time
from typing import TYPE_CHECKING

from styx.common.logging import logging

if TYPE_CHECKING:
    from styx.common.tcp_networking import NetworkingManager


class DeadlockDebugger:
    def __init__(
        self,
        networking_manager: NetworkingManager | None = None,
        sync_workers_event: dict | None = None,
        worker_id: int = -1,
        check_interval: float = 5.0,
        stall_threshold: float = 10.0,
        protocol: object | None = None,
    ) -> None:
        """
        Initialize the deadlock debugger.

        Args:
            networking_manager: The NetworkingManager instance to monitor for remote key waits
            sync_workers_event: Dict of MessageType -> asyncio.Event for worker sync barriers
            worker_id: ID of this worker for logging
            check_interval: How often to check for potential deadlocks (seconds)
            stall_threshold: How long a wait must be pending before warning (seconds)
            protocol: The AriaProtocol instance (to access current_sync_barrier)
        """
        self.networking_manager = networking_manager
        self.sync_workers_event = sync_workers_event or {}
        self.worker_id = worker_id
        self.check_interval = check_interval
        self.stall_threshold = stall_threshold
        self.protocol = protocol

        self._task: asyncio.Task | None = None
        self._running = False

        # Track when waits started for stall detection
        self._remote_key_wait_start: dict[tuple, dict] = {}  # op_partition -> {key: start_time}
        self._sync_wait_start: dict = {}  # msg_type -> start_time

    async def start(self) -> None:
        """Start the background monitoring task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logging.warning(f"[DEADLOCK_DEBUG] Worker {self.worker_id}: Deadlock debugger started")

    async def stop(self) -> None:
        """Stop the background monitoring task."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logging.warning(f"[DEADLOCK_DEBUG] Worker {self.worker_id}: Deadlock debugger stopped")

    async def _monitor_loop(self) -> None:
        """Main monitoring loop that periodically checks for stalled waits."""
        while self._running:
            try:
                await asyncio.sleep(self.check_interval)
                self._check_remote_key_waits()
                self._check_sync_barriers()
                self._check_fallback_waits()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"[DEADLOCK_DEBUG] Error in monitor loop: {e}")

    def _check_remote_key_waits(self) -> None:
        """Check for stalled remote key waits."""
        if self.networking_manager is None:
            return

        wait_events = getattr(self.networking_manager, "wait_remote_key_event", {})
        now = time.time()
        pending_waits = []

        for op_partition, keys_dict in wait_events.items():
            if op_partition not in self._remote_key_wait_start:
                self._remote_key_wait_start[op_partition] = {}

            for key, (event, waiter_count) in keys_dict.items():
                if not event.is_set():
                    # Track when this wait started
                    if key not in self._remote_key_wait_start[op_partition]:
                        self._remote_key_wait_start[op_partition][key] = now

                    wait_duration = now - self._remote_key_wait_start[op_partition][key]
                    pending_waits.append(
                        {
                            "operator_partition": op_partition,
                            "key": key,
                            "waiter_count": waiter_count,
                            "wait_duration_sec": round(wait_duration, 2),
                            "stalled": wait_duration > self.stall_threshold,
                        }
                    )
                else:
                    # Event is set, remove from tracking
                    self._remote_key_wait_start[op_partition].pop(key, None)

        if pending_waits:
            stalled = [w for w in pending_waits if w["stalled"]]
            if stalled:
                logging.warning(
                    f"[DEADLOCK_DEBUG] Worker {self.worker_id}: "
                    f"STALLED remote key waits (>{self.stall_threshold}s):\n"
                    + "\n".join(
                        f"  - {w['operator_partition']} key={w['key']} "
                        f"waiters={w['waiter_count']} duration={w['wait_duration_sec']}s"
                        for w in stalled
                    )
                )
            else:
                logging.debug(
                    f"[DEADLOCK_DEBUG] Worker {self.worker_id}: "
                    f"{len(pending_waits)} pending remote key waits (not yet stalled)"
                )

    def _check_sync_barriers(self) -> None:
        """Check for stalled sync barrier waits."""
        now = time.time()

        # Check if protocol has an active barrier it's waiting on
        active_barrier = None
        active_barrier_duration = 0.0
        if self.protocol is not None:
            active_barrier = getattr(self.protocol, "current_sync_barrier", None)
            barrier_start = getattr(self.protocol, "current_sync_barrier_start", 0.0)
            if active_barrier is not None and barrier_start > 0:
                active_barrier_duration = now - barrier_start

        # If we have an active barrier, report on that specifically
        if active_barrier is not None:
            barrier_name = active_barrier.name if hasattr(active_barrier, "name") else str(active_barrier)
            if active_barrier_duration > self.stall_threshold:
                epoch = getattr(self.protocol, "sequencer", None)
                epoch_num = epoch.epoch_counter if epoch else "?"
                logging.warning(
                    f"[DEADLOCK_DEBUG] Worker {self.worker_id}: "
                    f"BLOCKED on sync barrier '{barrier_name}' for {active_barrier_duration:.1f}s "
                    f"(epoch {epoch_num})"
                )
            return

        # Fallback: check all unset events (less precise)
        pending_syncs = []
        for msg_type, event in self.sync_workers_event.items():
            if not event.is_set():
                if msg_type not in self._sync_wait_start:
                    self._sync_wait_start[msg_type] = now

                wait_duration = now - self._sync_wait_start[msg_type]
                pending_syncs.append(
                    {
                        "message_type": msg_type.name if hasattr(msg_type, "name") else str(msg_type),
                        "wait_duration_sec": round(wait_duration, 2),
                        "stalled": wait_duration > self.stall_threshold,
                    }
                )
            else:
                self._sync_wait_start.pop(msg_type, None)

        if pending_syncs:
            stalled = [s for s in pending_syncs if s["stalled"]]
            if stalled:
                logging.warning(
                    f"[DEADLOCK_DEBUG] Worker {self.worker_id}: "
                    f"STALLED sync barriers (>{self.stall_threshold}s):\n"
                    + "\n".join(f"  - {s['message_type']} duration={s['wait_duration_sec']}s" for s in stalled)
                )

    def _check_fallback_waits(self) -> None:
        """Check for stalled fallback ack-waits and fallback dependency locks.

        This covers the chain-root hang: a fallback root parked in
        ``asyncio.gather(*fallback_tasks)`` is blocked either on
        ``networking.waited_ack_events[t_id].wait()`` (the chain never fully
        ACKed/aborted back) or on ``fallback_locking_event_map[dep].wait()``
        (a dependency transaction never unlocked). Neither is visible from the
        remote-key or sync-barrier checks.
        """
        if self.protocol is None:
            return
        now = time.time()

        # Only meaningful while a fallback barrier is the active wait, or while
        # there are pending ack events; otherwise these maps are stale leftovers.
        networking = getattr(self.protocol, "networking", None)
        waited_ack_events = getattr(networking, "waited_ack_events", {}) if networking else {}
        ack_fraction = getattr(networking, "ack_fraction", {}) if networking else {}
        chain_participants = getattr(networking, "chain_participants", {}) if networking else {}

        unset_acks = [
            (t_id, round(ack_fraction.get(t_id, 0.0), 6), list(chain_participants.get(t_id, [])))
            for t_id, event in waited_ack_events.items()
            if not event.is_set()
        ]

        fallback_locks = getattr(self.protocol, "fallback_locking_event_map", {})
        waiting_on = getattr(self.protocol, "waiting_on_transactions", {})
        unset_locks = [t_id for t_id, event in fallback_locks.items() if not event.is_set()]

        if not unset_acks and not unset_locks:
            return

        epoch = getattr(self.protocol, "sequencer", None)
        epoch_num = epoch.epoch_counter if epoch else "?"
        lines = [
            f"[DEADLOCK_DEBUG] Worker {self.worker_id}: pending fallback waits (epoch {epoch_num}):",
        ]
        if unset_acks:
            lines.append(f"  Unfinished chain ACKs ({len(unset_acks)}):")
            lines.extend(
                f"    - t_id={t_id} ack_fraction={frac} (need ~1.0) participants={parts}"
                for t_id, frac, parts in unset_acks[:25]
            )
        if unset_locks:
            lines.append(f"  Held fallback locks / unresolved deps ({len(unset_locks)}):")
            lines.extend(
                f"    - t_id={t_id} waiting_on={sorted(waiting_on.get(t_id, set()))}" for t_id in unset_locks[:25]
            )
        logging.warning("\n".join(lines))

    def dump_state(self) -> dict:
        """
        Return a snapshot of all current wait states.
        Can be called manually for immediate debugging.
        """
        now = time.time()
        state = {
            "worker_id": self.worker_id,
            "timestamp": now,
            "remote_key_waits": [],
            "sync_barriers": [],
            "active_sync_barrier": None,
        }

        # Check active sync barrier
        if self.protocol is not None:
            active_barrier = getattr(self.protocol, "current_sync_barrier", None)
            barrier_start = getattr(self.protocol, "current_sync_barrier_start", 0.0)
            if active_barrier is not None:
                epoch = getattr(self.protocol, "sequencer", None)
                state["active_sync_barrier"] = {
                    "barrier": active_barrier.name if hasattr(active_barrier, "name") else str(active_barrier),
                    "duration_sec": round(now - barrier_start, 2) if barrier_start > 0 else 0,
                    "epoch": epoch.epoch_counter if epoch else None,
                }

        # Remote key waits
        if self.networking_manager is not None:
            wait_events = getattr(self.networking_manager, "wait_remote_key_event", {})
            for op_partition, keys_dict in wait_events.items():
                for key, (event, waiter_count) in keys_dict.items():
                    state["remote_key_waits"].append(
                        {
                            "operator_partition": str(op_partition),
                            "key": str(key),
                            "waiter_count": waiter_count,
                            "is_set": event.is_set(),
                        }
                    )

        # Sync barriers (all of them, for completeness)
        for msg_type, event in self.sync_workers_event.items():
            state["sync_barriers"].append(
                {
                    "message_type": msg_type.name if hasattr(msg_type, "name") else str(msg_type),
                    "is_set": event.is_set(),
                }
            )

        return state

    def log_full_state(self) -> None:
        """Log a complete snapshot of all wait states."""
        state = self.dump_state()

        # Build active barrier section
        active_section = ""
        if state["active_sync_barrier"]:
            ab = state["active_sync_barrier"]
            active_section = (
                f"  >>> CURRENTLY BLOCKED ON: {ab['barrier']} for {ab['duration_sec']}s (epoch {ab['epoch']})\n"
            )
        else:
            active_section = "  >>> Not currently waiting on any sync barrier\n"

        # Build remote key waits section
        key_waits_section = f"  Remote key waits: {len(state['remote_key_waits'])}\n"
        if state["remote_key_waits"]:
            key_waits_section += (
                "\n".join(
                    f"    - {w['operator_partition']} key={w['key']} waiters={w['waiter_count']} is_set={w['is_set']}"
                    for w in state["remote_key_waits"]
                )
                + "\n"
            )

        # Build sync barriers section (show which are set vs unset)
        set_barriers = [s["message_type"] for s in state["sync_barriers"] if s["is_set"]]
        unset_barriers = [s["message_type"] for s in state["sync_barriers"] if not s["is_set"]]

        logging.warning(
            f"[DEADLOCK_DEBUG] Worker {self.worker_id} FULL STATE DUMP:\n"
            + active_section
            + key_waits_section
            + f"  Sync barriers SET (passed): {set_barriers or 'none'}\n"
            + f"  Sync barriers UNSET: {unset_barriers or 'none'}"
        )
