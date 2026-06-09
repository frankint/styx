from collections import defaultdict
import traceback
from typing import TYPE_CHECKING

from styx.common.logging import logging

from worker.operator_state.aria._aria_state import (
    commit as _cy_commit,
    state_get as _cy_state_get,
    state_get_immediate as _cy_state_get_immediate,
)
from worker.operator_state.aria.base_aria_state import BaseAriaState
from worker.operator_state.aria.fast_copy import fast_deepcopy

if TYPE_CHECKING:
    from styx.common.types import K, KVPairs, OperatorPartition, V


class InMemoryOperatorState(BaseAriaState):
    data: dict[OperatorPartition, KVPairs]
    delta_map: dict[OperatorPartition, KVPairs]

    def __init__(self, operator_partitions: set[OperatorPartition]) -> None:
        super().__init__(operator_partitions)
        self.data = {}
        self.delta_map = {}
        for operator_partition in self.operator_partitions:
            self.data[operator_partition] = {}
            self.delta_map[operator_partition] = {}
        # State migration data structures
        # Where do the keys belong (operator_partition: key: (worker_id, old partition))
        self.remote_keys: dict[OperatorPartition, dict[K, tuple[int, int]]] = {}
        # Used to track migration progress, the async migration and whether the operator still owns the specific key
        # (operator_name, old_partition): set of keys with the new partition
        self.keys_to_send: dict[OperatorPartition, set[tuple[K, int]]] = {}
        self.keys_sent: dict[K, int] = {}
        # Used to efficiently check if a key no longer should be in this worker due to migration
        self.set_keys_to_send: set[K] = set()
        self.keys_to_workers: dict[K, int] = {}
        # Partitions this worker owns under the current assignment
        self.owned_partitions: set[OperatorPartition] = set(self.operator_partitions)

    def add_keys_to_send(self, keys_to_send: dict[OperatorPartition, K]) -> None:
        self.keys_to_send = keys_to_send
        self.set_keys_to_send = set()
        self.keys_to_workers = {}

        for key_set in self.keys_to_send.values():
            for key, new_partition in key_set:
                self.set_keys_to_send.add(key)
                self.keys_to_workers[key] = new_partition

    def has_keys_to_send(self) -> bool:
        return bool(self.keys_to_send)

    def get_key_to_migrate(
        self,
        new_operator_partition: OperatorPartition,
        key: K,
        old_partition: int,
    ) -> K | None:
        operator_name, new_partition = new_operator_partition
        operator_partition: OperatorPartition = (operator_name, old_partition)
        if operator_partition not in self.data:
            return None
        data_to_send = self.data[operator_partition].pop(key, None)
        self.keys_sent[key] = new_partition

        if data_to_send is None:
            # Key was already transferred via async migration batch
            return None
        if operator_partition in self.keys_to_send:
            self.keys_to_send[operator_partition].discard((key, new_partition))
        return data_to_send

    def set_data_from_migration(
        self,
        operator_partition: OperatorPartition,
        key: K,
        data: KVPairs,
    ) -> None:
        operator_partition = tuple(operator_partition)
        if data is not None:
            if operator_partition not in self.data:
                self.add_new_operator_partition(operator_partition)
            self.data[operator_partition][key] = data
            # Only remove from remote_keys when we actually received the data.
            # A None response means the async migration batch already transferred
            # this key — the batch will arrive and set it via set_batch_data_from_migration.
            if operator_partition in self.remote_keys:
                self.remote_keys[operator_partition].pop(key, None)
                if not self.remote_keys[operator_partition]:
                    del self.remote_keys[operator_partition]

    def migrate_within_the_same_worker(
        self,
        operator_name: str,
        new_partition: int,
        key: K,
        old_partition: int,
    ) -> bool:
        """Move a key between partitions on the same worker.
        Returns True if the key data is now available in the new partition,
        False if the async migration batch already popped it but hasn't
        delivered it yet (caller must wait).
        """
        new_operator_partition: OperatorPartition = (operator_name, new_partition)
        data = self.get_key_to_migrate(new_operator_partition, key, old_partition)
        if data is not None:
            self.set_data_from_migration(new_operator_partition, key, data)
            return True
        # Key was already popped by async batch. Check if batch already
        # delivered it to the new partition.
        if key in self.data.get(new_operator_partition, {}):
            if new_operator_partition in self.remote_keys and key in self.remote_keys[new_operator_partition]:
                del self.remote_keys[new_operator_partition][key]
                if not self.remote_keys[new_operator_partition]:
                    del self.remote_keys[new_operator_partition]
            return True
        return False

    def keys_remaining_to_remote(self) -> int:
        c = 0
        for keys in self.remote_keys.values():
            c += len(keys)
        return c

    def keys_remaining_to_send(self) -> int:
        c = 0
        for keys in self.keys_to_send.values():
            c += len(keys)
        return c

    def log_state_summary(self, worker_id: int, context: str = "") -> None:
        """
        Log a detailed summary of the current state for debugging migration.
        """
        logging.warning(f"===== STATE SUMMARY (Worker {worker_id}) {context} =====")
        logging.warning(f"  Operator Partitions: {list(self.operator_partitions)}")

        # Log keys per partition
        for op_partition, kv_pairs in self.data.items():
            key_count = len(kv_pairs)
            logging.warning(f"  Partition {op_partition}: {key_count} keys")

        # Log migration state
        if self.keys_to_send:
            logging.warning("  Keys to send (migration outgoing):")
            for op_partition, keys in self.keys_to_send.items():
                logging.warning(f"    {op_partition}: {len(keys)} keys pending")
        else:
            logging.warning("  Keys to send: EMPTY (no outgoing migration)")

        if self.remote_keys:
            logging.warning("  Remote keys (migration incoming - waiting):")
            for op_partition, keys in self.remote_keys.items():
                logging.warning(f"    {op_partition}: {len(keys)} keys expected from remote")
        else:
            logging.warning("  Remote keys: EMPTY (no pending incoming)")

        logging.warning("===== END STATE SUMMARY =====")

    def get_async_migrate_batch(self, batch_size: int) -> dict[OperatorPartition, KVPairs]:
        batch_to_send: dict[OperatorPartition, KVPairs] = defaultdict(dict)
        c = 0
        operator_partitions_to_clear = []

        for operator_partition, keys in self.keys_to_send.items():
            operator_name, _ = operator_partition
            while keys and c < batch_size:
                key, new_partition = keys.pop()
                value = self.data[operator_partition].pop(key, None)
                if value is None:
                    # Key was deleted between rehash and transfer — skip it
                    continue
                self.keys_sent[key] = new_partition
                batch_to_send[(operator_name, new_partition)][key] = value
                c += 1
            if not keys:
                operator_partitions_to_clear.append(operator_partition)
            if c >= batch_size:
                break
        # Remove emptied partitions
        for operator_partition in operator_partitions_to_clear:
            del self.keys_to_send[operator_partition]

        all_partitions = set(self.data.keys())
        for operator_partition in all_partitions:
            if not self.data[operator_partition]:
                if operator_partition in self.remote_keys or operator_partition in self.owned_partitions:
                    continue  # still expecting incoming data
                del self.data[operator_partition]
                del self.write_sets[operator_partition]
                del self.reads[operator_partition]
                self.operator_partitions.remove(operator_partition)
        return batch_to_send

    def set_batch_data_from_migration(self, operator_partition: OperatorPartition, kv_pairs: KVPairs) -> None:
        operator_partition = tuple(operator_partition)  # new partitioning
        # Ensure the operator partition is initialized (defensive check for race conditions)
        if operator_partition not in self.data:
            self.add_new_operator_partition(operator_partition)
        self.data[operator_partition].update(kv_pairs)
        # Guard: remote_keys may not have entries if async batch arrived
        # before hash metadata, or entries were already removed.
        if operator_partition in self.remote_keys:
            for key in kv_pairs:
                self.remote_keys[operator_partition].pop(key, None)
            if not self.remote_keys[operator_partition]:
                del self.remote_keys[operator_partition]

    def get_worker_id_old_partition(
        self,
        operator_name: str,
        partition: int,
        key: K,
    ) -> tuple[int, int] | None:
        """
        Returns the worker ID and worker index for a given key from a previously assigned operator partition.

        This method is getting call only during a migration cycle. It can return None because the key could have been
        already migrated from a previous function call.

        Args:
            operator_name (str): The name of the operator.
            partition (int): The partition index of the operator.
            key (Any): The key whose worker mapping is being queried.

        Returns:
            tuple[int, int] | None: A tuple of (worker_id, worker_index) if the key is found,
            otherwise `None`.
        """
        operator_partition = (operator_name, partition)
        if operator_partition in self.remote_keys and key in self.remote_keys[operator_partition]:
            return self.remote_keys[operator_partition][key]
        return None

    def set_owned_partitions(self, operator_partitions: set[OperatorPartition]) -> None:
        self.owned_partitions = {tuple(op) for op in operator_partitions}

    def add_new_operator_partition(self, operator_partition: OperatorPartition) -> None:
        operator_partition = tuple(operator_partition)
        if operator_partition not in self.operator_partitions:
            self.operator_partitions.add(operator_partition)
            # InMemoryOperatorState fields
            self.data[operator_partition] = {}
            self.delta_map[operator_partition] = {}
            # BaseAriaState fields (read/write sets for transactional protocol)
            self.write_sets[operator_partition] = {}
            self.writes[operator_partition] = {}
            self.reads[operator_partition] = {}
            self.read_sets[operator_partition] = {}
            self.global_write_sets[operator_partition] = {}
            self.global_reads[operator_partition] = {}
            self.global_read_sets[operator_partition] = {}

    def discard_remote_key(self, operator_partition: OperatorPartition, key: K) -> None:
        """Drop a pending incoming-migration entry for a key.

        Called when the source worker reports it no longer holds the key (a None
        on-demand response). The key is treated as absent locally so the waiting
        transaction can proceed and the key is not re-requested every epoch.
        """
        operator_partition = tuple(operator_partition)
        if operator_partition in self.remote_keys:
            self.remote_keys[operator_partition].pop(key, None)
            if not self.remote_keys[operator_partition]:
                del self.remote_keys[operator_partition]

    def add_remote_keys(
        self,
        operator_partition: OperatorPartition,
        data: dict[K, tuple[int, int]],
    ) -> None:
        operator_partition = tuple(operator_partition)
        if operator_partition not in self.operator_partitions:
            self.add_new_operator_partition(operator_partition)
        if operator_partition in self.remote_keys:
            self.remote_keys[operator_partition].update(data)
        else:
            self.remote_keys[operator_partition] = data

    def set_data_from_snapshot(self, data: dict[OperatorPartition, KVPairs]) -> None:
        for operator_partition, kv_pairs in data.items():
            self.data[operator_partition] = kv_pairs

    def get_operator_partitions_to_repartition(
        self,
    ) -> dict[str, set[OperatorPartition]]:
        res = {operator_name: set() for operator_name, _ in self.operator_partitions}
        for operator_name, partition in self.operator_partitions:
            res[operator_name].add((operator_name, partition))
        return res

    def get_operator_data_for_repartitioning(
        self,
        operator: OperatorPartition,
    ) -> KVPairs:
        return self.data[operator]

    def get_data_for_snapshot(self) -> dict[OperatorPartition, KVPairs]:
        return self.delta_map

    def clear_delta_map(self) -> None:
        for operator_partition in self.operator_partitions:
            self.delta_map[operator_partition].clear()

    def commit_fallback_transaction(self, t_id: int) -> None:
        if t_id in self.fallback_commit_buffer:
            for operator_partition, kv_pairs in self.fallback_commit_buffer[t_id].items():
                for key, value in kv_pairs.items():
                    self.data[operator_partition][key] = value
                    self.delta_map[operator_partition][key] = value

    def get_all(
        self,
        t_id: int,
        operator_name: str,
        partition: int,
    ) -> dict[OperatorPartition, KVPairs]:
        operator_partition: OperatorPartition = (operator_name, partition)
        for key in self.data[operator_partition]:
            self.deal_with_reads(key, t_id, operator_partition)
        return fast_deepcopy(self.data[operator_partition])

    def batch_insert(self, kv_pairs: dict, operator_name: str, partition: int) -> None:
        operator_partition: OperatorPartition = (operator_name, partition)
        self.data[operator_partition].update(kv_pairs)
        self.delta_map[operator_partition].update(kv_pairs)

    def get(self, key: K, t_id: int, operator_name: str, partition: int) -> V:
        return _cy_state_get(
            self.data,
            self.write_sets,
            self.reads,
            self.read_sets,
            key,
            t_id,
            operator_name,
            partition,
        )

    def get_immediate(self, key: K, t_id: int, operator_name: str, partition: int) -> V:
        return _cy_state_get_immediate(
            self.data,
            self.fallback_commit_buffer,
            key,
            t_id,
            operator_name,
            partition,
            self.fallback_read_sets,
        )

    def delete(self, key: K, operator_name: str, partition: int) -> None:
        # Need to find a way to implement deletes
        pass

    def in_remote_keys(self, key: K, operator_name: str, partition: int) -> bool:
        operator_partition: OperatorPartition = (operator_name, partition)
        return operator_partition in self.remote_keys and key in self.remote_keys[operator_partition]

    def exists(self, key: K, operator_name: str, partition: int) -> bool:
        operator_partition: OperatorPartition = (operator_name, partition)
        if operator_partition not in self.data:
            return False
        if key in self.data[operator_partition]:
            # During migration: a key still in data but pending send to another
            # partition should not be considered as belonging here.
            if self.set_keys_to_send:
                return key not in self.set_keys_to_send
            return True
        # During migration: key is expected to arrive from a remote worker
        return self.in_remote_keys(key, operator_name, partition)

    def commit(self, aborted_from_remote: set[int]) -> set[int]:
        try:
            return _cy_commit(self.write_sets, self.data, self.delta_map, aborted_from_remote)
        except Exception as e:
            logging.warning(traceback.format_exc())
            raise e
