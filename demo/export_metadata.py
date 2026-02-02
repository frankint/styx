import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from decimal import Decimal

import requests
import re

PROM = "http://localhost:9090"


def save_data(data, save_dir, filename):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    with open(os.path.join(save_dir, filename), "w") as f:
        json.dump(data, f, indent=2)


@dataclass(frozen=True)
class MetadataParams:
    workload: str
    start: float
    end: float
    out_path: str
    n_partitions: int
    messages_per_second: int
    n_keys: int
    seconds: int
    epoch_size: int
    warmup_seconds: int
    migration_start_time: float
    migration_end_time: float
    zipf_const: Optional[float] = None
    manual_scale_sec: Optional[int] = None
    interval_seconds: Optional[int] = None
    delta_tps: Optional[int] = None
    n_threads: int = 1

def get_migration_times(url: str) -> tuple[float, float]:
    resp = requests.get(f"{url}")

    match = re.search(r'^migration_end_time_ms.*$', resp.text, re.MULTILINE)
    migration_end_time = match.group(0).split(" ")[1] if match else None
    match = re.search(r'^migration_start_time_ms.*$', resp.text, re.MULTILINE)
    migration_start_time = match.group(0).split(" ")[1] if match else None
    #print(f"Returning migration times: {migration_start_time}, {migration_end_time}")
    return float(Decimal(migration_start_time)), float(Decimal(migration_end_time)) # handle scientific notations safely

def save_metadata(params: MetadataParams):
    metadata = {
        "workload": params.workload,
        "messages_per_second": params.messages_per_second,
        "n_partitions": params.n_partitions,
        "n_keys": params.n_keys,
        "start": datetime.fromtimestamp(params.start).isoformat(),
        "end": datetime.fromtimestamp(params.end).isoformat(),
        "duration (s)": params.seconds, 
        "epoch_size": params.epoch_size,
        "warmup_seconds": params.warmup_seconds,
        "migration_start_time": params.migration_start_time,
        "migration_end_time": params.migration_end_time,
    }
    if params.zipf_const is not None:
        metadata["zipf_const"] = params.zipf_const
    if params.interval_seconds is not None:
        metadata["increase_interval"] = params.interval_seconds
    if params.delta_tps is not None:
        metadata["increase_amount"] = params.delta_tps
    if params.manual_scale_sec is not None:
        metadata["manual_scale_sec"] = params.manual_scale_sec

    metadata["n_threads"] = params.n_threads

    save_data(metadata, params.out_path, "metadata.json")


def export_all_metrics(workload, start, end, step, out_path, n_partitions, messages_per_second):
    metric_set = {
        "latency": "avg by(instance) (worker_cpu_usage_percent)",
        "memory": "avg by(instance) (worker_memory_usage_mb) * 1000000",
        "throughput": f"sum(rate(worker_epoch_throughput_tps[{step}]))",
        "latency_breakdown": "avg(latency_breakdown) by (component)",
        "transaction_latency": "avg(worker_epoch_latency_ms)",
        "snapshotting_time": "avg(worker_total_snapshotting_time_ms)",
        "backpressure": "sum(worker_backpressure)",
        "queue_backlog": "sum(queue_backlog)",
    }

    timestamp = datetime.now().strftime("%m%d_%H%M")
    save_dir = os.path.join(
        out_path, f"{workload}_{messages_per_second}tps_{n_partitions}partitions_{timestamp}"
    )
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    print(f"Exporting metrics to {save_dir}")
    for metric_name, metric_query in metric_set.items():
        data = get_metric_data(metric_query, start, end, step)
        with open(os.path.join(save_dir, f"{metric_name}.json"), "w") as f:
            json.dump(data, f, indent=2)


def get_metric_data(query, start, end, step):
    resp = requests.get(
        f"{PROM}/api/v1/query_range",
        params={
            "query": query,
            "start": start,
            "end": end,
            "step": step,
        },
    )
    resp.raise_for_status()
    return resp.json()
