import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import requests
import pandas as pd
from pathlib import Path

PROM = "http://localhost:9090"


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
    migrations: list[dict] = None
    zipf_const: Optional[float] = None
    interval_seconds: Optional[int] = None
    delta_tps: Optional[int] = None
    n_threads: int = 1


def query_prometheus_range(
    metric: str,
    start_time: float,  # Unix timestamp (seconds)
    end_time: float,
    step: str = "1s",
    prometheus_url: str = "http://localhost:9090",
) -> pd.DataFrame:
    """Query Prometheus for a time series over a range."""
    resp = requests.get(
        f"{prometheus_url}/api/v1/query_range",
        params={
            "query": metric,
            "start": start_time,
            "end": end_time,
            "step": step,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    
    if data["status"] != "success":
        raise ValueError(f"Prometheus query failed: {data}")
    
    results = data["data"]["result"]
    if not results:
        return pd.DataFrame(columns=["timestamp", "value"])
    
    # Handle multiple series (e.g., per-instance metrics)
    rows = []
    for series in results:
        labels = series.get("metric", {})
        for timestamp, value in series["values"]:
            row = {"timestamp": float(timestamp), "value": float(value)}
            row.update(labels)  # Add labels as columns
            rows.append(row)
    
    return pd.DataFrame(rows)

def export_metrics(
    save_dir: Path,
    start_time: float,
    end_time: float,
    prometheus_url: str = "http://localhost:9090",
    step: str = "1s",
):
    """Export key metrics from Prometheus to CSV files."""
    metrics = {
        "backlog": "sum(queue_backlog)",
        "num_workers": "live_worker_count",
    }
    
    save_dir = Path(save_dir)
    
    for name, query in metrics.items():
        try:
            df = query_prometheus_range(
                query, start_time, end_time, step, prometheus_url
            )
            if not df.empty:
                df.to_csv(save_dir / f"{name}.csv", index=False)
                print(f"Exported {name}: {len(df)} data points")
        except Exception as e:
            print(f"Failed to export {name}: {e}")


def save_data(data, save_dir, filename):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    with open(os.path.join(save_dir, filename), "w") as f:
        json.dump(data, f, indent=2)


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
        "migrations": params.migrations if params.migrations is not None else None,
    }
    if params.zipf_const is not None:
        metadata["zipf_const"] = params.zipf_const
    if params.interval_seconds is not None:
        metadata["increase_interval"] = params.interval_seconds
    if params.delta_tps is not None:
        metadata["increase_amount"] = params.delta_tps

    metadata["n_threads"] = params.n_threads

    save_data(metadata, params.out_path, "metadata.json")
