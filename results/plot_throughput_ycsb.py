import ast
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import rcParams

rcParams["figure.figsize"] = [14, 5]
plt.rcParams.update({"font.size": 22})


def _load_metadata(
    run_path: Path,
    default_warmup_seconds: int = 10,
    default_end_scale_sec: int = 10,
) -> tuple[int, int | None, int | None]:
    meta_path = run_path / "metadata.json"
    if not meta_path.is_file():
        return default_warmup_seconds, None, None

    with meta_path.open("r") as f:
        metadata = json.load(f)

    warmup_seconds = int(metadata.get("warmup_seconds", default_warmup_seconds))
    start_migration_ms_epoch = None
    end_migration_ms_epoch = None

    start_str = metadata.get("start")
    manual_scale_sec = metadata.get("manual_scale_sec")
    if start_str and manual_scale_sec is not None:
        try:
            start_dt = datetime.fromisoformat(start_str)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            start_ms = int(start_dt.timestamp() * 1000)
            start_migration_ms_epoch = int(manual_scale_sec) #start_ms + int(manual_scale_sec) * 1000
            end_migration_ms_epoch = (
                start_migration_ms_epoch + default_end_scale_sec
            )
        except ValueError:
            pass

    return warmup_seconds, start_migration_ms_epoch, end_migration_ms_epoch


def plot_throughput(
    run_path: Path,
    *,
    ax: plt.Axes | None = None,
    interval_size: int = 1,
    save_path: Path | None = None,
    show: bool = False,
) -> tuple[plt.Figure, plt.Axes]:
    run_path = Path(run_path)
    warmup_seconds, _, _ = _load_metadata(run_path)

    # Load CSVs
    input_df = pd.read_csv(run_path / "client_requests.csv")
    output_df = pd.read_csv(run_path / "output.csv")

    # Parse request_id byte strings
    input_df["request_id"] = input_df["request_id"].apply(ast.literal_eval)
    output_df["request_id"] = output_df["request_id"].apply(ast.literal_eval)

    # Normalize timestamps to start from 0 seconds
    t0 = min(input_df["timestamp"].min(), output_df["timestamp"].min())
    input_df["time_since_start_sec"] = (input_df["timestamp"] - t0) / 1000
    output_df["time_since_start_sec"] = (output_df["timestamp"] - t0) / 1000

    # Filter out warmup
    input_df_filtered = input_df[
        input_df["time_since_start_sec"] >= warmup_seconds
    ].copy()
    output_df_filtered = output_df[
        output_df["time_since_start_sec"] >= warmup_seconds
    ].copy()

    # Floor to time bucket
    input_df_filtered["time_bucket"] = (
        input_df_filtered["time_since_start_sec"] // interval_size
    ) * interval_size
    output_df_filtered["time_bucket"] = (
        output_df_filtered["time_since_start_sec"] // interval_size
    ) * interval_size

    # Count requests per bucket
    throughput_in_df = (
        input_df_filtered.groupby("time_bucket").size().reset_index(name="throughput_in")
    )
    throughput_out_df = (
        output_df_filtered.groupby("time_bucket").size().reset_index(name="throughput_out")
    )

    # Merge and fill gaps
    throughput_df = pd.merge(
        throughput_in_df, throughput_out_df, on="time_bucket", how="outer"
    ).fillna(0)

    # Shift x-axis to start from 0 post-warmup
    throughput_df["time_bucket_shifted"] = throughput_df["time_bucket"] - warmup_seconds

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = ax.figure

    # Plot throughput
    ax.plot(
        throughput_df["time_bucket_shifted"],
        throughput_df["throughput_in"],
        label="Input Throughput",
        linewidth=3,
    )
    ax.plot(
        throughput_df["time_bucket_shifted"],
        throughput_df["throughput_out"],
        label="Output Throughput",
        linewidth=3,
        alpha=0.8,
    )
    ax.set_xlabel("Time (s)")
    ax.grid(linestyle="dotted", linewidth=1.5, axis="y")
    ax.set_ylabel("Throughput (TPS)")
    ax.legend()

    if save_path is not None:
        fig.savefig(save_path)
    if show:
        plt.show()

    return fig, ax
