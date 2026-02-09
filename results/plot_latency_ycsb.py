import ast
from matplotlib import rcParams
import matplotlib.pyplot as plt
import pandas as pd
import json
from pathlib import Path

rcParams['figure.figsize'] = [14, 5]
plt.rcParams.update({'font.size': 22})


def _load_metadata(
    run_path: Path,
    default_warmup_seconds: int = 10,
) -> tuple[int, int | None, int | None]:
    meta_path = run_path / "metadata.json"
    if not meta_path.is_file():
        return default_warmup_seconds, None, None

    with meta_path.open("r") as f:
        metadata = json.load(f)

    warmup_seconds = int(metadata.get("warmup_seconds", default_warmup_seconds))
    start_migration_ms_epoch = metadata.get("migration_start_time", None)
    end_migration_ms_epoch = metadata.get("migration_end_time", None)

    return warmup_seconds, start_migration_ms_epoch, end_migration_ms_epoch

def plot_latency(
    run_path: Path,
    *,
    ax: plt.Axes | None = None,
    interval_size: int = 1,
    start_migration_ms_epoch: int | None = None,
    end_migration_ms_epoch: int | None = None,
    save_path: Path | None = None,
    show: bool = False,
) -> tuple[plt.Figure, plt.Axes]:
    run_path = Path(run_path)
    warmup_seconds, start_migration_ms_epoch, end_migration_ms_epoch = _load_metadata(run_path)

    input_df = pd.read_csv(run_path / "client_requests.csv")
    output_df = pd.read_csv(run_path / "output.csv")

    # Parse request_id byte strings
    input_df["request_id"] = input_df["request_id"].apply(ast.literal_eval)
    output_df["request_id"] = output_df["request_id"].apply(ast.literal_eval)

    # Join on request_id
    merged_df = pd.merge(input_df, output_df, on="request_id", suffixes=("_in", "_out"))

    # Compute latency in milliseconds
    merged_df["latency_ms"] = merged_df["timestamp_out"] - merged_df["timestamp_in"]

    # Normalize timestamps to start from 0 seconds
    t0 = merged_df["timestamp_in"].min()
    merged_df["time_since_start_sec"] = (merged_df["timestamp_in"] - t0) / 1000

    # Filter to show only warmup onwards
    filtered_df = merged_df[
        merged_df["time_since_start_sec"] >= warmup_seconds
    ].sort_values(by="time_since_start_sec")

    # Floor the time to the nearest interval
    filtered_df["time_bucket"] = (
        filtered_df["time_since_start_sec"] // interval_size
    ) * interval_size

    # Compute mean latency per bucket
    mean_latency_df = (
        filtered_df.groupby("time_bucket")["latency_ms"].mean().reset_index()
    )

    # Shift x-axis to start from 0 after warmup
    mean_latency_df["time_bucket_shifted"] = (
        mean_latency_df["time_bucket"] - warmup_seconds
    )

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = ax.figure

    # Plot mean latency
    ax.plot(
        mean_latency_df["time_bucket_shifted"],
        mean_latency_df["latency_ms"],
        label="Mean Latency",
        linewidth=3,
    )

    if start_migration_ms_epoch is not None and end_migration_ms_epoch is not None:
        start_migration_time = ((start_migration_ms_epoch - t0) // 1000) - warmup_seconds
        end_migration_time = ((end_migration_ms_epoch - t0) // 1000) - warmup_seconds
        ax.axvline(
            x=start_migration_time,
            color="red",
            linestyle="--",
            label="Start Migration",
            linewidth=3,
        )
        ax.text(
            start_migration_time - 3,
            -5,
            f"{start_migration_time}s",
            color="red",
            fontsize=20,
            ha="center",
            va="top",
        )
        ax.axvline(
            x=end_migration_time,
            color="green",
            linestyle="--",
            label="End Migration",
            linewidth=3,
        )
        ax.text(
            end_migration_time + 3,
            -5,
            f"{end_migration_time}s",
            color="green",
            fontsize=20,
            ha="center",
            va="top",
        )

    ax.set_xlabel("Time (s)")
    ax.grid(linestyle="dotted", linewidth=1.5, axis="y")
    ax.set_ylabel("Latency (ms)")
    ax.legend()

    if save_path is not None:
        fig.savefig(save_path)
    if show:
        plt.show()

    return fig, ax
