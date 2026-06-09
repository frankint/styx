import ast
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import rcParams
from matplotlib.ticker import EngFormatter

from plot_common import load_metadata, plot_migrations

rcParams["figure.figsize"] = [14, 5]
plt.rcParams.update({"font.size": 16})


def plot_latency(
    run_path: Path,
    *,
    ax: plt.Axes | None = None,
    interval_size: int = 1,
    save_path: Path | None = None,
    show: bool = False,
) -> tuple[plt.Figure, plt.Axes]:
    run_path = Path(run_path)
    warmup_seconds, migrations = load_metadata(run_path)

    # Parse request_id byte strings
    input_df = pd.read_csv(run_path / "client_requests.csv")
    output_df = pd.read_csv(run_path / "output.csv")

    # Join on request_id
    input_df["request_id"] = input_df["request_id"].apply(ast.literal_eval)
    output_df["request_id"] = output_df["request_id"].apply(ast.literal_eval)

    # Compute latency in milliseconds
    merged_df = pd.merge(input_df, output_df, on="request_id", suffixes=("_in", "_out"))
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
    # Shift x-axis to start from 0 post-warmup
    mean_latency_df["time_bucket_shifted"] = (
        mean_latency_df["time_bucket"] - warmup_seconds
    )

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = ax.figure

    ax.plot(
        mean_latency_df["time_bucket_shifted"],
        mean_latency_df["latency_ms"],
        label="Mean Latency",
        linewidth=3,
        color="blue",
    )

    ax.set_xlabel("Time (s)")
    ax.grid(linestyle="dotted", linewidth=1.5, axis="y")
    ax.set_ylabel("E2E Latency (ms)")
    ax.yaxis.set_major_formatter(EngFormatter(sep=""))

    plot_migrations(ax, migrations, t0, warmup_seconds)

    ax.legend(loc="upper right")

    if save_path is not None:
        fig.savefig(save_path)
    if show:
        plt.show()

    return fig, ax
