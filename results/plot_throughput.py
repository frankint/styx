import ast
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import EngFormatter, MultipleLocator

from plot_common import load_metadata, plot_migrations

plt.rcParams.update({"font.size": 16})

_SAVEFIG_KW = {"bbox_inches": "tight", "pad_inches": 0.05}


def _throughput_tick_step(max_tps: float) -> float:
    if max_tps <= 100:
        return 10
    if max_tps <= 1000:
        return 100
    if max_tps <= 10000:
        return 2000
    return 5000


def plot_throughput(
    run_path: Path,
    *,
    axes: plt.Axes | None = None,
    interval_size: int = 1,
    x_max_seconds: float | None = None,
    save_path: Path | str | None = None,
    show: bool = False,
) -> tuple[plt.Figure, plt.Axes]:
    run_path = Path(run_path)
    warmup_seconds, migrations = load_metadata(run_path)

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

    if x_max_seconds is not None:
        throughput_df = throughput_df[
            throughput_df["time_bucket_shifted"] <= x_max_seconds
        ].copy()

    if axes is None:
        fig, ax = plt.subplots(
            1,
            1,
            sharex=True,
            figsize=(12, 6),
        )
    else:
        ax = axes
        fig = ax.figure

    ax.plot(
        throughput_df["time_bucket_shifted"],
        throughput_df["throughput_in"],
        color="C0",
        linewidth=3,
        alpha=0.8,
        label="Input Rate (req/s)",
    )
    ax.plot(
        throughput_df["time_bucket_shifted"],
        throughput_df["throughput_out"],
        color="C1",
        linewidth=3,
        linestyle="--",
        alpha=1,
        label="Output TPS",
    )

    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Throughput (TPS)")
    ax.yaxis.set_major_formatter(EngFormatter(sep=""))
    ax.grid(linestyle="dotted", linewidth=1.5, axis="y")

    max_in = float(throughput_df["throughput_in"].max()) if not throughput_df.empty else 0.0
    max_out = float(throughput_df["throughput_out"].max()) if not throughput_df.empty else 0.0
    max_tps = max(max_in, max_out, 1.0)
    y_padding = max(max_tps * 0.1, _throughput_tick_step(max_tps))
    ax.yaxis.set_major_locator(MultipleLocator(_throughput_tick_step(max_tps)))

    if throughput_df.empty:
        x_right = float(x_max_seconds) if x_max_seconds is not None else 1.0
    elif x_max_seconds is not None:
        x_right = float(x_max_seconds)
    else:
        x_right = float(throughput_df["time_bucket_shifted"].max())
    ax.set_xlim(0, x_right)
    ax.set_ylim(0, max_tps + y_padding)

    plot_migrations(ax, migrations, t0, warmup_seconds)

    ax.legend()

    if save_path is not None:
        fig.tight_layout()
        fig.savefig(save_path, **_SAVEFIG_KW)
    if show:
        plt.show()

    return fig, ax
