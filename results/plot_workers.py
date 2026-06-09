from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import rcParams
from matplotlib.ticker import EngFormatter

from plot_common import load_metadata, plot_migrations

rcParams["figure.figsize"] = [14, 4]
plt.rcParams.update({"font.size": 22})


def _get_client_t0_ms(run_path: Path) -> float:
    input_df = pd.read_csv(run_path / "client_requests.csv")
    output_df = pd.read_csv(run_path / "output.csv")
    return min(input_df["timestamp"].min(), output_df["timestamp"].min())


def _load_prometheus_series(run_path: Path, filename: str) -> pd.DataFrame | None:
    path = run_path / filename
    if not path.is_file():
        return None

    df = pd.read_csv(path)
    if df.empty or "timestamp" not in df.columns or "value" not in df.columns:
        return None

    # One point per timestamp (sum() queries are already aggregated; raw metrics may have labels)
    return (
        df.groupby("timestamp", as_index=False)["value"]
        .mean()
        .sort_values("timestamp")
    )


def _align_to_workload_time(
    df: pd.DataFrame,
    t0_ms: float,
    warmup_seconds: int,
    x_max_seconds: float | None,
) -> pd.DataFrame:
    t0_sec = t0_ms / 1000.0
    aligned = df.copy()
    aligned["time_since_start_sec"] = aligned["timestamp"] - t0_sec
    aligned = aligned[aligned["time_since_start_sec"] >= warmup_seconds].copy()
    aligned["time_shifted"] = aligned["time_since_start_sec"] - warmup_seconds

    if x_max_seconds is not None:
        aligned = aligned[aligned["time_shifted"] <= x_max_seconds]

    return aligned.sort_values("time_shifted")


def _plot_prometheus_metric(
    run_path: Path,
    *,
    csv_name: str,
    ylabel: str,
    label: str,
    color: str,
    show_migrations: bool = False,
    ax: plt.Axes | None = None,
    x_max_seconds: float | None = None,
    save_path: Path | str | None = None,
    show: bool = False,
) -> tuple[plt.Figure, plt.Axes]:
    run_path = Path(run_path)
    warmup_seconds, migrations = load_metadata(run_path)
    series = _load_prometheus_series(run_path, csv_name)

    if ax is None:
        fig, ax = plt.subplots()
    else:
        fig = ax.figure

    if series is None:
        ax.text(
            0.5,
            0.5,
            f"Missing {csv_name}",
            transform=ax.transAxes,
            ha="center",
            va="center",
            fontsize=16,
        )
        ax.set_ylabel(ylabel)
        ax.set_xlabel("Time (s)")
        return fig, ax

    t0_ms = _get_client_t0_ms(run_path)
    aligned = _align_to_workload_time(series, t0_ms, warmup_seconds, x_max_seconds)

    ax.plot(
        #[0, 20, 30, 40, 50, 60, 75, 100, 110, 125, 140, 150, 170, 185, 200],
        #[1, 2, 3, 4, 5, 6, 6, 6, 5, 3, 2, 1, 2, 4, 4],
        aligned["time_shifted"],
        aligned["value"],
        label=label,
        linewidth=3,
        color=color,
    )

    ax.set_ylabel(ylabel)
    ax.yaxis.set_major_formatter(EngFormatter(sep=""))
    ax.grid(linestyle="dotted", linewidth=1.5, axis="y")

    if x_max_seconds is not None:
        x_right = float(x_max_seconds)
    elif aligned.empty:
        x_right = 1.0
    else:
        x_right = float(aligned["time_shifted"].max())
    ax.set_xlim(0, x_right)

    if show_migrations:
        plot_migrations(ax, migrations, t0_ms, warmup_seconds)
    else:
        ax.set_ylim(0, aligned["value"].max() + 1)
    ax.legend(loc="upper right")

    if save_path is not None:
        fig.savefig(save_path)
    if show:
        plt.show()

    return fig, ax


def plot_workers(
    run_path: Path,
    *,
    ax: plt.Axes | None = None,
    x_max_seconds: float | None = None,
    save_path: Path | str | None = None,
    show: bool = False,
) -> tuple[plt.Figure, plt.Axes]:
    return _plot_prometheus_metric(
        run_path,
        csv_name="num_workers.csv",
        ylabel="Workers",
        label="Live workers",
        color="C2",
        ax=ax,
        x_max_seconds=x_max_seconds,
        save_path=save_path,
        show=show,
    )


def plot_backlog(
    run_path: Path,
    *,
    ax: plt.Axes | None = None,
    x_max_seconds: float | None = None,
    save_path: Path | str | None = None,
    show: bool = False,
) -> tuple[plt.Figure, plt.Axes]:
    return _plot_prometheus_metric(
        run_path,
        csv_name="backlog.csv",
        ylabel="Total backlog",
        label="Total backlog",
        color="C3",
        show_migrations=True,
        ax=ax,
        x_max_seconds=x_max_seconds,
        save_path=save_path,
        show=show,
    )
