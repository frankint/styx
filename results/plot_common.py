import json
from pathlib import Path

import matplotlib.pyplot as plt


def load_metadata(
    run_path: Path,
    default_warmup_seconds: int = 10,
) -> tuple[int, list[dict]]:
    """Load metadata and return warmup_seconds and list of migrations.

    Each migration is a dict with 'start' and 'end' keys (ms epoch timestamps).
    Supports both new format (migrations list) and legacy format (single migration).
    """
    meta_path = run_path / "metadata.json"
    if not meta_path.is_file():
        return default_warmup_seconds, []

    with meta_path.open("r") as f:
        metadata = json.load(f)

    warmup_seconds = int(metadata.get("warmup_seconds", default_warmup_seconds))

    migrations = metadata.get("migrations", [])

    if not migrations:
        start = metadata.get("migration_start_time")
        end = metadata.get("migration_end_time")
        if start is not None and end is not None:
            migrations = [{"start": start, "end": end}]

    return warmup_seconds, migrations


def plot_migrations(
    ax: plt.Axes,
    migrations: list[dict],
    t0: float,
    warmup_seconds: int,
) -> None:
    """Plot vertical lines for migration start/end times."""
    if not migrations:
        return

    for mig in migrations:
        start_ms = mig.get("start")
        end_ms = mig.get("end")

        if start_ms is None:
            continue

        start_time = ((start_ms - t0) / 1000) - warmup_seconds

        ax.axvline(
            x=start_time,
            color="red",
            linestyle="--",
            linewidth=2,
            alpha=0.8,
        )

        if end_ms is not None:
            end_time = ((end_ms - t0) / 1000) - warmup_seconds
            ax.axvline(
                x=end_time,
                color="green",
                linestyle="--",
                linewidth=2,
                alpha=0.8,
            )
