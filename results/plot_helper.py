from pathlib import Path
from typing import List

import argparse
import matplotlib.pyplot as plt
import os

from matplotlib.lines import Line2D
from plot_latency import plot_latency
from plot_throughput import plot_throughput
from plot_workers import plot_backlog, plot_workers

RESULTS_DIR = Path(__file__).resolve().parent


def find_runs_by_keyword(keywords: List[str]) -> List[Path]:
    keywords = [keyword.lower() for keyword in keywords]
    runs: List[Path] = []
    if not RESULTS_DIR.is_dir():
        print(f"Results directory not found: {RESULTS_DIR}")
        return runs

    for child in sorted(RESULTS_DIR.iterdir()):
        if not child.is_dir():
            continue
        # Basic matching: directory name contains the keyword (e.g. "ycsb", "dhr")
        if not all(keyword in child.name.lower() for keyword in keywords):
            continue
        runs.append(child)

    return runs


# Select the most recent run as the default
def _select_default_run(runs: List[Path]) -> Path:
    def sort_key(run: Path) -> float:
        return run.stat().st_mtime

    return max(runs, key=sort_key)


def interactive_select(runs: List[Path]) -> Path | None:
    if not runs:
        print("No matching runs found.")
        return None

    default_run = _select_default_run(runs)

    print("Matching runs:")
    for i, run_dir in enumerate(runs, start=1):
        default_mark = " (default)" if run_dir == default_run else ""
        print(f"  [{i:>2}] {run_dir.name}{default_mark}")

    while True:
        choice = input(
            "\nSelect run number to plot (or 'q' to quit): "
        ).strip()
        if choice.lower() in {"q", "quit", "exit"}:
            return None

        if choice == "":
            return default_run

        if not choice.isdigit():
            print("Please enter a valid number or 'q'.")
            continue

        idx = int(choice)
        if not (1 <= idx <= len(runs)):
            print(f"Please enter a number between 1 and {len(runs)}.")
            continue

        return runs[idx - 1]


def _output_basename(keywords: List[str]) -> str:
    return "_".join(keywords).lower()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Interactive helper to plot throughput, latency, workers, and backlog "
            "(stacked vertically) for any benchmark run."
        )
    )
    parser.add_argument(
        "workload",
        nargs="+",
        help="Keywords to match run directories (e.g. ycsb, tpcc, dmr, dhr)",
    )
    parser.add_argument(
        "-c", "--combined-name",
        type=str,
        default=None,
        help="Name of the combined plot",
    )
    args = parser.parse_args()

    runs = find_runs_by_keyword(args.workload)
    if args.combined_name:
        output_name = f"{args.combined_name}.pdf"
    else:
        output_name = f"combined_{_output_basename(args.workload)}.pdf"

    while True:
        selected = interactive_select(runs)
        if not selected:
            return

        x_window_sec = None

        fig, axes = plt.subplots(
            4,
            1,
            sharex=True,
            figsize=(16, 16),
            gridspec_kw={"height_ratios": [3, 3, 2, 2]},
        )
        plot_throughput(
            selected,
            axes=axes[0],
            save_path=None,
            show=False,
            x_max_seconds=x_window_sec,
        )
        axes[0].set_xlabel("")
        plot_latency(selected, ax=axes[1], save_path=None)
        axes[1].set_xlabel("")
        plot_workers(
            selected,
            ax=axes[2],
            x_max_seconds=x_window_sec,
            save_path=None,
            show=False,
        )
        axes[2].set_xlabel("")
        plot_backlog(
            selected,
            ax=axes[3],
            x_max_seconds=x_window_sec,
            save_path=None,
            show=False,
        )

        fig.tight_layout()
        migration_handles = [
            Line2D([0], [0], color="red", linestyle="--", linewidth=2, alpha=0.8, label="Migration Start"),
            Line2D([0], [0], color="green", linestyle="--", linewidth=2, alpha=0.8, label="Migration End"),
        ]
        fig.legend(
            handles=migration_handles,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.0),
            ncol=2,
            frameon=False,
            fontsize=26,
        )
        fig.subplots_adjust(top=0.96)
        fig.savefig(
            os.path.join(RESULTS_DIR, output_name),
            bbox_inches="tight",
            pad_inches=0.05,
        )
        #combined_path = selected / args.combined_name
        #fig.savefig(combined_path)
        plt.show()


if __name__ == "__main__":
    main()
