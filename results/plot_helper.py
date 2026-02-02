from pathlib import Path
from typing import List

import argparse
import matplotlib.pyplot as plt
import os

from plot_latency_ycsb import plot_latency
from plot_throughput_ycsb import plot_throughput

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
            f"\nSelect run number to plot (or 'q' to quit): "
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactive helper to plot YCSB latency and throughput side-by-side."
    )
    parser.add_argument(
        "workload",
        nargs="+",
        help="Workloads to select",
    )
    args = parser.parse_args()

    runs = find_runs_by_keyword(args.workload)

    while True:
        selected = interactive_select(runs)
        if not selected:
            return

        fig, (ax_latency, ax_throughput) = plt.subplots(1, 2, figsize=(16, 5))
        plot_latency(
            selected,
            ax=ax_latency,
            save_path=os.path.join(RESULTS_DIR, "latency_ycsb.pdf"),
        )
        plot_throughput(
            selected,
            ax=ax_throughput,
            save_path=os.path.join(RESULTS_DIR, "throughput_ycsb.pdf"),
        )

        fig.tight_layout()
        #combined_path = selected / args.combined_name
        #fig.savefig(combined_path)
        plt.show()


if __name__ == "__main__":
    main()
