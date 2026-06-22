#!/usr/bin/env python3
"""
Automatically plot all Grafana CSV exports for a given run.
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import argparse

def get_latest_run_dir(results_dir="results"):
    p = Path(results_dir)
    if not p.is_dir():
        return None
    dirs = [d for d in p.iterdir() if d.is_dir()]
    if not dirs:
        return None
    # Sort by modification time, newest first
    dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return dirs[0]

def plot_all_csvs(run_dir):
    csv_files = list(run_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {run_dir}")
        return
        
    # We want to put plots in a plots directory inside the run_dir
    plots_dir = run_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    
    for csv_file in csv_files:
        print(f"Plotting {csv_file.name}...")
        try:
            df = pd.read_csv(csv_file)
            
            if "timestamp" not in df.columns:
                print(f"  Skipping {csv_file.name} (no timestamp column)")
                continue
                
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Make the timestamp relative to the start of the experiment
            start_time = df["timestamp"].min()
            df["Time (seconds)"] = (df["timestamp"] - start_time).dt.total_seconds()
            df.set_index("Time (seconds)", inplace=True)
            df.drop(columns=["timestamp"], inplace=True)
            
            plt.figure(figsize=(12, 6))
            
            # Extract common unit from column names
            units = set()
            import re
            
            for col in df.columns:
                match = re.search(r'\[(.*?)\]$', str(col))
                if match:
                    units.add(match.group(1))
            
            y_label = "Value"
            if len(units) == 1:
                y_label = f"Value ({list(units)[0]})"
            
            # Plot each column 
            for col in df.columns:
                # Remove unit from legend label if we made it the y-axis
                label = str(col)
                if len(units) == 1:
                    label = re.sub(r'\s*\[.*?\]$', '', label)
                
                label = label[:50] + ('...' if len(label)>50 else '')
                plt.plot(df.index, df[col], label=label, linewidth=2)
                
            title = csv_file.stem.replace("_", " ")
            plt.title(title, fontsize=16)
            plt.xlabel("Time since start (seconds)", fontsize=12)
            plt.ylabel(y_label, fontsize=12)
            
            plt.grid(True, linestyle="--", alpha=0.7)
            
            # Only show legend if there are not too many lines
            if len(df.columns) <= 15:
                # Place legend outside to not obscure the chart
                plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
            else:
                plt.tight_layout()
                
            save_path = plots_dir / f"{csv_file.stem}.png"
            plt.savefig(save_path)
            plt.close()
            print(f"  Saved plot to {save_path}")
        except Exception as e:
            print(f"  Failed to plot {csv_file.name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", help="Specific run directory. Defaults to interactive selection.", default=None)
    args = parser.parse_args()
    
    if args.run_dir:
        target = Path(args.run_dir)
    else:
        results_dir = Path("results")
        if not results_dir.is_dir():
            print("No 'results' directory found.")
            target = None
        else:
            dirs = [d for d in results_dir.iterdir() if d.is_dir()]
            if not dirs:
                print("No runs found in 'results/'.")
                target = None
            else:
                dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                print("Available runs:")
                for idx, d in enumerate(dirs):
                    print(f"[{idx}] {d.name}")
                
                try:
                    choice = int(input(f"\nSelect a run to plot (0-{len(dirs)-1}): "))
                    if 0 <= choice < len(dirs):
                        target = dirs[choice]
                    else:
                        print("Invalid choice.")
                        target = None
                except ValueError:
                    print("Invalid input. Please enter a number.")
                    target = None
        
    if target:
        print(f"\nGenerating plots for run: {target}")
        plot_all_csvs(target)
        print(f"\nAll plots have been saved to {target / 'plots'}")
    else:
        print("No run directory selected!")
