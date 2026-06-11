#!/usr/bin/env python3
"""
Automatically exports Prometheus metrics defined in the Grafana dashboard 
to CSV files in the latest experiment result folder.
"""

import json
import urllib.request
import urllib.parse
from pathlib import Path
import pandas as pd
from datetime import datetime
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

def export_metrics(run_dir):
    meta_path = run_dir / "metadata.json"
    if not meta_path.exists():
        print(f"No metadata.json in {run_dir}")
        return

    with open(meta_path) as f:
        meta = json.load(f)

    start_str = meta.get("start")
    end_str = meta.get("end")
    if not start_str or not end_str:
        print("Start or end time missing in metadata.json")
        return

    # Use local time for naive datetimes, just like the Grafana open script does
    start_dt = datetime.fromisoformat(start_str)
    end_dt = datetime.fromisoformat(end_str)

    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()

    dashboard_path = Path("grafana/dashboards/styx.json")
    if not dashboard_path.exists():
        print("Dashboard JSON not found.")
        return

    with open(dashboard_path, encoding='utf-8') as f:
        dashboard = json.load(f)

    panels = dashboard.get("panels", [])
    
    prometheus_url = "http://localhost:9090/api/v1/query_range"
    step = "1s" # Step size for the query

    print(f"Exporting metrics for time range: {start_dt} to {end_dt}")

    for panel in panels:
        title = panel.get("title", "Untitled").replace("/", "_").replace(" ", "_").replace("(", "").replace(")", "").replace("%", "percent")
        targets = panel.get("targets", [])
        for i, target in enumerate(targets):
            expr = target.get("expr")
            if not expr:
                continue

            # Skip prometheus variables or very short queries that might just be template vars
            if "$var" in expr or len(expr) < 3:
                continue
                
            print(f"Exporting panel '{title}' query {i}...")
            
            params = {
                "query": expr,
                "start": start_ts,
                "end": end_ts,
                "step": step
            }
            query_string = urllib.parse.urlencode(params)
            url = f"{prometheus_url}?{query_string}"
            
            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode())
                
                if data.get("status") == "success":
                    result = data.get("data", {}).get("result", [])
                    if not result:
                        print(f"  No data for {title}")
                        continue
                    
                    dfs = []
                    
                    legend_format = target.get("legendFormat", "")
                    unit = panel.get("fieldConfig", {}).get("defaults", {}).get("unit", "")
                    
                    for res in result:
                        metric = res.get("metric", {})
                        # create a readable column name
                        import re
                        col_name = "value"
                        if legend_format and legend_format != "__auto":
                            col_name = legend_format
                            # replace {{label}} with value
                            def repl(m):
                                return str(metric.get(m.group(1), m.group(0)))
                            col_name = re.sub(r'\{\{\s*(.*?)\s*\}\}', repl, col_name)
                        else:
                            if metric:
                                if "instance" in metric:
                                    col_name = f"{metric.get('__name__', 'value')}_{metric['instance']}"
                                elif "phase" in metric:
                                    col_name = f"{metric.get('__name__', 'value')}_{metric['phase']}"
                                else:
                                    col_name = "_".join(f"{k}={v}" for k, v in metric.items())
                        
                        if col_name == "value" and metric:
                            col_name = "_".join(f"{k}={v}" for k, v in metric.items())
                            
                        if unit:
                            col_name = f"{col_name} [{unit}]"
                            
                        values = res.get("values", [])
                        
                        df = pd.DataFrame(values, columns=["timestamp", col_name])
                        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                        df.set_index("timestamp", inplace=True)
                        dfs.append(df)
                    
                    if dfs:
                        # Find duplicate column names and rename them if necessary
                        final_df = pd.concat(dfs, axis=1)
                        # Fix duplicated columns
                        cols = pd.Series(final_df.columns)
                        for dup in cols[cols.duplicated()].unique():
                            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
                        final_df.columns = cols
                        
                        out_file = run_dir / f"{title}_{i}.csv"
                        final_df.to_csv(out_file)
                        print(f"  Saved {out_file}")
                else:
                    print(f"  Query failed for {title}")
            except Exception as e:
                print(f"  Error querying {title}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", help="Specific run directory. Defaults to latest in results/", default=None)
    args = parser.parse_args()

    if args.run_dir:
        target_dir = Path(args.run_dir)
    else:
        target_dir = get_latest_run_dir()
    
    if target_dir:
        print(f"Found run directory: {target_dir}")
        export_metrics(target_dir)
        
        # Clean up massive raw files to save storage space
        import os
        for huge_file in ["client_requests.csv", "output.csv"]:
            file_path = target_dir / huge_file
            if file_path.exists():
                os.remove(file_path)
                print(f"Cleaned up large raw file: {file_path}")
    else:
        print("No run directory found.")
