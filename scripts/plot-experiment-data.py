import os
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator
from matplotlib.lines import Line2D

def human_format(num, pos):
    if num == 0:
        return '0'
    magnitude = 0
    sign = -1 if num < 0 else 1
    num = abs(num)
    while num >= 1000 and magnitude < 4:
        magnitude += 1
        num /= 1000.0
    suffixes = ['', 'K', 'M', 'B', 'T']
    formatted_num = f"{num:g}" 
    return f"{sign * float(formatted_num):g}{suffixes[magnitude]}"

def normalize_time(series):
    try:
        numeric_series = series.astype(float)
        if numeric_series.dropna().max() > 1e9:
            return pd.to_datetime(numeric_series, unit='s')
    except (ValueError, TypeError):
        pass
    return pd.to_datetime(series)

def render_layout(layout_type, dir_name, plot_title, is_fixed, loaded_data, scaling_times, shortest_duration):
    """Renders and saves a specific layout configuration as an SVG with dynamic font scaling."""
    num_plots = 3 if is_fixed else 4

    # --- DYNAMIC PAPER SCALING ---
    if layout_type == 'horizontal':
        # Massive fonts so they survive being shrunk to fit a paper width
        rc_params = {
            'font.size': 22,
            'axes.titlesize': 26,
            'axes.labelsize': 22,
            'xtick.labelsize': 18,
            'ytick.labelsize': 18,
            'legend.fontsize': 18,
            'lines.linewidth': 3 
        }
        # Tighter width to naturally pull the subplots closer together
        fig_size = (5.5 * num_plots, 7)
        suffix = "-horizontal"
        rect = [0, 0, 1, 0.85] if is_fixed else [0, 0, 1, 0.80]
        title_y, legend_y = 0.98, 0.90
        title_size = 32
        
        # Reduced padding to eliminate whitespace between graphs
        w_pad_val = 0.5 

    else:
        # Standard sizes for vertical and 2x2
        rc_params = {
            'font.size': 14,
            'axes.titlesize': 16,
            'axes.labelsize': 14,
            'xtick.labelsize': 12,
            'ytick.labelsize': 12,
            'legend.fontsize': 12,
            'lines.linewidth': 2
        }
        
        w_pad_val = 2.0
        title_size = 22
        
        if layout_type == 'vertical':
            fig_size = (12, 4 * num_plots)
            suffix = ""
            rect = [0, 0, 1, 0.95] if is_fixed else [0, 0, 1, 0.92]
            title_y, legend_y = 0.98, 0.945
        elif layout_type == '2x2':
            fig_size = (16, 10)
            suffix = "-2x2"
            rect = [0, 0, 1, 0.93] if is_fixed else [0, 0, 1, 0.88]
            title_y, legend_y = 0.98, 0.93

    # Apply the context so these fonts only affect this specific layout render
    with plt.rc_context(rc_params):
        
        if layout_type == '2x2':
            fig, axes = plt.subplots(2, 2, figsize=fig_size)
            axes_list = axes.flatten()
        elif layout_type == 'horizontal':
            fig, axes = plt.subplots(1, num_plots, figsize=fig_size)
            axes_list = axes if num_plots > 1 else [axes]
        else:
            fig, axes = plt.subplots(num_plots, 1, figsize=fig_size)
            axes_list = axes if num_plots > 1 else [axes]

        axis_mapping = {
            "throughput": axes_list[0],
            "backlog": axes_list[1],
        }
        
        axes_list[0].set_title("Throughput Comparison", fontweight='bold')
        axes_list[0].set_ylabel("Rate (events/s)")
        axes_list[1].set_title("Backlog (Lag)", fontweight='bold')
        axes_list[1].set_ylabel("Messages")

        if is_fixed:
            axis_mapping["latency"] = axes_list[2]
            axes_list[2].set_title("Transaction Latency", fontweight='bold')
            axes_list[2].set_ylabel("Latency (ms)")
            
            if layout_type == '2x2':
                axes_list[3].axis('off')
        else:
            axis_mapping["workers"] = axes_list[2]
            axis_mapping["latency"] = axes_list[3]
            axes_list[2].set_title("Worker Count", fontweight='bold')
            axes_list[2].set_ylabel("Workers")
            axes_list[3].set_title("Transaction Latency", fontweight='bold')
            axes_list[3].set_ylabel("Latency (ms)")

        for data in loaded_data:
            ax = axis_mapping[data['plot_type']]
            df = data['df']
            for col in data['value_cols']:
                ax.plot(df['Seconds_From_Start'], df[col], label=data['clean_label'], zorder=3)

        for i, ax in enumerate(axes_list):
            if is_fixed and layout_type == '2x2' and i == 3:
                continue

            for t in scaling_times:
                ax.axvline(x=t, color='gold', linestyle='--', linewidth=plt.rcParams['lines.linewidth']*0.75, alpha=0.9, zorder=1)

            ax.set_xlabel("Time (Seconds from start)")
            ax.yaxis.set_major_formatter(FuncFormatter(human_format))
            ax.xaxis.set_major_locator(MultipleLocator(100))
            ax.set_xlim(left=0, right=shortest_duration) 
            ax.grid(True, linestyle='--', alpha=0.7, zorder=0)
            ax.legend(loc='upper right')

        # The reduced w_pad_val here tightens the spacing
        plt.tight_layout(rect=rect, h_pad=1.5, w_pad=w_pad_val)
        fig.suptitle(plot_title, fontweight='bold', y=title_y, fontsize=title_size)
        
        if not is_fixed and scaling_times:
            scaling_legend_handle = Line2D([0], [0], color='gold', linestyle='--', linewidth=plt.rcParams['lines.linewidth'], label='Scaling Event')
            fig.legend(
                handles=[scaling_legend_handle], loc='upper center', bbox_to_anchor=(0.5, legend_y), 
                ncol=1, frameon=True, edgecolor='black', facecolor='white', framealpha=1.0, 
                fontsize=rc_params['legend.fontsize'], borderpad=0.4, handletextpad=0.5
            )
        
        output_filename = f"{dir_name}_metrics{suffix}.svg"
        plt.savefig(output_filename, format='svg', bbox_inches='tight')
        plt.close(fig)

def process_directory(directory_path):
    dir_name = os.path.basename(os.path.normpath(directory_path))
    dir_lower = dir_name.lower()
    
    fixed_match = re.search(r'(\d+)workers?', dir_lower)
    is_fixed = bool(fixed_match)
    
    if is_fixed:
        num_workers = fixed_match.group(1)
        plot_title = f"Fixed {num_workers} Workers"
    else:
        model_name = "Unknown Model"
        if "chronos" in dir_lower:
            model_name = "Chronos"
        elif "lstm" in dir_lower:
            model_name = "LSTM"
        elif "river" in dir_lower:
            model_name = "River (SNARIMAX)"
        elif "gru" in dir_lower:
            model_name = "GRU"
        plot_title = f"Autoscaling: {model_name}"

    TARGET_FILES_MAP = {
        "Consumption_Rate_0.csv":    {"label": "Input Rate", "plot_type": "throughput"},
        "TPS_committed_0.csv":       {"label": "Output TPS", "plot_type": "throughput"},
        "backlog.csv":               {"label": "Queue Backlog", "plot_type": "backlog"},
        "Transaction_Latency_0.csv": {"label": "Avg Latency", "plot_type": "latency"}
    }
    if not is_fixed:
        TARGET_FILES_MAP["num_workers.csv"] = {"label": "Live Workers", "plot_type": "workers"}

    all_csvs = glob.glob(os.path.join(directory_path, "*.csv"))
    files_to_process = [f for f in all_csvs if os.path.basename(f) in TARGET_FILES_MAP]
    
    if not files_to_process:
        print(f"Skipping {dir_name}: No matching target files found.")
        return

    print(f"--- Processing Directory: {dir_name} ---")

    raw_datasets = []
    shortest_duration = float('inf')
    scaling_times = []  
    metadata_cols = ['__name__', 'instance', 'job']

    for file in files_to_process:
        filename = os.path.basename(file)
        config = TARGET_FILES_MAP[filename]
        
        df = pd.read_csv(file)
        time_col = next((col for col in df.columns if 'time' in col.lower()), df.columns[0])
        df[time_col] = normalize_time(df[time_col])
        df['Seconds_From_Start'] = (df[time_col] - df[time_col].min()).dt.total_seconds()
        df = df.sort_values(by='Seconds_From_Start')
        
        max_time = df['Seconds_From_Start'].max()
        if max_time < shortest_duration:
            shortest_duration = max_time

        value_cols = [c for c in df.columns if c not in [time_col, 'Seconds_From_Start'] + metadata_cols]

        if config["label"] == "Live Workers" and value_cols and not is_fixed:
            val_col = value_cols[0]
            diffs = df[val_col].diff().fillna(0)
            changes = df[diffs != 0]
            scaling_times.extend(changes['Seconds_From_Start'].tolist())

        raw_datasets.append({
            'df': df,
            'plot_type': config["plot_type"],
            'clean_label': config["label"], 
            'value_cols': value_cols
        })

    scaling_times = [t for t in scaling_times if t <= shortest_duration]
    loaded_data = []
    for data in raw_datasets:
        df_truncated = data['df'][data['df']['Seconds_From_Start'] <= shortest_duration]
        loaded_data.append({
            'df': df_truncated,
            'plot_type': data['plot_type'],
            'clean_label': data['clean_label'],
            'value_cols': data['value_cols']
        })

    # --- SUMMARY STATISTICS CALCULATIONS ---
    print(f"\nSummary Statistics for {dir_name}:")
    for data in loaded_data:
        df = data['df']
        label = data['clean_label']
        if not data['value_cols']:
            continue
        
        val_col = data['value_cols'][0]
        avg_val = df[val_col].mean()
        max_val = df[val_col].max()

        if label == "Avg Latency":
            print(f"  - Average Latency: {avg_val:.2f} ms (Max: {max_val:.2f} ms)")
        elif label == "Queue Backlog":
            total_backlog = df[val_col].sum()
            print(f"  - Total Backlog: {total_backlog:.0f} messages (Avg: {avg_val:.2f}, Max: {max_val:.0f})")
        elif label == "Live Workers":
            print(f"  - Average Workers: {avg_val:.2f} (Max: {max_val:.0f})")
        elif label == "Output TPS":
            print(f"  - Average Output TPS: {avg_val:.2f} events/s (Max: {max_val:.2f})")
        elif label == "Input Rate":
            print(f"  - Average Input Rate: {avg_val:.2f} events/s (Max: {max_val:.2f})")
    print("-" * 50)
    # ----------------------------------------

    layouts = ['vertical', '2x2', 'horizontal']
    for layout in layouts:
        render_layout(layout, dir_name, plot_title, is_fixed, loaded_data, scaling_times, shortest_duration)
        
    print(f"Generated all 3 SVG layouts for {dir_name}\n")


def batch_process_experiments(base_directory):
    subdirectories = [f.path for f in os.scandir(base_directory) if f.is_dir()]
    if not subdirectories:
        print(f"No subdirectories found in {base_directory}")
        return

    print(f"Found {len(subdirectories)} directories to scan.\n")

    for directory in subdirectories:
        process_directory(directory)

if __name__ == "__main__":
    BASE_TARGET_DIR = "./grafana_data" 
    batch_process_experiments(BASE_TARGET_DIR)