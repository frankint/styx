import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import json
import os

# --- Configuration ---
DATA_FILE = "./data/cluster_summary_1min.csv"
RESULTS_FILE = "experiment_results.json"
PLOTS_DIR = "./analysis_plots/"
SHOW_INTERACTIVE_POPUPS = False 

os.makedirs(PLOTS_DIR, exist_ok=True)

def load_ground_truth():
    print(f"Loading ground truth from {DATA_FILE}...")
    aggregated_df = pd.read_csv(DATA_FILE)
    aggregated_df.rename(columns={'total_throughput': 'actual_tps'}, inplace=True)
    return aggregated_df

def load_results():
    if not os.path.exists(RESULTS_FILE):
        raise FileNotFoundError(f"Cannot find {RESULTS_FILE}. Run the simulation first.")
        
    with open(RESULTS_FILE, 'r') as f:
        data = json.load(f)
        
    if not data:
        raise ValueError("Results file is empty.")
        
    rows = []
    for d in data:
        p = d['params']
        m = d['metrics']
        row = {
            'model': p['model'],
            'context_length': p['context_length'],
            'downsample_rate': p['downsample_rate'],
            'hidden_size': p.get('hidden_size'),
            'num_layers': p.get('num_layers'),
            'river_p': p.get('river_p'),
            'river_d': p.get('river_d'),
            'river_q': p.get('river_q'),
            'river_m': p.get('river_m'),
            'mae': float(m.get('mae', np.nan)),
            'directional_accuracy': float(m.get('directional_accuracy', np.nan)),
            'latency_sec': float(m.get('avg_time_per_prediction_sec', np.nan)),
            'avg_time_by_context_size': m.get('avg_time_by_context_size', {}),
            'raw_file': d.get('raw_file') # Keep one raw file reference for overlays
        }
        rows.append(row)
        
    df = pd.DataFrame(rows)
    hyperparams = ['model', 'context_length', 'downsample_rate', 'hidden_size', 'num_layers', 'river_p', 'river_d', 'river_q', 'river_m']
    df[hyperparams] = df[hyperparams].fillna('N/A')
    
    # Helper to average dictionaries containing the context timing arrays
    def agg_dicts(dicts):
        res, counts = {}, {}
        for d in dicts:
            if not isinstance(d, dict): continue
            for k, v in d.items():
                key = int(k)
                res[key] = res.get(key, 0) + float(v)
                counts[key] = counts.get(key, 0) + 1
        return {k: res[k]/counts[k] for k in res}

    # Group by hyperparameters (implicitly merging the 10 run_ids)
    grouped_df = df.groupby(hyperparams).agg({
        'mae': 'mean',
        'directional_accuracy': 'mean',
        'latency_sec': 'mean',
        'avg_time_by_context_size': agg_dicts,
        'raw_file': 'first'
    }).reset_index()
    
    return grouped_df

def plot_global_comparison(results_df, truth_df):
    valid_mae = results_df.dropna(subset=['mae']).copy()
    if valid_mae.empty:
        return

    # --- NEW: Calculate Actual Error Percentage (Normalized MAE) ---
    # We find the average TPS of the actual dataset to serve as our 100% baseline
    mean_actual_tps = truth_df['actual_tps'].mean()
    valid_mae['error_percentage'] = (valid_mae['mae'] / mean_actual_tps) * 100
        
    # --- UPDATED: Rank by error percentage (Lowest is best) ---
    best_by_err = valid_mae.loc[valid_mae.groupby('model')['error_percentage'].idxmin()].sort_values('error_percentage', ascending=True)
        
    # Font and Style Config for Two-Column Paper Consistency
    rc_params = {
        'font.size': 16,
        'axes.titlesize': 18,
        'axes.labelsize': 16,
        'xtick.labelsize': 14,
        'ytick.labelsize': 14,
        'legend.fontsize': 14
    }

    with plt.rc_context(rc_params):
        fig, ax1 = plt.subplots(figsize=(10, 6))
        width = 0.35

        models_err = best_by_err['model'].str.upper().tolist()
        errs = best_by_err['error_percentage'].tolist()
        lats = best_by_err['latency_sec'].tolist()
        x1 = np.arange(len(models_err))

        # --- PLOT METRICS ---
        # Changed color to Vermillion (Red) to signify an error metric
        ax1.bar(x1 - width/2, errs, width, label='Relative Error (%)', color='tab:red', alpha=0.85)
        ax1.set_ylabel('Mean Relative Error (%)', color='tab:red', fontweight='bold')
        ax1.tick_params(axis='y', labelcolor='tab:red')
        ax1.set_xticks(x1)
        ax1.set_xticklabels(models_err, fontweight='bold', rotation=10, ha='right', rotation_mode='anchor')

        ax1_lat = ax1.twinx()
        ax1_lat.bar(x1 + width/2, lats, width, label='Latency', color='#0173B2', alpha=0.85)
        ax1_lat.set_ylabel('Latency (s)', color='#0173B2', fontweight='bold')
        ax1_lat.tick_params(axis='y', labelcolor='#0173B2')
        
        plt.title('Best Configuration per Model (Ranked by Error %)', fontweight='bold')

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax1_lat.get_legend_handles_labels()
        
        # Attach to the top layer (ax1_lat) and force rendering priority (zorder)
        leg = ax1_lat.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        leg.set_zorder(100)

        fig.tight_layout()
        plot_path = os.path.join(PLOTS_DIR, '00_global_best_comparison.png')
        plt.savefig(plot_path, dpi=300)
        
        if SHOW_INTERACTIVE_POPUPS: plt.show() 
        plt.close()

def plot_per_model_comparisons(results_df):
    unique_models = results_df['model'].unique()
    
    for model_name in unique_models:
        model_data = results_df[results_df['model'] == model_name].copy()
        
        # --- NEW: Handle River ---
        # River isn't affected by context length, so drop duplicates with identical River parameters
        if model_name.lower() == 'river':
            model_data = model_data.drop_duplicates(subset=['river_p', 'river_d', 'river_q', 'river_m'])
            
        model_data = model_data.sort_values('mae').head(10)
        
        if len(model_data) <= 1:
            continue
            
        labels = []
        for _, row in model_data.iterrows():
            lbl = ""
            
            # --- NEW: Omit Context Length for River ---
            if model_name.lower() != 'river':
                lbl += f"Ctx:{row['context_length']} "
                
            if row['downsample_rate'] != 1 and row['downsample_rate'] != 'N/A': 
                lbl += f"DS:{row['downsample_rate']}"
                
            if row['hidden_size'] != 'N/A': 
                lbl += f"\nHL:{row['hidden_size']} L:{row['num_layers']}"
                
            if row['river_p'] != 'N/A': 
                lbl += f"\np:{row['river_p']} d:{row['river_d']} q:{row['river_q']} m:{row['river_m']}"
                
            labels.append(lbl.strip())
            
        maes = model_data['mae'].tolist()
        latencies = model_data['latency_sec'].tolist()

        fig, ax1 = plt.subplots(figsize=(max(10, len(labels) * 1.0), 6))
        x = np.arange(len(labels))
        width = 0.35

        ax1.bar(x - width/2, maes, width, color='tab:red', alpha=0.7)
        ax1.set_ylabel('Mean Absolute Error (TPS)', color='tab:red', fontweight='bold')
        ax1.tick_params(axis='y', labelcolor='tab:red')
        ax1.set_xticks(x)
        ax1.set_xticklabels(labels, rotation=45, ha='right', fontsize=9)
        
        ax1_lat = ax1.twinx()
        ax1_lat.bar(x + width/2, latencies, width, color='tab:blue', alpha=0.7)
        ax1_lat.set_ylabel('Latency (s)', color='tab:blue')
        
        plt.title(f'Intra-Model Tuning: {model_name.upper()} Parameters (Top 10)', fontsize=16, fontweight='bold')

        fig.tight_layout()
        
        plot_path = os.path.join(PLOTS_DIR, f'01_tuning_comparison_{model_name.lower()}.png')
        plt.savefig(plot_path, dpi=300)
        
        if SHOW_INTERACTIVE_POPUPS: plt.show() 
        plt.close()

def plot_context_time_evolution(results_df):
    valid_data = results_df.dropna(subset=['mae']).copy()
    if valid_data.empty:
        return
        
    valid_data['context_num'] = pd.to_numeric(valid_data['context_length'], errors='coerce').fillna(0)
    sorted_data = valid_data.sort_values(by=['context_num', 'mae'], ascending=[False, True])
    longest_ctx_by_model = sorted_data.drop_duplicates(subset=['model'])

    # --- UPDATED: Font and Style Config for Two-Column Paper ---
    rc_params = {
        'font.size': 16,           # Increased base font size
        'axes.titlesize': 18,      # Increased title size
        'axes.labelsize': 16,      # Increased axis label size
        'xtick.labelsize': 14,
        'ytick.labelsize': 14,
        'legend.fontsize': 14,
        'lines.linewidth': 3.5     # Thickened lines for scaled-down visibility
    }

    # Colorblind-friendly hex palette
    cb_colors = {
        'CHRONOS': '#0173B2', # Blue
        'RIVER': '#DE8F05',   # Orange
        'GRU': '#029E73',     # Green
        'LSTM': 'tab:red',    # Vermillion/Red
        'BASELINE': '#CC78BC' # Purple
    }

    # Apply formatting context
    with plt.rc_context(rc_params):
        plt.figure(figsize=(12, 6))

        for i, (_, row) in enumerate(longest_ctx_by_model.iterrows()):
            model = row['model'].upper()
            time_dict = row['avg_time_by_context_size']
            if not time_dict: 
                continue

            # Filter out the warm-up spike (first 20 steps)
            sorted_items = sorted([(int(k), float(v)) for k, v in time_dict.items() if int(k) > 20])
            if not sorted_items: 
                continue

            x = np.array([item[0] for item in sorted_items])
            y = np.array([item[1] for item in sorted_items])

            # Get color from palette, fallback to gray
            line_color = cb_colors.get(model, '#7f7f7f')

            # --- PLOT RAW DATA ---
            plt.plot(x, y, 
                     color=line_color,
                     linestyle='-', 
                     alpha=0.4, # Lowered alpha so the raw data sits in the background
                     label=f"{model} Raw (Max Ctx: {row['context_length']})")

            # --- CALCULATE AND PLOT TRENDLINE ---
            # Fit a 1st-degree polynomial (linear trendline: y = mx + c)
            coefficients = np.polyfit(x, y, 1)
            trend_function = np.poly1d(coefficients)
            y_trend = trend_function(x)

            # Plot the trendline
            plt.plot(x, y_trend, 
                     color=line_color,
                     linestyle='--', # Dashed line to indicate it's a trend
                     linewidth=3.5,  # Matches the rc_params thickness
                     alpha=1.0,      # Full opacity for visibility
                     label=f"{model} Trend")

        plt.title('Average Inference Time vs. Current Context Size', fontweight='bold')
        plt.xlabel('Current Context Size (Datapoints)')
        plt.ylabel('Inference Time per Step (Seconds)')
        
        # Reverted to default linear scale
        plt.yscale('linear')

        # Adding legend and standard grid
        plt.legend()
        plt.grid(True, linestyle='-', alpha=0.3) 
        plt.tight_layout()
        
        # Make sure PLOTS_DIR and SHOW_INTERACTIVE_POPUPS are accessible in your scope
        plot_path = os.path.join(PLOTS_DIR, '02_context_time_evolution.png')
        plt.savefig(plot_path, dpi=300)
        
        if SHOW_INTERACTIVE_POPUPS: plt.show()
        plt.close()

def plot_time_series_overlays(truth_df, results_df):
    split_idx = int(len(truth_df) * 0.66)
    time_axis = np.arange(len(truth_df)) 
    
    # --- NEW: Calculate the mean of actual throughput to compute Relative Error ---
    mean_actual_tps = truth_df['actual_tps'].mean()
    
    top_10_per_model = results_df.sort_values('mae').groupby('model').head(10)
    unique_models = top_10_per_model['model'].unique()

    def safe_fmt(val, fmt_str, suffix=""):
        if pd.isna(val) or val == 'N/A':
            return "N/A"
        try:
            return f"{float(val):{fmt_str}}{suffix}"
        except (ValueError, TypeError):
            return str(val)

    # --- MASSIVE FONT SCALING FOR PAPER PRESENTATION ---
    rc_params = {
        'font.size': 24,           # Base font size pushed to 24
        'axes.titlesize': 30,      # Massive titles
        'axes.labelsize': 26,      # Axis labels pushed to 26
        'xtick.labelsize': 22,     # Tick marks pushed to 22
        'ytick.labelsize': 22,
        'legend.fontsize': 20      # Legend pushed to 20
    }

    with plt.rc_context(rc_params):
        for model_name in unique_models:
            model_subset = top_10_per_model[top_10_per_model['model'] == model_name]
            
            for rank, (_, row) in enumerate(model_subset.iterrows(), start=1):
                raw_file = row['raw_file']
                model_display_name = model_name.upper()
                
                if not isinstance(raw_file, str) or not os.path.exists(raw_file): 
                    continue
                    
                predictions_data = np.load(raw_file)
                
                # Check dimensionality to handle confidence intervals safely
                if predictions_data.ndim == 2 and predictions_data.shape[1] == 3:
                    predictions = predictions_data[:, 0]
                    preds_low = predictions_data[:, 1]
                    preds_high = predictions_data[:, 2]
                    has_ci = True
                else:
                    predictions = predictions_data
                    has_ci = False
                    
                # Maintaining the larger 18x8 canvas
                plt.figure(figsize=(18, 8))
                
                # Increased line width to 3.0 to match the heavier text
                plt.plot(time_axis, truth_df['actual_tps'], label='Actual Throughput', color='black', alpha=0.4, linewidth=3.0)
                
                # Plot Confidence Interval
                if has_ci and model_name == 'chronos':
                    plt.fill_between(time_axis, preds_low, preds_high, color='tab:red', alpha=0.2, label='80% Confidence Interval')
                    
                # Increased prediction line width to 4.0
                plt.plot(time_axis, predictions, label=f'{model_display_name} Predicted', color='tab:red', linestyle='--', linewidth=4.0)
                
                # Increased split marker width
                plt.axvline(x=split_idx, color='blue', linestyle=':', label='Test Split Start', linewidth=3.0)
                plt.axvspan(split_idx, len(truth_df), color='blue', alpha=0.05)
                
                param_text = f"Context: {row['context_length']}\n"
                if row['downsample_rate'] != 'N/A' and row['downsample_rate'] != 1: param_text += f"Downsample: {row['downsample_rate']}\n"
                if row['hidden_size'] != 'N/A': param_text += f"Hidden Layers: {row['num_layers']} (Size: {row['hidden_size']})\n"
                if row['river_p'] != 'N/A': param_text += f"River: ARIMA({row['river_p']},{row['river_d']},{row['river_q']}) m={row['river_m']}\n"
                    
                # --- NEW: Calculate Relative Error percentage for this specific row ---
                rel_error = (row['mae'] / mean_actual_tps) * 100 if mean_actual_tps else np.nan

                # --- UPDATED: Swapped raw MAE for Rel. Error %, Removed Dir. Acc. ---
                param_text += (
                    f"--- ML Metrics ---\n"
                    f"Rel. Error: {safe_fmt(rel_error, '.1f', '%')}\n"
                    f"Latency: {safe_fmt(row['latency_sec'], '.4f', 's')}"
                )
                
                # Text box maintained at size 22
                plt.text(0.02, 0.95, param_text, transform=plt.gca().transAxes, fontsize=22,
                         verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.9), zorder=10)
                
                plt.title(f"{model_display_name} Top 10 | Rank #{rank} Overlay", fontweight='bold')
                plt.xlabel("Time Steps")
                plt.ylabel("Throughput (TPS)")
                plt.legend(loc='upper right')
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                
                plot_path = os.path.join(PLOTS_DIR, f'03_{model_name.lower()}_rank_{rank:02d}_overlay.png')
                plt.savefig(plot_path, dpi=300)
                
                if SHOW_INTERACTIVE_POPUPS: plt.show()
                plt.close()

def main():
    print("Loading Simulation Results...")
    try:
        results_df = load_results()
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return
        
    print("\n" + "="*95)
    print(" TOP 10 HYPERPARAMETER CONFIGURATIONS BY MAE (Averaged over 10 runs)")
    print("="*95)
    
    display_df = results_df.sort_values(by='mae').copy()
    
    display_df['mae'] = pd.to_numeric(display_df['mae'], errors='coerce').round(3)
    display_df['dir_acc'] = (pd.to_numeric(display_df['directional_accuracy'], errors='coerce') * 100).round(1).astype(str) + '%'
    display_df['latency_sec'] = pd.to_numeric(display_df['latency_sec'], errors='coerce').round(4)
    
    cols_to_show = ['model', 'context_length', 'downsample_rate', 'hidden_size', 'river_p', 'mae', 'dir_acc', 'latency_sec']
    print(display_df[cols_to_show].head(10).to_string(index=False))
    print("="*95 + "\n")

    print("Reconstructing Ground Truth Data...")
    try:
        truth_df = load_ground_truth()
    except Exception as e:
        print(f"Error loading truth CSVs: {e}")
        return

    print("Generating Analytical Plots...")
    
    plot_global_comparison(results_df, truth_df)
    plot_per_model_comparisons(results_df)
    plot_context_time_evolution(results_df)
    plot_time_series_overlays(truth_df, results_df)
    
    print("\nAnalysis Complete. Check the './analysis_plots/' directory for your graphs.")

if __name__ == "__main__":
    main()