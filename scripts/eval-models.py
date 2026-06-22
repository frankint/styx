import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
import time
import json
import hashlib
import itertools
from tqdm import tqdm

try:
    from chronos import Chronos2Pipeline
except ImportError:
    Chronos2Pipeline = None

try:
    from river import time_series
except ImportError:
    time_series = None

# --- Configuration ---
DATA_FILE = "./data/cluster_summary_1min.csv" 
RESULTS_FILE = "experiment_results.json"
RAW_PREDICTIONS_DIR = "./offline-raw_predictions/"

MAX_DATAPOINTS = None
HORIZON = 10  

# Reinstated full parameter grid
PARAM_GRID = {
    'run_id': [0], 
    'model': ['chronos', 'lstm', 'gru', 'river', "baseline", "no-forecasting"],
    'context_length': [10, 50, 100, 300, 600, 1000], 
    'downsample_rate': [1, 2, 5], 
    'hidden_size': [16, 32],
    'num_layers': [1, 2],
    'learning_rate': [0.01],
    'river_p': [1, 5, 10], 
    'river_d': [0, 1, 2], 
    'river_q': [0, 1, 2], 
    'river_m': [30] 
}

os.makedirs(RAW_PREDICTIONS_DIR, exist_ok=True)

# --- Data Extraction & Preprocessing ---
def load_and_preprocess_alibaba_data():
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Cannot find {DATA_FILE}. Please ensure the file exists.")
        
    print(f"Loading pre-aggregated data from {DATA_FILE}...")
    aggregated_df = pd.read_csv(DATA_FILE)
    aggregated_df.rename(columns={'total_throughput': 'throughput_tps'}, inplace=True)
    
    if MAX_DATAPOINTS is not None:
        print(f"[INFO] Truncating dataset to {MAX_DATAPOINTS} datapoints for testing.")
        aggregated_df = aggregated_df.head(MAX_DATAPOINTS)
        
    aggregated_df['throughput_tps'] = aggregated_df['throughput_tps'].astype(float)
    return aggregated_df

# --- Model Definitions ---
class TimeSeriesRNN(nn.Module):
    def __init__(self, rnn_type='LSTM', input_size=1, hidden_size=32, num_layers=1):
        super().__init__()
        self.rnn = (nn.LSTM if rnn_type == 'LSTM' else nn.GRU)(
            input_size, hidden_size, num_layers, batch_first=True
        )
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.rnn(x)
        return self.fc(out[:, -1, :])

def forecast_rnn(model, optimizer, criterion, history, horizon):
    if len(history) < 10: 
        return [history[-1]] * horizon
        
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(np.array(history).reshape(-1, 1))
    
    seq_length = min(len(data_scaled) - 1, 10)
    X = torch.tensor(np.array([data_scaled[i:i+seq_length] for i in range(len(data_scaled)-seq_length)]), dtype=torch.float32)
    y = torch.tensor(data_scaled[seq_length:], dtype=torch.float32)

    model.train()
    for _ in range(2): 
        optimizer.zero_grad()
        loss = criterion(model(X), y)
        loss.backward()
        optimizer.step()

    model.eval()
    preds_scaled = []
    current_seq = torch.tensor(data_scaled[-seq_length:].reshape(1, seq_length, 1), dtype=torch.float32)
    
    with torch.no_grad():
        for _ in range(horizon):
            pred = model(current_seq)
            preds_scaled.append(pred.item())
            current_seq = torch.cat((current_seq[:, 1:, :], pred.unsqueeze(1)), dim=1)
    
    preds = scaler.inverse_transform(np.array(preds_scaled).reshape(-1, 1)).flatten()
    return np.clip(preds, 0, max(history) * 1.5).tolist()

def forecast_chronos(pipeline, history_vals, horizon, downsample_rate=1):
    if len(history_vals) < 5 * downsample_rate:
        fallback = [history_vals[-1]] * horizon if history_vals else [0] * horizon
        return fallback, fallback, fallback
        
    trim_offset = len(history_vals) % downsample_rate
    trimmed_history = history_vals[trim_offset:]
    
    downsampled_history = [
        np.mean(trimmed_history[i:i+downsample_rate]) 
        for i in range(0, len(trimmed_history), downsample_rate)
    ]
    downsampled_horizon = max(1, (horizon + downsample_rate - 1) // downsample_rate)
    
    base_time = pd.Timestamp("2024-01-01 00:00:00")
    timestamps = [base_time + pd.Timedelta(minutes=i) for i in range(len(downsampled_history))]
    
    context_df = pd.DataFrame({
        "id": "stream", "timestamp": timestamps, "target": downsampled_history,
    })
    
    future_timestamps = [timestamps[-1] + pd.Timedelta(minutes=i+1) for i in range(downsampled_horizon)]
    future_df = pd.DataFrame({"id": "stream", "timestamp": future_timestamps})
    
    pred_df = pipeline.predict_df(
        context_df, future_df=future_df, prediction_length=downsampled_horizon,
        quantile_levels=[0.1, 0.5, 0.9], id_column="id", timestamp_column="timestamp", target="target"
    )
    
    raw_preds = pred_df["0.5"].values.tolist()
    raw_low = pred_df["0.1"].values.tolist()
    raw_high = pred_df["0.9"].values.tolist()
    
    expanded_preds, expanded_low, expanded_high = [], [], []
    for p, l, h in zip(raw_preds, raw_low, raw_high):
        expanded_preds.extend([p] * downsample_rate)
        expanded_low.extend([l] * downsample_rate)
        expanded_high.extend([h] * downsample_rate)
        
    return expanded_preds[:horizon], expanded_low[:horizon], expanded_high[:horizon]

# --- State Management ---
def load_checkpoint():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'r') as f:
            return json.load(f)
    return []

def save_checkpoint(results):
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=4)

def get_param_hash(params):
    param_str = json.dumps(params, sort_keys=True)
    return hashlib.md5(param_str.encode()).hexdigest()

# --- Simulation Logic ---
def run_simulation(df, params):
    model_type = params['model']
    context_length = params['context_length']
    downsample_rate = params['downsample_rate']
    
    split_idx = int(len(df) * 0.66)
    
    predictions = np.full(len(df), np.nan)
    predictions_lower = np.full(len(df), np.nan)
    predictions_upper = np.full(len(df), np.nan)
    
    history = []
    river_buffer = []
    inference_times = []
    full_context_inference_times = []
    times_by_context = {}  
    
    active_model, rnn_optimizer, rnn_criterion = None, None, None
    
    if model_type == 'river' and time_series:
        active_model = time_series.SNARIMAX(
            p=params['river_p'], d=params['river_d'], 
            q=params['river_q'], m=params['river_m']
        )
    elif model_type == 'chronos' and Chronos2Pipeline:
        active_model = Chronos2Pipeline.from_pretrained("amazon/chronos-2", device_map="cpu")
    elif model_type in ['lstm', 'gru']:
        torch.set_num_threads(1) # Kept thread optimization specifically for RNN models
        active_model = TimeSeriesRNN(
            rnn_type=model_type.upper(),
            hidden_size=params['hidden_size'],
            num_layers=params['num_layers']
        )
        rnn_optimizer = torch.optim.Adam(active_model.parameters(), lr=params['learning_rate'])
        rnn_criterion = nn.MSELoss()

    start_total_time = time.time()
    
    for i in tqdm(range(len(df)), desc=f"Simulating {model_type.upper()}", leave=False):
        val = df.loc[i, 'throughput_tps']
        history.append(val)
        
        if len(history) > context_length: 
            history.pop(0)

        step_start = time.time()
        
        forecasts = [val] * HORIZON
        forecasts_low = [val] * HORIZON
        forecasts_high = [val] * HORIZON
        
        if model_type == 'chronos' and active_model:
            if len(history) >= 10:
                forecasts, forecasts_low, forecasts_high = forecast_chronos(active_model, history, HORIZON, downsample_rate)
        else:
            if model_type == 'river' and active_model:
                river_buffer.append(val)
                if len(river_buffer) == downsample_rate:
                    active_model.learn_one(np.mean(river_buffer))
                    river_buffer = []
                if i > 20:
                    raw_forecasts = active_model.forecast(horizon=max(1, HORIZON // downsample_rate))
                    forecasts = [f for f in raw_forecasts for _ in range(downsample_rate)][:HORIZON]
                    
            elif model_type in ['lstm', 'gru']:
                forecasts = forecast_rnn(active_model, rnn_optimizer, rnn_criterion, history, HORIZON)
                
            elif model_type == 'baseline':
                if len(history) >= 2:
                    forecasts = [np.mean(history[-min(10, len(history)):])] * HORIZON

            elif model_type == 'no-forecasting':
                if len(history) >= 1:
                    forecasts = [history[-1]] * HORIZON
            
            forecasts_low = forecasts
            forecasts_high = forecasts

        step_time = time.time() - step_start

        target_idx = i + HORIZON
        if target_idx < len(df):
            predictions[target_idx] = forecasts[-1]
            predictions_lower[target_idx] = forecasts_low[-1]
            predictions_upper[target_idx] = forecasts_high[-1]

    total_time = time.time() - start_total_time
    avg_pred_time = float(np.mean(inference_times)) if inference_times else 0.0
    
    avg_time_by_context = {ctx: float(np.mean(times)) for ctx, times in times_by_context.items()}
    
    if full_context_inference_times:
        avg_full_context_time = float(np.mean(full_context_inference_times))
    elif inference_times:
        avg_full_context_time = float(np.mean(inference_times[-50:]))
    else:
        avg_full_context_time = 0.0

    eval_df = df.copy()
    eval_df['predicted'] = predictions
    test_eval_df = eval_df.iloc[split_idx:].dropna()
    
    if test_eval_df.empty:
        return {
            'total_time_sec': total_time, 'avg_time_per_prediction_sec': avg_pred_time,
            'avg_full_context_time_sec': avg_full_context_time,
            'avg_time_by_context_size': avg_time_by_context,  
            'mae': float('nan'), 'directional_accuracy': float('nan'),
            'predictions': predictions.tolist(),
            'predictions_lower': predictions_lower.tolist(),
            'predictions_upper': predictions_upper.tolist()
        }

    mae = float((test_eval_df['predicted'] - test_eval_df['throughput_tps']).abs().mean())
    actual_diff = test_eval_df['throughput_tps'].diff()
    pred_diff = test_eval_df['predicted'] - test_eval_df['throughput_tps'].shift(1)
    dir_acc = float((np.sign(actual_diff) == np.sign(pred_diff)).mean())

    return {
        'total_time_sec': total_time,
        'avg_time_per_prediction_sec': avg_pred_time,
        'avg_full_context_time_sec': avg_full_context_time,
        'avg_time_by_context_size': avg_time_by_context, 
        'mae': mae, 'directional_accuracy': dir_acc,
        'predictions': predictions.tolist(),
        'predictions_lower': predictions_lower.tolist(),
        'predictions_upper': predictions_upper.tolist()
    }

def main():
    try:
        df = load_and_preprocess_alibaba_data()
    except (FileNotFoundError, ValueError) as e:
        print(f"\n[!] Error during initialization: {e}")
        return

    keys, values = zip(*PARAM_GRID.items())
    experiments = [dict(zip(keys, v)) for v in itertools.product(*values)]
    
    cleaned_experiments = []
    seen = set()
    for exp in experiments:
        # Nullify irrelevant parameters to prevent duplicate identical runs
        if exp['model'] not in ['lstm', 'gru']:
            exp['hidden_size'] = None
            exp['num_layers'] = None
            exp['learning_rate'] = None
            
        if exp['model'] != 'river':
            exp['river_p'] = None
            exp['river_d'] = None
            exp['river_q'] = None
            exp['river_m'] = None
            
        phash = get_param_hash(exp)
        if phash not in seen:
            seen.add(phash)
            cleaned_experiments.append(exp)

    results = load_checkpoint()
    completed_hashes = {res['hash'] for res in results}

    print(f"\nStarting Grid Search: {len(cleaned_experiments)} total combinations.")
    print(f"Resuming from checkpoint: {len(completed_hashes)} already completed.\n")

    try:
        for params in tqdm(cleaned_experiments, desc="Total Progress"):
            phash = get_param_hash(params)
            if phash in completed_hashes:
                print(f"\n[INFO] Skipping {params['model'].upper()} - Already completed in {RESULTS_FILE}.")
                continue

            metrics = run_simulation(df, params)
            
            raw_filename = f"{RAW_PREDICTIONS_DIR}{params['model']}_{phash}.npy"
            
            stacked_preds = np.column_stack((
                metrics['predictions'],
                metrics['predictions_lower'],
                metrics['predictions_upper']
            ))
            np.save(raw_filename, stacked_preds)
            
            del metrics['predictions'] 
            del metrics['predictions_lower'] 
            del metrics['predictions_upper'] 

            record = {'hash': phash, 'params': params, 'metrics': metrics, 'raw_file': raw_filename}
            results.append(record)
            completed_hashes.add(phash)
            save_checkpoint(results)

    except KeyboardInterrupt:
        print("\n[!] Execution interrupted by user. State saved successfully.")
        return

    print("\n--- All Experiments Complete ---")
    if results:
        summary_df = pd.json_normalize(results)
        
        group_cols = [col for col in summary_df.columns if col.startswith('params.') and col != 'params.run_id']
        
        avg_summary_df = summary_df.groupby(group_cols).agg({
            'metrics.mae': 'mean',
            'metrics.directional_accuracy': 'mean',
            'metrics.avg_time_per_prediction_sec': 'mean',
            'metrics.avg_full_context_time_sec': 'mean'
        }).reset_index()

        avg_summary_df = avg_summary_df.sort_values(by='metrics.mae').reset_index(drop=True)
        
        print("\nTop 5 Models by Average MAE:")
        print(avg_summary_df[['params.model', 'params.context_length', 'metrics.mae', 'metrics.avg_time_per_prediction_sec']].head(5).to_string(index=False))

if __name__ == "__main__":
    main()