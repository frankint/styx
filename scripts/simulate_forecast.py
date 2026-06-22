import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from datetime import timedelta
import time

try:
    from chronos import Chronos2Pipeline
except ImportError:
    Chronos2Pipeline = None

# --- Configuration ---
CSV_FILE = "throughput_data_cosine.csv"
SELECTED_MODEL = 'all'  # Options: 'baseline', 'river', 'lstm', 'gru', 'chronos', 'all'
HORIZON = 10             # Seconds to predict into the future
CONTEXT_LENGTH = 600     # Points to keep in history for the models

# CHRONOS TUNING
CHRONOS_DOWNSAMPLE_RATE = 1 # Averages every N seconds to find macro-trends. 1 = No downsampling.
RIVER_DOWNSAMPLE_RATE = 1

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
    if len(history) < 31: 
        return [history[-1]] * horizon
        
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(np.array(history).reshape(-1, 1))
    
    seq_length = 30
    X = torch.tensor([data_scaled[i:i+seq_length] for i in range(len(data_scaled)-seq_length)], dtype=torch.float32)
    y = torch.tensor(data_scaled[seq_length:], dtype=torch.float32)

    # Put model in training mode and do a quick update (1-2 epochs is enough now)
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
    max_history = max(history) * 1.5
    preds = np.clip(preds, 0, max_history)
    return preds.tolist()

def forecast_chronos(pipeline, history_vals, horizon, downsample_rate=1):
    """
    Downsamples historical values to reduce jitter, fakes minute-level intervals 
    so Chronos behaves, and returns the median (0.5) forecast array.
    """
    if len(history_vals) < 5 * downsample_rate:
        return [history_vals[-1]] * horizon if history_vals else [0] * horizon
        
    # --- DOWNSAMPLING LOGIC ---
    trim_offset = len(history_vals) % downsample_rate
    trimmed_history = history_vals[trim_offset:]
    
    downsampled_history = [
        np.mean(trimmed_history[i:i+downsample_rate]) 
        for i in range(0, len(trimmed_history), downsample_rate)
    ]
    
    downsampled_horizon = (horizon + downsample_rate - 1) // downsample_rate
    
    base_time = pd.Timestamp("2024-01-01 00:00:00")
    timestamps = [base_time + pd.Timedelta(minutes=i) for i in range(len(downsampled_history))]
    
    context_df = pd.DataFrame({
        "id": "stream",
        "timestamp": timestamps,
        "target": downsampled_history,
        "hour_of_day": [ts.hour for ts in timestamps],
        "day_of_week": [ts.dayofweek for ts in timestamps],
        "is_business_hours": [1 if (9 <= ts.hour < 17 and ts.dayofweek < 5) else 0 for ts in timestamps],
    })
    
    last_timestamp = timestamps[-1]
    future_timestamps = [last_timestamp + pd.Timedelta(minutes=i+1) for i in range(downsampled_horizon)]
    
    future_df = pd.DataFrame({
        "id": "stream",
        "timestamp": future_timestamps,
        "hour_of_day": [ts.hour for ts in future_timestamps],
        "day_of_week": [ts.dayofweek for ts in future_timestamps],
        "is_business_hours": [1 if (9 <= ts.hour < 17 and ts.dayofweek < 5) else 0 for ts in future_timestamps],
    })
    
    pred_df = pipeline.predict_df(
        context_df, future_df=future_df, prediction_length=downsampled_horizon,
        quantile_levels=[0.5], id_column="id", timestamp_column="timestamp", target="target"
    )
    
    raw_preds = pred_df["0.5"].values.tolist()
    
    # --- UPSAMPLING LOGIC ---
    expanded_preds = []
    for p in raw_preds:
        expanded_preds.extend([p] * downsample_rate)
        
    return expanded_preds[:horizon]

# --- Simulation Logic ---

def main():
    if not os.path.exists(CSV_FILE):
        print(f"Error: {CSV_FILE} not found.")
        return

    original_df = pd.read_csv(CSV_FILE)
    original_df['timestamp'] = pd.to_datetime(original_df['timestamp'])
    
    # Determine which models to run
    if SELECTED_MODEL.lower() == 'all':
        models_to_run = ['baseline', 'river', 'lstm', 'gru', 'chronos']
    else:
        models_to_run = [SELECTED_MODEL.lower()]

    for current_model in models_to_run:
        print(f"\n==================================================")
        print(f" Starting Simulation for: {current_model.upper()}")
        print(f"==================================================")
        
        df = original_df.copy()
        df['predicted_for_now'] = np.nan
        history = []
        river_buffer = []
        
        # Initialize Models
        active_model = None
        rnn_optimizer = None
        rnn_criterion = None
        
        if current_model == 'river':
            from river import time_series
            active_model = time_series.SNARIMAX(p=5, d=0, q=1, m=30)
        elif current_model == 'chronos':
            if Chronos2Pipeline is None:
                print("Error: Chronos not installed. Skipping...")
                continue
            print("Loading Chronos model...")
            active_model = Chronos2Pipeline.from_pretrained("amazon/chronos-2", device_map="cpu")
        elif current_model in ['lstm', 'gru']:
            active_model = TimeSeriesRNN(rnn_type=current_model.upper())
            rnn_optimizer = torch.optim.Adam(active_model.parameters(), lr=0.01)
            rnn_criterion = nn.MSELoss()

        print(f"Simulating on {len(df)} points...")

        # --- NEW: Start timer for execution cost ---
        start_time = time.time()

        for i in range(len(df)):
            val = df.loc[i, 'throughput_tps']
            history.append(val)
            
            if len(history) > CONTEXT_LENGTH: 
                history.pop(0)

            # Generate Forecast
            forecasts = [val] * HORIZON
            
            if current_model == 'river':
                # Collect raw samples
                river_buffer.append(val)
                # Only train every N points
                if len(river_buffer) == RIVER_DOWNSAMPLE_RATE:
                    # Aggregate window
                    aggregated_val = np.mean(river_buffer)
                    # Train River on downsampled signal
                    active_model.learn_one(aggregated_val)
                    # Clear buffer
                    river_buffer = []
                # Forecast only after enough aggregated history exists
                if i > 50:
                    downsampled_horizon = max(1,(HORIZON + RIVER_DOWNSAMPLE_RATE - 1)// RIVER_DOWNSAMPLE_RATE)
                    raw_forecasts = active_model.forecast(horizon=downsampled_horizon)
                    # Expand back to second-level resolution
                    forecasts = []
                    for f in raw_forecasts:
                        forecasts.extend([f] * RIVER_DOWNSAMPLE_RATE)
                    forecasts = forecasts[:HORIZON]
                    # Safety clipping
                    max_safe_val = max(history) * 2
                    forecasts = [
                        max(0, min(f, max_safe_val))
                        for f in forecasts
                    ]
                
            elif current_model in ['lstm', 'gru']:
                forecasts = forecast_rnn(active_model, rnn_optimizer, rnn_criterion, history, HORIZON)
                
            elif current_model == 'baseline':
                if len(history) >= 10:
                    forecasts = [np.mean(history[-10:])] * HORIZON
                    
            elif current_model == 'chronos':
                if len(history) >= 10:
                    forecasts = forecast_chronos(active_model, history, HORIZON, CHRONOS_DOWNSAMPLE_RATE)

            target_idx = i + HORIZON
            if target_idx < len(df):
                df.loc[target_idx, 'predicted_for_now'] = forecasts[-1]
                
            if i % 5 == 0:
                print(f"Processed {i}/{len(df)} points...", end="\r")

        # --- NEW: End timer ---
        execution_time = time.time() - start_time
        print("\nSimulation complete.")

        # --- Analytics ---
        eval_df = df.dropna(subset=['predicted_for_now']).copy()
        eval_df['error'] = eval_df['predicted_for_now'] - eval_df['throughput_tps']
        
        mae = eval_df['error'].abs().mean()
        rmse = np.sqrt((eval_df['error']**2).mean())
        
        print("\n--- Analytics ---")
        print(f"Model: {current_model.upper()}")
        print(f"MAE:   {mae:.2f} TPS")
        print(f"RMSE:  {rmse:.2f} TPS")
        print(f"Time:  {execution_time:.2f} seconds")

        # --- Plotting & Saving (POSTER OPTIMIZED) ---
        # Increased base size slightly for higher resolution scaling
        plt.figure(figsize=(12, 6))
        
        # Thicker lines (linewidth=4) for better distance readability
        plt.plot(df['timestamp'], df['throughput_tps'], label='Actual', color='blue', alpha=0.6, linewidth=1)
        plt.plot(df['timestamp'], df['predicted_for_now'], label=f'Predicted ({HORIZON}s Lead)', color='red', linestyle='--', linewidth=1)
        
        plt.fill_between(df['timestamp'], df['throughput_tps'], df['predicted_for_now'], 
                         where=(df['predicted_for_now'] < df['throughput_tps']), color='red', alpha=0.3, label='Capacity Deficit')
        
        # Larger, bolder title
        plt.title(f"Simulation: {current_model.upper()} Accuracy", fontsize=20, fontweight='bold', pad=15)
        
        # Larger legend
        plt.legend(fontsize=14, loc='upper right')
        
        # Hide axis labels and tick numbers for a clean, minimalist look
        plt.xlabel("")
        plt.ylabel("")
        plt.xticks([])  # Hides time numbers
        plt.yticks([])  # Hides throughput numbers
        
        # Make the grid lines a bit more prominent to guide the eye without overwhelming
        plt.grid(True, alpha=0.4, linewidth=1.5)
        
        # Add the execution time "cell" in the top left corner
        # Using transform=plt.gca().transAxes places it relative to the graph (0 to 1 scale)
        cell_text = f"Compute Time: {execution_time:.2f}s\nMAE: {mae:.2f}"
        plt.text(0.02, 0.96, cell_text, 
                 transform=plt.gca().transAxes, 
                 fontsize=14, fontweight='bold',
                 verticalalignment='top',
                 bbox=dict(boxstyle='round,pad=0.6', facecolor='#f0f0f0', edgecolor='black', alpha=0.9, linewidth=1.5))
        
        # Save as SVG with tight bounding box so it doesn't leave unnecessary white space
        filename = f"{current_model}_forecast_results.svg"
        plt.savefig(filename, bbox_inches='tight', dpi=300) # Increased DPI for poster printing
        print(f"Graph saved locally as: {filename}")
        
        plt.close()

if __name__ == "__main__":
    main()