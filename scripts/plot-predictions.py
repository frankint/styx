import os
import glob
import json
import csv
import matplotlib.pyplot as plt

def main():
    # 1. Find all CSV files in the raw_predictions directory
    files = glob.glob("raw_predictions/predictions_vs_actual_*.csv")
    if not files:
        print("No prediction logs found in 'raw_predictions/' directory.")
        return

    # 2. Present options to the user
    print("Available prediction logs:")
    files.sort(reverse=True) # Show newest first
    for idx, filepath in enumerate(files):
        # Extract timestamp part from filename
        filename = os.path.basename(filepath)
        timestamp_str = filename.replace("predictions_vs_actual_", "").replace(".csv", "")
        print(f"[{idx}] {timestamp_str}")

    # 3. Get user selection
    try:
        choice = int(input("\nSelect a run to plot (enter the number): "))
        if choice < 0 or choice >= len(files):
            print("Invalid choice.")
            return
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    selected_file = files[choice]
    print(f"Loading {selected_file}...")

    # 4. Read the data
    timestamps = []
    actual_tps = []
    predictions = []

    with open(selected_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            timestamps.append(float(row["timestamp"]))
            actual_tps.append(float(row["actual_tps"]))
            predictions.append(json.loads(row["predicted_tps_horizon"]))

    if not timestamps:
        print("The selected file is empty.")
        return

    # Normalize timestamps to start at 0 (relative seconds)
    start_time = timestamps[0]
    relative_times = [t - start_time for t in timestamps]

    # 5. Plotting
    plt.figure(figsize=(12, 6))

    # Plot actual throughput
    plt.plot(relative_times, actual_tps, label="Actual TPS", color="black", linewidth=2)

    # Plot predictions
    # To avoid a messy plot, we'll plot the prediction horizon for every Nth point as a faint line.
    # step = max(1, len(relative_times) // 20) # Show ~20 distinct forecast horizons
    step = 1

    # Each prediction point corresponds to 1 second (since AggregatingMetricBuffer bucket_interval=1.0)
    time_per_prediction_step = 1.0

    for i in range(0, len(relative_times), step):
        t_start = relative_times[i]
        forecast_array = predictions[i]

        if not forecast_array:
            continue

        # Generate future timestamps for this specific forecast
        future_times = [t_start + (j + 1) * time_per_prediction_step for j in range(len(forecast_array))]

        # Plot the horizon
        plt.plot(future_times, forecast_array, color="blue", alpha=0.3,
                    label="Forecast Horizon" if i == 0 else "")
        # Connect the last actual point to the first forecast point so it visually branches off
        plt.plot([t_start, future_times[0]], [actual_tps[i], forecast_array[0]], color="blue", alpha=0.3)

    plt.title(f"Actual vs Predicted Throughput ({os.path.basename(selected_file)})")
    plt.xlabel("Relative Time (seconds)")
    plt.ylabel("Transactions Per Second (TPS)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("predictions.png")
    plt.show()

if __name__ == "__main__":
    main()