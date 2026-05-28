import time
import requests
import csv
from datetime import datetime

def get_current_throughput(prom_url):
    """Fetches the current throughput from Prometheus."""
    query = 'sum(rate(epoch_total_transactions_total[15s]))'
    try:
        response = requests.get(f"{prom_url}/api/v1/query", params={'query': query}, timeout=2)
        data = response.json()
        if data['status'] == 'success' and data['data']['result']:
            return float(data['data']['result'][0]['value'][1])
    except Exception as e:
        # Fails gracefully if Prometheus is unreachable
        print(e)
        pass
    return 0.0

def main():
    prom_url = "http://localhost:4002"
    output_file = "throughput_data.csv"
    interval = 1.0  # Run every 1 second

    print(f"Starting throughput logger. Saving data to '{output_file}'...")
    print("Press Ctrl+C to stop.")

    # Create the file and write the CSV headers
    with open(output_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "throughput_tps"])

    try:
        while True:
            loop_start = time.time()
            now = datetime.now()
            
            # Fetch throughput
            throughput = get_current_throughput(prom_url)
            
            # Print to console for visibility
            print(f"[{now.strftime('%H:%M:%S')}] Throughput: {throughput:>8.1f} TPS")
            
            # Save to the CSV file
            with open(output_file, mode='a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([now.isoformat(), throughput])
            
            # Ensure the loop runs exactly every `interval` seconds
            elapsed = time.time() - loop_start
            time.sleep(max(0, interval - elapsed))
            
    except KeyboardInterrupt:
        print("\nLogger stopped.")

if __name__ == "__main__":
    main()