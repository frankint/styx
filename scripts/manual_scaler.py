import time
import subprocess
import socket
import struct
import argparse
import sys
import threading
from datetime import datetime

# Styx Networking Protocol Constants
MSG_TYPE_REBALANCE = 102

# Global tracking variable for manual scale targets
target_workers = 1


def manual_input_listener():
    """Background thread to capture real-time user input for manual scaling."""
    global target_workers
    print("\n[!] MANUAL REAL-TIME SCALER ACTIVE [!]")
    print("    Type an integer and press ENTER at any time to scale workers.\n")
    
    while True:
        try:
            line = sys.stdin.readline()
            if line.strip():
                val = int(line.strip())
                if val > 0:
                    target_workers = val
                    print(f"\n[!] Input received: Target set to {val} workers.")
                else:
                    print("\n[!] Invalid: Please enter a positive integer greater than 0.\n")
        except ValueError:
            print("\n[!] Invalid input: Please enter a valid integer.\n")
        except Exception as e:
            print(f"\n[!] Input thread encountered an error: {e}")
            break


def send_rebalance_request(host, port, n_workers):
    """Sends a binary Styx protocol message to coordinate internal networking."""
    msg_body = struct.pack('>B', MSG_TYPE_REBALANCE) + struct.pack('>B', n_workers)
    msg = struct.pack('>Q', len(msg_body)) + msg_body
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((host, port))
            s.sendall(msg)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent network rebalance request to {host}:{port}")
    except Exception as e:
        print(f"[!] Rebalance networking failed: {e}")


def scale_containers(n_workers):
    """Executes the Docker compose scaling sequence directly."""
    try:
        subprocess.run(
            ["docker", "compose", "up", "--scale", f"worker={n_workers}", "-d"], 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.DEVNULL,
            check=True
        )
    except subprocess.SubprocessError as e:
        print(f"[!] Failed to execute docker compose scale command: {e}")


def main():
    global target_workers
    parser = argparse.ArgumentParser(description="Styx Real-Time Manual Scaler")
    parser.add_argument("--coord-host", default="localhost")
    parser.add_argument("--coord-port", type=int, default=8886)
    parser.add_argument("--interval", type=float, default=0.5, help="Polling frequency for state matching (seconds)")
    args = parser.parse_args()

    # Spin up the terminal reader thread
    input_thread = threading.Thread(target=manual_input_listener, daemon=True)
    input_thread.start()
        
    current_workers = 1
    
    # Real-time state enforcement loop
    while True:
        loop_start = time.time()
        
        # Local snapshot to avoid atomic race conditions mid-block
        required_workers = target_workers

        if required_workers != current_workers:
            print(f"\n[*] SCALING CHANGE DETECTED: {current_workers} -> {required_workers}")
            
            if required_workers > current_workers:
                print("    -> Scaling UP: Provisioning containers first...")
                scale_containers(required_workers)
                
                # Give the local environment a brief window to bind sockets before networking
                time.sleep(3) 
                send_rebalance_request(args.coord_host, args.coord_port, required_workers)
                
            elif required_workers < current_workers:
                print("    -> Scaling DOWN: Terminating excess containers...")
                scale_containers(required_workers)
                # Network structure usually handles worker drop-offs implicitly via closures
            
            current_workers = required_workers
            print(f"[*] State sync complete. Currently running: {current_workers} worker(s).\n")
        
        elapsed = time.time() - loop_start
        time.sleep(max(0.01, args.interval - elapsed))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Exiting Manual Scaler cleanly.")
        sys.exit(0)