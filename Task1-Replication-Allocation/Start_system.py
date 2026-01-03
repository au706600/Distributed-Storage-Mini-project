import subprocess
import sys
import os
import time

PYTHON = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

NUM_NODES = 4

processes = []

def start_master():
    print("Starting master node...")
    p = subprocess.Popen(
        [PYTHON, "Rest-Server.py"],
        cwd=BASE_DIR
    )
    processes.append(p)

def start_storage_nodes():
    for i in range(NUM_NODES):
        data_dir = f"node{i+1}_data"
        print(f"Starting storage node {i+1} ({data_dir})")
        p = subprocess.Popen(
            [PYTHON, "Storage-Node.py", data_dir],
            cwd=BASE_DIR
        )
        processes.append(p)

if __name__ == "__main__":
    try:
        start_master()
        time.sleep(2)  # allow master to bind ports
        start_storage_nodes()

        print("\nSystem running. Press Ctrl+C to stop.\n")

        for p in processes:
            p.wait()

    except KeyboardInterrupt:
        print("\nStopping system...")
        for p in processes:
            p.terminate()
