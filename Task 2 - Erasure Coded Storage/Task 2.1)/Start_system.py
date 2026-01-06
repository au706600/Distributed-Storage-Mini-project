import subprocess
import sys
import os
import time
import random
import threading

PYTHON = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

NUM_NODES = 4

master_process = None
storage_processes = {}  # node_id -> process


def start_master():
    global master_process
    print("Starting master node...")
    master_process = subprocess.Popen(
        [PYTHON, "rest_server.py"],
        cwd=BASE_DIR
    )


def start_storage_nodes():
    for i in range(NUM_NODES):
        node_id = i + 1
        data_dir = f"node{node_id}"
        print(f"Starting storage node {node_id} ({data_dir})")
        p = subprocess.Popen(
            [PYTHON, "storage_node.py", data_dir],
            cwd=BASE_DIR
        )
        storage_processes[node_id] = p


def kill_random_nodes(n):
    alive = {
        node_id: p
        for node_id, p in storage_processes.items()
        if p.poll() is None
    }

    if not alive:
        print("No alive storage nodes to kill.")
        return

    n = min(n, len(alive))
    victims = random.sample(list(alive.items()), n)

    for node_id, p in victims:
        print(f"Killing storage node {node_id}")
        p.terminate()


def input_loop():
    while True:
        try:
            cmd = input("> ").strip().lower()

            if cmd == "":
                continue

            if cmd == "exit":
                print("Exiting system...")
                shutdown()
                os._exit(0)

            if cmd.startswith("kill"):
                parts = cmd.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    print("Usage: kill <n>")
                    continue

                kill_random_nodes(int(parts[1]))
            else:
                print("Unknown command. Use: kill <n>, exit")

        except EOFError:
            break


def shutdown():
    if master_process:
        master_process.terminate()

    for p in storage_processes.values():
        if p.poll() is None:
            p.terminate()


if __name__ == "__main__":
    try:
        start_master()
        time.sleep(2)
        start_storage_nodes()

        print("\nSystem running.")
        print("Commands:")
        print("  kill <n>  → kill n random storage nodes")
        print("  exit      → stop all\n")

        threading.Thread(target=input_loop, daemon=True).start()

        # Wait for master to exit
        master_process.wait()

    except KeyboardInterrupt:
        print("\nStopping system...")
        shutdown()
