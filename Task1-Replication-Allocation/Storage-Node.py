import zmq
import messages_pb2
import os
import sys
import string
import random

# ------------------ helpers ------------------

def random_string(length=8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

def write_to_file(content, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'wb') as f:
        f.write(content)

# ------------------ setup ------------------

data_folder = sys.argv[1] if len(sys.argv) > 1 else "data"
data_folder = os.path.abspath(data_folder)
os.makedirs(data_folder, exist_ok=True)

print(f"Using data folder: {data_folder}")

context = zmq.Context()

# Controller binds, storage nodes connect
zmq_pull_socket = context.socket(zmq.PULL)
zmq_pull_socket.connect("tcp://localhost:5557")
print("Connected PULL -> tcp://localhost:5557")

zmq_sub_socket = context.socket(zmq.SUB)
zmq_sub_socket.connect("tcp://localhost:5559")
zmq_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
print("Connected SUB  -> tcp://localhost:5559")

zmq_push_socket = context.socket(zmq.PUSH)
zmq_push_socket.connect("tcp://localhost:5558")
print("Connected PUSH -> tcp://localhost:5558")

poller = zmq.Poller()
poller.register(zmq_pull_socket, zmq.POLLIN)
poller.register(zmq_sub_socket, zmq.POLLIN)

print("Storage node running...")

# ------------------ main loop ------------------

while True:
    socks = dict(poller.poll(100))

    # ---------- STORE ----------
    if zmq_pull_socket in socks and socks[zmq_pull_socket] == zmq.POLLIN:
        message = zmq_pull_socket.recv_multipart()

        if len(message) != 2:
            print("Invalid STORE message received")
            continue

        task = messages_pb2.storeData()
        task.ParseFromString(message[0])
        data = message[1]

        # Build unique chunk filename
        chunk_filename = (
            f"{task.filename}_R{task.replica_id}_C{task.chunk_id}.bin"
        )

        filepath = os.path.join(data_folder, chunk_filename)

        print(
            f"Store request: {task.filename} | "
            f"Replica {task.replica_id}, Chunk {task.chunk_id} "
            f"({len(data)} bytes)"
        )

        write_to_file(data, filepath)

        print(f"Stored at: {filepath}")

        # ACK includes replica + chunk info
        zmq_push_socket.send_string(
            f"ACK {task.filename} R{task.replica_id} C{task.chunk_id}"
        )

        print(
            f"ACK sent: {task.filename} "
            f"R{task.replica_id} C{task.chunk_id}"
        )

    # ---------- GET ----------
    if zmq_sub_socket in socks and socks[zmq_sub_socket] == zmq.POLLIN:
        message = zmq_sub_socket.recv()

        task = messages_pb2.getData()
        task.ParseFromString(message)

        filepath = os.path.join(data_folder, task.filename)
        print(f"Fetch request: {task.filename}")

        try:
            with open(filepath, 'rb') as f:
                data = f.read()

            zmq_push_socket.send_multipart([
                task.filename.encode(),
                data
            ])

            print(f"Fetch sent: {task.filename} ({len(data)} bytes)")

        except FileNotFoundError:
            print(f"File not found: {filepath}")
