from flask import Flask, request, jsonify
import zmq
import messages_pb2
import time

app = Flask(__name__)

# ---------- ZMQ ----------
context = zmq.Context()

store_socket = context.socket(zmq.PUSH)
store_socket.bind("tcp://*:5557")

ack_socket = context.socket(zmq.PULL)
ack_socket.bind("tcp://*:5558")

time.sleep(1)  # allow nodes to connect

# ---------- Routes ----------
@app.route("/")
def health():
    return "Master node running"

@app.route("/store", methods=["POST"])
def store_file():
    file = request.files["file"]
    filename = file.filename
    data = file.read()

    task = messages_pb2.storeData()
    task.filename = filename

    # replicate to all nodes (4)
    for _ in range(4):
        store_socket.send_multipart([
            task.SerializeToString(),
            data
        ])

    return jsonify({"status": "stored", "filename": filename})

# ---------- Start ----------
if __name__ == "__main__":
    print("Master node started")
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True,
        use_reloader=False  
    )
