from flask import Flask, request
from flask_restx import Api, Resource, fields
import messages_pb2

import zmq
import time
import string
import random

# ================= CONFIG =================

NUM_NODES = 4

# ================= FLASK / SWAGGER =================

app = Flask(__name__)

api = Api(
    app,
    version="1.0",
    title="Distributed Storage API",
    description="REST Master Node for Distributed Storage"
)

ns = api.namespace("files", description="File operations")

# Swagger model for file upload (THIS FIXES THE ERROR)
upload_model = api.model("FileUpload", {
    "file": fields.Raw(
        required=True,
        description="File to upload (multipart/form-data)"
    )
})

# ================= ZMQ SETUP =================

context = zmq.Context()

# REST → Storage Nodes (store requests)
zmq_push_store = context.socket(zmq.PUSH)
zmq_push_store.bind("tcp://*:5557")

# Storage Nodes → REST (ACKs and fetch replies)
zmq_pull_response = context.socket(zmq.PULL)
zmq_pull_response.bind("tcp://*:5558")

# REST → Storage Nodes (fetch requests)
zmq_pub_fetch = context.socket(zmq.PUB)
zmq_pub_fetch.bind("tcp://*:5559")

# Allow storage nodes to connect
time.sleep(1)

# ================= HELPERS =================

def random_string(length=8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

NUM_REPLICAS = 4
NUM_CHUNKS = 4

def nodeStore(file):
    print("REST: entering nodeStore()")

    data = file.read()
    size = len(data)
    chunk_size = size // NUM_CHUNKS

    for replica_id in range(NUM_REPLICAS):
        print(f"REST: processing replica {replica_id}")

        for chunk_id in range(NUM_CHUNKS):
            start = chunk_id * chunk_size
            end = size if chunk_id == NUM_CHUNKS - 1 else (chunk_id + 1) * chunk_size
            chunk = data[start:end]

            msg = messages_pb2.storeData()
            msg.filename = file.filename
            msg.replica_id = replica_id
            msg.chunk_id = chunk_id
            msg.total_chunks = NUM_CHUNKS

            zmq_push_store.send_multipart([
                msg.SerializeToString(),
                chunk
            ])

            print(
                f"REST: sent R{replica_id}C{chunk_id} "
                f"({len(chunk)} bytes) to node {chunk_id}"
            )

    # Wait for ACKs (16 total)
    for i in range(NUM_REPLICAS * NUM_CHUNKS):
        ack = zmq_pull_response.recv_string()
        print(f"REST: ACK {i+1}: {ack}")

    return True


def nodeFetchFile(filename):
    msg = messages_pb2.getData()
    msg.filename = filename

    zmq_pub_fetch.send(msg.SerializeToString())

    result = zmq_pull_response.recv_multipart()

    returned_filename = result[0].decode()
    data = result[1]

    return data

# ================= API ROUTES =================

@ns.route("/store")
class StoreFile(Resource):
    def post(self):
        # This is the ONLY correct way to upload files in Flask
        if "file" not in request.files:
            return {"message": "No file uploaded"}, 400

        file = request.files["file"]

        if file.filename == "":
            return {"message": "Empty filename"}, 400

        # DEBUG: prove we actually got bytes
        data = file.read()
        print("REST: received file bytes =", len(data))

        # rewind because nodeStore reads again
        file.stream.seek(0)

        if not nodeStore(file):
            return {"message": "Store failed"}, 500

        return {"message": "File stored successfully"}, 200



@ns.route("/receive/<string:filename>")
class ReceiveFile(Resource):
    def get(self, filename):
        """Retrieve a file by filename"""

        data = nodeFetchFile(filename)

        if data is None:
            return {"message": "File not found"}, 404

        return {
            "filename": filename,
            "content": data.decode(errors="ignore")
        }

# ================= START =================

if __name__ == "__main__":
    print("REST Master Node running...")
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True,
        use_reloader=False
    )
