
import random
import messages_pb2
import zmq
import time
import string
import sqlite3
import io 
from logging import exception
from base64 import b64decode
from flask import Flask, g, make_response, request, send_file

# Set up zmq channels
context = zmq.Context()

socket_push = context.socket(zmq.PUSH)
socket_push.bind("tcp://*:5557")

socket_pull = context.socket(zmq.PULL)
socket_pull.bind("tcp://*:5558")

socket_pub = context.socket(zmq.PUB)
socket_pub.bind("tcp://*:5559")

time.sleep(1)


#-------------------------------------------

# Utility functions

# Generate random file names
def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))


def write_to_file(data, filename = None):

    if not filename:
        filename = random_string(length = 8)
        filename += ".txt"
    
    try:
        with open('./' + filename, 'wb') as f:
            f.write(data)

    except EnvironmentError as e:
        print("Error format writing to file: {}".format(e))
        return None
    
    return filename

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect("database.db", detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
    return g.db

def init_db():
    db = sqlite3.connect("database.db")
    #db.execute("PRAGMA journal_mode=WAL;")
    if db is None:
        try: 
            with open('file.sql') as f:
                db.executescript(f.read())
        except EnvironmentError as e:
            pass
    db.close()

def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()

# Node placement strategies

def select_nodes(strategy, R, chunk_index, nodes):
    if strategy == "random_placement":
        return random_placement(nodes, R)

    elif strategy == "min_copy_sets":
        return min_copy_sets(nodes, R, chunk_index)

    elif strategy == "buddy_approach": 
        pass
    
    else:
        raise ValueError("Invalid node placement strategy")


"""
    Params: 
        R = Replication factor
    
    1. Get a list of available storage nodes from the sqlite database
    2. Get the file bytes and split it into k chunks
    3. Randomly select R storage nodes from the list of available storage nodes that we put the chunk replicas on
"""

def random_placement(nodes, R):
    return random.sample(nodes, R)

"""
def random_placement(R):
    # Get a list of available storage nodes from the database
    db = get_db()
    # Get the active storage nodes
    cursor = db.execute('SELECT id FROM storage_node where status = 1')
    if not cursor: 
        make_response({'message': 'Error connecting to database'}, 500)
    nodes = [row['id'] for row in cursor.fetchall()]
    if not nodes:
        make_response({'message': 'No available storage nodes found'}, 500)
    
    # Get the file bytes and split it into k chunks
    payload = request.get_json()
    file_id = payload.get('file_id')
    file_bytes = b64decode(payload.get('contents_b64'))
    chunk_size = 1024 * 1024  # 1 MB chunk size
    split_file_bytes = [file_bytes[i:i + chunk_size] for i in range(0, len(file_bytes), chunk_size)]

    # Randomly select R storage nodes from the list of available storage nodes that we put the chunk replicas on
    for chunk_index, chunk in enumerate(split_file_bytes):
        selected_nodes = random.sample(nodes, R)
        #random_task_file_name1 = [random_string(8), random_string(8)]
        #random_task_file_name2 = [random_string(8), random_string(8)]
        chunk_names = [random_string(8) for _ in range(R)] 
        for replica_index, storage_node_id in enumerate(selected_nodes):
            data_msg = messages_pb2.StoreData()
            data_msg.filename = chunk_names[replica_index]
            socket_push.send_multipart([
                data_msg.SerializeToString(),
                chunk
            ])
            db.execute(
                'INSERT INTO chunk (file_id, chunk_name, replica_index, chunk_index, storage_node_id) VALUES (?, ?, ?, ?, ?)',
                (file_id, data_msg.filename, replica_index, chunk_index, storage_node_id)
            )

    db.commit()
    return make_response({'message': 'File chunks stored successfully'}, 200)

"""

"""
    Params: 
        R = Replication factor

    1. Get a list of available storage nodes from the sqlite database
    2. Divide the storage nodes into groups of size R through deterministic grouping
    3. Get the file bytes and split it into k chunks
    4. For each incoming chunk, we assign it to the copysets
    5. For each chunk-replica pair, we send a "Store chunk" message to the corresponding storage node in the copyset
""" 
def min_copy_sets(nodes, R, chunk_index):
    divide_nodes= [[nodes[(i + j) % len(nodes)] for j in range(R)] for i in range(len(nodes))]
    return divide_nodes[chunk_index % len(nodes)]

"""
def min_copy_sets(R):
    db = get_db()
    cursor = db.execute(
        'SELECT id FROM storage_node where status = 1 order by id'
        )
    if not cursor:
        make_response({'message': 'Error connecting to database'}, 500)
    nodes = [row['id'] for row in cursor.fetchall()]
    if not nodes:
        make_response({'message': 'No available storage nodes found'}, 500)
    
    # Divide the storage nodes into groups of size R through deterministic grouping (cyclic sliding window)
    divide_nodes = [[nodes[(i + j) % len(nodes)] for j in range(R)] for i in range(len(nodes))]

    # Get the file bytes and split it into k chunks
    payload = request.get_json()
    file_id = payload.get('file_id')
    file_bytes = b64decode(payload.get('contents_b64'))
    # Split the file into k chunks
    chunk_size = 1024 * 1024  # 1 MB chunk size
    split_file_bytes = [file_bytes[i : i + chunk_size] for i in range(0, len(file_bytes), chunk_size)]
    
    for chunk_index, chunk in enumerate(split_file_bytes):
        assign_copyset = divide_nodes[chunk_index % len(nodes)]
        chunk_names = [random_string(8) for _ in range(R)]

        for replica_index, storage_node_id in enumerate(assign_copyset):
            data_msg = messages_pb2.StoreData()
            data_msg.filename = chunk_names[replica_index]
            socket_push.send_multipart([
                data_msg.SerializeToString(), 
                chunk
            ])
            db.execute(
            'INSERT INTO chunk (file_id, chunk_name, replica_index, chunk_index, storage_node_id) VALUES (?, ?, ?, ?, ?)',
            (file_id, data_msg.filename, replica_index, chunk_index, storage_node_id)
        )

    db.commit()
    return make_response({'message': 'File chunks stored successfully'}, 200)

"""

"""
def buddy_approach():
    pass
"""


#-------------------------------------------

# REST api's

# Flask instance
init_db()
app = Flask(__name__)
app.teardown_appcontext(close_db)

@app.route('/files/<int:file_id>',  methods=['GET'])
def get_file_metadata(file_id):
    """
        Get metadata for a file with the given file ID
    """
    db = get_db()
    cursor = db.execute('SELECT * FROM file where id = ?', [file_id])
    if not cursor:
        return make_response({'message': 'Error connecting to database'}, 500)

    f = cursor.fetchone()
    if f is None:
        return make_response({'message': f'File {file_id} not found'}, 404)
    
    # Convert to a Python dictionary for easier conversion to JSON
    f = dict(f)

    # Create an HTTP response containing the file metadata
    return make_response(f)

@app.route('/files/<int:file_id>/chunks', methods=['GET'])
def get_file_metachunks(file_id):
    """
        Get chunk metadata for a file with given file ID
    """
    db = get_db()
    cursor = db.execute('SELECT * FROM chunk where file_id = ? order by chunk_index, replica_index', [file_id])
    if not cursor:
        return make_response({'message': 'Erorr connecting to datanase'}, 500)
    
    # Fetch all chunk metadata rows
    f = cursor.fetchall()
    if f is None: 
        return make_response({'message': f'File {file_id} not found'}, 404)
    
    # Convert to a list of Python dictionaries for easier conversion to JSON
    f = [dict(row) for row in f]

    return make_response(f)

@app.route('/files/<int:file_id>/download',  methods=['GET'])
def download_files(file_id): 
    """
        Download a file with the given file ID

        The downloading process consists of the following steps:

        1. The controller receives a GET request for a file with the given file ID
        2. The controller looks up the file metadata in the sqlite database.
        3. The controller then queries the sqlite database to retrieve the list of chunks and replicas associated with the file.
        4. For each chunk index, we select one of the replica and retrieve the chunk from the corresponding storage node. 
        5. Receive the chunk data from the storage node
        6. Reassemble the chunks to reconstruct the original file in the correct order (ascending order) using the chunk indices.
        7. Send the reconstructed file back to the client in the HTTP response.
 
    """
    db = get_db()
    file_metadata = db.execute('SELECT * FROM file where id = ?', [file_id])
    if not file_metadata: 
        return make_response({'message': 'Error connecting to database'}, 500)
    
    f = file_metadata.fetchone()
    if f is None:
        return make_response({'message': f'File {file_id} not found'}, 404)
    
    f = dict(f)
    print("File requested: {}".format(f))

    chunks_replicas = db.execute(
        'SELECT * FROM chunk where file_id = ? order by chunk_index, replica_index', [file_id]
    )

    if not chunks_replicas:
        return make_response({'message': 'Error connecting to database'}, 500)
    chunk_replicas_rows = chunks_replicas.fetchall()
    if len(chunk_replicas_rows) == 0: 
        return make_response({'message': f'File {file_id} not found'}, 404)
    
    chunk_replicas_rows = [dict(row) for row in chunk_replicas_rows]
    group_chunks = {}
    for row in chunk_replicas_rows:
        idx = row['chunk_index']
        if idx not in group_chunks:
            group_chunks[idx] = []
        group_chunks[idx].append(row)
    
    file_data_part = []
    for chunk_idx in sorted(group_chunks.keys()):
        replicas = group_chunks[chunk_idx]
        selected_replica = random.choice(replicas)
        data_msg = messages_pb2.GetData()
        data_msg.filename = selected_replica['chunk_name']
        socket_pub.send(
            data_msg.SerializeToString()
        )
        while True:
            message = socket_pull.recv_multipart()
            chunk_name_part = message[0].decode('utf-8')
            chunk_data_part = message[1]
            if chunk_name_part == selected_replica['chunk_name']:
                file_data_part.append(chunk_data_part)
                print(f"Received chunk: {chunk_name_part} for file ID: {file_id}")
                break
    print(f"All chunks received successfully for file ID: {file_id}")
    file_data_part = b''.join(file_data_part)
    return send_file(io.BytesIO(file_data_part),
                     mimetype=f['content_type'],)



@app.route('/files', methods=['POST'])
def add_files():
    """
        Ingest a new file into the distributed storage system

        In our system, the ingest process consists of the following steps: 

        1. The file arrives from the client in a HTTP POST request
        2. Decode the serialized file from base64 string to binary
        3. Slice the file into k chunks
        4. generate unique chunk names for each chunk
        5. Select N storage nodes according to the selected node placement strategy
        6. For each chunk-replica pair, send a "Store chunk" message to the selected storage node
        7. Store the file metadata into the sqlite database
        8. Store the chunk metadata (replica_index, chunk_index, storage_node_id) to the sqlite database

    """
    payload = request.get_json()
    file_id = payload.get('file_id')
    file_bytes = b64decode(payload.get('contents_b64'))
    strategy = payload.get('node_placement_strategy')
    replication_factor = int(payload.get('replication_factor'))
    chunk_size= 1024 * 1024  # 1 MB chunk size
    split_file_bytes = [file_bytes[i: i + chunk_size] for i in range(0, len(file_bytes), chunk_size)]

    db = get_db()

    # We get sqlite3.IntegrityError if the UNIQUE constraint of file.id is failed. 
    db.execute(
                'INSERT INTO file (id, filename, size, content_type) VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET filename=excluded.filename, size=excluded.size, content_type=excluded.content_type', 
                (file_id, payload.get('filename'), len(file_bytes), payload.get('content_type'))          
            )
    #db.commit()
    cursor = db.execute(
        'SELECT id FROM storage_node where status = 1'
    )
    nodes = [row['id'] for row in cursor.fetchall()]

    #random_select = payload.get('random_placement')
    #min_copysets_placemnet = payload.get('min_copy_sets')

    for chunk_index, chunk in enumerate(split_file_bytes):
        selected_nodes = select_nodes(strategy, replication_factor, chunk_index, nodes)
        chunk_names = [random_string(8) for _ in range(replication_factor)]

        for replica_index, storage_node_id in enumerate(selected_nodes):
            data_msg = messages_pb2.StoreData()
            data_msg.filename = chunk_names[replica_index]
            socket_push.send_multipart([
                data_msg.SerializeToString(), 
                chunk
            ]) 

            db.execute(
                'INSERT INTO chunk (file_id, chunk_name, replica_index, chunk_index, storage_node_id) VALUES (?, ?, ?, ?, ?)',
                (file_id, data_msg.filename, replica_index, chunk_index, storage_node_id)
            )
    db.commit()
    return make_response({'message': 'File chunks stored successfully'}, 200)
            


@app.errorhandler(500)
def server_error(e):
    exception("Internal error: %s", e)
    return make_response({'message': 'Internal server error'}, 500)


host_local_computer = "localhost"
host_local_network = "0.0.0.0"
app.run(host = host_local_computer, port = 9000) # The base url is http://localhost:9000


