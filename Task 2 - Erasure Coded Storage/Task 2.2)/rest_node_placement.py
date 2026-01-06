import zmq
import random
import string
import time
import sqlite3
import io
import logging
from Reed_Solomon import store_file, get_file
from flask import Flask, g, make_response, request, send_file, jsonify
from logging import exception


#-----------------ZMQ Setup-----------------#
context = zmq.Context()

socket_push = context.socket(zmq.PUSH)
socket_push.bind("tcp://*:5557")

socket_pull = context.socket(zmq.PULL)
socket_pull.bind("tcp://*:5558")

socket_pub = context.socket(zmq.PUB)
socket_pub.bind("tcp://*:5559")

time.sleep(1)

#------------  Utility Functions-------------#

def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

def write_to_file(data, filename = None):
    if filename is None: 
        filename = random_string(8)
        filename += '.bin'
    
    try: 
        with open('./' + filename, 'wb') as f: 
            f.write(data)
    except EnvironmentError as e: 
        logging.error(f"Error writing to file: {e}")
        return None
    
    return filename

def get_db():
    if 'db' not in g: 
        g.db = sqlite3.connect(
            "database.db",
            detect_types = sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db 

def init_db():
    db = sqlite3.connect("database.db")
    if db is None:
        try:
            with open("file.sql") as f: 
                db.executescript(f.read())
        
        except EnvironmentError as e: 
            print("Error initializing database: {}".format(e))
    
    db.close()

def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()



#------------------Configurable Placement of Fragments------------------#

def placement_strategy(strategy, R, chunk_index, nodes):
    if strategy == "random_placement":
        return random_placement(nodes, R)

    elif strategy == "min_copy_sets":
        return min_copy_sets(nodes, R, chunk_index)

    elif strategy == "buddy_approach": 
        return buddy_approach(nodes, R, chunk_index)
    
    else:
        raise ValueError("Invalid node placement strategy")
    
def random_placement(nodes, R):
    return random.sample(nodes, R)

def min_copy_sets(nodes, R, chunk_index):
    divide_nodes = [[nodes[(i + j) % len(nodes)] for j in range(R)] for i in range(len(nodes))]
    return divide_nodes[chunk_index % len(nodes)]

def buddy_approach(nodes, R, chunk_index):

    if R != 2:
        raise ValueError("R needs to be bigger than 2")

    if len(nodes) % 2 != 0:
        raise ValueError("Must have even number of nodes")

    # Create buddy groups
    buddy_groups = [
        nodes[i:i + 2]
        for i in range(0, len(nodes), 2)
    ]

    # Select buddy group based on chunk index
    group = buddy_groups[chunk_index % len(buddy_groups)]

    return group

#-----------------Flask Setup-----------------#

init_db()
app = Flask(__name__)
app.teardown_appcontext(close_db)

@app.route('/files/<int:file_id>', methods=['GET'])
def get_file_metadata(file_id):
    db = get_db()
    cursor = db.execute('SELECT * FROM file where id = ?', [file_id])
    if cursor is None: 
        return make_response({'message': 'Error connecting to database'}, 500)
    
    f = cursor.fetchone()
    if f is None:
        return make_response({'message': f'File {file_id} not found'}, 404)
    
    f = dict(f)
    return make_response(f)

@app.route('/files/<int:file_id>/fragments', methods=['GET'])
def get_file_fragments(file_id):
    db = get_db()
    cursor = db.execute('SELECT * FROM file_fragment where file_id = ? order by fragment_index', [file_id])
    if cursor is None: 
        return make_response({'message': 'Error connecting to database'}, 500) 
    f = cursor.fetchall()
    if f is None: 
        return make_response({'message': f'File {file_id} not found'}, 404)
    
    fragments = []

    for row in f: 
        d = dict(row)
        d.pop('coefficients', None)
        fragments.append(d)

    return jsonify(fragments)


@app.route('/files/<int:file_id>/download', methods=['GET'])
def download_file(file_id):
    db = get_db()
    cursor = db.execute('SELECT * FROM file where id = ?', [file_id])
    if not cursor: 
        return make_response({'message': 'Error connecting to database'}, 500)
    
    f = cursor.fetchone()
    if f is None:
        return make_response({'message': f'File {file_id} not found'}, 404)
    
    f = dict(f)
    print(f"Requested file metadata: {f}")

    get_id = db.execute(
        'SELECT fragment_name, fragment_index, coefficients FROM file_fragment WHERE file_id = ? GROUP BY fragment_name ORDER BY fragment_index',
        [file_id]
    )
    fragment_rows = get_id.fetchall()
    coded_fragments = [row['fragment_name'] for row in fragment_rows]
    fragment_meta = {}
    matrix = []

    for row in fragment_rows:
        fragment_meta[row['fragment_name']] = row['fragment_index']
        matrix.append(bytearray(row['coefficients']))
    
    file_data = get_file(
        coded_fragments = coded_fragments, 
        fragment_meta = fragment_meta, 
        matrix = matrix, 
        file_size = f['size'],
        data_req_socket = socket_pub,
        response_socket = socket_pull,
        k = f['k_fragments'],
        l = f['node_losses']
    )

    return send_file(
        io.BytesIO(file_data), 
        mimetype = f['content_type']
    )


@app.route('/files', methods=['POST'])
def add_files():
    payload = request.form
    k = int(payload.get('k'))
    l = int(payload.get('l'))
    replication_factor = int(payload.get('r'))
    strategy = payload.get('fragment_strategy')
    files = request.files

    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request")
        return make_response({'message': 'No file uploaded'}, 400)
    
    file = files.get('file')
    filename = file.filename
    content_type = file.mimetype
    file_data = file.read()
    size = len(file_data)
    c = k + l

    db = get_db()
    insert_into_file = db.execute(
        'INSERT INTO file (filename, size, content_type, k_fragments, node_losses, c_fragments) VALUES (?, ?, ?, ?, ?, ?)',
        (filename, size, content_type, k, l, c)
    )

    file_id = insert_into_file.lastrowid

    cursor = db.execute('SELECT COUNT(*) as count from storage_node where status = 1')
    storage_nodes_count = cursor.fetchone()['count']

    fragment_meta, matrix = store_file(
        file_data = file_data, 
        send_task_socket = socket_push, 
        response_socket = socket_pull, 
        k = k, 
        l = l,
        storage_nodes_count=storage_nodes_count
    )

    retrieve_active_nodes = db.execute('SELECT id from storage_node where status = 1')
    storage_nodes = [row['id'] for row in retrieve_active_nodes.fetchall()]

    for name, index in fragment_meta.items():
        selected_nodes = placement_strategy(strategy, replication_factor, index, storage_nodes)
        for node in selected_nodes:
            db.execute(
                'INSERT INTO file_fragment (file_id, storage_node_id, fragment_name, fragment_index, coefficients) VALUES (?, ?, ?, ?, ?)',
                (file_id, node, name, index, bytes(matrix[index]))
            )
    
    db.commit()

    return make_response({'file_id': file_id}, 201)


@app.errorhandler(500)
def server_error(e):
    exception("Internal error: %s", e)
    return make_response({'message': 'Internal server error'}, 500)

host_local_computer = "localhost"
host_local_addr = "0.0.0.0"

app.run(host = host_local_computer, port = 9000) # The base url is http://localhost:9000