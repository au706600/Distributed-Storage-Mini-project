
import messages_pb2
import zmq
import os
import random
import string
import sys

# Easier way of using this function in this file would be to import from rest_server.py, but there was not a way to avoid
# circular import error, so we defined it here again. 
def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

def write_to_file(data, filename=None):
    if filename is None: 
        filename = random_string(8)
        filename += ".bin" 
    try:
        with open('./' + filename, "wb") as f:
            f.write(data)
    
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None
    
    return filename

data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    try:
        os.mkdir('./'+data_folder)
    except FileExistsError as _:
        pass

print(f"Data folder: {data_folder}")

context = zmq.Context()

socket_pull = context.socket(zmq.PULL)
socket_pull.connect("tcp://localhost:5557")

socket_push = context.socket(zmq.PUSH)
socket_push.connect("tcp://localhost:5558")

socket_sub = context.socket(zmq.SUB)
socket_sub.connect("tcp://localhost:5559")
socket_sub.setsockopt(zmq.SUBSCRIBE, b'')

# To listen for multiple sockets, we can use a ZMQ Poller object
poller = zmq.Poller()
poller.register(socket_pull, zmq.POLLIN)
poller.register(socket_sub, zmq.POLLIN)

while True:
    socks = dict(poller.poll())

    if socket_pull in socks:
        message = socket_pull.recv_multipart()
        file_msg = messages_pb2.StoreData()
        file_msg.ParseFromString(message[0])
        chunk_data = message[1]
        print(f"Chunk to store: {file_msg.filename} with size {len(chunk_data)} bytes")

        filename = write_to_file(chunk_data, filename = os.path.join(data_folder, file_msg.filename))
        print(f"Data stored  in data folder: /{file_msg.filename}")
        socket_push.send_string(file_msg.filename)
        continue

    if socket_sub in socks: 
        message = socket_sub.recv_multipart()
        #file_msg = messages_pb2.GetData()
        header = messages_pb2.header()
        header.ParseFromString(message[0])
        
        if header.request_type == messages_pb2.FRAGMENT_STATUS_REQ:
            req = messages_pb2.Fragment_Status_Request()
            req.ParseFromString(message[1])
            fragment_path = os.path.join(data_folder, req.fragment_name)
            check_exists = os.path.exists(fragment_path)

            response = messages_pb2.Fragment_Status_Response(
                fragment_name = req.fragment_name, 
                is_present = check_exists,
                node_id = data_folder
            )
            socket_push.send(response.SerializeToString())
            continue

        
        elif header.request_type == messages_pb2.FRAGMENT_DATA_REQ:
            req = messages_pb2.GetData()
            req.ParseFromString(message[1])
            try:
                with open(os.path.join(data_folder, req.filename), "rb") as f:
                    data = f.read()
                socket_push.send_multipart([
                    header.SerializeToString(),
                    req.filename.encode('utf-8'), 
                    data
                ])
                print(f"Sent data for fragment: {req.filename} with size {len(data)} bytes")
            except FileNotFoundError as e:
                pass
            continue
        else:
            pass




