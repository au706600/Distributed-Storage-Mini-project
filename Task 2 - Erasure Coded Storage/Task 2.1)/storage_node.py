
import messages_pb2
import zmq
import os
import random
import string
import sys

# Generate random string names for the stored fragments
def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

# Write data to the fragments so that they can be retrieved later
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

# Allow the user to set a folder name via command line argument
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    try:
        os.mkdir('./'+data_folder)
    except FileExistsError as _:
        pass

print(f"Data folder: {data_folder}")

# Create an instance of ZMQ context for communication
context = zmq.Context()

# Socket to receive store requests (storeData message) from the lead node (rest_server.py)
socket_pull = context.socket(zmq.PULL)
socket_pull.connect("tcp://localhost:5557")

# Socket to send results back to the lead node (rest_server.py)
socket_push = context.socket(zmq.PUSH)
socket_push.connect("tcp://localhost:5558")

# Socket to receive requests for fragment status and data
socket_sub = context.socket(zmq.SUB)
socket_sub.connect("tcp://localhost:5559")
socket_sub.setsockopt(zmq.SUBSCRIBE, b'')

# To listen for multiple sockets, we can use a ZMQ Poller object
poller = zmq.Poller()
poller.register(socket_pull, zmq.POLLIN)
poller.register(socket_sub, zmq.POLLIN)

while True:
    # poll the sockets to check if we have any incoming messages
    socks = dict(poller.poll())

    if socket_pull in socks:
        # If we have a store request, parse it as multipart message that consists of
        # fragment name and fragment data. 
        message = socket_pull.recv_multipart()
        file_msg = messages_pb2.StoreData()
        file_msg.ParseFromString(message[0])
        chunk_data = message[1]
        print(f"Chunk to store: {file_msg.filename} with size {len(chunk_data)} bytes")

        # Write the fragment data to a file that is randomly named
        filename = write_to_file(chunk_data, filename = os.path.join(data_folder, file_msg.filename))
        print(f"Data stored  in data folder: /{file_msg.filename}")
        
        # Send fragment name back to lead node as confirmation
        socket_push.send_string(file_msg.filename)
        continue

    if socket_sub in socks:
        # If we have a request for fragment status or data, parse the message header
        message = socket_sub.recv_multipart()
        #file_msg = messages_pb2.GetData()
        header = messages_pb2.header()
        header.ParseFromString(message[0])
        
        if header.request_type == messages_pb2.FRAGMENT_STATUS_REQ:
            # If we have a fragment status request, check if the fragment exists in our data folder
            req = messages_pb2.Fragment_Status_Request()
            req.ParseFromString(message[1])
            fragment_path = os.path.join(data_folder, req.fragment_name)
            check_exists = os.path.exists(fragment_path)

            # If it exists, send a response back to the lead node indicating that the fragment is present
            response = messages_pb2.Fragment_Status_Response(
                fragment_name = req.fragment_name, 
                is_present = check_exists,
                node_id = data_folder
            )
            socket_push.send(response.SerializeToString())
            continue

        
        elif header.request_type == messages_pb2.FRAGMENT_DATA_REQ:
            # If we have a fragment data request, read the fragment data from our data folder and
            # send the fragment data back to the lead node as a multipart message consisting of 
            # a header that indicates the type of message, fragment name, and fragment data
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




