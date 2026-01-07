# https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pushpull.html 
# http://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/multisocket/zmqpoller.html

import messages_pb2
import zmq
import random
import string
import os
import sys

# Generate a random file names for chunks
def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

# Write replicated chunk to file so that it can be retrieved later
def write_to_file(data, filename=None):
    if filename is None:
        filename = random_string(length=8)
        filename += ".bin"
    try: 
        with open('./' + filename, 'wb') as f: 
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None
    return filename


# Allow the user to set a folder name via command line argument
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"

try:
    if data_folder != "./":
        os.mkdir('./' + data_folder)

except FileExistsError as _:
    pass

print(f"Data folder: {data_folder}")

# Set up zmq channels
context = zmq.Context()

# Socket to StoreData messages from controller
socket_pull = context.socket(zmq.PULL)
socket_pull.connect("tcp://localhost:5557")

# Socket to send results back to controller
socket_push = context.socket(zmq.PUSH)
socket_push.connect("tcp://localhost:5558")

# Socket to listen for GetData messages from controller
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

    #if(socket_pull in socks and socks[socket_pull] == zmq.POLLIN):

    if socket_pull in socks: 

        # If we have a StoreData message, parse it as multipart message that consists of
        # filename (file metadata) and and the actual data content to be stored
        message = socket_pull.recv_multipart()
        data_msg = messages_pb2.StoreData()
        data_msg.ParseFromString(message[0])
        data = message[1]
        print(f"Chunk to store: {data_msg.filename} with size {len(data)} bytes")
        
        #with open('/' + data_msg.filename, 'wb') as f:
            #f.write(data)

        # Store the data in the specified data folder with random filename
        write_to_file(data, filename = os.path.join(data_folder, data_msg.filename))
        print(f"Data stored in data folder: /{data_msg.filename}")

        # Send back the filename as acknowledgement. 
        socket_push.send_string(data_msg.filename)

    #if(socket_sub in socks and socks[socket_sub] == zmq.POLLIN):

    if socket_sub in socks:

        # If we have a GetData message, we parse it as a single message that contains the filename
        message = socket_sub.recv()
        data_msg = messages_pb2.GetData()
        data_msg.ParseFromString(message)
        print(f"Chunk to retrieve: {data_msg.filename}")

        # Read the data from the specified data folder and send it as a multipart message back to controller
        try:
            with open(os.path.join(data_folder, data_msg.filename), 'rb') as f:
                data = f.read()
            socket_push.send_multipart([
                data_msg.filename.encode('utf-8'), 
                data
            ])
        except FileNotFoundError as _:
            pass


