
# https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pushpull.html 
# http://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/multisocket/zmqpoller.html

""" 
Implementation of the Storage Node component

This should run forever and listen on two ZMQ channels. There are two sockets: 

- A PULL socket on port 5557 that receives store requests from the controller that consists of two things: 

1) a serialized storeData protobuf message
2) the message is then followed by the raw binary chunk data

- A separate SUB socket on port 5559 that listens for getData requests from the controller, 
which is a simple ZMQ message that holds a serialized getData protobuf message.


The storage node then responds on a PUSH socket on port 5558 to the controller

"""

import zmq
import messages_pb2
import os
import sys
import string
import random

# For generating random file names
def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))


# write content to file for sending back to controller
def write_to_file(content, filename = None):

    if filename is None: 
        filename = random_string(length = 8)
        filename += ".txt"

    try: 
        with open(filename, 'wb') as f: 
            f.write(content)
    except FileExistsError: 
        print("file.txt already exists")
    return filename


# User can custom data folder name, where the chunks should be stored and retrieved from
# via command line argument
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"

try:
    if data_folder != "./":
        os.mkdir("./" + data_folder)

except FileExistsError as _:
    pass

print(f"Using data folder: {data_folder}")

# ZMQ instantiation and socket setup
context = zmq.Context()

zmq_pull_socket = context.socket(zmq.PULL)
zmq_pull_socket.connect("tcp://localhost:5557")


zmq_sub_socket = context.socket(zmq.SUB)
zmq_sub_socket.connect(f"tcp://localhost:5559")

# subscribe to all topics
zmq_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

zmq_push_socket = context.socket(zmq.PUSH)
zmq_push_socket.connect("tcp://localhost:5558")

# To listen for multiple sockets, we can use a ZMQ Poller object
poller = zmq.Poller()
poller.register(zmq_pull_socket, zmq.POLLIN)
poller.register(zmq_sub_socket, zmq.POLLIN)

# We poll the sockets to check if we have messages to recv and process.
while True: 
    socks = dict(poller.poll())

    # Store data handler
    # When we want to store data, we receive two parts: 
    # - storeData protobuf object
    # - raw binary chunk data

    # After reading both parts, we store the data in a data folder with the filename
    # and return the filename back to the controller via the PUSH socket

    if(zmq_pull_socket in socks and socks[zmq_pull_socket] == zmq.POLLIN):
        message = zmq_pull_socket.recv_multipart()
        task = messages_pb2.storeData()
        # Parse the first part of the multipart message 
        task.ParseFromString(message[0])
        # Parse the second part as the raw binary data
        data = message[1]
        
        write_to_file(data, filename = data_folder + '/' + task.filename)
        print(f"Data stored in data folder: {data_folder}")
        zmq_push_socket.send_string(task.filename)
    
    # Get data handler
    # Here we have a serialized getData protobuf message. We use the with open function
    # to open the file and read the binary data and then send it back to the controller
    # via the PUSH socket as a multipart message ZMQ message consisting of two parts:
    # - chunk name
    # - The content/data itself
    
    if(zmq_sub_socket in socks and socks[zmq_sub_socket] == zmq.POLLIN):
        message = zmq_sub_socket.recv()
        task = messages_pb2.getData()
        task.ParseFromStrong(message)

        # Read the file and send the chunk name and data back to the controller via PUSH socket
        try:
            with open(data_folder + '/' + task.filename, 'rb') as f:
                data = f.read()
                zmq_push_socket.send_multipart([
                    task.filename, 
                    data
                ])
        except FileNotFoundError as _:
            pass

    




