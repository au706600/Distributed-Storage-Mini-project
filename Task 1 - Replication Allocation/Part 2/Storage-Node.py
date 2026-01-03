# https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pushpull.html 
# http://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/multisocket/zmqpoller.html

import messages_pb2
import zmq
import random
import string
import os
import sys


def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

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


# Allow the user to set a folder name, where the chunks should be stored to and retrieved from
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"

try:
    if data_folder != "./":
        os.mkdir('./' + data_folder)

except FileExistsError as _:
    pass

print(f"Data folder: {data_folder}")

# Set up zmq channels
context = zmq.Context()

# Socket to receive messages
socket_pull = context.socket(zmq.PULL)
socket_pull.connect("tcp://localhost:5557")

# Socket to send messages
socket_push = context.socket(zmq.PUSH)
socket_push.connect("tcp://localhost:5558")

# Socket to subscribe messages
socket_sub = context.socket(zmq.SUB)
socket_sub.connect("tcp://localhost:5559")
socket_sub.setsockopt(zmq.SUBSCRIBE, b'')

# To listen for multiple sockets, we can use a ZMQ Poller object
poller = zmq.Poller()
poller.register(socket_pull, zmq.POLLIN)
poller.register(socket_sub, zmq.POLLIN)

while True:
    socks = dict(poller.poll())

    #if(socket_pull in socks and socks[socket_pull] == zmq.POLLIN):

    if socket_pull in socks: 

        message = socket_pull.recv_multipart()
        data_msg = messages_pb2.StoreData()
        data_msg.ParseFromString(message[0])
        data = message[1]
        print(f"Chunk to store: {data_msg.filename} with size {len(data)} bytes")
        #with open('/' + data_msg.filename, 'wb') as f:
            #f.write(data)
        write_to_file(data, filename = os.path.join(data_folder, data_msg.filename))
        print(f"Data stored in data folder: /{data_msg.filename}")
        socket_push.send_string(data_msg.filename)

    #if(socket_sub in socks and socks[socket_sub] == zmq.POLLIN):

    if socket_sub in socks: 
        message = socket_sub.recv()
        data_msg = messages_pb2.GetData()
        data_msg.ParseFromString(message)
        print(f"Chunk to retrieve: {data_msg.filename}")
        try:
            with open(os.path.join(data_folder, data_msg.filename), 'rb') as f:
                data = f.read()
            socket_push.send_multipart([
                data_msg.filename.encode('utf-8'), 
                data
            ])
        except FileNotFoundError as _:
            pass


