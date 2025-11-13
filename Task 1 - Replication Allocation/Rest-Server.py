from flask import Flask, g
import sqlite3
import messages_pb2
import zmq
import string
import random
import time
import math

# Create Flask instance
app = Flask(__name__)


def random_string(length=8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))


def get_db():
    pass

# ZMQ context and socket setup
context = zmq.Context()

zmq_pull_socket = context.socket(zmq.PULL)
zmq_pull_socket.bind("tcp://*:5557")

zmq_push_socket = context.socket(zmq.PUSH)
zmq_push_socket.bind("tcp://*:5558")

zmq_pub_socket = context.socket(zmq.PUB)
zmq_pub_socket.bind("tcp://*:5559")

time.sleep(1)

