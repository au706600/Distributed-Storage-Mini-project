#import copy
import math
import random
import string
import time

import zmq
import messages_pb2
import pyerasure
import pyerasure.finite_field
import pyerasure.generator

def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

def rs_cauchy_coeffs(k, l):
    field = pyerasure.finite_field.Binary8()
    generator = pyerasure.generator.RSCauchy(field, k)
    matrix = []

    for i in range(k):
        row = bytearray(k)
        row[i] = 1
        matrix.append(row)
    
    for _ in range(l):
        coeffs, _ = generator.generate()
        matrix.append(coeffs)
    
    return matrix

def store_file(file_data, send_task_socket, response_socket, k, l, storage_nodes_count):
    file_data = bytearray(file_data)
    c = k + l
    assert c >= 0
    assert c <= storage_nodes_count

    symbols = k
    symbol_size = math.ceil(len(file_data) / symbols)
    field = pyerasure.finite_field.Binary8()
    encoder = pyerasure.Encoder(
        field = field, 
        symbols = symbols, 
        symbol_bytes = symbol_size
    )
    
    encoder.set_symbols(file_data.ljust(symbol_size * symbols, b'\0'))
    symbol = bytearray(encoder.symbol_bytes)
    #fragment_names = []
    matrix = rs_cauchy_coeffs(k, l)
    fragment_meta = {}

    for i in range(c):
        coeffs = matrix[i]
        symbol = encoder.encode_symbol(coeffs)
        name = random_string(8)
        fragment_meta[name] = i
        #fragment_names.append(name)

        task = messages_pb2.StoreData()
        task.filename = name

        send_task_socket.send_multipart([
            task.SerializeToString(), 
            symbol
        ])
    
    for _ in range(c):
        resp = response_socket.recv_string()
        print("Received fragments %s" % resp)
    
    return fragment_meta, matrix


def get_file(coded_fragments, fragment_meta, matrix, file_size, data_req_socket, response_socket, k, l):
    #c = k + l
    #fragnames = copy.deepcopy(coded_fragments)

    #for _ in range(l):
    #    fragnames.remove(random.choice(fragnames))

    #assert len(fragnames) == k
    
    available = [] 
    for fragments in coded_fragments:
        header = messages_pb2.header()
        header.request_type = messages_pb2.FRAGMENT_STATUS_REQ
        request = messages_pb2.Fragment_Status_Request()
        request.fragment_name = fragments
        data_req_socket.send_multipart([
            header.SerializeToString(), 
            request.SerializeToString()
        ])
    
    poller = zmq.Poller()
    poller.register(response_socket, zmq.POLLIN)

    start = time.time()

    while len(available) < k and time.time() - start < 3:
        socks = dict(poller.poll(500)) 
        if response_socket in socks:
            response_status = messages_pb2.Fragment_Status_Response()
            response_status.ParseFromString(response_socket.recv())
            if response_status.is_present:
                available.append(response_status.fragment_name)
    
    if len(available) < k:
        raise Exception("Not enough fragments available to reconstruct the file")
    
    fragnames = available[:k]
    
    for name in fragnames: 
        header = messages_pb2.header()
        header.request_type = messages_pb2.FRAGMENT_DATA_REQ
        task = messages_pb2.GetData()
        task.filename = name
        data_req_socket.send_multipart([
            header.SerializeToString(),
            task.SerializeToString()
        ])

    symbols = []
    #matrix = rs_cauchy_coeffs(k, l)


    while len(symbols) < len(fragnames):
        msg = response_socket.recv_multipart()
        if len(msg) < 3:
            continue

        header = messages_pb2.header()
        header.ParseFromString(msg[0])
        if header.request_type != messages_pb2.FRAGMENT_DATA_REQ:
            continue

        symbols.append({
            "chunkname": msg[1].decode("utf-8"), "data": bytearray(msg[2])
        })

    print("All fragments received!")

    #symbols_num = len(symbols)
    symbol_size = len(symbols[0]["data"])
    field = pyerasure.finite_field.Binary8()

    decoder = pyerasure.Decoder(
        field = field, 
        symbols = k, 
        symbol_bytes = symbol_size
    )

    for i in symbols:
        #coeffx_idx = coded_fragments.index(i["chunkname"])
        coeffx_idx = fragment_meta[i["chunkname"]]
        coeffs = matrix[coeffx_idx]
        decoder.decode_symbol(i["data"], bytearray(coeffs[:k]))
    

    assert decoder.is_complete()
    data_out = bytearray(decoder.block_data())
    return data_out[:file_size]


    