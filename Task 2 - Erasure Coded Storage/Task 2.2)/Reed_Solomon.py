import math
import random
import string
import time

import zmq
import messages_pb2
import pyerasure
import pyerasure.generator
import pyerasure.finite_field

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
    symbol_size = math.ceil(len(file_data)/symbols)
    field = pyerasure.finite_field.Binary8()
    encoder = pyerasure.Encoder(
        field = field, 
        symbols = symbols, 
        symbol_bytes = symbol_size
    )

    encoder.set_symbols(file_data.ljust(symbols * symbol_size, b'\0'))
    symbol = bytearray(encoder.symbol_bytes)
    matrix = rs_cauchy_coeffs(k, l)
    fragment_meta = {}

    for i in range(c):
        coeffs = matrix[i]
        symbol = encoder.encode_symbol(coeffs)
        name = random_string(8)
        fragment_meta[name] = i

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
    
    available_fragments = []
    for fragments in coded_fragments:
        header = messages_pb2.header()
        header.request_type = messages_pb2.FRAGMENT_STATUS_REQ
        task = messages_pb2.Fragment_Status_Request()
        task.fragment_name = fragments
        data_req_socket.send_multipart([
            header.SerializeToString(), 
            task.SerializeToString()
        ])

    poller = zmq.Poller()
    poller.register(response_socket, zmq.POLLIN)

    start_time = time.time()

    while len(available_fragments) < k and time.time() - start_time < 3:
        socks = dict(poller.poll(500))
        if response_socket in socks: 
            response_status = messages_pb2.Fragment_Status_Response()
            response_status.ParseFromString(response_socket.recv())
            if response_status.is_present:
                available_fragments.append(response_status.fragment_name)
        
    if len(available_fragments) < k:
        raise Exception("Not enough fragments to reconstruct the file")
    

    fragnames = available_fragments[:k]

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
    
    print("All fragments received")

    symbol_size = len(symbols[0]["data"])
    field = pyerasure.finite_field.Binary8()
    decoder = pyerasure.Decoder(
        field = field, 
        symbols = k, 
        symbol_bytes=symbol_size
    )

    for i in symbols:
        coeffx_idx = fragment_meta[i["chunkname"]]
        coeffx = matrix[coeffx_idx]
        decoder.decode_symbol(i["data"], bytearray(coeffx[:k]))
    
    assert decoder.is_complete()
    data_out = bytearray(decoder.block_data())
    return data_out[:file_size]

