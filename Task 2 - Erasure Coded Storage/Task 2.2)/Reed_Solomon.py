import math
import random
import string
import time

import zmq
import messages_pb2
import pyerasure
import pyerasure.generator
import pyerasure.finite_field

# Generate random string names for the stored fragments
def random_string(length = 8):
    return ''.join(random.SystemRandom().choice(string.ascii_letters) for _ in range(length))

# Generate Reed-Solomon Cauchy matrix coefficients
def rs_cauchy_coeffs(k, l):

    # Through pyerasure.finite_field we can create a finite field instance that is GF(2^8)
    field = pyerasure.finite_field.Binary8()

    # Based on the field and number of data symbols k, we can create a RSCauchy generator instance that 
    # can generate the coefficients for us
    generator = pyerasure.generator.RSCauchy(field, k)
    matrix = []

    # First k rows are identity matrix
    for i in range(k):
        row = bytearray(k)
        row[i] = 1
        matrix.append(row)
    
    # Next l rows are generated using the RSCauchy generator
    for _ in range(l):
        coeffs, _ = generator.generate()
        matrix.append(coeffs)
    
    return matrix

"""
    Store file by encoding it into k + l fragments and sending them to storage node

    Params:
    - file_data: bytearray of the file data to be stored
    - send_task_socket: ZMQ socket to send store requests to storage nodes
    - response_socket: ZMQ socket to receive responses from storage nodes
    - k: number of data fragments
    - l: number of parity fragments
    - storage_nodes_count: total number of available storage nodes
"""
def store_file(file_data, send_task_socket, response_socket, k, l, storage_nodes_count):

    # fragment data will be stored as bytearray, since pyerasure works with bytearrays
    file_data = bytearray(file_data)

    # total fragments
    c = k + l
    
    # Ensure we do not try to create more fragments than available storage nodes
    assert c >= 0
    assert c <= storage_nodes_count

    # In this section we set up the encoder using pyerasure that will help us encode the file data into fragments
    # The Encoder takes in the finite field, number of symbols (fragments), and size of each symbol in bytes
    symbols = k
    symbol_size = math.ceil(len(file_data)/symbols)
    field = pyerasure.finite_field.Binary8()
    encoder = pyerasure.Encoder(
        field = field, 
        symbols = symbols, 
        symbol_bytes = symbol_size
    )

    # Assign the file data to the encoder so that we can generate encoded symbols (fragments)
    encoder.set_symbols(file_data.ljust(symbols * symbol_size, b'\0'))
    symbol = bytearray(encoder.symbol_bytes)
    matrix = rs_cauchy_coeffs(k, l)
    fragment_meta = {}

    for i in range(c):
        # Generate one coded fragment for each row in the matrix
        coeffs = matrix[i]
        # Perform encoding using linear combination of data symbols with the coefficients
        symbol = encoder.encode_symbol(coeffs)
        name = random_string(8)
        fragment_meta[name] = i

        # Send StoreData request to the storage nodes
        task = messages_pb2.StoreData()
        task.filename = name

        send_task_socket.send_multipart([
            task.SerializeToString(),
            symbol
        ])

    for _ in range(c):
        # Wait for confirmation responses from storage nodes
        resp = response_socket.recv_string()
        print("Received fragments %s" % resp)
    
    return fragment_meta, matrix


"""
    Retrieve and reconstruct file from available fragments

    Params:
    - coded_fragments: list of fragment names that were stored
    - fragment_meta: dictionary mapping fragment names to their indices
    - matrix: encoding coefficient matrix used for generating fragments
    - file_size: original size of the file to be reconstructed
    - data_req_socket: ZMQ socket to send data requests to storage nodes
    - response_socket: ZMQ socket to receive responses from storage nodes
    - k: number of data fragments
"""
def get_file(coded_fragments, fragment_meta, matrix, file_size, data_req_socket, response_socket, k, l):
    
    # Check which fragments are available from the storage nodes
    available_fragments = []

    # For each coded fragment, send a multipart message that consists of a header, signifying the type of request
    # and the fragment name we are checking for
    for fragments in coded_fragments:
        header = messages_pb2.header()
        header.request_type = messages_pb2.FRAGMENT_STATUS_REQ
        task = messages_pb2.Fragment_Status_Request()
        task.fragment_name = fragments
        data_req_socket.send_multipart([
            header.SerializeToString(), 
            task.SerializeToString()
        ])

    # Set up a poller to wait for responses from storage nodes
    poller = zmq.Poller()
    poller.register(response_socket, zmq.POLLIN)

    # Wait up to 3 seconds to gather available fragments from storage nodes
    start_time = time.time()

    # Keep polling until we have k available fragments or timeout
    while len(available_fragments) < k and time.time() - start_time < 3:
        socks = dict(poller.poll(500))
        if response_socket in socks: 
            # If we have a response from a storage node then check the status response
            # and if the fragment is present, add it to the available list
            response_status = messages_pb2.Fragment_Status_Response()
            response_status.ParseFromString(response_socket.recv())
            if response_status.is_present:
                available_fragments.append(response_status.fragment_name)

    # Check the MDS property: we need at least k fragments to reconstruct the file    
    if len(available_fragments) < k:
        raise Exception("Not enough fragments to reconstruct the file")
    
    # Select the k available fragments
    fragnames = available_fragments[:k]

    for name in fragnames:
        # For each available fragment, we send a multipart message consisting of a header, 
        # and the fragment name we are requesting
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
        # Wait for fragment data responses from storage nodes
        msg = response_socket.recv_multipart()
        if len(msg) < 3: 
            continue

        # Parse the header to ensure it is a fragment data response
        header = messages_pb2.header()
        header.ParseFromString(msg[0])
        if header.request_type != messages_pb2.FRAGMENT_DATA_REQ:
            continue

        # Store the received fragment data along with its name
        symbols.append({
            "chunkname": msg[1].decode("utf-8"), 
            "data": bytearray(msg[2])
        })
    
    print("All fragments received")

    # Reconstruct the original data with a decoder
    symbol_size = len(symbols[0]["data"])
    field = pyerasure.finite_field.Binary8()
    decoder = pyerasure.Decoder(
        field = field, 
        symbols = k, 
        symbol_bytes=symbol_size
    )

    for i in symbols:
        # Find which coefficient vector produced this fragment by looking chunkname up in fragment_meta
        coeffx_idx = fragment_meta[i["chunkname"]]
        coeffx = matrix[coeffx_idx]

        # Use the decoder to decode the symbol using the corresponding coefficients
        decoder.decode_symbol(i["data"], bytearray(coeffx[:k]))
    
    # Make sure the decoder has successfully reconstructed the data
    assert decoder.is_complete()
    data_out = bytearray(decoder.block_data())
    return data_out[:file_size]

