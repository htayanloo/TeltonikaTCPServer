import socket
import struct
import threading
from datetime import datetime

def start_server(host='0.0.0.0', port=5000):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Server is listening on {host}:{port}")

    while True:
        client_socket, addr = server_socket.accept()
        print(f"New connection from {addr}")
        threading.Thread(target=handle_client, args=(client_socket, addr)).start()

def handle_client(client_socket, addr):
    """
    Handles communication with a Teltonika device.
    """
    print(f"Connection established with {addr}")
    try:
        # Set a timeout for the client socket
        client_socket.settimeout(60)

        # Step 1: Receive preamble (2 bytes) to determine IMEI length
        preamble = receive_all(client_socket, 2)
        if not preamble or len(preamble) < 2:
            print(f"Incomplete preamble received from {addr}")
            return

        # The second byte of the preamble indicates the IMEI length
        imei_length = preamble[1]
        imei_data = receive_all(client_socket, imei_length)
        if not imei_data or len(imei_data) < imei_length:
            print(f"Incomplete IMEI received from {addr}")
            return

        # Decode the IMEI
        imei = imei_data.decode('utf-8', errors='ignore')
        print(f"IMEI received from {addr}: {imei}")

        # Step 2: Send acknowledgment (0x01) to the device
        client_socket.send(struct.pack('!B', 0x01))

        # Step 3: Enter a loop to receive AVL data packets
        while True:
            # Receive data length (4 bytes)
            data_length_bytes = receive_all(client_socket, 4)
            if not data_length_bytes or len(data_length_bytes) < 4:
                print(f"Connection from {addr} closed (no data length received).")
                break

            data_length = struct.unpack('!I', data_length_bytes)[0]
            print(f"Data length from {addr}: {data_length}")

            if data_length == 0:
                print(f"No data received from {addr}. Waiting for data...")
                continue  # Continue waiting for data

            # Receive the AVL data packet
            data = receive_all(client_socket, data_length)
            if not data or len(data) < data_length:
                print(f"Incomplete AVL data received from {addr}")
                break

            # Step 4: Process AVL data
            records, codec_id = process_avl_data(data)
            if records is None:
                print(f"Failed to process AVL data from {addr}")
                break

            # Print or store the received records
            for record in records:
                print(f"Record received from {addr}: {record}")

            # Step 5: Send acknowledgment (number of accepted records)
            num_records = len(records)
            response = struct.pack('!I', num_records)
            client_socket.send(response)
            print(f"Acknowledgment sent to {addr} for {num_records} records.")

    except socket.timeout:
        print(f"Connection with {addr} timed out.")
    except Exception as e:
        print(f"Error with {addr}: {e}")
    finally:
        client_socket.close()
        print(f"Connection with {addr} closed.")

def receive_all(sock, length):
    """
    Receives exactly the specified number of bytes from the socket.
    """
    data = b''
    while len(data) < length:
        try:
            more = sock.recv(length - len(data))
            if not more:
                return None
            data += more
        except socket.timeout:
            return None
    return data

def process_avl_data(data):
    """
    Parses AVL data and returns a list of records and the codec ID.
    """
    offset = 0
    records = []

    # Parse the header
    codec_id = data[offset]
    offset += 1

    # Number of data records (1 byte for Codec8, 2 bytes for Codec8 Extended)
    if codec_id == 0x08:
        record_count = data[offset]
        offset += 1
    elif codec_id == 0x8E:
        record_count = struct.unpack('!H', data[offset:offset+2])[0]
        offset += 2
    else:
        print(f"Unsupported codec ID: {codec_id}")
        return None, codec_id

    print(f"Codec ID: {codec_id}, Number of records: {record_count}")

    for _ in range(record_count):
        # Parse a single record
        record, size = parse_record(data[offset:], codec_id)
        if record is None:
            print("Failed to parse record.")
            return None, codec_id
        records.append(record)
        offset += size

    # CRC (4 bytes at the end of the packet)
    # You may implement CRC check here if necessary

    return records, codec_id

def parse_record(data, codec_id):
    """
    Parses a single AVL record.
    """
    offset = 0

    # Timestamp (8 bytes)
    timestamp = struct.unpack('!Q', data[offset:offset+8])[0]
    offset += 8

    # Convert timestamp to readable format
    timestamp = datetime.utcfromtimestamp(timestamp / 1000)

    # Priority (1 byte)
    priority = data[offset]
    offset += 1

    # GPS Element (15 bytes)
    gps_element = parse_gps_element(data[offset:offset+15])
    offset += 15

    # IO Element
    io_element, io_size = parse_io_element(data[offset:], codec_id)
    offset += io_size

    record = {
        'timestamp': timestamp.isoformat(),
        'priority': priority,
        'gps': gps_element,
        'io': io_element,
    }

    return record, offset

def parse_gps_element(data):
    """
    Parses the GPS element and returns a dictionary of values.
    """
    gps_element = {}

    # Longitude and latitude (each 4 bytes)
    longitude = struct.unpack('!i', data[0:4])[0] / 10000000
    latitude = struct.unpack('!i', data[4:8])[0] / 10000000

    # Altitude (2 bytes)
    altitude = struct.unpack('!h', data[8:10])[0]

    # Angle (2 bytes)
    angle = struct.unpack('!H', data[10:12])[0]

    # Satellites (1 byte)
    satellites = data[12]

    # Speed (2 bytes)
    speed = struct.unpack('!H', data[13:15])[0]

    gps_element['longitude'] = longitude
    gps_element['latitude'] = latitude
    gps_element['altitude'] = altitude
    gps_element['angle'] = angle
    gps_element['satellites'] = satellites
    gps_element['speed'] = speed

    return gps_element

def parse_io_element(data, codec_id):
    """
    Parses the IO element and returns a dictionary of values and the data size consumed.
    """
    io_element = {}
    offset = 0

    if codec_id == 0x08:
        # Codec8 IO parsing
        event_id = data[offset]
        offset += 1

        element_count = data[offset]
        offset += 1

        # 1-byte IO elements
        n1 = data[offset]
        offset += 1
        for _ in range(n1):
            io_id = data[offset]
            offset += 1
            io_value = data[offset]
            offset += 1
            io_element[io_id] = io_value

        # 2-byte IO elements
        n2 = data[offset]
        offset += 1
        for _ in range(n2):
            io_id = data[offset]
            offset += 1
            io_value = struct.unpack('!H', data[offset:offset+2])[0]
            offset += 2
            io_element[io_id] = io_value

        # 4-byte IO elements
        n4 = data[offset]
        offset += 1
        for _ in range(n4):
            io_id = data[offset]
            offset += 1
            io_value = struct.unpack('!I', data[offset:offset+4])[0]
            offset += 4
            io_element[io_id] = io_value

        # 8-byte IO elements
        n8 = data[offset]
        offset += 1
        for _ in range(n8):
            io_id = data[offset]
            offset += 1
            io_value = struct.unpack('!Q', data[offset:offset+8])[0]
            offset += 8
            io_element[io_id] = io_value

    elif codec_id == 0x8E:
        # Codec8 Extended IO parsing
        event_id = data[offset]
        offset += 1

        properties_count = data[offset]
        offset += 1

        # Parse properties
        for _ in range(properties_count):
            io_id = data[offset]
            offset += 1
            io_value_length = data[offset]
            offset += 1
            if io_value_length == 1:
                io_value = data[offset]
                offset += 1
            elif io_value_length == 2:
                io_value = struct.unpack('!H', data[offset:offset+2])[0]
                offset += 2
            elif io_value_length == 4:
                io_value = struct.unpack('!I', data[offset:offset+4])[0]
                offset += 4
            elif io_value_length == 8:
                io_value = struct.unpack('!Q', data[offset:offset+8])[0]
                offset += 8
            else:
                # Skip unsupported IO value lengths
                offset += io_value_length
                continue
            io_element[io_id] = io_value

    else:
        print(f"Unsupported codec ID for IO parsing: {codec_id}")
        return io_element, 0

    total_size = offset
    return io_element, total_size

if __name__ == '__main__':
    start_server()
