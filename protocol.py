"""
protocol.py - Message protocol for TCP communication between services.

All inter-service communication uses JSON messages over TCP.
This module provides helpers to send and receive length-prefixed messages,
preventing partial reads and ensuring complete message delivery.

Protocol format:
    [4 bytes: message length as big-endian int][JSON payload]
"""

import json
import struct
import socket
from common.config import BUFFER_SIZE, ENCODING


"""    
1. Serialize the dict to JSON bytes
2. Prepend 4-byte length header (big-endian unsigned int)
3. Send the entire frame
"""

def send_message(sock: socket.socket, message: dict) -> None:

    data = json.dumps(message).encode(ENCODING)
    header = struct.pack("!I", len(data))
    sock.sendall(header + data)

"""
Receive a length-prefixed JSON message from a TCP socket.
1. Read exactly 4 bytes for the length header
2. Read exactly that many bytes for the payload
3. Deserialize JSON back to a dict
Returns:
    Deserialized dictionary from the JSON payload   
Raises:
    ConnectionError: If the connection is closed mid-message
"""

def receive_message(sock: socket.socket) -> dict:

    # Read the 4-byte length header
    header = _recv_exact(sock, 4)
    if not header:
        raise ConnectionError("Connection closed while reading header")
    msg_length = struct.unpack("!I", header)[0]
    
    # Read exactly msg_length bytes of payload
    data = _recv_exact(sock, msg_length)
    if not data:
        raise ConnectionError("Connection closed while reading payload")
    
    return json.loads(data.decode(ENCODING))


"""
Read exactly num_bytes from the socket.
TCP is a stream protocol — a single recv() call may return fewer
bytes than requested. This function loops until all bytes arrive.
Returns:
    Bytes read, or empty bytes if connection closed
"""
def _recv_exact(sock: socket.socket, num_bytes: int) -> bytes:

    chunks = []
    bytes_received = 0
    while bytes_received < num_bytes:
        chunk = sock.recv(min(num_bytes - bytes_received, BUFFER_SIZE))
        if not chunk:
            return b""
        chunks.append(chunk)
        bytes_received += len(chunk)
    return b"".join(chunks)


"""
Create a  request message.
Every message has an "action" field that tells the
receiver what operation to perform
Args:
    action: The operation name (e.g., "book", "cancel", "check_availability")
    **kwargs: Additional fields for the request
Returns:
    Formatted request dictionary
"""
def create_request(action: str, **kwargs) -> dict:

    msg = {"action": action}
    msg.update(kwargs)
    return msg


"""
Create a standardized response message.   
Returns:
    Formatted response dictionary
"""
def create_response(status: str, **kwargs) -> dict:

    msg = {"status": status}
    msg.update(kwargs)
    return msg
