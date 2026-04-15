"""
protocol.py - Message protocol for TCP communication between services.

All inter-service communication uses JSON messages over TCP.
This module provides helpers to send and receive length-prefixed messages,
preventing partial reads and ensuring complete message delivery.

Protocol format:
    [4 bytes: message length as big-endian int][JSON payload]

This is a common pattern in distributed systems to frame messages
over a stream-oriented protocol like TCP.
"""

import json
import struct
import socket
from common.config import BUFFER_SIZE, ENCODING


def send_message(sock: socket.socket, message: dict) -> None:
    """
    Send a JSON message over a TCP socket with length-prefix framing.
    
    Steps:
    1. Serialize the dict to JSON bytes
    2. Prepend 4-byte length header (big-endian unsigned int)
    3. Send the entire frame
    
    Args:
        sock: Connected TCP socket
        message: Dictionary to send (must be JSON-serializable)
    """
    data = json.dumps(message).encode(ENCODING)
    # Pack the length as a 4-byte big-endian unsigned integer
    header = struct.pack("!I", len(data))
    sock.sendall(header + data)


def receive_message(sock: socket.socket) -> dict:
    """
    Receive a length-prefixed JSON message from a TCP socket.
    
    Steps:
    1. Read exactly 4 bytes for the length header
    2. Read exactly that many bytes for the payload
    3. Deserialize JSON back to a dict
    
    Args:
        sock: Connected TCP socket
        
    Returns:
        Deserialized dictionary from the JSON payload
        
    Raises:
        ConnectionError: If the connection is closed mid-message
    """
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


def _recv_exact(sock: socket.socket, num_bytes: int) -> bytes:
    """
    Read exactly num_bytes from the socket.
    
    TCP is a stream protocol — a single recv() call may return fewer
    bytes than requested. This function loops until all bytes arrive.
    
    Args:
        sock: Connected TCP socket
        num_bytes: Exact number of bytes to read
        
    Returns:
        Bytes read, or empty bytes if connection closed
    """
    chunks = []
    bytes_received = 0
    while bytes_received < num_bytes:
        chunk = sock.recv(min(num_bytes - bytes_received, BUFFER_SIZE))
        if not chunk:
            return b""
        chunks.append(chunk)
        bytes_received += len(chunk)
    return b"".join(chunks)


def create_request(action: str, **kwargs) -> dict:
    """
    Create a standardized request message.
    
    Every message in our system has an "action" field that tells the
    receiver what operation to perform, plus additional fields as needed.
    
    Args:
        action: The operation name (e.g., "book", "cancel", "check_availability")
        **kwargs: Additional fields for the request
        
    Returns:
        Formatted request dictionary
    """
    msg = {"action": action}
    msg.update(kwargs)
    return msg


def create_response(status: str, **kwargs) -> dict:
    """
    Create a standardized response message.   
    Returns:
        Formatted response dictionary
    """
    msg = {"status": status}
    msg.update(kwargs)
    return msg
