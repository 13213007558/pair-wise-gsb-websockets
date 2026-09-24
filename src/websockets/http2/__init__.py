"""
WebSocket over HTTP/2 (RFC 8441) adapter based on hyper-h2.

This package provides Sans-I/O client and server adapters that establish
WebSocket connections with Extended CONNECT requests over an HTTP/2
connection, reusing the WebSocket protocol implementation of this library.

It requires the optional h2 dependency; install it with:
python -m pip install 'websockets[http2]'

"""

from .client import H2ClientConnection, H2ClientWebSocket
from .connection import (
    ConnectionTerminated,
    H2Connection,
    StreamEnded,
    StreamReset,
    WebSocketRequest,
    WebSocketResponse,
)
from .server import H2ServerConnection, H2ServerWebSocket


__all__ = [
    "ConnectionTerminated",
    "H2ClientConnection",
    "H2ClientWebSocket",
    "H2Connection",
    "H2ServerConnection",
    "H2ServerWebSocket",
    "StreamEnded",
    "StreamReset",
    "WebSocketRequest",
    "WebSocketResponse",
]
