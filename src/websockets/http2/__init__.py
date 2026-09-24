"""
:mod:`websockets.http2` provides WebSocket over HTTP/2 (:rfc:`8441`).

It adapts the Sans-I/O WebSocket protocol implementation to HTTP/2
connections managed with h2_, the HTTP/2 protocol stack.

.. _h2: https://python-hyper.org/projects/h2/

WebSocket connections are established with the Extended CONNECT method and
multiplexed over a single HTTP/2 connection, alongside regular HTTP/2
streams.

This subpackage requires the h2_ package, which isn't a dependency of
websockets. Install it with ``python -m pip install h2``.

"""

from .client import ClientConnection
from .connection import Connection
from .events import (
    ConnectionClosed,
    WebSocketClosed,
    WebSocketEndOfStream,
    WebSocketFrame,
    WebSocketHandshakeFailed,
    WebSocketOpen,
    WebSocketRequest,
    WebSocketReset,
    WebSocketWritable,
)
from .server import ServerConnection


__all__ = [
    "ClientConnection",
    "Connection",
    "ConnectionClosed",
    "ServerConnection",
    "WebSocketClosed",
    "WebSocketEndOfStream",
    "WebSocketFrame",
    "WebSocketHandshakeFailed",
    "WebSocketOpen",
    "WebSocketRequest",
    "WebSocketReset",
    "WebSocketWritable",
]
