from __future__ import annotations

import dataclasses

from ..datastructures import Headers
from ..frames import Frame


__all__ = [
    "WebSocketRequest",
    "WebSocketOpen",
    "WebSocketHandshakeFailed",
    "WebSocketFrame",
    "WebSocketWritable",
    "WebSocketClosed",
    "WebSocketEndOfStream",
    "WebSocketReset",
    "ConnectionClosed",
]


@dataclasses.dataclass(frozen=True)
class WebSocketRequest:
    """
    A WebSocket handshake request was received on an HTTP/2 stream.

    The request uses the Extended CONNECT method defined in :rfc:`8441`.
    The application must accept it with
    :meth:`~websockets.http2.server.ServerConnection.accept` or reject it with
    :meth:`~websockets.http2.server.ServerConnection.reject`.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the handshake."""

    headers: Headers
    """Request headers, including the ``:method``, ``:protocol``, ``:scheme``,
    ``:authority``, and ``:path`` pseudo-headers."""


@dataclasses.dataclass(frozen=True)
class WebSocketOpen:
    """
    The server accepted the WebSocket handshake with a 2xx status code.

    The stream is ready to exchange WebSocket frames.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the WebSocket connection."""

    status_code: int
    """Status code of the handshake response, in the 2xx range."""

    headers: Headers
    """Response headers."""


@dataclasses.dataclass(frozen=True)
class WebSocketHandshakeFailed:
    """
    The WebSocket handshake failed on an HTTP/2 stream.

    Two failures are reported with this event:

    - the server responded with a non-2xx status code; then
      :attr:`status_code`, :attr:`headers`, and :attr:`body` are set;
    - the response was unacceptable, e.g. extension negotiation failed;
      then :attr:`exception` is set.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the handshake."""

    status_code: int | None = None
    """Status code of the handshake response, if a response was received."""

    headers: Headers | None = None
    """Response headers, if a response was received."""

    body: bytes = b""
    """Body of the handshake response, if any."""

    exception: Exception | None = None
    """Error that made the handshake response unacceptable, if any."""


@dataclasses.dataclass(frozen=True)
class WebSocketFrame:
    """
    A WebSocket frame was parsed from DATA frames of an HTTP/2 stream.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the WebSocket connection."""

    frame: Frame
    """WebSocket frame."""


@dataclasses.dataclass(frozen=True)
class WebSocketWritable:
    """
    A stream whose sends were paused by HTTP/2 flow control flushed all
    pending data; sending may resume.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the WebSocket connection."""


@dataclasses.dataclass(frozen=True)
class WebSocketClosed:
    """
    The WebSocket closing handshake completed and the HTTP/2 stream ended.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the WebSocket connection."""

    code: int
    """WebSocket close code."""

    reason: str
    """WebSocket close reason."""


@dataclasses.dataclass(frozen=True)
class WebSocketEndOfStream:
    """
    The HTTP/2 stream ended (END_STREAM) without a complete WebSocket
    closing handshake; this is an abnormal closure.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the WebSocket connection."""


@dataclasses.dataclass(frozen=True)
class WebSocketReset:
    """
    The HTTP/2 stream was reset (RST_STREAM); other streams are unaffected.

    """

    stream_id: int
    """Identifier of the HTTP/2 stream carrying the WebSocket connection."""

    error_code: int
    """HTTP/2 error code from the RST_STREAM frame."""


@dataclasses.dataclass(frozen=True)
class ConnectionClosed:
    """
    The entire HTTP/2 connection terminated: the peer sent GOAWAY, a
    connection-level error occurred, or :meth:`receive_eof` was called.

    All WebSocket streams open on the connection are lost.

    """

    error_code: int | None
    """HTTP/2 error code, if the connection terminated with an error."""

    reason: str
    """Debug data provided by the peer, if any."""
