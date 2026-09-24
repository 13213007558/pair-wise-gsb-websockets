from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Sequence
from typing import cast

from ..datastructures import Headers
from ..exceptions import (
    InvalidHandshake,
    InvalidHeader,
    InvalidHeaderValue,
    InvalidMethod,
    InvalidProtocol,
    InvalidState,
)
from ..extensions import ServerExtensionFactory
from ..protocol import OPEN
from ..server import ServerProtocol
from ..typing import LoggerLike, Origin, StatusLike, Subprotocol
from .connection import Connection, WebSocketStream
from .events import WebSocketRequest, WebSocketReset


try:
    import h2.events
except ImportError:  # pragma: no cover
    raise ImportError(
        "websockets.http2 requires the h2 package; "
        "install it with: python -m pip install h2"
    ) from None


__all__ = ["ServerConnection"]


class ServerConnection(Connection):
    """
    Sans-I/O implementation of a WebSocket over HTTP/2 server connection.

    The handshake uses the Extended CONNECT method defined in :rfc:`8441`
    instead of the HTTP/1.1 Upgrade mechanism.

    Usage:

    1. :meth:`~websockets.http2.connection.Connection.initiate` the
       connection and exchange data.
    2. When a :class:`~websockets.http2.events.WebSocketRequest` event
       arrives, call :meth:`accept` or :meth:`reject` for that
       stream.

    Args:
        origins: Acceptable values of the ``Origin`` header, like in
            :class:`websockets.server.ServerProtocol`.
        extensions: List of supported extensions, in order in which they
            should be tried.
        subprotocols: List of supported subprotocols, in order of decreasing
            preference.
        select_subprotocol: Callback for selecting a subprotocol among those
            supported by the client and the server.
        max_size: Maximum size of incoming WebSocket messages in bytes.
        logger: Logger for this connection;
            defaults to ``logging.getLogger("websockets.server")``.

    """

    def __init__(
        self,
        *,
        origins: Sequence[Origin | re.Pattern[str] | None] | None = None,
        extensions: Sequence[ServerExtensionFactory] | None = None,
        subprotocols: Sequence[Subprotocol] | None = None,
        select_subprotocol: (
            Callable[
                [ServerProtocol, Sequence[Subprotocol]],
                Subprotocol | None,
            ]
            | None
        ) = None,
        max_size: int | None | tuple[int | None, int | None] = 2**20,
        logger: LoggerLike | None = None,
    ) -> None:
        if logger is None:
            logger = logging.getLogger("websockets.server")
        super().__init__(client_side=False, max_size=max_size, logger=logger)
        self.origins = origins
        self.available_extensions = extensions
        self.available_subprotocols = subprotocols
        self.select_subprotocol_callback = select_subprotocol
        # Handshake requests awaiting accept() or reject().
        self.pending: dict[int, Headers] = {}

    def accept(self, stream_id: int) -> None:
        """
        Accept a WebSocket handshake request received on an HTTP/2 stream.

        Validate the Extended CONNECT request, negotiate extensions and
        subprotocol, and send a 200 response. The stream is then ready to
        exchange WebSocket frames.

        Args:
            stream_id: Identifier of the HTTP/2 stream carrying the handshake.

        Raises:
            InvalidHandshake: If the handshake request is invalid; then the
                application may reject it with :meth:`reject`.
            InvalidState: If there's no pending handshake on this stream.

        """
        try:
            headers = self.pending.pop(stream_id)
        except KeyError:
            raise InvalidState(
                f"no pending WebSocket handshake on stream {stream_id}"
            ) from None

        protocol = ServerProtocol(
            origins=self.origins,
            extensions=self.available_extensions,
            subprotocols=self.available_subprotocols,
            select_subprotocol=self.select_subprotocol_callback,
            state=OPEN,
            max_size=self.max_size,
            logger=self.logger,
        )

        # Validate the Extended CONNECT pseudo-headers (RFC 8441, section 4).
        method = headers.get(":method", "")
        if method != "CONNECT":
            raise InvalidMethod(method)
        protocol_header = headers.get(":protocol", "")
        if protocol_header != "websocket":
            raise InvalidProtocol(protocol_header)
        for pseudo_header in (":scheme", ":authority", ":path"):
            if headers.get(pseudo_header) is None:
                raise InvalidHandshake(f"missing {pseudo_header} pseudo-header")
        try:
            version = headers["sec-websocket-version"]
        except KeyError:
            raise InvalidHeader("Sec-WebSocket-Version") from None
        if version != "13":
            raise InvalidHeaderValue("Sec-WebSocket-Version", version)

        # Reuse the HTTP/1.1 negotiation logic for origin, extensions, and
        # subprotocol; only extensions explicitly supported by both sides
        # are enabled.
        protocol.origin = protocol.process_origin(headers)
        extensions_header, protocol.extensions = protocol.process_extensions(headers)
        protocol.subprotocol = protocol.process_subprotocol(headers)

        response_headers = [(":status", "200")]
        if extensions_header is not None:
            response_headers.append(("sec-websocket-extensions", extensions_header))
        if protocol.subprotocol is not None:
            response_headers.append(("sec-websocket-protocol", protocol.subprotocol))
        self.h2.send_headers(stream_id, response_headers)

        stream = WebSocketStream(self, stream_id, protocol)
        stream.handshake_done = True
        self.streams[stream_id] = stream

    def reject(
        self,
        stream_id: int,
        status: StatusLike = 400,
        *,
        headers: Headers | None = None,
        body: bytes = b"",
    ) -> None:
        """
        Reject a WebSocket handshake request with a regular HTTP response.

        Args:
            stream_id: Identifier of the HTTP/2 stream carrying the handshake.
            status: HTTP status code of the response.
            headers: Additional response headers.
            body: Response body.

        """
        self.pending.pop(stream_id, None)
        response_headers = [(":status", str(int(status)))]
        if headers is not None:
            response_headers += list(headers.raw_items())
        self.h2.send_headers(stream_id, response_headers, end_stream=not body)
        if body:
            self.h2.send_data(stream_id, body, end_stream=True)

    def _request_received(self, event: h2.events.RequestReceived) -> None:
        # header_encoding="utf-8" guarantees that headers are str tuples.
        headers = Headers(cast(Iterable[tuple[str, str]], event.headers))
        if (
            headers.get(":method") == "CONNECT"
            and headers.get(":protocol") == "websocket"
        ):
            self.pending[event.stream_id] = headers
            self.events.append(WebSocketRequest(event.stream_id, headers))
        else:
            # Regular HTTP/2 stream driven by the application.
            self.events.append(event)

    def _stream_reset(self, event: h2.events.StreamReset) -> None:
        if event.stream_id in self.pending:
            del self.pending[event.stream_id]
            self.events.append(WebSocketReset(event.stream_id, event.error_code))
            return
        super()._stream_reset(event)

    def _terminate(self, error_code: int | None, reason: str) -> None:
        self.pending.clear()
        super()._terminate(error_code, reason)
