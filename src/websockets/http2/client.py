from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import cast

from ..client import ClientProtocol
from ..datastructures import Headers
from ..exceptions import InvalidHandshake, InvalidState
from ..extensions import ClientExtensionFactory
from ..headers import (
    build_authorization_basic,
    build_extension,
    build_host,
    build_subprotocol,
)
from ..protocol import OPEN
from ..typing import LoggerLike, Origin, Subprotocol
from ..uri import WebSocketURI
from .connection import Connection, WebSocketStream
from .events import WebSocketHandshakeFailed, WebSocketOpen, WebSocketReset


try:
    import h2.events
except ImportError:  # pragma: no cover
    raise ImportError(
        "websockets.http2 requires the h2 package; "
        "install it with: python -m pip install h2"
    ) from None


__all__ = ["ClientConnection"]


class ClientConnection(Connection):
    """
    Sans-I/O implementation of a WebSocket over HTTP/2 client connection.

    The handshake uses the Extended CONNECT method defined in :rfc:`8441`
    instead of the HTTP/1.1 Upgrade mechanism.

    Usage:

    1. :meth:`~websockets.http2.connection.Connection.initiate` the
       connection and exchange data until
       :attr:`~websockets.http2.connection.Connection.extended_connect_enabled`
       is :obj:`True`.
    2. Call :meth:`connect` for each WebSocket connection to open.
    3. Process events; a :class:`~websockets.http2.events.WebSocketOpen`
       event confirms that the WebSocket connection is established.

    Args:
        max_size: Maximum size of incoming WebSocket messages in bytes.
        logger: Logger for this connection;
            defaults to ``logging.getLogger("websockets.client")``.

    """

    def __init__(
        self,
        *,
        max_size: int | None | tuple[int | None, int | None] = 2**20,
        logger: LoggerLike | None = None,
    ) -> None:
        if logger is None:
            logger = logging.getLogger("websockets.client")
        super().__init__(client_side=True, max_size=max_size, logger=logger)
        # Handshakes rejected with a non-2xx status code, awaiting the
        # response body and the end of the stream.
        self.failed_handshakes: dict[int, tuple[int, Headers, bytearray]] = {}

    def connect(
        self,
        uri: WebSocketURI,
        *,
        origin: Origin | None = None,
        extensions: Sequence[ClientExtensionFactory] | None = None,
        subprotocols: Sequence[Subprotocol] | None = None,
    ) -> int:
        """
        Send a WebSocket handshake request on a new HTTP/2 stream.

        The request uses the Extended CONNECT method: ``:method`` is
        CONNECT, ``:protocol`` is websocket, and ``:scheme``,
        ``:authority``, and ``:path`` identify the target.
        There are no Upgrade, Connection, or Sec-WebSocket-Key headers.

        Args:
            uri: URI of the WebSocket server, parsed
                with :func:`~websockets.uri.parse_uri`.
            origin: Value of the ``Origin`` header.
            extensions: List of supported extensions, in order in which they
                should be tried.
            subprotocols: List of supported subprotocols, in order of
                decreasing preference.

        Returns:
            Identifier of the HTTP/2 stream carrying the handshake.

        Raises:
            InvalidState: If the server didn't advertise
                ENABLE_CONNECT_PROTOCOL yet.

        """
        if not self.extended_connect_enabled:
            raise InvalidState("server didn't advertise ENABLE_CONNECT_PROTOCOL")
        protocol = ClientProtocol(
            uri,
            origin=origin,
            extensions=extensions,
            subprotocols=subprotocols,
            state=OPEN,
            max_size=self.max_size,
            logger=self.logger,
        )
        headers = [
            (":method", "CONNECT"),
            (":protocol", "websocket"),
            (":scheme", "https" if uri.secure else "http"),
            (":authority", build_host(uri.host, uri.port, uri.secure)),
            (":path", uri.resource_name),
            ("sec-websocket-version", "13"),
        ]
        if uri.user_info:
            headers.append(("authorization", build_authorization_basic(*uri.user_info)))
        if origin is not None:
            headers.append(("origin", origin))
        if extensions is not None:
            headers.append(
                (
                    "sec-websocket-extensions",
                    build_extension(
                        [
                            (factory.name, factory.get_request_params())
                            for factory in extensions
                        ]
                    ),
                )
            )
        if subprotocols is not None:
            headers.append(("sec-websocket-protocol", build_subprotocol(subprotocols)))
        stream_id = self.h2.get_next_available_stream_id()
        self.h2.send_headers(stream_id, headers)
        self.streams[stream_id] = WebSocketStream(self, stream_id, protocol)
        return stream_id

    def _response_received(self, event: h2.events.ResponseReceived) -> None:
        stream = self.streams.get(event.stream_id)
        if stream is None:
            # Regular HTTP/2 stream driven by the application.
            self.events.append(event)
            return
        # header_encoding="utf-8" guarantees that headers are str tuples.
        headers = Headers(cast(Iterable[tuple[str, str]], event.headers))
        try:
            status_code = int(headers[":status"])
        except (KeyError, ValueError) as exc:
            del self.streams[event.stream_id]
            self.h2.reset_stream(event.stream_id)
            self.events.append(WebSocketHandshakeFailed(event.stream_id, exception=exc))
            return
        # RFC 8441: any 2xx status code indicates a successful handshake.
        if 200 <= status_code < 300:
            protocol = stream.protocol
            assert isinstance(protocol, ClientProtocol)
            try:
                protocol.extensions = protocol.process_extensions(headers)
                protocol.subprotocol = protocol.process_subprotocol(headers)
            except InvalidHandshake as exc:
                del self.streams[event.stream_id]
                self.h2.reset_stream(event.stream_id)
                self.events.append(
                    WebSocketHandshakeFailed(
                        event.stream_id,
                        status_code,
                        headers,
                        exception=exc,
                    )
                )
                return
            stream.handshake_done = True
            self.events.append(WebSocketOpen(event.stream_id, status_code, headers))
        else:
            # The response is a regular HTTP response; collect its body
            # until the stream ends.
            del self.streams[event.stream_id]
            self.failed_handshakes[event.stream_id] = (
                status_code,
                headers,
                bytearray(),
            )

    def _data_received(self, event: h2.events.DataReceived) -> None:
        failed = self.failed_handshakes.get(event.stream_id)
        if failed is not None:
            self.h2.acknowledge_received_data(
                event.flow_controlled_length, event.stream_id
            )
            failed[2].extend(event.data)
            return
        super()._data_received(event)

    def _stream_ended(self, event: h2.events.StreamEnded) -> None:
        failed = self.failed_handshakes.pop(event.stream_id, None)
        if failed is not None:
            status_code, headers, body = failed
            self.events.append(
                WebSocketHandshakeFailed(
                    event.stream_id,
                    status_code,
                    headers,
                    bytes(body),
                )
            )
            return
        super()._stream_ended(event)

    def _stream_reset(self, event: h2.events.StreamReset) -> None:
        if event.stream_id in self.failed_handshakes:
            del self.failed_handshakes[event.stream_id]
            self.events.append(WebSocketReset(event.stream_id, event.error_code))
            return
        super()._stream_reset(event)

    def _terminate(self, error_code: int | None, reason: str) -> None:
        self.failed_handshakes.clear()
        super()._terminate(error_code, reason)
