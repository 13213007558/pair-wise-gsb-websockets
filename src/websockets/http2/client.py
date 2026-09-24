from __future__ import annotations

import logging
from collections.abc import Generator, Sequence
from typing import Any

from ..client import ClientProtocol
from ..datastructures import Headers
from ..exceptions import InvalidHandshake, InvalidStatus, NegotiationError
from ..extensions import ClientExtensionFactory
from ..headers import build_extension, build_host, build_subprotocol
from ..http11 import Response
from ..protocol import OPEN, Protocol
from ..typing import LoggerLike, Origin, Subprotocol
from ..uri import WebSocketURI, parse_uri
from .connection import H2Connection, WebSocketResponse


try:
    import h2.events
except ImportError:  # pragma: no cover
    raise ImportError(
        "websockets.http2 requires the h2 package; "
        "install it with: python -m pip install 'websockets[http2]'"
    ) from None


__all__ = ["H2ClientConnection", "H2ClientWebSocket"]


class H2ClientWebSocket(ClientProtocol):
    """
    WebSocket client connection established over HTTP/2 (RFC 8441).

    The opening handshake uses an Extended CONNECT request instead of an
    HTTP/1.1 Upgrade; it is driven by H2ClientConnection.

    """

    def __init__(self, uri: WebSocketURI, *, stream_id: int, **kwargs: Any) -> None:
        super().__init__(uri, **kwargs)
        self.stream_id = stream_id

    def parse(self) -> Generator[None]:
        # Skip the HTTP/1.1 handshake parser of ClientProtocol; the HTTP/2
        # adapter performs the opening handshake with Extended CONNECT.
        yield from Protocol.parse(self)

    def connect_headers(self) -> list[tuple[str, str]]:
        """
        Build the Extended CONNECT request headers for this connection.

        Returns:
            HTTP/2 headers including the ``:method``, ``:protocol``,
            ``:scheme``, ``:authority``, and ``:path`` pseudo-headers.

        """
        headers = [
            (":method", "CONNECT"),
            (":protocol", "websocket"),
            (":scheme", "https" if self.uri.secure else "http"),
            (":authority", build_host(self.uri.host, self.uri.port, self.uri.secure)),
            (":path", self.uri.resource_name),
        ]
        if self.origin is not None:
            headers.append(("origin", self.origin))
        if self.available_extensions is not None:
            headers.append(
                (
                    "sec-websocket-extensions",
                    build_extension(
                        [
                            (
                                extension_factory.name,
                                extension_factory.get_request_params(),
                            )
                            for extension_factory in self.available_extensions
                        ]
                    ),
                )
            )
        if self.available_subprotocols is not None:
            headers.append(
                (
                    "sec-websocket-protocol",
                    build_subprotocol(self.available_subprotocols),
                )
            )
        return headers

    def process_response_h2(self, status_code: int, headers: Headers) -> None:
        """
        Check an Extended CONNECT response and negotiate parameters.

        Args:
            status_code: Value of the ``:status`` pseudo-header.
            headers: Regular (non-pseudo) response headers.

        Raises:
            InvalidStatus: If the status code isn't 2xx.
            NegotiationError: If extension or subprotocol negotiation fails.

        """
        if not 200 <= status_code < 300:
            raise InvalidStatus(Response(status_code, "", headers))
        self.extensions = self.process_extensions(headers)
        self.subprotocol = self.process_subprotocol(headers)


class H2ClientConnection(H2Connection):
    """
    Sans-I/O WebSocket client adapter for HTTP/2 (RFC 8441).

    The application drives this adapter; it doesn't perform DNS resolution,
    TLS, or socket I/O. Typical usage:

    1. call initiate() and exchange data with the server until
       connect_protocol_enabled returns True;
    2. call connect() for each WebSocket to open;
    3. feed received data with receive_data(), process events_received(),
       and write data_to_send().

    Args:
        origin: Value of the Origin header.
        extensions: List of supported extensions, in order in which they
            should be tried.
        subprotocols: List of supported subprotocols, in order of decreasing
            preference.
        max_size: Maximum size of incoming WebSocket messages in bytes.
        logger: Logger for this connection.

    """

    client_side = True

    def __init__(
        self,
        *,
        origin: Origin | None = None,
        extensions: Sequence[ClientExtensionFactory] | None = None,
        subprotocols: Sequence[Subprotocol] | None = None,
        max_size: int | None | tuple[int | None, int | None] = 2**20,
        logger: LoggerLike | None = None,
    ) -> None:
        if logger is None:
            logger = logging.getLogger("websockets.client")
        super().__init__(max_size=max_size, logger=logger)
        self.origin = origin
        self.available_extensions = extensions
        self.available_subprotocols = subprotocols

    def connect(
        self,
        uri: str | WebSocketURI,
        *,
        origin: Origin | None = None,
        extensions: Sequence[ClientExtensionFactory] | None = None,
        subprotocols: Sequence[Subprotocol] | None = None,
    ) -> H2ClientWebSocket:
        """
        Open a WebSocket connection with an Extended CONNECT request.

        The request is sent only if the server advertised
        ENABLE_CONNECT_PROTOCOL; check connect_protocol_enabled.

        Args:
            uri: URI of the WebSocket server.
            origin: Value of the Origin header; defaults to the
                connection-level origin.
            extensions: Supported extensions; defaults to the
                connection-level extensions.
            subprotocols: Supported subprotocols; defaults to the
                connection-level subprotocols.

        Returns:
            WebSocket connection in the CONNECTING state; it becomes OPEN
            when a successful response is received.

        Raises:
            NegotiationError: If the server didn't advertise
                ENABLE_CONNECT_PROTOCOL.

        """
        if not self.connect_protocol_enabled:
            raise NegotiationError("server didn't advertise ENABLE_CONNECT_PROTOCOL")
        if isinstance(uri, str):
            uri = parse_uri(uri)
        protocol = H2ClientWebSocket(
            uri,
            origin=self.origin if origin is None else origin,
            extensions=(
                self.available_extensions if extensions is None else extensions
            ),
            subprotocols=(
                self.available_subprotocols if subprotocols is None else subprotocols
            ),
            max_size=self.max_size,
            logger=self.logger,
            stream_id=self.h2.get_next_available_stream_id(),
        )
        self.h2.send_headers(
            protocol.stream_id,
            protocol.connect_headers(),
            end_stream=False,
        )
        self.websockets[protocol.stream_id] = protocol
        return protocol

    def _handle_headers_event(self, event: Any) -> None:
        if isinstance(event, h2.events.ResponseReceived):
            self._receive_response(event)
        else:
            self.events.append(event)

    def _receive_response(self, event: Any) -> None:
        protocol = self.websockets.get(event.stream_id)
        if protocol is None:
            # Regular HTTP/2 stream: let the application handle it.
            self.events.append(event)
            return
        headers = Headers(
            (name, value) for name, value in event.headers if not name.startswith(":")
        )
        status_code = int(dict(event.headers)[":status"])
        try:
            protocol.process_response_h2(status_code, headers)
        except InvalidHandshake as exc:
            protocol.handshake_exc = exc
            if 200 <= status_code < 300:
                # Negotiation failed after a successful status code.
                self.h2.reset_stream(event.stream_id)
                del self.websockets[event.stream_id]
                self._abort(protocol)
            # Else, the server will end the stream; the handshake failure
            # becomes visible when StreamEnded arrives.
        else:
            protocol.state = OPEN
        self.events.append(
            WebSocketResponse(event.stream_id, status_code, headers, protocol)
        )
