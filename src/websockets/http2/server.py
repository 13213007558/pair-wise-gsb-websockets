from __future__ import annotations

import http
import logging
import re
from collections.abc import Callable, Generator, Sequence
from typing import Any

from ..datastructures import Headers
from ..exceptions import InvalidHandshake, InvalidOrigin
from ..extensions import ServerExtensionFactory
from ..protocol import OPEN, Protocol
from ..server import ServerProtocol
from ..typing import LoggerLike, Origin, StatusLike, Subprotocol
from .connection import H2Connection, WebSocketRequest


try:
    import h2.events
    import h2.settings
except ImportError:  # pragma: no cover
    raise ImportError(
        "websockets.http2 requires the h2 package; "
        "install it with: python -m pip install 'websockets[http2]'"
    ) from None


__all__ = ["H2ServerConnection", "H2ServerWebSocket"]


class H2ServerWebSocket(ServerProtocol):
    """
    WebSocket server connection established over HTTP/2 (RFC 8441).

    The opening handshake uses an Extended CONNECT request instead of an
    HTTP/1.1 Upgrade; it is driven by H2ServerConnection.

    """

    def __init__(self, *, stream_id: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.stream_id = stream_id

    def parse(self) -> Generator[None]:
        # Skip the HTTP/1.1 handshake parser of ServerProtocol; the HTTP/2
        # adapter performs the opening handshake with Extended CONNECT.
        yield from Protocol.parse(self)


class H2ServerConnection(H2Connection):
    """
    Sans-I/O WebSocket server adapter for HTTP/2 (RFC 8441).

    The application drives this adapter; it doesn't listen on ports, perform
    TLS, or do socket I/O. Typical usage:

    1. call initiate() and write data_to_send() to the network; this
       advertises ENABLE_CONNECT_PROTOCOL;
    2. feed received data with receive_data() and process
       events_received(); each WebSocketRequest event is a new WebSocket
       handshake;
    3. complete handshakes with accept() or reject().

    Args:
        origins: Acceptable values of the Origin header, like in
            websockets.server.ServerProtocol.
        extensions: List of supported extensions, in order in which they
            should be tried.
        subprotocols: List of supported subprotocols, in order of decreasing
            preference.
        select_subprotocol: Callback for selecting a subprotocol.
        enable_connect_protocol: Advertise ENABLE_CONNECT_PROTOCOL and
            accept Extended CONNECT requests.
        max_size: Maximum size of incoming WebSocket messages in bytes.
        logger: Logger for this connection.

    """

    client_side = False

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
        enable_connect_protocol: bool = True,
        max_size: int | None | tuple[int | None, int | None] = 2**20,
        logger: LoggerLike | None = None,
    ) -> None:
        if logger is None:
            logger = logging.getLogger("websockets.server")
        super().__init__(max_size=max_size, logger=logger)
        self.origins = origins
        self.available_extensions = extensions
        self.available_subprotocols = subprotocols
        self.select_subprotocol_callback = select_subprotocol
        self.enable_connect_protocol = enable_connect_protocol
        # Headers of Extended CONNECT requests awaiting accept() / reject().
        self._pending_requests: dict[int, Headers] = {}

    def initiate(self) -> None:
        """
        Send the HTTP/2 connection preface and initial SETTINGS frame.

        The SETTINGS frame advertises ENABLE_CONNECT_PROTOCOL unless the
        adapter was created with ``enable_connect_protocol=False``.

        """
        super().initiate()
        if self.enable_connect_protocol:
            self.h2.update_settings(
                {h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL: 1}
            )

    def accept(self, stream_id: int) -> H2ServerWebSocket:
        """
        Accept a WebSocket handshake received as a WebSocketRequest event.

        Sends a 200 response and moves the connection to the OPEN state.

        Args:
            stream_id: HTTP/2 stream of the WebSocketRequest event.

        Returns:
            The WebSocket connection for this stream, in the OPEN state.

        Raises:
            InvalidHandshake: If the request is unacceptable; then the
                handshake is rejected automatically with a 403 or 400
                response and the exception is re-raised.

        """
        protocol = self.websockets[stream_id]
        headers = self._pending_requests.pop(stream_id)
        assert isinstance(protocol, H2ServerWebSocket)
        try:
            protocol.origin = protocol.process_origin(headers)
            extensions_header, protocol.extensions = protocol.process_extensions(
                headers
            )
            protocol.subprotocol = protocol.process_subprotocol(headers)
        except InvalidOrigin:
            self.reject(stream_id, http.HTTPStatus.FORBIDDEN)
            raise
        except InvalidHandshake:
            self.reject(stream_id, http.HTTPStatus.BAD_REQUEST)
            raise
        response_headers = [(":status", "200")]
        if extensions_header is not None:
            response_headers.append(("sec-websocket-extensions", extensions_header))
        if protocol.subprotocol is not None:
            response_headers.append(("sec-websocket-protocol", protocol.subprotocol))
        self.h2.send_headers(stream_id, response_headers, end_stream=False)
        protocol.state = OPEN
        return protocol

    def reject(
        self,
        stream_id: int,
        status: StatusLike = http.HTTPStatus.BAD_REQUEST,
        headers: Headers | None = None,
    ) -> None:
        """
        Reject a WebSocket handshake received as a WebSocketRequest event.

        Sends a response with the given status code and ends the stream.

        Args:
            stream_id: HTTP/2 stream of the WebSocketRequest event.
            status: HTTP status code of the response.
            headers: Additional response headers.

        """
        response_headers = [(":status", str(int(status)))]
        if headers is not None:
            response_headers += list(headers.raw_items())
        self.h2.send_headers(stream_id, response_headers, end_stream=True)
        self._pending_requests.pop(stream_id, None)
        protocol = self.websockets.pop(stream_id, None)
        if protocol is not None:
            self._abort(protocol)

    def _handle_headers_event(self, event: Any) -> None:
        if isinstance(event, h2.events.RequestReceived):
            self._receive_request(event)
        else:
            self.events.append(event)

    def _receive_request(self, event: Any) -> None:
        headers = dict(event.headers)
        if headers.get(":method") != "CONNECT" or ":protocol" not in headers:
            # Regular HTTP/2 request: let the application handle it.
            self.events.append(event)
            return
        if (
            not self.enable_connect_protocol
            or headers[":protocol"] != "websocket"
            or ":scheme" not in headers
            or ":authority" not in headers
            or ":path" not in headers
        ):
            # Not an Extended CONNECT request for WebSocket per RFC 8441.
            self.reject_malformed(event.stream_id)
            return
        protocol = H2ServerWebSocket(
            origins=self.origins,
            extensions=self.available_extensions,
            subprotocols=self.available_subprotocols,
            select_subprotocol=self.select_subprotocol_callback,
            max_size=self.max_size,
            logger=self.logger,
            stream_id=event.stream_id,
        )
        regular_headers = Headers(
            (name, value) for name, value in event.headers if not name.startswith(":")
        )
        self.websockets[event.stream_id] = protocol
        self._pending_requests[event.stream_id] = regular_headers
        self.events.append(
            WebSocketRequest(
                event.stream_id,
                headers[":scheme"],
                headers[":authority"],
                headers[":path"],
                regular_headers,
                protocol,
            )
        )

    def reject_malformed(self, stream_id: int) -> None:
        """
        Reject a malformed Extended CONNECT request with a 400 response.

        """
        self.h2.send_headers(stream_id, [(":status", "400")], end_stream=True)
