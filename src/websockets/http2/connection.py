from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from typing import Any

from ..datastructures import Headers
from ..exceptions import InvalidMessage
from ..protocol import CLOSED, CLOSING, CONNECTING, OPEN, SEND_EOF, Protocol
from ..typing import LoggerLike


try:
    import h2.config
    import h2.connection
    import h2.events
    import h2.exceptions
except ImportError:  # pragma: no cover
    raise ImportError(
        "websockets.http2 requires the h2 package; "
        "install it with: python -m pip install 'websockets[http2]'"
    ) from None


__all__ = [
    "H2Connection",
    "WebSocketRequest",
    "WebSocketResponse",
    "StreamEnded",
    "StreamReset",
    "ConnectionTerminated",
]


@dataclass
class WebSocketRequest:
    """
    Extended CONNECT request received by a server, per RFC 8441.

    Attributes:
        stream_id: HTTP/2 stream on which the request was received.
        scheme: Value of the ``:scheme`` pseudo-header.
        authority: Value of the ``:authority`` pseudo-header.
        path: Value of the ``:path`` pseudo-header.
        headers: Regular (non-pseudo) request headers.
        protocol: WebSocket connection for this stream, in the CONNECTING
            state. Complete the handshake with H2ServerConnection.accept()
            or H2ServerConnection.reject().

    """

    stream_id: int
    scheme: str
    authority: str
    path: str
    headers: Headers
    protocol: Protocol


@dataclass
class WebSocketResponse:
    """
    Response to an Extended CONNECT request received by a client.

    The handshake succeeded if and only if ``200 <= status_code < 300`` and
    `protocol` moved to the OPEN state. Else, the protocol's
    ``handshake_exc`` attribute explains the failure.

    Attributes:
        stream_id: HTTP/2 stream on which the response was received.
        status_code: Value of the ``:status`` pseudo-header.
        headers: Regular (non-pseudo) response headers.
        protocol: WebSocket connection for this stream.

    """

    stream_id: int
    status_code: int
    headers: Headers
    protocol: Protocol


@dataclass
class StreamEnded:
    """
    The remote endpoint sent END_STREAM on a WebSocket stream.

    After a complete WebSocket closing handshake, this is a normal closure:
    `protocol` moves to the CLOSED state and its ``close_code`` and
    ``close_reason`` are set. Without a closing handshake, this is an
    abnormal closure (close code 1006).

    """

    stream_id: int
    protocol: Protocol


@dataclass
class StreamReset:
    """
    A WebSocket stream was reset with RST_STREAM.

    `protocol` moves to the CLOSED state with close code 1006 (abnormal
    closure). Other streams on the same HTTP/2 connection aren't affected.
    `protocol` is None when the reset stream wasn't a WebSocket stream
    managed by this adapter.

    """

    stream_id: int
    error_code: int
    protocol: Protocol | None


@dataclass
class ConnectionTerminated:
    """
    The HTTP/2 connection itself ended, affecting all of its streams.

    When the remote endpoint sent GOAWAY, `error_code` and
    `last_stream_id` come from the GOAWAY frame. When the underlying
    transport reached EOF, they're None.

    """

    error_code: int | None
    last_stream_id: int | None


# Besides the events defined above, events from h2.events that don't belong
# to a WebSocket stream managed by this adapter are passed through, so the
# application can drive regular HTTP/2 streams on the same connection.
Event = (
    WebSocketRequest
    | WebSocketResponse
    | StreamEnded
    | StreamReset
    | ConnectionTerminated
    | Any
)


class H2Connection:
    """
    Sans-I/O adapter running WebSocket connections over HTTP/2 (RFC 8441).

    This adapter owns an h2.connection.H2Connection state machine and one
    websockets.protocol.Protocol per WebSocket stream. The application
    drives it:

    - feed bytes from the network with receive_data();
    - process resulting events with events_received();
    - write data_to_send() to the network.

    It doesn't perform DNS resolution, TLS, or socket I/O. Regular HTTP/2
    streams may coexist with WebSocket streams; drive them with the `h2`
    state machine directly and handle their events, which are passed
    through by events_received().

    Args:
        max_size: Maximum size of incoming WebSocket messages in bytes.
        logger: Logger for this connection.

    """

    client_side: bool

    def __init__(
        self,
        *,
        max_size: int | None | tuple[int | None, int | None] = 2**20,
        logger: LoggerLike | None = None,
    ) -> None:
        if logger is None:
            logger = logging.getLogger("websockets.http2")
        self.logger = logger
        self.max_size = max_size
        self.h2 = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=self.client_side,
                header_encoding="utf-8",
            )
        )
        # WebSocket streams managed by this adapter: stream_id -> Protocol.
        self.websockets: dict[int, Protocol] = {}
        # Outbound WebSocket data paused by HTTP/2 flow control.
        self._outbound: dict[int, deque[bytes]] = {}
        self.events: list[Event] = []

    # Public methods for the connection lifecycle.

    def initiate(self) -> None:
        """
        Send the HTTP/2 connection preface and initial SETTINGS frame.

        Call this method once, then write data_to_send() to the network.

        """
        self.h2.initiate_connection()

    @property
    def connect_protocol_enabled(self) -> bool:
        """
        Whether the peer advertised ENABLE_CONNECT_PROTOCOL (RFC 8441).

        """
        return bool(self.h2.remote_settings.enable_connect_protocol)

    def close(self, error_code: int = 0) -> None:
        """
        Send a GOAWAY frame to shut down the HTTP/2 connection.

        """
        self.h2.close_connection(error_code)

    # Public methods for receiving data and events.

    def receive_data(self, data: bytes | bytearray) -> None:
        """
        Receive data from the network.

        After calling this method, call events_received() and process
        events, then write data_to_send() to the network.

        """
        for event in self.h2.receive_data(data):
            self._handle_event(event)

    def receive_eof(self) -> None:
        """
        Receive the end of the data stream from the network.

        All WebSocket connections are closed abnormally (close code 1006)
        and a ConnectionTerminated event is emitted.

        """
        for stream_id in list(self.websockets):
            protocol = self.websockets.pop(stream_id)
            self._outbound.pop(stream_id, None)
            self._abort(protocol)
        self.events.append(ConnectionTerminated(error_code=None, last_stream_id=None))

    def events_received(self) -> list[Event]:
        """
        Fetch events generated by data received from the network.

        Returns:
            Events read from the connection.

        """
        events, self.events = self.events, []
        return events

    # Public methods for sending data.

    def data_to_send(self) -> bytes:
        """
        Obtain data to send to the network.

        Call this method after any other method and write the result to the
        network. WebSocket frames are sent as DATA frames, subject to HTTP/2
        flow control: data that doesn't fit in the flow control window is
        paused and resumes automatically when WINDOW_UPDATE is received.

        Returns:
            Data to write to the connection.

        """
        for stream_id, protocol in list(self.websockets.items()):
            writes = protocol.data_to_send()
            if writes:
                self._outbound.setdefault(stream_id, deque()).extend(writes)
        for stream_id in list(self._outbound):
            self._pump_stream(stream_id)
        return self.h2.data_to_send()

    def pending_bytes(self, stream_id: int) -> int:
        """
        Amount of WebSocket data paused by HTTP/2 flow control on a stream.

        """
        return sum(len(data) for data in self._outbound.get(stream_id, ()))

    def reset_stream(self, stream_id: int, error_code: int = 0) -> None:
        """
        Reset a stream with RST_STREAM, without affecting other streams.

        If the stream is a WebSocket connection, it's closed abnormally
        (close code 1006).

        """
        self.h2.reset_stream(stream_id, error_code)
        protocol = self.websockets.pop(stream_id, None)
        self._outbound.pop(stream_id, None)
        if protocol is not None:
            self._abort(protocol)

    # Private methods for dispatching HTTP/2 events.

    def _handle_event(self, event: Any) -> None:
        if isinstance(event, h2.events.DataReceived):
            self._receive_stream_data(event)
        elif isinstance(event, h2.events.StreamEnded):
            self._receive_stream_end(event)
        elif isinstance(event, h2.events.StreamReset):
            self._receive_stream_reset(event)
        elif isinstance(event, h2.events.WindowUpdated):
            # Flow control window opened: resume paused streams.
            if event.stream_id == 0:
                for stream_id in list(self._outbound):
                    self._pump_stream(stream_id)
            else:
                self._pump_stream(event.stream_id)
            self.events.append(event)
        elif isinstance(event, h2.events.ConnectionTerminated):
            self.events.append(
                ConnectionTerminated(event.error_code, event.last_stream_id)
            )
        else:
            self._handle_headers_event(event)

    def _handle_headers_event(self, event: Any) -> None:
        # Overridden by the client and server adapters to handle responses
        # and requests. The default passes everything else through.
        self.events.append(event)

    def _receive_stream_data(self, event: Any) -> None:
        protocol = self.websockets.get(event.stream_id)
        if protocol is None:
            # Regular HTTP/2 stream: let the application handle it.
            self.events.append(event)
        elif protocol.state in (OPEN, CLOSING):
            # Only DATA of a stream with a successful handshake reaches the
            # WebSocket frame parser.
            protocol.receive_data(event.data)
        # else: handshake pending or failed; DATA isn't WebSocket frames.
        try:
            self.h2.acknowledge_received_data(
                event.flow_controlled_length, event.stream_id
            )
        except h2.exceptions.StreamClosedError:
            pass

    def _receive_stream_end(self, event: Any) -> None:
        protocol = self.websockets.get(event.stream_id)
        if protocol is None:
            self.events.append(event)
            return
        if protocol.state is CONNECTING:
            # The stream ended before the handshake completed.
            if protocol.handshake_exc is None:
                protocol.handshake_exc = InvalidMessage(
                    "stream ended before the opening handshake completed"
                )
            self._abort(protocol)
        else:
            protocol.receive_eof()
            # The protocol may have queued a close frame or END_STREAM in
            # response; flush it before considering the stream for cleanup.
            writes = protocol.data_to_send()
            if writes:
                self._outbound.setdefault(event.stream_id, deque()).extend(writes)
                self._pump_stream(event.stream_id)
        self.events.append(StreamEnded(event.stream_id, protocol))
        self._maybe_forget(event.stream_id)

    def _receive_stream_reset(self, event: Any) -> None:
        protocol = self.websockets.pop(event.stream_id, None)
        self._outbound.pop(event.stream_id, None)
        if protocol is not None:
            self._abort(protocol)
        self.events.append(StreamReset(event.stream_id, event.error_code, protocol))

    # Private methods for sending WebSocket data over HTTP/2 streams.

    def _pump_stream(self, stream_id: int) -> None:
        queue = self._outbound.get(stream_id)
        if queue is None:
            return
        while queue:
            data = queue[0]
            if data == SEND_EOF:
                queue.popleft()
                try:
                    self.h2.end_stream(stream_id)
                except h2.exceptions.StreamClosedError:
                    pass
                continue
            try:
                window = self.h2.local_flow_control_window(stream_id)
            except h2.exceptions.StreamClosedError:
                queue.clear()
                break
            if window == 0:
                # Paused by flow control; WINDOW_UPDATE resumes the stream.
                return
            chunk_size = min(window, self.h2.remote_settings.max_frame_size, len(data))
            try:
                self.h2.send_data(stream_id, data[:chunk_size])
            except h2.exceptions.StreamClosedError:
                queue.clear()
                break
            if chunk_size == len(data):
                queue.popleft()
            else:
                queue[0] = data[chunk_size:]
        if not queue:
            del self._outbound[stream_id]
        self._maybe_forget(stream_id)

    def _maybe_forget(self, stream_id: int) -> None:
        protocol = self.websockets.get(stream_id)
        if (
            protocol is not None
            and protocol.state is CLOSED
            and not self._outbound.get(stream_id)
        ):
            del self.websockets[stream_id]

    @staticmethod
    def _abort(protocol: Protocol) -> None:
        # Close abnormally, without a closing handshake: close code 1006.
        protocol.parser.close()
        if protocol.state is not CLOSED:
            protocol.state = CLOSED
