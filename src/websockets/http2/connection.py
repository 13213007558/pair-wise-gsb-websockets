from __future__ import annotations

import logging

from ..exceptions import InvalidState, ProtocolError
from ..frames import Frame
from ..protocol import CLOSED, SEND_EOF, Protocol
from ..typing import BytesLike, LoggerLike
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


try:
    import h2.config
    import h2.connection
    import h2.events
    import h2.exceptions
    import h2.settings
except ImportError:  # pragma: no cover
    raise ImportError(
        "websockets.http2 requires the h2 package; "
        "install it with: python -m pip install h2"
    ) from None


__all__ = ["Connection"]


Event = (
    WebSocketRequest
    | WebSocketOpen
    | WebSocketFrame
    | WebSocketWritable
    | WebSocketClosed
    | WebSocketEndOfStream
    | WebSocketReset
    | WebSocketHandshakeFailed
    | ConnectionClosed
    | h2.events.Event
)


class WebSocketStream:
    """
    State of a WebSocket connection multiplexed in an HTTP/2 stream.

    """

    def __init__(
        self, connection: Connection, stream_id: int, protocol: Protocol
    ) -> None:
        self.connection = connection
        self.stream_id = stream_id
        self.protocol = protocol
        # Handshake completed; DATA frames may be exchanged.
        self.handshake_done = False
        # Serialized WebSocket frames waiting for HTTP/2 flow control credit.
        self.pending = bytearray()
        # True while pending isn't empty i.e. sends are paused.
        self.paused = False
        # The WebSocket protocol asked to end the stream (SEND_EOF).
        self.end_stream_requested = False
        self.end_stream_sent = False
        # A terminal event was emitted for this stream.
        self.terminal_event_emitted = False

    def flush(self) -> None:
        """
        Move data from the WebSocket protocol to the HTTP/2 connection,
        subject to HTTP/2 flow control, and end the stream if requested.

        """
        h2_connection = self.connection.h2
        for write in self.protocol.data_to_send():
            if write == SEND_EOF:
                self.end_stream_requested = True
            else:
                self.pending += write
        while self.pending and not self.end_stream_sent:
            try:
                window = min(
                    h2_connection.local_flow_control_window(self.stream_id),
                    h2_connection.max_outbound_frame_size,
                )
            except (
                h2.exceptions.NoSuchStreamError,
                h2.exceptions.StreamClosedError,
            ):
                self.pending.clear()
                break
            if window == 0:
                break
            chunk = bytes(self.pending[:window])
            del self.pending[:window]
            try:
                h2_connection.send_data(self.stream_id, chunk)
            except (
                h2.exceptions.NoSuchStreamError,
                h2.exceptions.StreamClosedError,
            ):
                self.pending.clear()
                break
        if self.pending:
            self.paused = True
        elif self.paused:
            self.paused = False
            self.connection.events.append(WebSocketWritable(self.stream_id))
        if self.end_stream_requested and not self.pending and not self.end_stream_sent:
            self.end_stream_sent = True
            try:
                h2_connection.end_stream(self.stream_id)
            except (
                h2.exceptions.NoSuchStreamError,
                h2.exceptions.StreamClosedError,
            ):
                pass


class Connection:
    """
    Sans-I/O implementation of WebSocket over HTTP/2 (:rfc:`8441`).

    This class manages an HTTP/2 connection where WebSocket connections,
    negotiated with the Extended CONNECT method, are multiplexed. It reuses
    the Sans-I/O WebSocket protocol implementation for framing.

    The application drives the connection:

    - call :meth:`initiate`, then send :meth:`data_to_send` to the network;
    - pass incoming bytes to :meth:`receive_data`;
    - process events returned by :meth:`events_received`.

    WebSocket streams are managed with the `send_*` methods. Regular
    HTTP/2 streams may coexist on the same connection: events for streams
    that don't carry WebSocket connections are passed through as raw h2_
    events and the application may drive them with the :attr:`h2` object.

    .. _h2: https://python-hyper.org/projects/h2/

    This class doesn't perform DNS resolution, TLS, or network I/O of any
    kind.

    Args:
        client_side: :obj:`True` for a client, :obj:`False` for a server.
        max_size: Maximum size of incoming WebSocket messages in bytes.
        logger: Logger for this connection.

    """

    def __init__(
        self,
        client_side: bool,
        *,
        max_size: int | None | tuple[int | None, int | None] = 2**20,
        logger: LoggerLike | None = None,
    ) -> None:
        self.h2 = h2.connection.H2Connection(
            h2.config.H2Configuration(
                client_side=client_side,
                header_encoding="utf-8",
            )
        )
        self.max_size = max_size
        self.logger = logger if logger is not None else logging.getLogger("websockets")
        # WebSocket streams, keyed by HTTP/2 stream ID.
        self.streams: dict[int, WebSocketStream] = {}
        # Events generated by receive_data() and friends.
        self.events: list[Event] = []
        # Set when the connection terminated; guards event emission.
        self.terminated = False
        # Set by receive_eof(); receive_data() raises afterwards.
        self.eof_received = False

    @property
    def extended_connect_enabled(self) -> bool:
        """
        Whether the peer advertised ENABLE_CONNECT_PROTOCOL in its SETTINGS.

        A client must wait until this property is :obj:`True` before
        sending WebSocket handshake requests.

        """
        try:
            return bool(
                self.h2.remote_settings[
                    h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL
                ]
            )
        except KeyError:
            return False

    def initiate(self) -> None:
        """
        Initiate the HTTP/2 connection and advertise ENABLE_CONNECT_PROTOCOL.

        """
        self.h2.initiate_connection()
        self.h2.update_settings({h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL: 1})

    # Public methods for receiving data.

    def receive_data(self, data: bytes | bytearray) -> None:
        """
        Receive data from the network.

        After calling this method:

        - You must call :meth:`data_to_send` and send this data to the network.
        - You should call :meth:`events_received` and process resulting events.

        """
        if self.eof_received:
            raise InvalidState("connection is closed")
        try:
            h2_events = self.h2.receive_data(bytes(data))
        except h2.exceptions.ProtocolError as exc:
            # h2 already queued a GOAWAY frame; terminate the connection.
            self._terminate(int(exc.error_code), str(exc))
            return
        for event in h2_events:
            self._process_event(event)

    def receive_eof(self) -> None:
        """
        Receive the end of the data stream from the network.

        This terminates the HTTP/2 connection and all its WebSocket streams.

        """
        self.eof_received = True
        self._terminate(None, "")

    # Public methods for sending events on WebSocket streams.

    def send_text(self, stream_id: int, data: BytesLike, fin: bool = True) -> None:
        """
        Send a Text frame on a WebSocket stream.

        """
        stream = self._stream(stream_id)
        stream.protocol.send_text(data, fin)
        stream.flush()

    def send_binary(self, stream_id: int, data: BytesLike, fin: bool = True) -> None:
        """
        Send a Binary frame on a WebSocket stream.

        """
        stream = self._stream(stream_id)
        stream.protocol.send_binary(data, fin)
        stream.flush()

    def send_continuation(self, stream_id: int, data: BytesLike, fin: bool) -> None:
        """
        Send a Continuation frame on a WebSocket stream.

        """
        stream = self._stream(stream_id)
        stream.protocol.send_continuation(data, fin)
        stream.flush()

    def send_ping(self, stream_id: int, data: BytesLike) -> None:
        """
        Send a Ping frame on a WebSocket stream.

        """
        stream = self._stream(stream_id)
        stream.protocol.send_ping(data)
        stream.flush()

    def send_pong(self, stream_id: int, data: BytesLike) -> None:
        """
        Send a Pong frame on a WebSocket stream.

        """
        stream = self._stream(stream_id)
        stream.protocol.send_pong(data)
        stream.flush()

    def send_close(
        self, stream_id: int, code: int | None = None, reason: str = ""
    ) -> None:
        """
        Send a Close frame on a WebSocket stream, starting the closing
        handshake; the HTTP/2 stream ends when the handshake completes.

        """
        stream = self._stream(stream_id)
        stream.protocol.send_close(code, reason)
        stream.flush()

    def reset_stream(self, stream_id: int, error_code: int = 0) -> None:
        """
        Reset an HTTP/2 stream (RST_STREAM); other streams are unaffected.

        """
        self.h2.reset_stream(stream_id, error_code)
        stream = self.streams.pop(stream_id, None)
        if stream is not None:
            stream.terminal_event_emitted = True

    def send_paused(self, stream_id: int) -> bool:
        """
        Tell whether sending on a WebSocket stream is paused because the
        HTTP/2 flow control window is exhausted.

        """
        return self._stream(stream_id).paused

    def close_connection(self, error_code: int = 0) -> None:
        """
        Close the HTTP/2 connection by sending a GOAWAY frame.

        """
        self.h2.close_connection(error_code=error_code)

    # Public methods for getting events and data to send.

    def events_received(self) -> list[Event]:
        """
        Fetch events generated from data received from the network.

        Events for HTTP/2 streams that don't carry WebSocket connections are
        passed through as raw h2_ events.

        .. _h2: https://python-hyper.org/projects/h2/

        """
        events, self.events = self.events, []
        return events

    def data_to_send(self) -> bytes:
        """
        Obtain data to send to the network.

        """
        return self.h2.data_to_send()

    # Private methods for processing HTTP/2 events.

    def _stream(self, stream_id: int) -> WebSocketStream:
        try:
            return self.streams[stream_id]
        except KeyError:
            raise InvalidState(f"no WebSocket connection on stream {stream_id}")

    def _process_event(self, event: h2.events.Event) -> None:
        if isinstance(event, h2.events.RequestReceived):
            self._request_received(event)
        elif isinstance(event, h2.events.ResponseReceived):
            self._response_received(event)
        elif isinstance(event, h2.events.DataReceived):
            self._data_received(event)
        elif isinstance(event, h2.events.WindowUpdated):
            self._window_updated(event)
        elif isinstance(event, h2.events.StreamEnded):
            self._stream_ended(event)
        elif isinstance(event, h2.events.StreamReset):
            self._stream_reset(event)
        elif isinstance(event, h2.events.ConnectionTerminated):
            self._terminate(
                event.error_code,
                event.additional_data.decode("utf-8", "replace")
                if event.additional_data
                else "",
            )
        else:
            # RemoteSettingsChanged, SettingsAcknowledged, etc.
            self.events.append(event)

    def _request_received(self, event: h2.events.RequestReceived) -> None:
        # Overridden by ServerConnection; passed through by default.
        self.events.append(event)

    def _response_received(self, event: h2.events.ResponseReceived) -> None:
        # Overridden by ClientConnection; passed through by default.
        self.events.append(event)

    def _data_received(self, event: h2.events.DataReceived) -> None:
        stream = self.streams.get(event.stream_id)
        if stream is None:
            # Regular HTTP/2 stream driven by the application.
            self.events.append(event)
            return
        self.h2.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
        if not stream.handshake_done:
            # DATA before the handshake completes is a protocol error.
            self.reset_stream(event.stream_id)
            self.events.append(
                WebSocketHandshakeFailed(
                    event.stream_id,
                    exception=ProtocolError("received DATA before handshake"),
                )
            )
            return
        stream.protocol.receive_data(bytes(event.data))
        self._drain(stream)

    def _window_updated(self, event: h2.events.WindowUpdated) -> None:
        if event.stream_id == 0:
            # Connection-level window update: resume all paused streams.
            for ws_stream in list(self.streams.values()):
                ws_stream.flush()
                self._maybe_forget(ws_stream)
            return
        stream = self.streams.get(event.stream_id)
        if stream is None:
            # Regular HTTP/2 stream driven by the application.
            self.events.append(event)
            return
        stream.flush()
        self._maybe_forget(stream)

    def _stream_ended(self, event: h2.events.StreamEnded) -> None:
        stream = self.streams.get(event.stream_id)
        if stream is None:
            # Regular HTTP/2 stream driven by the application.
            self.events.append(event)
            return
        stream.protocol.receive_eof()
        self._drain(stream)

    def _stream_reset(self, event: h2.events.StreamReset) -> None:
        stream = self.streams.pop(event.stream_id, None)
        if stream is None:
            # Regular HTTP/2 stream driven by the application.
            self.events.append(event)
            return
        stream.terminal_event_emitted = True
        self.events.append(WebSocketReset(event.stream_id, event.error_code))

    def _drain(self, stream: WebSocketStream) -> None:
        protocol = stream.protocol
        for event in protocol.events_received():
            if isinstance(event, Frame):
                self.events.append(WebSocketFrame(stream.stream_id, event))
            else:  # pragma: no cover
                raise AssertionError(f"unexpected event: {event!r}")
        stream.flush()
        if protocol.state is CLOSED and not stream.terminal_event_emitted:
            stream.terminal_event_emitted = True
            if protocol.close_rcvd is not None and protocol.close_sent is not None:
                self.events.append(
                    WebSocketClosed(
                        stream.stream_id,
                        protocol.close_rcvd.code,
                        protocol.close_rcvd.reason,
                    )
                )
            else:
                self.events.append(WebSocketEndOfStream(stream.stream_id))
            stream.flush()
        self._maybe_forget(stream)

    def _maybe_forget(self, stream: WebSocketStream) -> None:
        if stream.terminal_event_emitted and (
            stream.end_stream_sent or not stream.end_stream_requested
        ):
            if not stream.pending:
                self.streams.pop(stream.stream_id, None)

    def _terminate(self, error_code: int | None, reason: str) -> None:
        if self.terminated:
            return
        self.terminated = True
        self.streams.clear()
        self.events.append(ConnectionClosed(error_code, reason))
