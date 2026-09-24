import http
import unittest

import h2.events
from h2.settings import SettingCodes

from websockets.exceptions import (
    ConnectionClosedOK,
    InvalidStatus,
    NegotiationError,
)
from websockets.extensions.permessage_deflate import (
    ClientPerMessageDeflateFactory,
    PerMessageDeflate,
    ServerPerMessageDeflateFactory,
)
from websockets.frames import BINARY, PING, PONG, TEXT, CloseCode, Frame
from websockets.http2 import (
    ConnectionTerminated,
    H2ClientConnection,
    H2ServerConnection,
    StreamEnded,
    StreamReset,
    WebSocketRequest,
    WebSocketResponse,
)
from websockets.protocol import CLOSED, OPEN


def exchange(client, server):
    """Move data between both ends of an in-memory HTTP/2 connection."""
    for _ in range(1000):
        client_data = client.data_to_send()
        server_data = server.data_to_send()
        if not client_data and not server_data:
            return
        if client_data:
            server.receive_data(client_data)
        if server_data:
            client.receive_data(server_data)
    raise AssertionError("data exchange didn't settle")


def get_event(adapter, event_type):
    """Pop the first event of the expected type, dropping earlier events."""
    for event in adapter.events_received():
        if isinstance(event, event_type):
            return event
    raise AssertionError(f"no {event_type.__name__} event")


class InteropTestCase(unittest.TestCase):
    """Test WebSocket over HTTP/2 with two in-memory h2 state machines."""

    def setUp(self):
        self.client = H2ClientConnection()
        self.server = H2ServerConnection()
        self.client.initiate()
        self.server.initiate()
        exchange(self.client, self.server)

    def open_websocket(self, uri="wss://example.com/chat", **kwargs):
        if uri.startswith("/"):
            uri = "wss://example.com" + uri
        ws_client = self.client.connect(uri, **kwargs)
        exchange(self.client, self.server)
        request = get_event(self.server, WebSocketRequest)
        ws_server = self.server.accept(request.stream_id)
        exchange(self.client, self.server)
        response = get_event(self.client, WebSocketResponse)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ws_client.state, OPEN)
        self.assertEqual(ws_server.state, OPEN)
        return ws_client, ws_server, request


class HandshakeTests(InteropTestCase):
    def test_connect_requires_enable_connect_protocol(self):
        """Client sends requests only after ENABLE_CONNECT_PROTOCOL."""
        client = H2ClientConnection()
        client.initiate()
        self.assertFalse(client.connect_protocol_enabled)
        with self.assertRaises(NegotiationError):
            client.connect("wss://example.com/chat")

    def test_enable_connect_protocol_advertised(self):
        """Server advertises ENABLE_CONNECT_PROTOCOL; client detects it."""
        self.assertTrue(self.client.connect_protocol_enabled)
        self.assertTrue(self.server.h2.local_settings.enable_connect_protocol)

    def test_extended_connect_request(self):
        """Request uses :protocol and friends, not HTTP/1.1 Upgrade headers."""
        ws_client = self.client.connect("wss://example.com/chat?x=1")
        self.assertEqual(
            ws_client.connect_headers(),
            [
                (":method", "CONNECT"),
                (":protocol", "websocket"),
                (":scheme", "https"),
                (":authority", "example.com"),
                (":path", "/chat?x=1"),
            ],
        )
        exchange(self.client, self.server)
        request = get_event(self.server, WebSocketRequest)
        self.assertEqual(request.scheme, "https")
        self.assertEqual(request.authority, "example.com")
        self.assertEqual(request.path, "/chat?x=1")
        with self.assertRaises(KeyError):
            request.headers["Upgrade"]
        with self.assertRaises(KeyError):
            request.headers["Connection"]

    def test_ws_uri_scheme(self):
        """A ws:// URI maps to the http scheme."""
        ws_client = self.client.connect("ws://example.com:8080/chat")
        headers = dict(ws_client.connect_headers())
        self.assertEqual(headers[":scheme"], "http")
        self.assertEqual(headers[":authority"], "example.com:8080")

    def test_successful_handshake(self):
        """Handshake completes with a 200 response; streams are OPEN."""
        ws_client, ws_server, request = self.open_websocket()
        self.assertEqual(request.protocol, ws_server)
        self.assertEqual(ws_client.stream_id, request.stream_id)

    def test_rejected_handshake(self):
        """Rejection produces a distinct result: status and handshake_exc."""
        ws_client = self.client.connect("wss://example.com/chat")
        exchange(self.client, self.server)
        request = get_event(self.server, WebSocketRequest)
        self.server.reject(request.stream_id, http.HTTPStatus.NOT_FOUND)
        exchange(self.client, self.server)
        response = get_event(self.client, WebSocketResponse)
        self.assertEqual(response.status_code, 404)
        self.assertIsInstance(ws_client.handshake_exc, InvalidStatus)
        self.assertEqual(ws_client.state, CLOSED)

    def test_data_before_handshake_is_not_parsed(self):
        """DATA on a stream without a successful handshake isn't WS frames."""
        ws_client = self.client.connect("wss://example.com/chat")
        exchange(self.client, self.server)
        request = get_event(self.server, WebSocketRequest)
        # A misbehaving client sends DATA before the handshake completes.
        self.client.h2.send_data(ws_client.stream_id, b"\x81\x04junk")
        exchange(self.client, self.server)
        self.assertEqual(request.protocol.events_received(), [])
        # The handshake can still complete and the stream works normally.
        ws_server = self.server.accept(request.stream_id)
        exchange(self.client, self.server)
        ws_client.send_text(b"hello")
        exchange(self.client, self.server)
        self.assertEqual(ws_server.events_received(), [Frame(TEXT, b"hello")])


class FrameTests(InteropTestCase):
    def test_text_and_binary_frames(self):
        """WebSocket frames are exchanged over DATA frames both ways."""
        ws_client, ws_server, _ = self.open_websocket()
        ws_client.send_text(b"hello")
        ws_server.send_binary(b"\x01\x02")
        exchange(self.client, self.server)
        self.assertEqual(ws_server.events_received(), [Frame(TEXT, b"hello")])
        self.assertEqual(ws_client.events_received(), [Frame(BINARY, b"\x01\x02")])

    def test_ping_pong(self):
        """Control frames work over HTTP/2 streams."""
        ws_client, ws_server, _ = self.open_websocket()
        ws_client.send_ping(b"ping")
        exchange(self.client, self.server)
        self.assertEqual(ws_server.events_received(), [Frame(PING, b"ping")])
        self.assertEqual(ws_client.events_received(), [Frame(PONG, b"ping")])

    def test_small_window_splits_frame_and_updates_window(self):
        """A frame split across DATA frames is reassembled in order.

        With a small flow control window, sending pauses when the window is
        exhausted and resumes when WINDOW_UPDATE arrives.

        """
        client = H2ClientConnection()
        server = H2ServerConnection()
        client.initiate()
        server.initiate()
        server.h2.update_settings({SettingCodes.INITIAL_WINDOW_SIZE: 16})
        exchange(client, server)
        self.assertEqual(client.h2.remote_settings.initial_window_size, 16)

        self.client, self.server = client, server
        ws_client, ws_server, _ = self.open_websocket()

        # Count DATA frames received on the server side.
        data_events = []
        receive_stream_data = server._receive_stream_data

        def spy(event):
            data_events.append(event)
            receive_stream_data(event)

        server._receive_stream_data = spy

        ws_client.send_text(b"a" * 40)
        ws_client.send_binary(b"b" * 40)

        # The first frame (42 bytes) doesn't fit in the 16-byte window.
        client_data = client.data_to_send()
        self.assertGreater(client.pending_bytes(ws_client.stream_id), 0)
        server.receive_data(client_data)
        self.assertEqual(ws_server.events_received(), [])

        # The server acknowledges DATA, which produces WINDOW_UPDATE frames.
        server_data = server.data_to_send()
        self.assertTrue(server_data)
        client.receive_data(server_data)
        window_updates = [
            event
            for event in client.events_received()
            if isinstance(event, h2.events.WindowUpdated)
        ]
        self.assertTrue(window_updates)

        # Pump until flow control unblocks and all data is transferred.
        exchange(client, server)
        self.assertEqual(client.pending_bytes(ws_client.stream_id), 0)

        # Frames were split across several DATA frames and reassembled in
        # order.
        self.assertGreater(len(data_events), 2)
        self.assertEqual(
            ws_server.events_received(),
            [Frame(TEXT, b"a" * 40), Frame(BINARY, b"b" * 40)],
        )


class MultiplexingTests(InteropTestCase):
    def test_websockets_and_regular_stream_coexist(self):
        """Two WebSocket streams and a regular HTTP/2 stream coexist."""
        ws1_client, ws1_server, _ = self.open_websocket("/one")
        ws2_client, ws2_server, _ = self.open_websocket("/two")

        # The application drives a regular HTTP/2 stream on the same
        # connection.
        stream_id = self.client.h2.get_next_available_stream_id()
        self.client.h2.send_headers(
            stream_id,
            [
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "example.com"),
                (":path", "/api"),
            ],
            end_stream=True,
        )
        exchange(self.client, self.server)
        requests = [
            event
            for event in self.server.events_received()
            if isinstance(event, h2.events.RequestReceived)
        ]
        self.assertEqual([event.stream_id for event in requests], [stream_id])
        self.server.h2.send_headers(stream_id, [(":status", "200")], end_stream=True)
        exchange(self.client, self.server)
        responses = [
            event
            for event in self.client.events_received()
            if isinstance(event, h2.events.ResponseReceived)
        ]
        self.assertEqual([event.stream_id for event in responses], [stream_id])

        # Both WebSocket connections still work independently.
        ws1_client.send_text(b"one")
        ws2_client.send_text(b"two")
        exchange(self.client, self.server)
        self.assertEqual(ws1_server.events_received(), [Frame(TEXT, b"one")])
        self.assertEqual(ws2_server.events_received(), [Frame(TEXT, b"two")])

    def test_stream_reset_does_not_affect_other_websockets(self):
        """RST_STREAM closes one WebSocket; the other keeps working."""
        ws1_client, ws1_server, _ = self.open_websocket("/one")
        ws2_client, ws2_server, _ = self.open_websocket("/two")

        self.server.reset_stream(ws1_server.stream_id, error_code=0x8)
        exchange(self.client, self.server)

        reset = get_event(self.client, StreamReset)
        self.assertEqual(reset.stream_id, ws1_client.stream_id)
        self.assertEqual(reset.error_code, 0x8)
        self.assertIs(reset.protocol, ws1_client)
        self.assertEqual(ws1_client.state, CLOSED)
        self.assertEqual(ws1_client.close_code, CloseCode.ABNORMAL_CLOSURE)
        self.assertEqual(ws1_server.state, CLOSED)

        ws2_client.send_text(b"still alive")
        exchange(self.client, self.server)
        self.assertEqual(ws2_server.events_received(), [Frame(TEXT, b"still alive")])


class ClosingTests(InteropTestCase):
    def test_close_handshake_then_end_stream(self):
        """Closing handshake completes, then both sides send END_STREAM."""
        ws_client, ws_server, _ = self.open_websocket()
        ws_client.send_close(1000, "bye")
        exchange(self.client, self.server)

        self.assertEqual(ws_client.state, CLOSED)
        self.assertEqual(ws_server.state, CLOSED)
        self.assertEqual(ws_client.close_code, 1000)
        self.assertEqual(ws_server.close_code, 1000)
        self.assertEqual(ws_client.close_reason, "bye")
        self.assertIsInstance(ws_client.close_exc, ConnectionClosedOK)
        self.assertIsInstance(ws_server.close_exc, ConnectionClosedOK)

        # END_STREAM is observable on both sides, distinct from a reset.
        self.assertIs(get_event(self.client, StreamEnded).protocol, ws_client)
        self.assertIs(get_event(self.server, StreamEnded).protocol, ws_server)

    def test_end_stream_without_close_handshake(self):
        """END_STREAM without a closing handshake is an abnormal closure."""
        ws_client, ws_server, _ = self.open_websocket()
        self.server.h2.end_stream(ws_server.stream_id)
        exchange(self.client, self.server)

        ended = get_event(self.client, StreamEnded)
        self.assertIs(ended.protocol, ws_client)
        self.assertEqual(ws_client.state, CLOSED)
        self.assertEqual(ws_client.close_code, CloseCode.ABNORMAL_CLOSURE)

    def test_connection_terminated(self):
        """GOAWAY and EOF are observable as connection-level events."""
        ws_client, ws_server, _ = self.open_websocket()

        self.client.close()
        exchange(self.client, self.server)
        terminated = get_event(self.server, ConnectionTerminated)
        self.assertEqual(terminated.error_code, 0)

        # The transport goes away: all WebSocket connections close
        # abnormally, independently of per-stream events.
        self.client.receive_eof()
        self.server.receive_eof()
        self.assertIsNotNone(get_event(self.client, ConnectionTerminated))
        self.assertIsNotNone(get_event(self.server, ConnectionTerminated))
        self.assertEqual(ws_client.state, CLOSED)
        self.assertEqual(ws_server.state, CLOSED)
        self.assertEqual(ws_client.close_code, CloseCode.ABNORMAL_CLOSURE)
        self.assertEqual(ws_server.close_code, CloseCode.ABNORMAL_CLOSURE)


class NegotiationTests(InteropTestCase):
    def test_extensions_mutually_supported(self):
        """Extensions supported by both sides are negotiated."""
        self.client = H2ClientConnection(extensions=[ClientPerMessageDeflateFactory()])
        self.server = H2ServerConnection(extensions=[ServerPerMessageDeflateFactory()])
        self.client.initiate()
        self.server.initiate()
        exchange(self.client, self.server)
        ws_client, ws_server, _ = self.open_websocket()

        self.assertEqual(len(ws_client.extensions), 1)
        self.assertIsInstance(ws_client.extensions[0], PerMessageDeflate)
        self.assertEqual(len(ws_server.extensions), 1)
        self.assertIsInstance(ws_server.extensions[0], PerMessageDeflate)

        ws_client.send_text(b"compressed")
        exchange(self.client, self.server)
        self.assertEqual(ws_server.events_received(), [Frame(TEXT, b"compressed")])

    def test_extensions_not_mutually_supported(self):
        """Extensions supported by one side only aren't negotiated."""
        self.client = H2ClientConnection(extensions=[ClientPerMessageDeflateFactory()])
        self.server = H2ServerConnection()
        self.client.initiate()
        self.server.initiate()
        exchange(self.client, self.server)
        ws_client, ws_server, _ = self.open_websocket()

        self.assertEqual(ws_client.extensions, [])
        self.assertEqual(ws_server.extensions, [])

        ws_client.send_text(b"uncompressed")
        exchange(self.client, self.server)
        self.assertEqual(ws_server.events_received(), [Frame(TEXT, b"uncompressed")])

    def test_subprotocols(self):
        """Subprotocols are negotiated like over HTTP/1.1."""
        self.client = H2ClientConnection(subprotocols=["chat", "super"])
        self.server = H2ServerConnection(subprotocols=["chat"])
        self.client.initiate()
        self.server.initiate()
        exchange(self.client, self.server)
        ws_client, ws_server, _ = self.open_websocket()

        self.assertEqual(ws_client.subprotocol, "chat")
        self.assertEqual(ws_server.subprotocol, "chat")


del InteropTestCase  # base class, not a test


if __name__ == "__main__":
    unittest.main()
