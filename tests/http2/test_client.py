import unittest

import h2.config
import h2.connection
import h2.events
import h2.settings
from hyperframe.frame import Frame, RstStreamFrame

from websockets.exceptions import InvalidState, NegotiationError
from websockets.http2 import (
    ClientConnection,
    ConnectionClosed,
    WebSocketHandshakeFailed,
    WebSocketOpen,
)

from ..extensions.utils import ClientNoOpExtensionFactory
from .utils import URI


def parse_h2_frames(data):
    frames = []
    while data:
        frame, length = Frame.parse_frame_header(memoryview(data[:9]))
        frame.parse_body(memoryview(data[9 : 9 + length]))
        frames.append(frame)
        data = data[9 + length :]
    return frames


class RawServerPeer:
    """Server-side h2 state machine driven directly, for testing the client."""

    def __init__(self, enable_connect_protocol=True):
        self.h2 = h2.connection.H2Connection(
            h2.config.H2Configuration(client_side=False, header_encoding="utf-8")
        )
        self.h2.initiate_connection()
        if enable_connect_protocol:
            self.h2.update_settings(
                {h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL: 1}
            )
        self.received = bytearray()
        self.events = []

    def exchange(self, client):
        moved = True
        while moved:
            moved = False
            data = client.data_to_send()
            if data:
                self.received += data
                self.events.extend(self.h2.receive_data(data))
                moved = True
            data = self.h2.data_to_send()
            if data:
                client.receive_data(data)
                moved = True


class ClientTests(unittest.TestCase):
    def make_client(self, enable_connect_protocol=True):
        client = ClientConnection()
        peer = RawServerPeer(enable_connect_protocol)
        client.initiate()
        peer.exchange(client)
        client.events_received()
        return client, peer

    def test_connect_requires_enable_connect_protocol(self):
        """Client doesn't send a request before ENABLE_CONNECT_PROTOCOL."""
        client, peer = self.make_client(enable_connect_protocol=False)
        self.assertFalse(client.extended_connect_enabled)
        with self.assertRaises(InvalidState):
            client.connect(URI)

    def test_connect_sends_extended_connect_headers(self):
        """Request uses Extended CONNECT pseudo-headers, not HTTP/1.1 Upgrade."""
        client, peer = self.make_client()
        self.assertTrue(client.extended_connect_enabled)
        stream_id = client.connect(
            URI, origin="https://example.com", subprotocols=["chat"]
        )
        peer.exchange(client)
        [request] = [
            event
            for event in peer.events
            if isinstance(event, h2.events.RequestReceived)
        ]
        self.assertEqual(request.stream_id, stream_id)
        headers = dict(request.headers)
        self.assertEqual(headers[":method"], "CONNECT")
        self.assertEqual(headers[":protocol"], "websocket")
        self.assertEqual(headers[":scheme"], "https")
        self.assertEqual(headers[":authority"], "example.com")
        self.assertEqual(headers[":path"], "/chat")
        self.assertEqual(headers["sec-websocket-version"], "13")
        self.assertEqual(headers["origin"], "https://example.com")
        self.assertEqual(headers["sec-websocket-protocol"], "chat")
        # No HTTP/1.1 Upgrade mechanism headers.
        for header in ("upgrade", "connection", "sec-websocket-key"):
            self.assertNotIn(header, headers)

    def test_successful_handshake(self):
        """A 2xx response establishes the WebSocket connection."""
        client, peer = self.make_client()
        stream_id = client.connect(URI, subprotocols=["chat"])
        peer.exchange(client)
        peer.h2.send_headers(
            stream_id,
            [(":status", "200"), ("sec-websocket-protocol", "chat")],
        )
        peer.exchange(client)
        [event] = [
            event
            for event in client.events_received()
            if isinstance(event, WebSocketOpen)
        ]
        self.assertEqual(event.stream_id, stream_id)
        self.assertEqual(event.status_code, 200)
        protocol = client.streams[stream_id].protocol
        self.assertEqual(protocol.subprotocol, "chat")

    def test_failed_handshake_status(self):
        """A non-2xx response fails the handshake with status and body."""
        client, peer = self.make_client()
        stream_id = client.connect(URI)
        peer.exchange(client)
        peer.h2.send_headers(stream_id, [(":status", "403")])
        peer.h2.send_data(stream_id, b"Forbidden", end_stream=True)
        peer.exchange(client)
        [event] = [
            event
            for event in client.events_received()
            if isinstance(event, WebSocketHandshakeFailed)
        ]
        self.assertEqual(event.stream_id, stream_id)
        self.assertEqual(event.status_code, 403)
        self.assertEqual(event.body, b"Forbidden")
        self.assertNotIn(stream_id, client.streams)

    def test_unsupported_extension_rejected(self):
        """Client rejects an extension it didn't offer and resets the stream."""
        client, peer = self.make_client()
        stream_id = client.connect(URI, extensions=[ClientNoOpExtensionFactory()])
        peer.exchange(client)
        # Server accepts an extension with wrong parameters.
        peer.h2.send_headers(
            stream_id,
            [(":status", "200"), ("sec-websocket-extensions", "x-no-op; unknown=1")],
        )
        peer.exchange(client)
        [event] = [
            event
            for event in client.events_received()
            if isinstance(event, WebSocketHandshakeFailed)
        ]
        self.assertIsInstance(event.exception, NegotiationError)
        self.assertNotIn(stream_id, client.streams)
        # The client reset the stream.
        resets = [
            frame
            for frame in parse_h2_frames(bytes(peer.received)[24:])
            if isinstance(frame, RstStreamFrame) and frame.stream_id == stream_id
        ]
        self.assertEqual(len(resets), 1)

    def test_data_before_handshake_response(self):
        """DATA before response headers is a connection-level protocol error."""
        client, peer = self.make_client()
        stream_id = client.connect(URI)
        peer.exchange(client)
        peer.h2.send_data(stream_id, b"\x81\x00")
        peer.exchange(client)
        [event] = [
            event
            for event in client.events_received()
            if isinstance(event, ConnectionClosed)
        ]
        self.assertEqual(event.error_code, 0x1)  # PROTOCOL_ERROR
        self.assertNotIn(stream_id, client.streams)
