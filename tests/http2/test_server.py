import unittest

import h2.config
import h2.connection
import h2.events
import h2.settings

from websockets.exceptions import (
    InvalidHeaderValue,
    InvalidOrigin,
    InvalidState,
)
from websockets.http2 import (
    ServerConnection,
    WebSocketRequest,
    WebSocketReset,
)

from ..extensions.utils import ClientNoOpExtensionFactory, ServerNoOpExtensionFactory
from .utils import URI, make_link


class RawClientPeer:
    """Client-side h2 state machine driven directly, for testing the server."""

    def __init__(self):
        self.h2 = h2.connection.H2Connection(
            h2.config.H2Configuration(client_side=True, header_encoding="utf-8")
        )
        self.h2.initiate_connection()
        self.h2.update_settings({h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL: 1})

    def exchange(self, server):
        moved = True
        while moved:
            moved = False
            data = server.data_to_send()
            if data:
                self.h2.receive_data(data)
                moved = True
            data = self.h2.data_to_send()
            if data:
                server.receive_data(data)
                moved = True

    def connect(self, server, headers):
        stream_id = self.h2.get_next_available_stream_id()
        self.h2.send_headers(
            stream_id,
            [
                (":method", "CONNECT"),
                (":protocol", "websocket"),
                (":scheme", "https"),
                (":authority", "example.com"),
                (":path", "/chat"),
                ("sec-websocket-version", "13"),
            ]
            + headers,
        )
        self.exchange(server)
        return stream_id


class ServerTests(unittest.TestCase):
    def make_server(self, **kwargs):
        server = ServerConnection(**kwargs)
        peer = RawClientPeer()
        server.initiate()
        peer.exchange(server)
        server.events_received()
        return server, peer

    def test_request_event(self):
        """Extended CONNECT requests produce WebSocketRequest events."""
        server, peer = self.make_server()
        stream_id = peer.connect(server, [])
        [event] = [
            event
            for event in server.events_received()
            if isinstance(event, WebSocketRequest)
        ]
        self.assertEqual(event.stream_id, stream_id)
        self.assertEqual(event.headers[":method"], "CONNECT")
        self.assertEqual(event.headers[":protocol"], "websocket")
        self.assertEqual(event.headers[":scheme"], "https")
        self.assertEqual(event.headers[":authority"], "example.com")
        self.assertEqual(event.headers[":path"], "/chat")

    def test_regular_request_passed_through(self):
        """Regular HTTP/2 requests are passed through as raw h2 events."""
        server, peer = self.make_server()
        stream_id = peer.h2.get_next_available_stream_id()
        peer.h2.send_headers(
            stream_id,
            [
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "example.com"),
                (":path", "/index.html"),
            ],
        )
        peer.exchange(server)
        [event] = [
            event
            for event in server.events_received()
            if isinstance(event, h2.events.RequestReceived)
        ]
        self.assertEqual(event.stream_id, stream_id)
        self.assertEqual(server.pending, {})

    def test_accept_unknown_stream(self):
        """accept() without a pending handshake fails."""
        server, peer = self.make_server()
        with self.assertRaises(InvalidState):
            server.accept(1)

    def test_accept_invalid_version(self):
        """accept() rejects an unsupported WebSocket version."""
        server, peer = self.make_server()
        stream_id = peer.h2.get_next_available_stream_id()
        peer.h2.send_headers(
            stream_id,
            [
                (":method", "CONNECT"),
                (":protocol", "websocket"),
                (":scheme", "https"),
                (":authority", "example.com"),
                (":path", "/chat"),
                ("sec-websocket-version", "12"),
            ],
        )
        peer.exchange(server)
        server.events_received()
        with self.assertRaises(InvalidHeaderValue):
            server.accept(stream_id)

    def test_accept_invalid_origin(self):
        """accept() enforces the origins allowlist."""
        server, peer = self.make_server(origins=["https://allowed.example"])
        stream_id = peer.connect(server, [("origin", "https://denied.example")])
        server.events_received()
        with self.assertRaises(InvalidOrigin):
            server.accept(stream_id)

    def test_reject(self):
        """reject() sends a regular HTTP response and ends the stream."""
        server, peer = self.make_server()
        stream_id = peer.connect(server, [])
        server.events_received()
        server.reject(stream_id, 403, body=b"Forbidden")
        peer.exchange(server)
        self.assertEqual(server.pending, {})
        self.assertNotIn(stream_id, server.streams)

    def test_extension_negotiation(self):
        """Only extensions explicitly supported by both sides are enabled."""
        link = make_link(
            server=ServerConnection(extensions=[ServerNoOpExtensionFactory()])
        )
        stream_id = link.client.connect(URI, extensions=[ClientNoOpExtensionFactory()])
        link.pump()
        link.server.accept(stream_id)
        link.pump()
        self.assertEqual(
            [
                extension.name
                for extension in link.server.streams[stream_id].protocol.extensions
            ],
            ["x-no-op"],
        )
        self.assertEqual(
            [
                extension.name
                for extension in link.client.streams[stream_id].protocol.extensions
            ],
            ["x-no-op"],
        )

    def test_extension_not_supported_by_server(self):
        """An extension offered by the client but unsupported by the server
        isn't enabled."""
        from websockets.extensions.permessage_deflate import (
            ClientPerMessageDeflateFactory,
        )

        link = make_link()
        stream_id = link.client.connect(
            URI, extensions=[ClientPerMessageDeflateFactory()]
        )
        link.pump()
        link.server.accept(stream_id)
        link.pump()
        self.assertEqual(link.server.streams[stream_id].protocol.extensions, [])
        self.assertEqual(link.client.streams[stream_id].protocol.extensions, [])

    def test_reset_pending_handshake(self):
        """Resetting a stream with a pending handshake emits WebSocketReset."""
        server, peer = self.make_server()
        stream_id = peer.connect(server, [])
        server.events_received()
        peer.h2.reset_stream(stream_id, 0x08)
        peer.exchange(server)
        [event] = [
            event
            for event in server.events_received()
            if isinstance(event, WebSocketReset)
        ]
        self.assertEqual((event.stream_id, event.error_code), (stream_id, 0x08))
        self.assertEqual(server.pending, {})
