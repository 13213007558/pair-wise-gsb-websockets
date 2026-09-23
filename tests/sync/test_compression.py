import socket
import unittest

from websockets.client import ClientProtocol
from websockets.extensions.permessage_deflate import (
    ClientPerMessageDeflateFactory,
    ServerPerMessageDeflateFactory,
)
from websockets.server import ServerProtocol
from websockets.sync.connection import Connection
from websockets.uri import parse_uri

from ..utils import MS, handshake, record_rsv1


BIG_TEXT = "hello websocket " * 500
SMALL_TEXT = "hi"
SIMILAR_BIG_TEXT = "hello websocket " * 499 + "hello websockets!"


class PerMessageCompressionTests(unittest.TestCase):
    """Test per-message compression policies between real connections."""

    def tearDown(self):
        for connection in getattr(self, "connections", []):
            connection.close()

    def open_connections(self, no_context_takeover=False):
        """Connect two endpoints after a real handshake over a socket pair."""
        if no_context_takeover:
            client_factory = ClientPerMessageDeflateFactory(
                server_no_context_takeover=True,
                client_no_context_takeover=True,
            )
            server_factory = ServerPerMessageDeflateFactory(
                server_no_context_takeover=True,
                client_no_context_takeover=True,
            )
        else:
            client_factory = ClientPerMessageDeflateFactory()
            server_factory = ServerPerMessageDeflateFactory()
        client_protocol = ClientProtocol(
            parse_uri("ws://localhost/"), extensions=[client_factory]
        )
        server_protocol = ServerProtocol(extensions=[server_factory])
        handshake(client_protocol, server_protocol)

        client_socket, server_socket = socket.socketpair()
        client = Connection(client_socket, client_protocol, close_timeout=2 * MS)
        server = Connection(server_socket, server_protocol, close_timeout=2 * MS)
        self.connections = getattr(self, "connections", []) + [client, server]
        return client, server

    def test_mixed_compressed_and_uncompressed_messages(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Large text message: compressed by default.
                server.send(BIG_TEXT)
                # Small message: explicitly sent uncompressed.
                server.send(SMALL_TEXT, compress=False)
                # Similar large message: compressed; decoding it proves that
                # sending the small message uncompressed didn't desynchronize
                # the compression context between the two endpoints.
                server.send(SIMILAR_BIG_TEXT)

                self.assertEqual(client.recv(), BIG_TEXT)
                self.assertEqual(client.recv(), SMALL_TEXT)
                self.assertEqual(client.recv(), SIMILAR_BIG_TEXT)
                self.assertEqual(rsv1_records, [True, False, True])

    def test_forced_compression(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Small message: compressed even though it doesn't benefit.
                server.send(SMALL_TEXT, compress=True)

                self.assertEqual(client.recv(), SMALL_TEXT)
                self.assertEqual(rsv1_records, [True])

    def test_auto_compression_policy(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Large text message: compression is beneficial.
                server.send(BIG_TEXT, compress="auto")
                # Small message: compression isn't beneficial.
                server.send(SMALL_TEXT, compress="auto")
                # Similar large message: compressed; decoding it proves that
                # the trial compression of the small message didn't pollute
                # the compression context.
                server.send(SIMILAR_BIG_TEXT, compress="auto")

                self.assertEqual(client.recv(), BIG_TEXT)
                self.assertEqual(client.recv(), SMALL_TEXT)
                self.assertEqual(client.recv(), SIMILAR_BIG_TEXT)
                self.assertEqual(rsv1_records, [True, False, True])

    def test_auto_compression_policy_with_iterator(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Evaluating the benefit of compression requires the complete
                # message; with a one-shot iterator, "auto" falls back to the
                # default behavior i.e. the message is compressed.
                server.send(iter([BIG_TEXT]), compress="auto")

                self.assertEqual(client.recv(), BIG_TEXT)
                self.assertEqual(rsv1_records, [True])

    def test_fragmented_message_with_interleaved_ping(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                fragment1, fragment2 = BIG_TEXT[:4000], BIG_TEXT[4000:]

                # Compressed fragmented message with a ping between fragments.
                with server.send_context():
                    server.protocol.send_text(
                        fragment1.encode(), fin=False, compress=True
                    )
                pong_received = server.ping()
                with server.send_context():
                    server.protocol.send_continuation(fragment2.encode(), fin=True)

                # Uncompressed fragmented message.
                with server.send_context():
                    server.protocol.send_text(
                        fragment1.encode(), fin=False, compress=False
                    )
                with server.send_context():
                    server.protocol.send_continuation(fragment2.encode(), fin=True)

                self.assertEqual(client.recv(), BIG_TEXT)
                self.assertEqual(client.recv(), BIG_TEXT)
                # The ping sent between fragments was answered.
                self.assertTrue(pong_received.wait(1))
                self.assertEqual(rsv1_records, [True, False])

    def test_invalid_compression_policy(self):
        _client, server = self.open_connections()
        with self.assertRaises(ValueError):
            server.send(SMALL_TEXT, compress="sometimes")
