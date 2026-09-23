import asyncio
import socket
import unittest

from websockets.asyncio.connection import Connection
from websockets.client import ClientProtocol
from websockets.extensions.permessage_deflate import (
    ClientPerMessageDeflateFactory,
    ServerPerMessageDeflateFactory,
)
from websockets.server import ServerProtocol
from websockets.uri import parse_uri

from ..utils import MS, handshake, record_rsv1


BIG_TEXT = "hello websocket " * 500
SMALL_TEXT = "hi"
SIMILAR_BIG_TEXT = "hello websocket " * 499 + "hello websockets!"


class PerMessageCompressionTests(unittest.IsolatedAsyncioTestCase):
    """Test per-message compression policies between real connections."""

    async def asyncTearDown(self):
        for connection in getattr(self, "connections", []):
            await connection.close()

    async def open_connections(self, no_context_takeover=False):
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
        loop = asyncio.get_running_loop()
        _client_transport, client = await loop.create_connection(
            lambda: Connection(client_protocol, close_timeout=2 * MS),
            sock=client_socket,
        )
        _server_transport, server = await loop.create_connection(
            lambda: Connection(server_protocol, close_timeout=2 * MS),
            sock=server_socket,
        )
        self.connections = getattr(self, "connections", []) + [client, server]
        return client, server

    async def test_mixed_compressed_and_uncompressed_messages(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = await self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Large text message: compressed by default.
                await server.send(BIG_TEXT)
                # Small message: explicitly sent uncompressed.
                await server.send(SMALL_TEXT, compress=False)
                # Similar large message: compressed; decoding it proves that
                # sending the small message uncompressed didn't desynchronize
                # the compression context between the two endpoints.
                await server.send(SIMILAR_BIG_TEXT)

                self.assertEqual(await client.recv(), BIG_TEXT)
                self.assertEqual(await client.recv(), SMALL_TEXT)
                self.assertEqual(await client.recv(), SIMILAR_BIG_TEXT)
                self.assertEqual(rsv1_records, [True, False, True])

    async def test_forced_compression(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = await self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Small message: compressed even though it doesn't benefit.
                await server.send(SMALL_TEXT, compress=True)

                self.assertEqual(await client.recv(), SMALL_TEXT)
                self.assertEqual(rsv1_records, [True])

    async def test_auto_compression_policy(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = await self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Large text message: compression is beneficial.
                await server.send(BIG_TEXT, compress="auto")
                # Small message: compression isn't beneficial.
                await server.send(SMALL_TEXT, compress="auto")
                # Similar large message: compressed; decoding it proves that
                # the trial compression of the small message didn't pollute
                # the compression context.
                await server.send(SIMILAR_BIG_TEXT, compress="auto")

                self.assertEqual(await client.recv(), BIG_TEXT)
                self.assertEqual(await client.recv(), SMALL_TEXT)
                self.assertEqual(await client.recv(), SIMILAR_BIG_TEXT)
                self.assertEqual(rsv1_records, [True, False, True])

    async def test_auto_compression_policy_with_iterator(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = await self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                # Evaluating the benefit of compression requires the complete
                # message; with a one-shot iterator, "auto" falls back to the
                # default behavior i.e. the message is compressed.
                await server.send(iter([BIG_TEXT]), compress="auto")

                self.assertEqual(await client.recv(), BIG_TEXT)
                self.assertEqual(rsv1_records, [True])

    async def test_auto_compression_policy_with_async_iterator(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = await self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                async def fragments():
                    yield BIG_TEXT

                # Same fallback as with a regular iterator.
                await server.send(fragments(), compress="auto")

                self.assertEqual(await client.recv(), BIG_TEXT)
                self.assertEqual(rsv1_records, [True])

    async def test_fragmented_message_with_interleaved_ping(self):
        for no_context_takeover in [False, True]:
            with self.subTest(no_context_takeover=no_context_takeover):
                client, server = await self.open_connections(no_context_takeover)
                rsv1_records = record_rsv1(client)

                fragment1, fragment2 = BIG_TEXT[:4000], BIG_TEXT[4000:]

                # Compressed fragmented message with a ping between fragments.
                async with server.send_context():
                    server.protocol.send_text(
                        fragment1.encode(), fin=False, compress=True
                    )
                pong_waiter = await server.ping()
                async with server.send_context():
                    server.protocol.send_continuation(fragment2.encode(), fin=True)

                # Uncompressed fragmented message.
                async with server.send_context():
                    server.protocol.send_text(
                        fragment1.encode(), fin=False, compress=False
                    )
                async with server.send_context():
                    server.protocol.send_continuation(fragment2.encode(), fin=True)

                self.assertEqual(await client.recv(), BIG_TEXT)
                self.assertEqual(await client.recv(), BIG_TEXT)
                # The ping sent between fragments was answered.
                await asyncio.wait_for(asyncio.shield(pong_waiter), timeout=1)
                self.assertEqual(rsv1_records, [True, False])

    async def test_invalid_compression_policy(self):
        _client, server = await self.open_connections()
        with self.assertRaises(ValueError):
            await server.send(SMALL_TEXT, compress="sometimes")
