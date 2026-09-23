import unittest

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.extensions.permessage_deflate import (
    ClientPerMessageDeflateFactory,
    ServerPerMessageDeflateFactory,
)
from websockets.frames import CONT, PING, TEXT

from .server import get_uri


BIG_TEXT = '{"data": "' + "x" * 1000 + '"}'
SMALL_TEXT = "{}"
SIMILAR_TEXT = '{"data": "' + "x" * 1000 + '!", "v": 2}'
FRAGMENT_1 = "café "
FRAGMENT_2 = "croissants"


class PerMessageCompressionTests(unittest.IsolatedAsyncioTestCase):
    async def run_scenario(self, context_takeover):
        frames_rcvd = []

        async def handler(ws):
            # Large messages are compressed, the small one is sent as-is.
            await ws.send(BIG_TEXT, compress="auto")
            await ws.send(SMALL_TEXT, compress="auto")
            # The last message must decode correctly: it fails if the trial
            # compression of the small message polluted the encoder.
            await ws.send(SIMILAR_TEXT, compress="auto")
            # Control frames may be interleaved in a fragmented message.
            async with ws.send_context():
                ws.protocol.send_text(FRAGMENT_1.encode(), fin=False, compress=True)
            await ws.ping(b"ping")
            async with ws.send_context():
                ws.protocol.send_continuation(FRAGMENT_2.encode(), fin=True)

        if context_takeover:
            server_extensions = client_extensions = None
        else:
            server_extensions = [ServerPerMessageDeflateFactory(True, True)]
            client_extensions = [ClientPerMessageDeflateFactory(True, True)]

        async with serve(
            handler, "localhost", 0, extensions=server_extensions
        ) as server:
            async with connect(get_uri(server), extensions=client_extensions) as client:
                # Record frames before the extension decodes them.
                extension = client.protocol.extensions[0]
                decode = extension.decode

                def record_decode(frame, *, max_size=None):
                    frames_rcvd.append(frame)
                    return decode(frame, max_size=max_size)

                extension.decode = record_decode

                self.assertEqual(await client.recv(), BIG_TEXT)
                self.assertEqual(await client.recv(), SMALL_TEXT)
                self.assertEqual(await client.recv(), SIMILAR_TEXT)
                self.assertEqual(await client.recv(), FRAGMENT_1 + FRAGMENT_2)

        return [frame for frame in frames_rcvd if frame.opcode in (TEXT, CONT, PING)]

    def assert_frames(self, frames):
        self.assertEqual(
            [frame.opcode for frame in frames],
            [TEXT, TEXT, TEXT, TEXT, PING, CONT],
        )
        self.assertEqual(
            [frame.rsv1 for frame in frames],
            [True, False, True, True, False, False],
        )
        self.assertEqual(
            [frame.fin for frame in frames],
            [True, True, True, False, True, True],
        )

    async def test_per_message_compression_with_context_takeover(self):
        frames = await self.run_scenario(context_takeover=True)
        self.assert_frames(frames)

    async def test_per_message_compression_without_context_takeover(self):
        frames = await self.run_scenario(context_takeover=False)
        self.assert_frames(frames)

    async def test_explicit_compression_policy(self):
        frames_rcvd = []

        async def handler(ws):
            await ws.send(SMALL_TEXT, compress=True)
            await ws.send(BIG_TEXT, compress=False)

        async with serve(handler, "localhost", 0) as server:
            async with connect(get_uri(server)) as client:
                extension = client.protocol.extensions[0]
                decode = extension.decode

                def record_decode(frame, *, max_size=None):
                    frames_rcvd.append(frame)
                    return decode(frame, max_size=max_size)

                extension.decode = record_decode

                self.assertEqual(await client.recv(), SMALL_TEXT)
                self.assertEqual(await client.recv(), BIG_TEXT)

        frames = [frame for frame in frames_rcvd if frame.opcode is TEXT]
        self.assertEqual([frame.rsv1 for frame in frames], [True, False])

    async def test_invalid_compression_policy(self):
        async def handler(ws):
            await ws.wait_closed()

        async with serve(handler, "localhost", 0) as server:
            async with connect(get_uri(server)) as client:
                with self.assertRaises(ValueError) as raised:
                    await client.send("😀", compress="yes")
                self.assertEqual(
                    str(raised.exception),
                    "compress must be True, False, 'auto', or None",
                )
