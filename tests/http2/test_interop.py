"""
Interoperability tests between in-memory client and server h2 state machines.

"""

import unittest

import h2.events
import h2.settings
from hyperframe.frame import DataFrame, Frame, GoAwayFrame, WindowUpdateFrame

from websockets.extensions.permessage_deflate import (
    ClientPerMessageDeflateFactory,
    ServerPerMessageDeflateFactory,
)
from websockets.frames import CloseCode, Opcode
from websockets.http2 import (
    ClientConnection,
    ConnectionClosed,
    ServerConnection,
    WebSocketClosed,
    WebSocketEndOfStream,
    WebSocketFrame,
    WebSocketOpen,
    WebSocketReset,
    WebSocketWritable,
)

from .utils import URI, make_link, open_websocket


def parse_h2_frames(data):
    """Parse raw HTTP/2 bytes into a list of hyperframe frames."""
    frames = []
    data = bytes(data)
    if data.startswith(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"):
        data = data[24:]  # skip the client connection preface
    while data:
        frame, length = Frame.parse_frame_header(memoryview(data[:9]))
        frame.parse_body(memoryview(data[9 : 9 + length]))
        frames.append(frame)
        data = data[9 + length :]
    return frames


class InteropTests(unittest.TestCase):
    def test_text_messages_in_both_directions(self):
        """WebSocket frames are exchanged over DATA frames, order preserved."""
        link = make_link()
        stream_id = open_websocket(link)

        link.client.send_text(stream_id, b"hello")
        link.client.send_binary(stream_id, b"\x00\x01\x02")
        link.pump()
        events = link.server.events_received()
        frames = [event.frame for event in events if isinstance(event, WebSocketFrame)]
        self.assertEqual(
            [(frame.opcode, bytes(frame.data)) for frame in frames],
            [(Opcode.TEXT, b"hello"), (Opcode.BINARY, b"\x00\x01\x02")],
        )

        link.server.send_text(stream_id, b"world")
        link.pump()
        events = link.client.events_received()
        frames = [event.frame for event in events if isinstance(event, WebSocketFrame)]
        self.assertEqual(
            [(frame.opcode, bytes(frame.data)) for frame in frames],
            [(Opcode.TEXT, b"world")],
        )

    def test_ping_pong(self):
        """Ping frames are answered with pong frames by the protocol layer."""
        link = make_link()
        stream_id = open_websocket(link)

        link.client.send_ping(stream_id, b"ping")
        link.pump()
        events = link.server.events_received()
        [ping] = [event.frame for event in events if isinstance(event, WebSocketFrame)]
        self.assertEqual((ping.opcode, bytes(ping.data)), (Opcode.PING, b"ping"))
        events = link.client.events_received()
        [pong] = [event.frame for event in events if isinstance(event, WebSocketFrame)]
        self.assertEqual((pong.opcode, bytes(pong.data)), (Opcode.PONG, b"ping"))

    def test_small_window_splits_frame_and_updates_window(self):
        """
        With a tiny flow-control window, a single WebSocket frame is split
        across several DATA frames; the peer emits WINDOW_UPDATE frames and
        the message is reassembled in order.

        """
        server = ServerConnection()
        link = make_link(server=server)
        # Shrink the server's inbound window: the client may only send
        # 8 bytes per stream until the server acknowledges data.
        server.h2.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 8})
        link.pump()
        link.client.events_received()
        link.server.events_received()
        stream_id = open_websocket(link)

        message = bytes(range(256)) * 2  # 512 bytes, one binary WebSocket frame
        link.client.send_binary(stream_id, message)
        # The window is exhausted: sending is paused.
        self.assertTrue(link.client.send_paused(stream_id))

        link.pump()

        # Sending resumed and completed.
        self.assertFalse(link.client.send_paused(stream_id))
        events = link.client.events_received()
        writable = [event for event in events if isinstance(event, WebSocketWritable)]
        self.assertEqual([event.stream_id for event in writable], [stream_id])

        # The frame was split into several DATA frames.
        data_frames = [
            frame
            for frame in parse_h2_frames(link.client_bytes)
            if isinstance(frame, DataFrame) and frame.stream_id == stream_id
        ]
        self.assertGreater(len(data_frames), 1)
        self.assertEqual(
            b"".join(bytes(frame.data) for frame in data_frames)[: len(message) + 14][
                :6
            ][:2],
            b"\x82\xfe",  # binary frame, 16-bit extended payload length
        )

        # The server sent WINDOW_UPDATE frames to refill the window.
        window_updates = [
            frame
            for frame in parse_h2_frames(link.server_bytes)
            if isinstance(frame, WindowUpdateFrame)
        ]
        self.assertGreater(len(window_updates), 1)

        # The message was reassembled intact.
        events = link.server.events_received()
        frames = [event.frame for event in events if isinstance(event, WebSocketFrame)]
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].opcode, Opcode.BINARY)
        self.assertEqual(bytes(frames[0].data), message)

    def test_permessage_deflate(self):
        """An extension explicitly supported by both sides is negotiated."""
        link = make_link(
            client=ClientConnection(),
            server=ServerConnection(
                extensions=[ServerPerMessageDeflateFactory()],
            ),
        )
        stream_id = link.client.connect(
            URI, extensions=[ClientPerMessageDeflateFactory()]
        )
        link.pump()
        link.server.accept(stream_id)
        link.pump()
        [event] = [
            event
            for event in link.client.events_received()
            if isinstance(event, WebSocketOpen)
        ]
        self.assertIn("permessage-deflate", event.headers["sec-websocket-extensions"])

        # The extension is active on both sides.
        self.assertEqual(
            [
                extension.name
                for extension in link.client.streams[stream_id].protocol.extensions
            ],
            ["permessage-deflate"],
        )
        self.assertEqual(
            [
                extension.name
                for extension in link.server.streams[stream_id].protocol.extensions
            ],
            ["permessage-deflate"],
        )

        message = b"compress me" * 100
        link.client.send_text(stream_id, message)
        link.pump()
        events = link.server.events_received()
        [frame] = [event.frame for event in events if isinstance(event, WebSocketFrame)]
        self.assertEqual(bytes(frame.data), message)
        # The frame was compressed on the wire.
        data_frames = [
            frame
            for frame in parse_h2_frames(link.client_bytes)
            if isinstance(frame, DataFrame) and frame.stream_id == stream_id
        ]
        self.assertLess(sum(len(frame.data) for frame in data_frames), len(message))

    def test_two_websockets_and_regular_stream_coexist(self):
        """Two WebSocket streams and a regular HTTP/2 stream are multiplexed."""
        link = make_link()
        first = open_websocket(link)
        second = open_websocket(link)
        self.assertNotEqual(first, second)

        # Regular HTTP/2 stream, driven by the application via raw h2 APIs.
        regular = link.client.h2.get_next_available_stream_id()
        link.client.h2.send_headers(
            regular,
            [
                (":method", "GET"),
                (":scheme", "https"),
                (":authority", "example.com"),
                (":path", "/index.html"),
            ],
        )
        link.pump()
        events = link.server.events_received()
        [request] = [
            event for event in events if isinstance(event, h2.events.RequestReceived)
        ]
        self.assertEqual(request.stream_id, regular)
        link.server.h2.send_headers(regular, [(":status", "200")])
        link.server.h2.send_data(regular, b"<html></html>", end_stream=True)
        link.pump()
        events = link.client.events_received()
        response = [
            event for event in events if isinstance(event, h2.events.ResponseReceived)
        ]
        self.assertEqual([event.stream_id for event in response], [regular])

        # Both WebSocket streams exchange frames independently.
        link.client.send_text(first, b"one")
        link.client.send_text(second, b"two")
        link.pump()
        events = link.server.events_received()
        received = {
            event.stream_id: bytes(event.frame.data)
            for event in events
            if isinstance(event, WebSocketFrame)
        }
        self.assertEqual(received, {first: b"one", second: b"two"})

        # Resetting one WebSocket stream doesn't affect the other one.
        link.server.reset_stream(first, 0x08)
        link.pump()
        events = link.client.events_received()
        [reset] = [event for event in events if isinstance(event, WebSocketReset)]
        self.assertEqual((reset.stream_id, reset.error_code), (first, 0x08))

        link.client.send_text(second, b"still alive")
        link.pump()
        events = link.server.events_received()
        [frame] = [event.frame for event in events if isinstance(event, WebSocketFrame)]
        self.assertEqual(bytes(frame.data), b"still alive")

    def test_close_handshake(self):
        """The closing handshake produces WebSocketClosed on both sides."""
        link = make_link()
        stream_id = open_websocket(link)

        link.client.send_close(stream_id, CloseCode.NORMAL_CLOSURE, "bye")
        link.pump()

        events = link.server.events_received()
        [closed] = [event for event in events if isinstance(event, WebSocketClosed)]
        self.assertEqual(
            (closed.stream_id, closed.code, closed.reason),
            (stream_id, CloseCode.NORMAL_CLOSURE, "bye"),
        )
        events = link.client.events_received()
        [closed] = [event for event in events if isinstance(event, WebSocketClosed)]
        self.assertEqual(
            (closed.stream_id, closed.code, closed.reason),
            (stream_id, CloseCode.NORMAL_CLOSURE, "bye"),
        )
        # Streams are cleaned up on both sides.
        self.assertEqual(link.client.streams, {})
        self.assertEqual(link.server.streams, {})

    def test_end_stream_without_close_handshake(self):
        """END_STREAM without a close frame is an abnormal closure."""
        link = make_link()
        stream_id = open_websocket(link)

        link.server.h2.end_stream(stream_id)
        link.pump()
        events = link.client.events_received()
        [ended] = [event for event in events if isinstance(event, WebSocketEndOfStream)]
        self.assertEqual(ended.stream_id, stream_id)

    def test_connection_terminated_by_goaway(self):
        """GOAWAY terminates the connection and all its WebSocket streams."""
        link = make_link()
        stream_id = open_websocket(link)

        link.server.close_connection()
        link.pump()
        events = link.client.events_received()
        [closed] = [event for event in events if isinstance(event, ConnectionClosed)]
        self.assertEqual(closed.error_code, 0)
        self.assertEqual(link.client.streams, {})
        self.assertNotIn(stream_id, link.client.streams)

    def test_connection_terminated_by_eof(self):
        """End of the transport terminates the connection distinctly."""
        link = make_link()
        open_websocket(link)

        link.client.receive_eof()
        events = link.client.events_received()
        [closed] = [event for event in events if isinstance(event, ConnectionClosed)]
        self.assertIsNone(closed.error_code)
        self.assertEqual(link.client.streams, {})

    def test_goaway_frame_is_sent(self):
        """close_connection() emits a GOAWAY frame on the wire."""
        link = make_link()
        open_websocket(link)
        link.client.close_connection()
        link.pump()
        goaway_frames = [
            frame
            for frame in parse_h2_frames(link.client_bytes)
            if isinstance(frame, GoAwayFrame)
        ]
        self.assertEqual(len(goaway_frames), 1)
