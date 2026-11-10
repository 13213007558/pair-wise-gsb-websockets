"""
Regression tests for extension negotiation and the resulting transform chain.

These tests check that what a peer announces in the ``Sec-WebSocket-Extensions``
handshake header matches the extension objects that are actually enabled on the
connection, i.e. the objects that get threaded into frame encoding and
decoding. They focus on ``permessage-deflate`` (RFC 7692), on offers and
responses containing several header lines, several candidates with the same
name, duplicate parameters, illegal window sizes, parameters that weren't
offered, and duplicate extensions.

Repeated extension names are not always invalid: an offer may contain several
candidates for the same extension name as alternatives. Whether a duplicate is
acceptable depends on which side of the handshake we're on, so client and
server rules are exercised separately.

"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from websockets.client import ClientProtocol
from websockets.datastructures import Headers
from websockets.exceptions import (
    DuplicateParameter,
    InvalidHeaderFormat,
    InvalidParameterValue,
    NegotiationError,
    ProtocolError,
)
from websockets.extensions.base import (
    ClientExtensionFactory,
    Extension,
    ServerExtensionFactory,
)
from websockets.extensions.permessage_deflate import (
    ClientPerMessageDeflateFactory,
    PerMessageDeflate,
    ServerPerMessageDeflateFactory,
)
from websockets.frames import CONT, PING, TEXT, Frame
from websockets.http11 import Request, Response
from websockets.protocol import CONNECTING, OPEN
from websockets.server import ServerProtocol
from websockets.uri import parse_uri
from websockets.utils import accept_key

from ..test_frames import FramesTestCase
from ..test_utils import KEY


URI = parse_uri("wss://example.com/test")


# Test extension that records the order in which its transforms run.


class RecordingExtension(Extension):
    """Extension that records every encode/decode call on a shared log."""

    def __init__(self, name: str, log: list[str]) -> None:
        self.name = name
        self.log = log

    def __repr__(self) -> str:
        return f"RecordingExtension({self.name!r})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, RecordingExtension)
            and self.name == other.name
            and self.log is other.log
        )

    def encode(self, frame: Frame) -> Frame:
        self.log.append(f"encode {self.name}")
        return frame

    def decode(self, frame: Frame, *, max_size: int | None = None) -> Frame:
        self.log.append(f"decode {self.name}")
        return frame


class ClientRecordingExtensionFactory(ClientExtensionFactory):
    def __init__(self, name: str, log: list[str]) -> None:
        self.name = name
        self.log = log

    def get_request_params(self):
        return []

    def process_response_params(self, params, accepted_extensions):
        if params:
            raise NegotiationError("recording extension takes no parameters")
        if any(other.name == self.name for other in accepted_extensions):
            raise NegotiationError(f"received duplicate {self.name}")
        return RecordingExtension(self.name, self.log)


class ServerRecordingExtensionFactory(ServerExtensionFactory):
    def __init__(self, name: str, log: list[str]) -> None:
        self.name = name
        self.log = log

    def process_request_params(self, params, accepted_extensions):
        if any(other.name == self.name for other in accepted_extensions):
            raise NegotiationError(f"skipped duplicate {self.name}")
        return [], RecordingExtension(self.name, self.log)


# Helpers driving the Sans-I/O handshake without any I/O.


def make_request() -> Request:
    return Request(
        path="/test",
        headers=Headers(
            {
                "Host": "example.com",
                "Upgrade": "websocket",
                "Connection": "Upgrade",
                "Sec-WebSocket-Key": KEY,
                "Sec-WebSocket-Version": "13",
            }
        ),
    )


def make_response(key: str) -> Response:
    return Response(
        status_code=101,
        reason_phrase="Switching Protocols",
        headers=Headers(
            {
                "Upgrade": "websocket",
                "Connection": "Upgrade",
                "Sec-WebSocket-Accept": accept_key(key),
            }
        ),
    )


def client_handshake(client: ClientProtocol, header_values: list[str] | None):
    """Feed a server response, optionally carrying extension header lines."""
    response = make_response(client.key)
    if header_values is not None:
        for header_value in header_values:
            response.headers["Sec-WebSocket-Extensions"] = header_value
    client.receive_data(response.serialize())
    return response


def server_handshake(server: ServerProtocol, header_values: list[str] | None):
    """Run a handshake on the server's Sans-I/O parser and return the response."""
    request = make_request()
    if header_values is not None:
        for header_value in header_values:
            request.headers["Sec-WebSocket-Extensions"] = header_value
    server.receive_data(request.serialize())
    [parsed_request] = server.events_received()
    response = server.accept(parsed_request)
    server.send_response(response)
    return response


def assert_not_open(testcase: unittest.TestCase, protocol) -> None:
    """A failed handshake never reaches OPEN and no extension is enabled."""
    testcase.assertEqual(protocol.state, CONNECTING)
    testcase.assertIsNotNone(protocol.handshake_exc)
    testcase.assertEqual(protocol.extensions, [])


def masked_text_frame(payload: bytes, *, rsv1: bool = False) -> bytes:
    """Build a masked text frame as a client would send on the wire."""
    mask = b"\x12\x34\x56\x78"
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    first_byte = 0xC1 if rsv1 else 0x81
    return bytes([first_byte, 0x85]) + mask + masked


class FactoryParameterValidationTests(unittest.TestCase):
    """The factories themselves distinguish the parameter error kinds."""

    def test_client_factory_rejects_duplicate_parameter(self):
        factory = ClientPerMessageDeflateFactory()
        with self.assertRaises(DuplicateParameter):
            factory.process_response_params(
                [("server_no_context_takeover", None)] * 2,
                [],
            )

    def test_client_factory_rejects_illegal_window_value(self):
        factory = ClientPerMessageDeflateFactory()
        with self.assertRaises(InvalidParameterValue):
            factory.process_response_params(
                [("server_max_window_bits", "16")],
                [],
            )

    def test_server_factory_rejects_duplicate_parameter(self):
        factory = ServerPerMessageDeflateFactory()
        with self.assertRaises(DuplicateParameter):
            factory.process_request_params(
                [("client_no_context_takeover", None)] * 2,
                [],
            )

    def test_server_factory_rejects_illegal_window_value(self):
        factory = ServerPerMessageDeflateFactory()
        with self.assertRaises(InvalidParameterValue):
            factory.process_request_params(
                [("server_max_window_bits", "7")],
                [],
            )

    def test_client_factory_rejects_unoffered_parameter(self):
        factory = ClientPerMessageDeflateFactory(client_max_window_bits=None)
        with self.assertRaises(NegotiationError):
            factory.process_response_params(
                [("client_max_window_bits", "10")],
                [],
            )

    def test_client_factory_rejects_window_larger_than_offer(self):
        factory = ClientPerMessageDeflateFactory(server_max_window_bits=10)
        with self.assertRaises(NegotiationError):
            factory.process_response_params(
                [("server_max_window_bits", "15")],
                [],
            )

    def test_client_factory_rejects_duplicate_extension(self):
        factory = ClientPerMessageDeflateFactory()
        with self.assertRaises(NegotiationError):
            factory.process_response_params(
                [],
                [PerMessageDeflate(False, False, 15, 15)],
            )

    def test_server_factory_rejects_duplicate_extension(self):
        factory = ServerPerMessageDeflateFactory()
        with self.assertRaises(NegotiationError):
            factory.process_request_params(
                [],
                [PerMessageDeflate(False, False, 15, 15)],
            )


class ClientNegotiationTests(unittest.TestCase):
    """Client-side processing of Sec-WebSocket-Extensions response headers."""

    def check_failure(
        self,
        client: ClientProtocol,
        header_values: list[str],
    ) -> None:
        client_handshake(client, header_values)
        assert_not_open(self, client)
        # The protocol layer reports every mismatch as a NegotiationError.
        self.assertIsInstance(client.handshake_exc, NegotiationError)
        # The failed handshake is reported as a response carrying the error.
        [response] = client.events_received()
        self.assertIs(response._exception, client.handshake_exc)
        # A business frame received afterwards is discarded, not reported.
        client.receive_data(masked_text_frame(b"hello"))
        self.assertEqual(client.events_received(), [])
        self.assertEqual(client.state, CONNECTING)

    def test_multiple_header_lines_accept_two_extensions(self):
        """Two header lines enable two distinct extensions in response order."""
        log = []
        client = ClientProtocol(
            URI,
            extensions=[
                ClientRecordingExtensionFactory("x-a", log),
                ClientRecordingExtensionFactory("x-b", log),
            ],
        )
        client_handshake(client, ["x-a", "x-b"])

        self.assertEqual(client.state, OPEN)
        self.assertEqual(
            client.extensions,
            [RecordingExtension("x-a", log), RecordingExtension("x-b", log)],
        )

    def test_multiple_header_lines_permessage_deflate(self):
        """A valid permessage-deflate response yields the expected object."""
        factory = ClientPerMessageDeflateFactory(
            server_no_context_takeover=True,
            server_max_window_bits=10,
        )
        client = ClientProtocol(URI, extensions=[factory])
        response = client_handshake(
            client,
            [
                "permessage-deflate; server_no_context_takeover; "
                "server_max_window_bits=10"
            ],
        )

        self.assertEqual(client.state, OPEN)
        self.assertEqual(
            response.headers["Sec-WebSocket-Extensions"],
            "permessage-deflate; server_no_context_takeover; server_max_window_bits=10",
        )
        self.assertEqual(len(client.extensions), 1)
        extension = client.extensions[0]
        self.assertIsInstance(extension, PerMessageDeflate)
        self.assertEqual(extension.remote_no_context_takeover, True)
        self.assertEqual(extension.local_no_context_takeover, False)
        self.assertEqual(extension.remote_max_window_bits, 10)
        self.assertEqual(extension.local_max_window_bits, 15)

    def test_same_name_candidates_picks_compatible_alternative(self):
        """Repeated names in the offer are alternatives; the response picks one."""
        client = ClientProtocol(
            URI,
            extensions=[
                # First alternative requires 10-bit windows; the server's
                # response doesn't satisfy it, so it must be skipped.
                ClientPerMessageDeflateFactory(server_max_window_bits=10),
                # Second alternative accepts the default 15-bit windows.
                ClientPerMessageDeflateFactory(),
            ],
        )
        client_handshake(client, ["permessage-deflate"])

        self.assertEqual(client.state, OPEN)
        self.assertEqual(len(client.extensions), 1)
        self.assertIsInstance(client.extensions[0], PerMessageDeflate)
        self.assertEqual(client.extensions[0].remote_max_window_bits, 15)

    def test_duplicate_parameter_rejected(self):
        """A parameter repeated within one extension item is rejected."""
        client = ClientProtocol(
            URI,
            extensions=[ClientPerMessageDeflateFactory()],
        )
        self.check_failure(
            client,
            [
                "permessage-deflate; server_no_context_takeover; "
                "server_no_context_takeover"
            ],
        )

    def test_illegal_window_value_rejected(self):
        """Window sizes outside 8..15 fail parameter validation."""
        for value in ["7", "16", "42"]:
            with self.subTest(value=value):
                client = ClientProtocol(
                    URI,
                    extensions=[ClientPerMessageDeflateFactory()],
                )
                self.check_failure(
                    client,
                    [f"permessage-deflate; server_max_window_bits={value}"],
                )

    def test_unoffered_parameter_rejected(self):
        """The server may not enable a parameter the client didn't offer."""
        client = ClientProtocol(
            URI,
            extensions=[ClientPerMessageDeflateFactory(client_max_window_bits=None)],
        )
        self.check_failure(
            client,
            ["permessage-deflate; client_max_window_bits=10"],
        )

    def test_response_window_larger_than_offer_rejected(self):
        """A response window larger than the offer expands decoding rights."""
        client = ClientProtocol(
            URI,
            extensions=[ClientPerMessageDeflateFactory(server_max_window_bits=10)],
        )
        self.check_failure(
            client,
            ["permessage-deflate; server_max_window_bits=15"],
        )

    def test_duplicate_extension_in_response_rejected(self):
        """Two accepted permessage-deflate items in a response are invalid."""
        client = ClientProtocol(
            URI,
            extensions=[
                ClientPerMessageDeflateFactory(),
                ClientPerMessageDeflateFactory(),
            ],
        )
        self.check_failure(
            client,
            ["permessage-deflate", "permessage-deflate"],
        )

    def test_failed_handshake_does_not_enter_business_handling(self):
        """An unsupported extension name aborts before any frame processing."""
        client = ClientProtocol(
            URI,
            extensions=[ClientPerMessageDeflateFactory()],
        )
        self.check_failure(client, ["x-unknown"])


class ServerNegotiationTests(unittest.TestCase):
    """Server-side processing of Sec-WebSocket-Extensions request headers."""

    def test_multiple_header_lines_two_extensions(self):
        """Two request header lines can enable two distinct extensions."""
        log = []
        server = ServerProtocol(
            extensions=[
                ServerRecordingExtensionFactory("x-a", log),
                ServerRecordingExtensionFactory("x-b", log),
            ],
        )
        response = server_handshake(server, ["x-a", "x-b"])

        self.assertEqual(server.state, OPEN)
        self.assertEqual(response.headers["Sec-WebSocket-Extensions"], "x-a, x-b")
        self.assertEqual(len(server.extensions), 2)
        self.assertEqual(
            server.extensions,
            [RecordingExtension("x-a", log), RecordingExtension("x-b", log)],
        )

    def test_same_name_candidates_alternative_accepted(self):
        """A second, acceptable alternative is selected when the first fails."""
        server = ServerProtocol(
            extensions=[
                # The default factory cannot honor a 10-bit offer due to its
                # configuration, so the explicitly configured one is used.
                ServerPerMessageDeflateFactory(),
                ServerPerMessageDeflateFactory(server_max_window_bits=10),
            ],
        )
        response = server_handshake(
            server,
            ["permessage-deflate; server_max_window_bits=10"],
        )

        self.assertEqual(server.state, OPEN)
        self.assertEqual(
            response.headers["Sec-WebSocket-Extensions"],
            "permessage-deflate; server_max_window_bits=10",
        )
        self.assertEqual(len(server.extensions), 1)
        extension = server.extensions[0]
        self.assertIsInstance(extension, PerMessageDeflate)
        self.assertEqual(extension.local_max_window_bits, 10)

    def test_same_name_candidates_duplicate_after_accept_rejected(self):
        """Once the name is accepted, a second offer of it is a duplicate."""
        server = ServerProtocol(
            extensions=[
                ServerPerMessageDeflateFactory(),
                ServerPerMessageDeflateFactory(),
            ],
        )
        response = server_handshake(
            server, ["permessage-deflate", "permessage-deflate"]
        )

        # The duplicate is skipped: the handshake still succeeds but only one
        # extension is enabled and only one item appears in the response.
        self.assertEqual(server.state, OPEN)
        self.assertEqual(
            response.headers["Sec-WebSocket-Extensions"], "permessage-deflate"
        )
        self.assertEqual(len(server.extensions), 1)
        self.assertIsInstance(server.extensions[0], PerMessageDeflate)

    def test_duplicate_parameter_declines_offer(self):
        """A repeated parameter makes that offer unmatchable and it is skipped."""
        server = ServerProtocol(
            extensions=[ServerPerMessageDeflateFactory()],
        )
        response = server_handshake(
            server,
            [
                "permessage-deflate; client_no_context_takeover; "
                "client_no_context_takeover"
            ],
        )

        self.assertEqual(server.state, OPEN)
        self.assertNotIn("Sec-WebSocket-Extensions", response.headers)
        self.assertEqual(server.extensions, [])

    def test_illegal_window_value_declines_offer(self):
        """Illegal window sizes make the offer unmatchable, not fatal."""
        for value in ["7", "16"]:
            with self.subTest(value=value):
                server = ServerProtocol(
                    extensions=[ServerPerMessageDeflateFactory()],
                )
                response = server_handshake(
                    server,
                    [f"permessage-deflate; server_max_window_bits={value}"],
                )

                self.assertEqual(server.state, OPEN)
                self.assertNotIn("Sec-WebSocket-Extensions", response.headers)
                self.assertEqual(server.extensions, [])

    def test_declined_offer_does_not_process_compressed_frame(self):
        """No extension enabled means a compressed (RSV1) frame is rejected."""
        server = ServerProtocol(
            extensions=[ServerPerMessageDeflateFactory()],
        )
        server_handshake(
            server,
            ["permessage-deflate; server_max_window_bits=16"],
        )
        self.assertEqual(server.extensions, [])

        # A masked frame with RSV1 set cannot be decoded without the extension.
        server.receive_data(masked_text_frame(b"hello", rsv1=True))
        self.assertIsInstance(server.parser_exc, ProtocolError)
        self.assertEqual(str(server.parser_exc), "reserved bits must be 0")
        # The invalid frame must not surface as a business message.
        self.assertEqual(server.events_received(), [])

    def test_malformed_extension_header_aborts_handshake(self):
        """An unparseable extension header aborts the handshake outright."""
        server = ServerProtocol(
            extensions=[ServerPerMessageDeflateFactory()],
        )
        request = make_request()
        # '=' without a value is not a valid extension header.
        request.headers["Sec-WebSocket-Extensions"] = "permessage-deflate; ="
        response = server.accept(request)
        server.send_response(response)

        assert_not_open(self, server)
        self.assertIsInstance(server.handshake_exc, InvalidHeaderFormat)
        # The handshake failed, so a business frame must be discarded.
        server.receive_data(masked_text_frame(b"hello"))
        self.assertEqual(server.events_received(), [])
        self.assertEqual(server.state, CONNECTING)

    def test_duplicate_extension_does_not_double_transform(self):
        """Accepting the same name twice must not install two transformers."""
        server = ServerProtocol(
            extensions=[
                ServerPerMessageDeflateFactory(),
                ServerPerMessageDeflateFactory(),
            ],
        )
        server_handshake(server, ["permessage-deflate", "permessage-deflate"])
        self.assertEqual(len(server.extensions), 1)


class NegotiatedTransformChainTests(FramesTestCase):
    """End-to-end checks that the negotiated header matches the transform chain."""

    def negotiate_pair(self, client_factories, server_factories, request_header):
        with patch("websockets.client.generate_key", return_value=KEY):
            client = ClientProtocol(URI, extensions=client_factories)
        server = ServerProtocol(extensions=server_factories)

        request = make_request()
        request.headers["Sec-WebSocket-Extensions"] = request_header
        response = server.accept(request)
        server.send_response(response)
        client.receive_data(response.serialize())

        self.assertEqual(client.state, OPEN)
        self.assertEqual(server.state, OPEN)
        return client, server, response

    def test_deflate_fragmented_message_roundtrip(self):
        """A fragmented message is compressed once and decompresses correctly."""
        client, server, response = self.negotiate_pair(
            [ClientPerMessageDeflateFactory()],
            [ServerPerMessageDeflateFactory()],
            "permessage-deflate",
        )

        # The negotiated header matches the single enabled extension object.
        self.assertEqual(
            response.headers["Sec-WebSocket-Extensions"], "permessage-deflate"
        )
        self.assertEqual(len(client.extensions), 1)
        self.assertEqual(len(server.extensions), 1)
        self.assertIsInstance(client.extensions[0], PerMessageDeflate)
        self.assertIsInstance(server.extensions[0], PerMessageDeflate)

        encode_calls = CountCalls(client.extensions[0], "encode")
        decode_calls = CountCalls(server.extensions[0], "decode")

        payload = b"compressed fragmented message payload" * 8
        fragments = [payload[:20], payload[20:60], payload[60:]]
        client.send_text(fragments[0], fin=False)
        client.send_continuation(fragments[1], fin=False)
        client.send_continuation(fragments[2], fin=True)

        wire = b"".join(client.data_to_send())
        wire_rsv1 = [
            bool(first_byte & 0x40) for first_byte in self.frame_header_bytes(wire)
        ]
        frames = self.parse_frames(wire, mask=True, extensions=server.extensions)

        # One transform per frame: the first frame carries RSV1 on the wire,
        # continuations keep RSV1 clear, and the reassembled payload matches.
        self.assertEqual(encode_calls.count, 3)
        self.assertEqual(decode_calls.count, 3)
        self.assertEqual(wire_rsv1, [True, False, False])
        # After decoding, RSV1 is cleared on every frame.
        self.assertFalse(any(frame.rsv1 for frame in frames))
        self.assertEqual([frame.opcode for frame in frames], [TEXT, CONT, CONT])
        self.assertEqual(b"".join(frame.data for frame in frames), payload)

    def test_deflate_parameters_match_extension_object(self):
        """Each negotiated parameter is reflected on the enabled object."""
        client, server, response = self.negotiate_pair(
            [
                ClientPerMessageDeflateFactory(
                    server_no_context_takeover=True,
                    client_no_context_takeover=True,
                    server_max_window_bits=12,
                    client_max_window_bits=12,
                )
            ],
            [
                ServerPerMessageDeflateFactory(
                    server_max_window_bits=12, client_max_window_bits=12
                )
            ],
            "permessage-deflate; client_no_context_takeover; "
            "server_no_context_takeover; server_max_window_bits=12; "
            "client_max_window_bits=12",
        )

        header = response.headers["Sec-WebSocket-Extensions"]
        self.assertIn("server_no_context_takeover", header)
        self.assertIn("client_no_context_takeover", header)
        self.assertIn("server_max_window_bits=12", header)
        self.assertIn("client_max_window_bits=12", header)

        client_ext = client.extensions[0]
        server_ext = server.extensions[0]
        # Client view: server side is remote. Server view: client is remote.
        self.assertTrue(client_ext.remote_no_context_takeover)
        self.assertTrue(client_ext.local_no_context_takeover)
        self.assertEqual(client_ext.remote_max_window_bits, 12)
        self.assertEqual(client_ext.local_max_window_bits, 12)
        self.assertTrue(server_ext.remote_no_context_takeover)
        self.assertTrue(server_ext.local_no_context_takeover)
        self.assertEqual(server_ext.remote_max_window_bits, 12)
        self.assertEqual(server_ext.local_max_window_bits, 12)

    def test_control_frames_bypass_extensions(self):
        """Control frames are not transformed and don't touch RSV1."""
        client, server, _ = self.negotiate_pair(
            [ClientPerMessageDeflateFactory()],
            [ServerPerMessageDeflateFactory()],
            "permessage-deflate",
        )

        client.send_ping(b"hi")

        wire = b"".join(client.data_to_send())
        # The encoder skips control frames: the ping keeps RSV1 clear and its
        # payload is untouched on the wire.
        self.assertFalse(bool(wire[0] & 0x40))
        frame = self.parse(wire, mask=True, extensions=server.extensions)
        self.assertFalse(frame.rsv1)
        self.assertEqual(frame.opcode, PING)
        self.assertEqual(frame.data, b"hi")

    def test_multi_extension_encode_and_decode_order(self):
        """With several extensions, encode runs in order and decode reversed."""
        client_log: list[str] = []
        server_log: list[str] = []
        client, server, _ = self.negotiate_pair(
            [
                ClientRecordingExtensionFactory("x-a", client_log),
                ClientRecordingExtensionFactory("x-b", client_log),
            ],
            [
                ServerRecordingExtensionFactory("x-a", server_log),
                ServerRecordingExtensionFactory("x-b", server_log),
            ],
            "x-a, x-b",
        )

        self.assertEqual(
            [extension.name for extension in client.extensions], ["x-a", "x-b"]
        )
        self.assertEqual(
            [extension.name for extension in server.extensions], ["x-a", "x-b"]
        )

        # Encoding on the way out applies extensions in negotiated order.
        client.send_text(b"ordered")
        wire = b"".join(client.data_to_send())
        self.assertEqual(client_log, ["encode x-a", "encode x-b"])

        # Decoding on the way in applies them in reverse order.
        self.parse_frames(wire, mask=True, extensions=server.extensions)
        self.assertEqual(server_log, ["decode x-b", "decode x-a"])

    def test_multi_extension_with_deflate_rsv1_and_order(self):
        """Deflate sets RSV1 while a second recording extension still runs."""
        log: list[str] = []
        client, server, response = self.negotiate_pair(
            [
                ClientPerMessageDeflateFactory(),
                ClientRecordingExtensionFactory("x-trace", log),
            ],
            [
                ServerPerMessageDeflateFactory(),
                ServerRecordingExtensionFactory("x-trace", log),
            ],
            "permessage-deflate, x-trace",
        )

        self.assertEqual(
            response.headers["Sec-WebSocket-Extensions"],
            "permessage-deflate, x-trace",
        )
        self.assertIsInstance(client.extensions[0], PerMessageDeflate)
        self.assertIsInstance(client.extensions[1], RecordingExtension)

        client.send_text(b"hello deflate plus trace")
        wire = b"".join(client.data_to_send())
        frames = self.parse_frames(wire, mask=True, extensions=server.extensions)

        self.assertEqual(len(frames), 1)
        # After decode the frame is uncompressed and RSV1 is cleared.
        self.assertFalse(frames[0].rsv1)
        self.assertEqual(frames[0].data, b"hello deflate plus trace")
        # The recording extension saw the frame once on encode and once decode.
        self.assertIn("encode x-trace", log)
        self.assertIn("decode x-trace", log)

    def frame_header_bytes(self, data):
        """Return the first header byte of every frame contained in data."""
        first_bytes = []
        remaining = memoryview(data)
        while len(remaining) > 0:
            first_bytes.append(int(remaining[0]))
            length = int(remaining[1]) & 0x7F
            header_size = 2
            if length == 126:
                length = int.from_bytes(remaining[2:4], "big")
                header_size = 4
            elif length == 127:
                length = int.from_bytes(remaining[2:10], "big")
                header_size = 10
            if int(remaining[1]) & 0x80:
                header_size += 4
            remaining = remaining[header_size + length :]
        return first_bytes

    def parse_frames(self, data, mask, extensions):
        """Parse every frame contained in data using the given extensions."""
        frames = []
        remaining = memoryview(data)
        while len(remaining) > 0:
            length = int(remaining[1]) & 0x7F
            header_size = 2
            if length == 126:
                length = int.from_bytes(remaining[2:4], "big")
                header_size = 4
            elif length == 127:
                length = int.from_bytes(remaining[2:10], "big")
                header_size = 10
            if mask:
                header_size += 4
            frame_size = header_size + length
            frame = self.parse(
                bytes(remaining[:frame_size]),
                mask=mask,
                extensions=extensions,
            )
            frames.append(frame)
            remaining = remaining[frame_size:]
        return frames


class CountCalls:
    """Wrap a bound method to count invocations without changing behavior."""

    def __init__(self, target, method_name: str) -> None:
        original = getattr(target, method_name)
        self.count = 0

        def wrapper(*args, **kwargs):
            self.count += 1
            return original(*args, **kwargs)

        setattr(target, method_name, wrapper)


if __name__ == "__main__":
    unittest.main()
