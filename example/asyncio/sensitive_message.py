#!/usr/bin/env python

"""Send a sensitive message without compressing it (asyncio API).

This example sends a normal -> sensitive -> normal sequence over a WebSocket
connection with permessage-deflate enabled. Only the message carrying a
one-time credential is sent with ``compress=False``. It inspects the RSV1 bit
of the outgoing frames and decodes the sequence through a real receiver.

"""

import asyncio
import socket

from websockets.asyncio.connection import Connection
from websockets.extensions.permessage_deflate import PerMessageDeflate
from websockets.frames import Frame
from websockets.protocol import CLIENT, Protocol, SERVER


class RecordingPerMessageDeflate(PerMessageDeflate):
    """PerMessageDeflate that records the RSV1 bit of every encoded frame."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rsv1_bits = []

    def encode(self, frame: Frame) -> Frame:
        encoded = super().encode(frame)
        self.rsv1_bits.append(encoded.rsv1)
        return encoded


async def main():
    socket_a, socket_b = socket.socketpair()

    loop = asyncio.get_running_loop()
    client_protocol = Protocol(CLIENT)
    server_protocol = Protocol(SERVER)
    # Both sides negotiate permessage-deflate, with context takeover enabled.
    client_extension = RecordingPerMessageDeflate(False, False, 15, 15)
    client_protocol.extensions = [client_extension]
    server_protocol.extensions = [PerMessageDeflate(False, False, 15, 15)]

    client_transport, client = await loop.create_connection(
        lambda: Connection(client_protocol),
        sock=socket_a,
    )
    server_transport, server = await loop.create_connection(
        lambda: Connection(server_protocol),
        sock=socket_b,
    )

    try:
        messages = [
            (b'{"event": "ordinary update"}', True),
            (b'{"credential": "one-time-secret-token"}', False),
            (b'{"event": "ordinary update"}', True),
        ]

        for message, compress in messages:
            # Only the message containing the credential disables compression.
            await client.send(message, compress=compress)
            print(
                f"sent {message.decode()!r:48} compress={compress!s:5} "
                f"RSV1 on wire={client_extension.rsv1_bits[-1]}"
            )

        # The real receiver decodes the entire sequence, including the
        # uncompressed message between two compressed ones.
        for message, _ in messages:
            received = await server.recv()
            print(f"receiver decoded {received!r}")
            assert received == message

        # The sensitive frame is the only one sent without the RSV1 bit.
        assert client_extension.rsv1_bits == [True, False, True]
    finally:
        await client.close()
        await server.close()
        client_transport.close()
        server_transport.close()


if __name__ == "__main__":
    asyncio.run(main())
