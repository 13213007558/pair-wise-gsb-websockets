#!/usr/bin/env python

import asyncio

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosedOK


def accept(first_fragment):
    """Business validation of a message based on its first fragment."""
    return "forbidden" not in first_fragment


async def handler(websocket):
    """Reject a message after reading its first fragment.

    The iterator returned by recv_streaming() is always consumed to the
    end, even when the message is rejected after its first fragment. This
    discards the rest of the rejected message and keeps the connection
    usable for receiving the next message.

    If you stop consuming the iterator in the middle of a message, the
    connection becomes unusable: recv() and recv_streaming() raise
    ConcurrencyError because the rest of the rejected message is still
    waiting to be delivered. In that case, you MUST close the connection
    with websocket.close().
    """
    try:
        while True:
            fragments = websocket.recv_streaming()
            rejected = not accept(await anext(fragments))
            # Discard the rest of the message before receiving the next one.
            async for _ in fragments:
                pass
            if rejected:
                await websocket.send("error: message rejected")
            else:
                await websocket.send("ok: message accepted")
    except ConnectionClosedOK:
        pass


async def main():
    server = await serve(handler, "localhost", 8765)
    await server.serve_forever()


asyncio.run(main())
