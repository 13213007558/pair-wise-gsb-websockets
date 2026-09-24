from __future__ import annotations

from websockets.http2 import ClientConnection, ServerConnection
from websockets.uri import parse_uri


URI = parse_uri("wss://example.com/chat")


class Link:
    """
    In-memory link between the h2 state machines of a client and a server.

    Bytes exchanged in each direction are recorded for inspection.

    """

    def __init__(self, client: ClientConnection, server: ServerConnection):
        self.client = client
        self.server = server
        self.client_bytes = bytearray()  # client -> server
        self.server_bytes = bytearray()  # server -> client

    def pump(self) -> None:
        """Exchange data between both ends until neither has more to send."""
        moved = True
        while moved:
            moved = False
            data = self.client.data_to_send()
            if data:
                self.client_bytes += data
                self.server.receive_data(data)
                moved = True
            data = self.server.data_to_send()
            if data:
                self.server_bytes += data
                self.client.receive_data(data)
                moved = True

    def forward(self) -> None:
        """Send pending data from client to server only."""
        data = self.client.data_to_send()
        if data:
            self.client_bytes += data
            self.server.receive_data(data)

    def backward(self) -> None:
        """Send pending data from server to client only."""
        data = self.server.data_to_send()
        if data:
            self.server_bytes += data
            self.client.receive_data(data)


def make_link(
    client: ClientConnection | None = None,
    server: ServerConnection | None = None,
) -> Link:
    """Create connected and initiated client and server, linked in memory."""
    if client is None:
        client = ClientConnection()
    if server is None:
        server = ServerConnection()
    link = Link(client, server)
    client.initiate()
    server.initiate()
    link.pump()
    client.events_received()
    server.events_received()
    return link


def open_websocket(link: Link, uri=URI, **kwargs) -> int:
    """Open a WebSocket stream on the client and accept it on the server."""
    stream_id = link.client.connect(uri, **kwargs)
    link.pump()
    link.server.accept(stream_id)
    link.pump()
    link.client.events_received()
    link.server.events_received()
    return stream_id
