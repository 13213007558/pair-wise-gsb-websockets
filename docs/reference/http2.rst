HTTP/2 (Sans-I/O)
=================

.. _Sans-I/O: https://sans-io.readthedocs.io/

.. currentmodule:: websockets.http2

:mod:`websockets.http2` provides WebSocket over HTTP/2 (:rfc:`8441`).

WebSocket connections are established with the Extended CONNECT method and
multiplexed over a single HTTP/2 connection, alongside regular HTTP/2
streams. Framing reuses the Sans-I/O_ WebSocket protocol implementation.

This subpackage requires the h2_ package, which isn't a dependency of
websockets. Install it with ``python -m pip install h2``.

.. _h2: https://python-hyper.org/projects/h2/

Client
------

.. autoclass:: ClientConnection

    .. automethod:: connect

Server
------

.. autoclass:: ServerConnection

    .. automethod:: accept

    .. automethod:: reject

Connection
----------

.. autoclass:: Connection

    .. automethod:: initiate

    .. automethod:: receive_data

    .. automethod:: receive_eof

    .. automethod:: send_continuation

    .. automethod:: send_text

    .. automethod:: send_binary

    .. automethod:: send_close

    .. automethod:: send_ping

    .. automethod:: send_pong

    .. automethod:: reset_stream

    .. automethod:: send_paused

    .. automethod:: close_connection

    .. automethod:: events_received

    .. automethod:: data_to_send

    .. autoproperty:: extended_connect_enabled

Events
------

.. currentmodule:: websockets.http2.events

.. autoclass:: WebSocketRequest

.. autoclass:: WebSocketOpen

.. autoclass:: WebSocketHandshakeFailed

.. autoclass:: WebSocketFrame

.. autoclass:: WebSocketWritable

.. autoclass:: WebSocketClosed

.. autoclass:: WebSocketEndOfStream

.. autoclass:: WebSocketReset

.. autoclass:: ConnectionClosed

