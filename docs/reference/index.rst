API reference
=============

.. currentmodule:: websockets

Features
--------

Check which implementations support which features and known limitations.

.. toctree::
   :titlesonly:

   features

:mod:`asyncio`
--------------

It's ideal for servers that handle many clients concurrently.

This is the default implementation.

.. toctree::
   :titlesonly:

   asyncio/server
   asyncio/client

:mod:`threading`
----------------

This alternative implementation can be a good choice for clients.

.. toctree::
   :titlesonly:

   sync/server
   sync/client

:mod:`trio`
------------

This is another option for servers that handle many clients concurrently.

.. toctree::
   :titlesonly:

   trio/server
   trio/client

`Sans-I/O`_
-----------

This layer is designed for integrating in third-party libraries, typically
application servers.

.. _Sans-I/O: https://sans-io.readthedocs.io/

.. toctree::
   :titlesonly:

   sansio/server
   sansio/client

:mod:`http2`
--------------

This layer provides WebSocket over HTTP/2 (:rfc:`8441`) on top of h2_.

.. _h2: https://python-hyper.org/projects/h2/

.. toctree::
   :titlesonly:

   http2

Legacy
------

This is the historical implementation. It is deprecated. It will be removed by
2030.

.. toctree::
   :titlesonly:

   legacy/server
   legacy/client

Extensions
----------

The Per-Message Deflate extension is built-in. You may also define custom
extensions.

.. toctree::
   :titlesonly:

   extensions

Shared
------

These low-level APIs are shared by all implementations.

.. toctree::
   :titlesonly:

   datastructures
   exceptions
   types
   variables

API stability
-------------

Public APIs documented in this API reference are subject to the
:ref:`backwards-compatibility policy <backwards-compatibility policy>`.

Anything that isn't listed in the API reference is a private API. There's no
guarantees of behavior or backwards-compatibility for private APIs.

Convenience imports
-------------------

For convenience, some public APIs can be imported directly from the
``websockets`` package.
