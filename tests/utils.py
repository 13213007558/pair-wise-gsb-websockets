import contextlib
import email.utils
import os
import pathlib
import platform
import ssl
import tempfile
import time
import unittest
import warnings

from websockets.frames import BINARY, TEXT
from websockets.version import released


# Generate TLS certificate with:
# $ openssl req -x509 -config test_localhost.cnf -days 15340 -newkey rsa:2048 \
#       -out test_localhost.crt -keyout test_localhost.key
# $ cat test_localhost.key test_localhost.crt > test_localhost.pem
# $ rm test_localhost.key test_localhost.crt

CERTIFICATE = pathlib.Path(__file__).with_name("test_localhost.pem")

CLIENT_CONTEXT = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
CLIENT_CONTEXT.load_verify_locations(bytes(CERTIFICATE))

SERVER_CONTEXT = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
SERVER_CONTEXT.load_cert_chain(bytes(CERTIFICATE))

# Work around https://github.com/openssl/openssl/issues/7967

# This bug causes connect() to hang in tests for the client. Including this
# workaround acknowledges that the issue could happen outside of the test suite.

# It shouldn't happen too often, or else OpenSSL 1.1.1 would be unusable. If it
# happens, we can look for a library-level fix, but it won't be easy.

SERVER_CONTEXT.num_tickets = 0


DATE = email.utils.formatdate(usegmt=True)


# Unit for timeouts. May be increased in slow or noisy environments by setting
# the WEBSOCKETS_TESTS_TIMEOUT_FACTOR environment variable.

# Downstream distributors insist on running the test suite despites my plea to
# the contrary. They do it on build farms with unstable performance, leading to
# flakiness, and then they file bugs. Make tests 100x slower to avoid flakiness.

MS = 0.001 * float(
    os.environ.get(
        "WEBSOCKETS_TESTS_TIMEOUT_FACTOR",
        "100" if released else "1",
    )
)

# PyPy, asyncio's debug mode, and coverage penalize performance of this
# test suite. Increase timeouts to reduce the risk of spurious failures.
if platform.python_implementation() == "PyPy":  # pragma: no cover
    MS *= 2
if os.environ.get("PYTHONASYNCIODEBUG"):  # pragma: no cover
    MS *= 2
if os.environ.get("COVERAGE_RUN"):  # pragma: no branch
    MS *= 2

# Ensure that timeouts are larger than the clock's resolution (for Windows).
MS = max(MS, 2.5 * time.get_clock_info("monotonic").resolution)


def handshake(client_protocol, server_protocol):
    """
    Run a real opening handshake between two Sans-I/O protocols.

    """
    request = client_protocol.connect()
    client_protocol.send_request(request)
    server_protocol.receive_data(b"".join(client_protocol.data_to_send()))
    (request_event,) = server_protocol.events_received()
    response = server_protocol.accept(request_event)
    server_protocol.send_response(response)
    client_protocol.receive_data(b"".join(server_protocol.data_to_send()))
    client_protocol.events_received()


def record_rsv1(connection):
    """
    Record the RSV1 bit of incoming data frames on a connection.

    Return a list that grows by one boolean per incoming message.

    """
    rsv1_records = []
    for extension in connection.protocol.extensions:
        decode = extension.decode

        def decode_and_record(frame, *, max_size=None, decode=decode):
            if frame.opcode in (TEXT, BINARY):
                rsv1_records.append(frame.rsv1)
            return decode(frame, max_size=max_size)

        extension.decode = decode_and_record
    return rsv1_records


class GeneratorTestCase(unittest.TestCase):
    """
    Base class for testing generator-based coroutines.

    """

    def assertGeneratorRunning(self, gen):
        """
        Check that a generator-based coroutine hasn't completed yet.

        """
        next(gen)

    def assertGeneratorReturns(self, gen):
        """
        Check that a generator-based coroutine completes and return its value.

        """
        with self.assertRaises(StopIteration) as raised:
            next(gen)
        return raised.exception.value


class LoggingTestCase(unittest.TestCase):
    """
    Base class for testing logging output.

    """

    def assertExceptionLogged(self, logs, log_message, exc_message):
        """
        Check that a log record with the given message carries the given exception.

        """
        messages = [record.getMessage() for record in logs.records]
        self.assertIn(log_message, messages)
        record = logs.records[messages.index(log_message)]
        self.assertEqual(str(record.exc_info[1]), exc_message)


class DeprecationTestCase(unittest.TestCase):
    """
    Base class for testing deprecations.

    """

    @contextlib.contextmanager
    def assertDeprecationWarning(self, message):
        """
        Check that a deprecation warning was raised with the given message.

        """
        with warnings.catch_warnings(record=True) as recorded_warnings:
            warnings.simplefilter("always")
            yield

        self.assertEqual(len(recorded_warnings), 1)
        warning = recorded_warnings[0]
        self.assertEqual(warning.category, DeprecationWarning)
        self.assertEqual(str(warning.message), message)


async def alist(async_iterable):
    items = []
    async for item in async_iterable:
        items.append(item)
    return items


@contextlib.contextmanager
def temp_unix_socket_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield str(pathlib.Path(temp_dir) / "websockets")
