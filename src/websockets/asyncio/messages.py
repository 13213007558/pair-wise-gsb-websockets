from __future__ import annotations

import asyncio
import codecs
import collections
from collections.abc import AsyncIterator, Iterable
from typing import Any, Callable, Generic, Literal, TypeVar, overload

from ..exceptions import ConcurrencyError
from ..frames import BINARY, CONT, TEXT, Frame
from ..typing import Data


__all__ = ["Assembler", "MessageReader"]

UTF8Decoder = codecs.getincrementaldecoder("utf-8")

T = TypeVar("T")


class SimpleQueue(Generic[T]):
    """
    Simplified version of :class:`asyncio.Queue`.

    Provides only the subset of functionality needed by :class:`Assembler`.

    """

    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.get_waiter: asyncio.Future[None] | None = None
        self.queue: collections.deque[T] = collections.deque()

    def __len__(self) -> int:
        return len(self.queue)

    def put(self, item: T) -> None:
        """Put an item into the queue."""
        self.queue.append(item)
        if self.get_waiter is not None and not self.get_waiter.done():
            self.get_waiter.set_result(None)

    async def get(self, block: bool = True) -> T:
        """Remove and return an item from the queue, waiting if necessary."""
        if not self.queue:
            if not block:
                raise EOFError("stream of frames ended")
            assert self.get_waiter is None, "cannot call get() concurrently"
            self.get_waiter = self.loop.create_future()
            try:
                await self.get_waiter
            finally:
                self.get_waiter.cancel()
                self.get_waiter = None
        return self.queue.popleft()

    def reset(self, items: Iterable[T]) -> None:
        """Put back items into an empty, idle queue."""
        assert self.get_waiter is None, "cannot reset() while get() is running"
        assert not self.queue, "cannot reset() while queue isn't empty"
        self.queue.extend(items)

    def abort(self) -> None:
        """Close the queue, raising EOFError in get() if necessary."""
        if self.get_waiter is not None and not self.get_waiter.done():
            self.get_waiter.set_exception(EOFError("stream of frames ended"))


class Assembler:
    """
    Assemble messages from frames.

    :class:`Assembler` expects only data frames. The stream of frames must
    respect the protocol; if it doesn't, the behavior is undefined.

    Args:
        pause: Called when the buffer of frames goes above the high water mark;
            should pause reading from the network.
        resume: Called when the buffer of frames goes below the low water mark;
            should resume reading from the network.

    """

    def __init__(
        self,
        high: int | None = None,
        low: int | None = None,
        pause: Callable[[], Any] = lambda: None,
        resume: Callable[[], Any] = lambda: None,
    ) -> None:
        # Queue of incoming frames.
        self.frames: SimpleQueue[Frame] = SimpleQueue()

        # We cannot put a hard limit on the size of the queue because a single
        # call to Protocol.data_received() could produce thousands of frames,
        # which must be buffered. Instead, we pause reading when the buffer goes
        # above the high limit and we resume when it goes under the low limit.
        if high is not None and low is None:
            low = high // 4
        if high is None and low is not None:
            high = low * 4
        if high is not None and low is not None:
            if low < 0:
                raise ValueError("low must be positive or equal to zero")
            if high < low:
                raise ValueError("high must be greater than or equal to low")
        self.high, self.low = high, low
        self.pause = pause
        self.resume = resume
        self.paused = False

        # This flag prevents concurrent calls to get() by user code.
        self.get_in_progress = False

        # This flag marks the end of the connection.
        self.closed = False

    @overload
    async def get(self, decode: Literal[True]) -> str: ...

    @overload
    async def get(self, decode: Literal[False]) -> bytes: ...

    @overload
    async def get(self, decode: bool | None = None) -> Data: ...

    async def get(self, decode: bool | None = None) -> Data:
        """
        Read the next message.

        :meth:`get` returns a single :class:`str` or :class:`bytes`.

        If the message is fragmented, :meth:`get` waits until the last frame is
        received, then it reassembles the message and returns it. To receive
        messages frame by frame, use :meth:`get_iter` instead.

        Args:
            decode: :obj:`False` disables UTF-8 decoding of text frames and
                returns :class:`bytes`. :obj:`True` forces UTF-8 decoding of
                binary frames and returns :class:`str`.

        Raises:
            EOFError: If the stream of frames has ended.
            UnicodeDecodeError: If a text frame contains invalid UTF-8.
            ConcurrencyError: If two coroutines run :meth:`get` or
                :meth:`get_iter` concurrently.

        """
        if self.get_in_progress:
            raise ConcurrencyError("get() or get_iter() is already running")
        self.get_in_progress = True

        # Locking with get_in_progress prevents concurrent execution
        # until get() fetches a complete message or is canceled.

        try:
            # Fetch the first frame.
            frame = await self.frames.get(not self.closed)
            self.maybe_resume()
            assert frame.opcode is TEXT or frame.opcode is BINARY
            if decode is None:
                decode = frame.opcode is TEXT
            frames = [frame]

            # Fetch subsequent frames for fragmented messages.
            while not frame.fin:
                try:
                    frame = await self.frames.get(not self.closed)
                except asyncio.CancelledError:
                    # Put frames already received back into the queue
                    # so that future calls to get() can return them.
                    self.frames.reset(frames)
                    raise
                self.maybe_resume()
                assert frame.opcode is CONT
                frames.append(frame)

        finally:
            self.get_in_progress = False

        # This converts frame.data to bytes when it's a bytearray.
        data = b"".join(frame.data for frame in frames)
        if decode:
            return data.decode()
        else:
            return data

    @overload
    def get_iter(self, decode: Literal[True]) -> AsyncIterator[str]: ...

    @overload
    def get_iter(self, decode: Literal[False]) -> AsyncIterator[bytes]: ...

    @overload
    def get_iter(self, decode: bool | None = None) -> AsyncIterator[Data]: ...

    async def get_iter(self, decode: bool | None = None) -> AsyncIterator[Data]:
        """
        Stream the next message.

        Iterating the return value of :meth:`get_iter` asynchronously yields a
        :class:`str` or :class:`bytes` for each frame in the message.

        The iterator must be fully consumed before calling :meth:`get_iter` or
        :meth:`get` again. Else, :exc:`ConcurrencyError` is raised.

        This method only makes sense for fragmented messages. If messages aren't
        fragmented, use :meth:`get` instead.

        Args:
            decode: :obj:`False` disables UTF-8 decoding of text frames and
                returns :class:`bytes`. :obj:`True` forces UTF-8 decoding of
                binary frames and returns :class:`str`.

        Raises:
            EOFError: If the stream of frames has ended.
            UnicodeDecodeError: If a text frame contains invalid UTF-8.
            ConcurrencyError: If two coroutines run :meth:`get` or
                :meth:`get_iter` concurrently.

        """
        if self.get_in_progress:
            raise ConcurrencyError("get() or get_iter() is already running")
        self.get_in_progress = True

        # Locking with get_in_progress prevents concurrent execution
        # until get_iter() fetches a complete message or is canceled.

        # If get_iter() raises an exception e.g. in decoder.decode(),
        # get_in_progress remains set and the connection becomes unusable.

        # Yield the first frame.
        try:
            frame = await self.frames.get(not self.closed)
        except asyncio.CancelledError:
            self.get_in_progress = False
            raise
        self.maybe_resume()
        assert frame.opcode is TEXT or frame.opcode is BINARY
        if decode is None:
            decode = frame.opcode is TEXT
        if decode:
            decoder = UTF8Decoder()
            yield decoder.decode(frame.data, frame.fin)
        else:
            # Convert to bytes when frame.data is a bytearray.
            yield bytes(frame.data)

        # Yield subsequent frames for fragmented messages.
        while not frame.fin:
            # We cannot handle asyncio.CancelledError because we don't buffer
            # previous fragments — we're streaming them. Canceling get_iter()
            # here will leave the assembler in a stuck state. Future calls to
            # get() or get_iter() will raise ConcurrencyError.
            frame = await self.frames.get(not self.closed)
            self.maybe_resume()
            assert frame.opcode is CONT
            if decode:
                yield decoder.decode(frame.data, frame.fin)
            else:
                # Convert to bytes when frame.data is a bytearray.
                yield bytes(frame.data)

        self.get_in_progress = False

    async def get_reader(self, decode: bool | None = None) -> MessageReader:
        """
        Read the next message incrementally.

        :meth:`get_reader` returns a :class:`MessageReader`. Its
        :meth:`MessageReader.read` method fetches the message chunk by chunk;
        its :meth:`MessageReader.discard` method skips the rest of the
        message.

        The message must be fully read or discarded before calling
        :meth:`get`, :meth:`get_iter`, or :meth:`get_reader` again. Else,
        :exc:`ConcurrencyError` is raised.

        Args:
            decode: :obj:`False` disables UTF-8 decoding of text frames and
                reads :class:`bytes`. :obj:`True` forces UTF-8 decoding of
                binary frames and reads :class:`str`.

        Raises:
            EOFError: If the stream of frames has ended.
            UnicodeDecodeError: If a text frame contains invalid UTF-8.
            ConcurrencyError: If two coroutines run :meth:`get`,
                :meth:`get_iter`, or :meth:`get_reader` concurrently.

        """
        if self.get_in_progress:
            raise ConcurrencyError(
                "get(), get_iter(), or get_reader() is already running"
            )
        self.get_in_progress = True

        # Locking with get_in_progress prevents concurrent execution
        # until the reader fetches the complete message or discards it.

        # Fetch the first frame.
        try:
            frame = await self.frames.get(not self.closed)
        except (asyncio.CancelledError, EOFError):
            self.get_in_progress = False
            raise
        self.maybe_resume()
        assert frame.opcode is TEXT or frame.opcode is BINARY
        return MessageReader(self, frame, decode)

    def put(self, frame: Frame) -> None:
        """
        Add ``frame`` to the next message.

        Raises:
            EOFError: If the stream of frames has ended.

        """
        if self.closed:
            raise EOFError("stream of frames ended")

        self.frames.put(frame)
        self.maybe_pause()

    def maybe_pause(self) -> None:
        """Pause the writer if queue is above the high water mark."""
        # Skip if flow control is disabled.
        if self.high is None:
            return

        # Check for "> high" to support high = 0.
        if len(self.frames) > self.high and not self.paused:
            self.paused = True
            self.pause()

    def maybe_resume(self) -> None:
        """Resume the writer if queue is below the low water mark."""
        # Skip if flow control is disabled.
        if self.low is None:
            return

        # Check for "<= low" to support low = 0.
        if len(self.frames) <= self.low and self.paused:
            self.paused = False
            self.resume()

    def close(self) -> None:
        """
        End the stream of frames.

        Calling :meth:`close` concurrently with :meth:`get`, :meth:`get_iter`,
        or :meth:`put` is safe. They will raise :exc:`EOFError`.

        """
        if self.closed:
            return

        self.closed = True

        # Unblock get() or get_iter().
        self.frames.abort()


class MessageReader:
    """
    Read a message chunk by chunk.

    :class:`MessageReader` is returned by :meth:`Assembler.get_reader`. It
    shouldn't be instantiated directly. See that method for the constraints
    on concurrent calls.

    """

    def __init__(
        self,
        assembler: Assembler,
        frame: Frame,
        decode: bool | None,
    ) -> None:
        self.assembler = assembler

        if decode is None:
            decode = frame.opcode is TEXT
        self.decode = decode
        self.decoder = UTF8Decoder() if decode else None

        # Whether the final frame of the message was fetched from the queue.
        self.fin = frame.fin

        # Buffer of data fetched from the queue but not returned by read()
        # yet. It's a str when decode is True and bytes otherwise.
        self.buffer: Data = self.decode_frame(frame)

        # Whether the reader released the assembler's lock. This happens when
        # the message is fully consumed by read() or dropped by discard().
        self.done = False

    def decode_frame(self, frame: Frame) -> Data:
        """Decode a frame, updating the state of the UTF-8 decoder."""
        # Convert to bytes when frame.data is a bytearray.
        data = bytes(frame.data)
        if self.decoder is not None:
            return self.decoder.decode(data, frame.fin)
        else:
            return data

    async def fetch_frame(self) -> Frame:
        """Fetch the next frame of the message from the queue."""
        frame = await self.assembler.frames.get(not self.assembler.closed)
        self.assembler.maybe_resume()
        assert frame.opcode is CONT
        self.fin = frame.fin
        return frame

    def extend_buffer(self, data: Data) -> None:
        """Append decoded data to the buffer."""
        if isinstance(self.buffer, str):
            assert isinstance(data, str)
            self.buffer += data
        else:
            assert isinstance(data, bytes)
            self.buffer += data

    async def read(self, n: int = -1) -> Data:
        """
        Read up to ``n`` characters or bytes of the message.

        Return a :class:`str` for text messages and a :class:`bytes` for
        binary messages, unless this behavior is overridden with the
        ``decode`` argument of :meth:`Assembler.get_reader`.

        When fewer than ``n`` characters or bytes remain in the message,
        return them. At the end of the message, return an empty string or
        bytestring.

        Set ``n`` to a negative value to read until the end of the message.

        Canceling :meth:`read` is safe. Data received before the cancellation
        remains available to the next call to :meth:`read` or
        :meth:`discard`.

        Raises:
            EOFError: If the stream of frames has ended.
            UnicodeDecodeError: If a text frame contains invalid UTF-8.

        """
        if self.done:
            return "" if self.decode else b""

        if n < 0:
            # Read until the end of the message.
            while not self.fin:
                self.extend_buffer(self.decode_frame(await self.fetch_frame()))
            result, self.buffer = self.buffer, self.buffer[:0]
        else:
            while not self.fin and len(self.buffer) < n:
                self.extend_buffer(self.decode_frame(await self.fetch_frame()))
            result, self.buffer = self.buffer[:n], self.buffer[n:]

        if self.fin and not self.buffer:
            # The message is fully consumed; release the lock.
            self.done = True
            self.assembler.get_in_progress = False

        return result

    async def discard(self) -> None:
        """
        Discard the rest of the message.

        Frames are still parsed and decoded in order to keep the connection
        synchronized: control frames are processed, protocol errors are
        reported, and text is checked for UTF-8 validity. Only the message
        content isn't returned to the caller.

        Canceling :meth:`discard` is safe. It may be called again to finish
        discarding the message.

        Raises:
            EOFError: If the stream of frames has ended.
            UnicodeDecodeError: If a text frame contains invalid UTF-8.

        """
        if self.done:
            return

        while not self.fin:
            # Decode data to validate UTF-8 and maintain the decoder state,
            # then drop it instead of buffering it for the caller.
            self.decode_frame(await self.fetch_frame())

        self.buffer = self.buffer[:0]
        self.done = True
        self.assembler.get_in_progress = False
