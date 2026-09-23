from __future__ import annotations

import asyncio
import codecs
import collections
from collections.abc import AsyncIterator, Awaitable, Iterable
from typing import Any, Callable, Generic, Literal, TypeVar, cast, overload

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
            raise ConcurrencyError(
                "get(), get_iter(), or get_reader() is already running"
            )
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
            raise ConcurrencyError(
                "get(), get_iter(), or get_reader() is already running"
            )
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

    async def get_reader(
        self,
        decode: bool | None = None,
        fail: Callable[[UnicodeDecodeError], Awaitable[None]] | None = None,
        eof: Callable[[], Awaitable[None]] | None = None,
    ) -> MessageReader:
        """
        Read the next message with a :class:`MessageReader`.

        The reader provides :meth:`MessageReader.read` for consuming the
        message chunk by chunk and :meth:`MessageReader.discard` for
        skipping the rest of the message.

        Until the message is entirely read or discarded, calling
        :meth:`get`, :meth:`get_iter`, or :meth:`get_reader` raises
        :exc:`ConcurrencyError`.

        Canceling :meth:`get_reader` is safe. There's no risk of losing
        data. The next invocation will return a reader for the same message.

        Args:
            decode: :obj:`False` disables UTF-8 decoding of text frames and
                provides :class:`bytes`. :obj:`True` forces UTF-8 decoding
                of binary frames and provides :class:`str`.
            fail: Called when a text message contains invalid UTF-8, with the
                :exc:`UnicodeDecodeError` as argument; it may raise an
                exception to report it to the caller of
                :meth:`MessageReader.read` or :meth:`MessageReader.discard`.
            eof: Called when the stream of frames ends in the middle of the
                message; it may raise an exception to report it to the caller
                of :meth:`MessageReader.read` or :meth:`MessageReader.discard`.

        Raises:
            EOFError: If the stream of frames has ended.
            ConcurrencyError: If two coroutines run :meth:`get`,
                :meth:`get_iter`, or :meth:`get_reader` concurrently.

        """
        if self.get_in_progress:
            raise ConcurrencyError(
                "get(), get_iter(), or get_reader() is already running"
            )
        self.get_in_progress = True

        # Locking with get_in_progress prevents concurrent execution until
        # the reader consumes the message entirely or discard() is called.

        try:
            # Fetch the first frame.
            frame = await self.frames.get(not self.closed)
        except (asyncio.CancelledError, EOFError):
            self.get_in_progress = False
            raise
        self.maybe_resume()
        assert frame.opcode is TEXT or frame.opcode is BINARY
        if decode is None:
            decode = frame.opcode is TEXT
        return MessageReader(self, frame, decode, fail, eof)

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

    :class:`MessageReader` is returned by :meth:`Assembler.get_reader`.
    It shouldn't be created directly.

    """

    def __init__(
        self,
        assembler: Assembler,
        frame: Frame,
        decode: bool,
        fail: Callable[[UnicodeDecodeError], Awaitable[None]] | None,
        eof: Callable[[], Awaitable[None]] | None,
    ) -> None:
        self.assembler = assembler
        # Incremental decoder for decoding and validating text messages.
        self.decoder = UTF8Decoder() if decode else None
        # Called when a text message contains invalid UTF-8.
        self.fail = fail
        # Called when the stream of frames ends in the middle of the message.
        self.eof = eof
        # Data of the current frame that wasn't decoded yet.
        self.frame_data = frame.data
        # Whether the current frame is the last frame of the message.
        self.fin = frame.fin
        # Decoded data that wasn't consumed by read() yet.
        self.buffer: Data = "" if decode else b""
        # Whether the data of the final frame went through the decoder.
        self.final_decoded = False
        # Whether the message was consumed entirely.
        self.exhausted = False

    async def read(self, n: int) -> Data:
        """
        Read up to ``n`` bytes or characters of the message.

        Return a bytestring (:class:`bytes`) or a string (:class:`str`),
        depending on the message type and the ``decode`` argument of
        :meth:`Assembler.get_reader`.

        Return an empty bytestring or string when the end of the message is
        reached.

        Canceling :meth:`read` is safe until it starts consuming data from
        the next frame. In that case, no data is lost; calling :meth:`read`
        again returns the same data. Once :meth:`read` returns, the position
        in the message advances by the length of the returned data.

        Raises:
            EOFError: If the stream of frames has ended.
            UnicodeDecodeError: If a text frame contains invalid UTF-8.

        """
        parts: list[Data] = []
        remaining = n
        while remaining > 0 and not self.exhausted:
            if self.buffer:
                chunk = self.buffer[:remaining]
                self.buffer = self.buffer[len(chunk) :]
                parts.append(chunk)
                remaining -= len(chunk)
            elif self.frame_data or (self.fin and not self.final_decoded):
                try:
                    self.buffer = self.decode_frame_data()
                except UnicodeDecodeError as exc:
                    await self.invalid_data(exc)
            elif self.fin:
                self.exhausted = True
                self.release()
            else:
                # This is the only checkpoint where read() can be canceled.
                # Since the next frame wasn't consumed yet, canceling is
                # safe; calling read() again returns the same data.
                await self.fetch_frame()
        if (
            not self.exhausted
            and self.fin
            and self.final_decoded
            and not self.frame_data
            and not self.buffer
        ):
            # read() consumed the end of the final frame exactly.
            self.exhausted = True
            self.release()
        if self.decoder is None:
            return b"".join(cast("list[bytes]", parts))
        else:
            return "".join(cast("list[str]", parts))

    async def discard(self) -> None:
        """
        Discard the rest of the message.

        This method consumes frames until the end of the message, decoding
        them to validate UTF-8 in text messages and to keep decompression
        state consistent, but without returning their content.

        Raises:
            EOFError: If the stream of frames has ended.
            UnicodeDecodeError: If a text frame contains invalid UTF-8.

        """
        while not self.exhausted:
            if self.frame_data or (self.fin and not self.final_decoded):
                try:
                    self.decode_frame_data()
                except UnicodeDecodeError as exc:
                    await self.invalid_data(exc)
            elif self.fin:
                self.exhausted = True
                self.release()
            else:
                await self.fetch_frame()

    def decode_frame_data(self) -> Data:
        """Decode the data of the current frame."""
        frame_data, self.frame_data = self.frame_data, b""
        if self.fin:
            # Decode the final frame exactly once, even when it's empty,
            # in order to flush the decoder of a text message.
            self.final_decoded = True
        if self.decoder is None:
            # Convert to bytes when frame_data is a bytearray.
            return bytes(frame_data)
        # Decode and validate UTF-8, raising UnicodeDecodeError on invalid
        # data. Decoding the final frame flushes the decoder, which raises
        # UnicodeDecodeError on a truncated multi-byte character.
        return self.decoder.decode(frame_data, self.fin)

    async def fetch_frame(self) -> None:
        """Fetch the next frame of the message."""
        try:
            frame = await self.assembler.frames.get(not self.assembler.closed)
        except EOFError:
            # The stream of frames ended in the middle of the message.
            self.exhausted = True
            self.release()
            if self.eof is not None:
                await self.eof()
            raise
        self.assembler.maybe_resume()
        assert frame.opcode is CONT
        self.frame_data = frame.data
        self.fin = frame.fin

    async def invalid_data(self, exc: UnicodeDecodeError) -> None:
        """Handle invalid UTF-8 in a text message."""
        # The connection will be closed; the message cannot be consumed.
        self.exhausted = True
        self.release()
        if self.fail is not None:
            await self.fail(exc)
        raise exc

    def release(self) -> None:
        """Release the message, allowing the assembler to read the next one."""
        self.assembler.get_in_progress = False
