"""Read one immutable asset from the first replica that can prove every block.

A :class:`VerifiedFetcher` binds one manifest -- a total size, a block size and
one SHA-256 digest per block.  ``fetch`` walks the addresses a client view
currently resolves for a service and streams blocks from them, verifying each
whole block before it is handed to an append-only downstream.  Because the
downstream can never take bytes back, only a complete, digest-matching block is
ever emitted; a replica that fails part-way simply resumes at the next undelivered
block boundary on the next candidate.

The reader keeps at most one block in memory.  ``scratch_dir`` is accepted for a
caller that provides local scratch space; this implementation verifies one block
at a time and needs no staging, so it never writes there.

Content faults (a whole block whose digest does not match, or bytes past the end
of the manifest) are remembered per address for later independent fetches.  A
short read, an early end of stream, an ``OSError`` while opening or reading, or a
downstream rejection is *not* a content fault: the replica may be tried again.
"""

import hashlib
import os

_MAX_BLOCK_SIZE = 65536
_MAX_BLOCKS = 128
_MAX_REPLICAS = 8
_MAX_FAULTS = 64

_HEX_DIGITS = frozenset("0123456789abcdef")


class FetchIncomplete(RuntimeError):
    """No replica could deliver the whole manifest.

    ``bytes_written`` is exactly the number of verified bytes that were handed
    to ``emit``; the downstream therefore holds the empty string or a complete,
    verified, contiguous prefix of the manifest.
    """

    def __init__(self, bytes_written):
        super().__init__(
            "fetch incomplete: %d verified bytes delivered" % (bytes_written,))
        self.bytes_written = bytes_written


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _validated_digests(digests, block_count):
    if not isinstance(digests, tuple) or len(digests) != block_count:
        raise ValueError("digests must be a tuple with one entry per block")
    for digest in digests:
        if (not isinstance(digest, str) or len(digest) != 64
                or any(character not in _HEX_DIGITS for character in digest)):
            raise ValueError(
                "digests must be lowercase hexadecimal SHA-256 strings")
    return digests


class VerifiedFetcher:
    """A serial, single-manifest reader over a caller-managed view."""

    def __init__(self, size, block_size, digests, *, scratch_dir):
        if not _is_int(size) or size < 1:
            raise ValueError("size must be a positive integer")
        if not _is_int(block_size) or not 1 <= block_size <= _MAX_BLOCK_SIZE:
            raise ValueError("block_size must be an integer from 1 to 65536")
        block_count = (size + block_size - 1) // block_size
        if block_count > _MAX_BLOCKS:
            raise ValueError("manifest has more than 128 blocks")
        validated = _validated_digests(digests, block_count)
        try:
            scratch = os.fspath(scratch_dir)
        except TypeError:
            raise ValueError("scratch_dir must be a filesystem path") from None
        if not isinstance(scratch, str):
            scratch = os.fsdecode(scratch)

        self._size = size
        self._block_size = block_size
        self._block_count = block_count
        self._digests = validated
        self._scratch_dir = scratch
        # Insertion-ordered so the oldest content fault is the first to go.
        self._faults = {}

    @property
    def size(self):
        return self._size

    @property
    def block_size(self):
        return self._block_size

    @property
    def block_count(self):
        return self._block_count

    def _block_length(self, index):
        if index == self._block_count - 1:
            return self._size - index * self._block_size
        return self._block_size

    def _record_fault(self, endpoint):
        if endpoint in self._faults:
            return
        if len(self._faults) >= _MAX_FAULTS:
            oldest = next(iter(self._faults))
            del self._faults[oldest]
        self._faults[endpoint] = None

    def fetch(self, view, service, open_stream, emit):
        """Deliver every block of the manifest from a usable replica.

        ``open_stream(endpoint, offset)`` must return a readable byte stream that
        starts at the given block boundary; each ``read(n)`` asks for at most one
        block.  ``emit(block)`` is called once per verified block, in order.  A
        downstream exception propagates unchanged and never causes a replica
        switch.

        Returns the number of bytes delivered on success, or raises
        :class:`FetchIncomplete` carrying what was actually written.
        """
        endpoints = view.resolve(service)
        if len(endpoints) > _MAX_REPLICAS:
            raise ValueError("too many replicas to read")

        candidates = []
        seen = set()
        for endpoint in endpoints:
            if endpoint not in seen:
                seen.add(endpoint)
                candidates.append(endpoint)

        block_size = self._block_size
        next_index = 0
        delivered = 0

        for endpoint in candidates:
            if next_index >= self._block_count:
                break
            # An address with a recorded content fault is never even opened,
            # even if the view still ranks it first.
            if endpoint in self._faults:
                continue
            offset = next_index * block_size
            try:
                stream = open_stream(endpoint, offset)
            except OSError:
                continue
            try:
                status, next_index, delivered = self._consume(
                    endpoint, stream, next_index, emit)
            finally:
                # Always close before moving on, and before letting a downstream
                # exception escape.
                try:
                    stream.close()
                except Exception:
                    pass
            if status == "complete":
                return delivered
        raise FetchIncomplete(delivered)

    def _consume(self, endpoint, stream, start_index, emit):
        """Stream and verify blocks from ``start_index``.

        Returns ``(status, next_index, delivered)`` where ``status`` is
        ``complete``, ``fault`` or ``failed``.  Content faults are recorded here;
        transfer failures are not.  A downstream rejection propagates unchanged.
        """
        index = start_index
        block_size = self._block_size
        last_index = self._block_count - 1

        while index < self._block_count:
            length = self._block_length(index)
            buffer = bytearray()
            while len(buffer) < length:
                try:
                    chunk = stream.read(length - len(buffer))
                except OSError:
                    return "failed", index, index * block_size
                if not chunk:
                    # Early end of stream: the transfer is incomplete, but this
                    # says nothing about the content the replica already gave.
                    return "failed", index, index * block_size
                buffer.extend(chunk)
            block = bytes(buffer)

            if index == last_index:
                # The manifest must end exactly here.  Anything after it means
                # the replica served bytes outside the manifest.
                try:
                    extra = stream.read(block_size)
                except OSError:
                    return "failed", index, index * block_size
                if extra:
                    self._record_fault(endpoint)
                    return "fault", index, index * block_size

            if hashlib.sha256(block).hexdigest() != self._digests[index]:
                self._record_fault(endpoint)
                return "fault", index, index * block_size

            emit(block)
            index += 1

        return "complete", index, self._size
