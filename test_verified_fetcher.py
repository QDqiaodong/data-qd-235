"""Acceptance tests for the verified replica reader.

All replica failures are simulated with small fixed blocks and scripted byte
streams -- no network timing and no third-party code.  The one real-network
case uses the repository's loopback HTTP pattern.
"""
import hashlib
import os
import tempfile
import unittest
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from directory import Directory, Endpoint
from verified_fetcher import FetchIncomplete, VerifiedFetcher


BLOCK = 4
BLOCKS = (b"abcd", b"efgh", b"ijkl")
DATA = b"".join(BLOCKS)
DIGESTS = tuple(hashlib.sha256(block).hexdigest() for block in BLOCKS)
SERVICE = "assets"

X = Endpoint("127.0.0.1", 9101)
Y = Endpoint("127.0.0.1", 9102)


def corrupt(data, block_index, block_size=BLOCK):
    start = block_index * block_size
    damaged = bytearray(data)
    for offset in range(start, start + block_size):
        damaged[offset] = (damaged[offset] + 1) % 256
    return bytes(damaged)


class _Stream:
    """A scripted byte stream over a fixed payload, starting at an offset."""

    def __init__(self, endpoint, data, start, events, *, error_at=None, eof_at=None):
        self.endpoint = endpoint
        self._data = data
        self._pos = start
        self._offset = start
        self._error_at = error_at
        self._eof_at = eof_at
        self._events = events
        self.closed = False

    def read(self, n):
        if self.closed:
            raise ValueError("read from closed stream")
        if n <= 0 or n > BLOCK:
            raise AssertionError("reader asked for %r bytes" % (n,))
        if self._error_at is not None and self._pos >= self._error_at:
            raise OSError("transport interrupted")
        if self._eof_at is not None and self._pos >= self._eof_at:
            return b""
        limit = min(self._pos + n, len(self._data))
        if self._error_at is not None:
            limit = min(limit, self._error_at)
        if self._eof_at is not None:
            limit = min(limit, self._eof_at)
        chunk = self._data[self._pos:limit]
        self._pos = limit
        return chunk

    def close(self):
        if not self.closed:
            self.closed = True
            self._events.append(("close", self.endpoint, self._offset))


class _Replica:
    def __init__(self, endpoint, data, *, error_at=None, eof_at=None,
                 open_error=False, dribble=False):
        self.endpoint = endpoint
        self.data = data
        self.error_at = error_at
        self.eof_at = eof_at
        self.open_error = open_error
        self.dribble = dribble


class _DribbleReplica(_Replica):
    def __init__(self, endpoint, data):
        super().__init__(endpoint, data, dribble=True)


class _DribbleStream(_Stream):
    """Returns at most one byte per ``read`` to exercise short reads."""

    def read(self, n):
        return super().read(min(n, 1))


class Harness:
    """Owns replicas, records opens/closes, and enforces one open stream."""

    def __init__(self):
        self.replicas = {}
        self.events = []
        self.streams = []
        self.open_offsets = []

    def add(self, replica):
        self.replicas[replica.endpoint] = replica
        return replica

    def open_stream(self, endpoint, offset):
        if any(not stream.closed for stream in self.streams):
            raise AssertionError("a stream was still open when switching replicas")
        replica = self.replicas[endpoint]
        self.events.append(("open", endpoint, offset))
        if replica.open_error:
            raise OSError("cannot open replica")
        self.open_offsets.append((endpoint, offset))
        stream_type = _DribbleStream if replica.dribble else _Stream
        stream = stream_type(endpoint, replica.data, offset, self.events,
                             error_at=replica.error_at, eof_at=replica.eof_at)
        self.streams.append(stream)
        return stream

    def all_closed(self):
        return all(stream.closed for stream in self.streams)


def make_manifest():
    scratch = tempfile.mkdtemp(prefix="verified-fetch-")
    fetcher = VerifiedFetcher(len(DATA), BLOCK, DIGESTS, scratch_dir=scratch)
    return fetcher, scratch


def make_view(directory, endpoints):
    directory.replace(SERVICE, tuple(endpoints))
    return directory.watch()


class AcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.scratch_dirs = []

    def tearDown(self):
        for scratch in self.scratch_dirs:
            if os.path.isdir(scratch):
                for name in os.listdir(scratch):
                    os.remove(os.path.join(scratch, name))
                os.rmdir(scratch)

    def fetcher(self):
        fetcher, scratch = make_manifest()
        self.scratch_dirs.append(scratch)
        return fetcher, scratch

    # -- 1. content fault then fallback, and later skip ---------------------
    def test_content_fault_falls_back_then_address_is_skipped(self):
        fetcher, scratch = self.fetcher()
        harness = Harness()
        harness.add(_Replica(X, corrupt(DATA, 1)))   # block 1 is bad
        harness.add(_Replica(Y, DATA))
        directory = Directory(capacity=8)
        view = make_view(directory, (X, Y))
        try:
            emitted = []
            written = fetcher.fetch(view, SERVICE, harness.open_stream, emitted.append)
            self.assertEqual(written, len(DATA))
            self.assertEqual(emitted, list(BLOCKS))
            # X really was tried first, then Y resumed at the second block.
            self.assertEqual(harness.open_offsets, [(X, 0), (Y, BLOCK)])
            self.assertEqual(
                harness.events,
                [("open", X, 0), ("close", X, 0),
                 ("open", Y, BLOCK), ("close", Y, BLOCK)],
            )
            self.assertTrue(harness.all_closed())

            # X is repaired, but this reader remembers its content fault and
            # the view still ranks it first: only Y may be opened.
            harness.replicas[X].data = DATA
            start = len(harness.open_offsets)
            emitted_again = []
            written_again = fetcher.fetch(
                view, SERVICE, harness.open_stream, emitted_again.append)
            self.assertEqual(written_again, len(DATA))
            self.assertEqual(emitted_again, list(BLOCKS))
            self.assertEqual(harness.open_offsets[start:], [(Y, 0)])

            # A new reader does not inherit the fault record: X is used first.
            fresh = self.fetcher()[0]
            emitted_fresh = []
            fresh.fetch(view, SERVICE, harness.open_stream, emitted_fresh.append)
            self.assertEqual(harness.open_offsets[start + 1:], [(X, 0)])
            self.assertEqual(emitted_fresh, list(BLOCKS))
        finally:
            view.close()
            directory.close()
        self.assertEqual(os.listdir(scratch), [])

    def test_transport_interrupt_is_not_a_content_fault(self):
        fetcher, scratch = self.fetcher()
        harness = Harness()
        # X serves block 0, then the transfer breaks at the block-1 boundary.
        harness.add(_Replica(X, DATA, error_at=BLOCK))
        harness.add(_Replica(Y, DATA))
        directory = Directory(capacity=8)
        view = make_view(directory, (X, Y))
        try:
            emitted = []
            written = fetcher.fetch(view, SERVICE, harness.open_stream, emitted.append)
            self.assertEqual(written, len(DATA))
            self.assertEqual(emitted, list(BLOCKS))
            self.assertEqual(harness.open_offsets, [(X, 0), (Y, BLOCK)])
            self.assertTrue(harness.all_closed())

            # X recovers; because the interrupt was not a content fault, the
            # same reader tries the still-first X and needs no fallback.
            harness.replicas[X].error_at = None
            start = len(harness.open_offsets)
            recovered = []
            fetcher.fetch(view, SERVICE, harness.open_stream, recovered.append)
            self.assertEqual(harness.open_offsets[start:], [(X, 0)])
            self.assertEqual(recovered, list(BLOCKS))
        finally:
            view.close()
            directory.close()
        self.assertEqual(os.listdir(scratch), [])

    def test_trailing_bytes_and_open_failure(self):
        fetcher, scratch = self.fetcher()
        harness = Harness()
        # X serves all three blocks but then keeps going past the manifest;
        # the trailing byte makes the whole delivery unsafe to complete.
        harness.add(_Replica(X, DATA + b"junk"))
        harness.add(_Replica(Y, DATA))
        directory = Directory(capacity=8)
        view = make_view(directory, (X, Y))
        try:
            emitted = []
            written = fetcher.fetch(view, SERVICE, harness.open_stream, emitted.append)
            self.assertEqual(written, len(DATA))
            self.assertEqual(emitted, list(BLOCKS))
            # X delivered blocks 0 and 1, then its extra trailing byte faulted
            # block 2; Y resumes exactly at the third block.
            self.assertEqual(harness.open_offsets, [(X, 0), (Y, 2 * BLOCK)])
            self.assertTrue(harness.all_closed())

            # The trailing-byte fault sticks: X is skipped on the next delivery.
            harness.replicas[X].data = DATA
            start = len(harness.open_offsets)
            fetcher.fetch(view, SERVICE, harness.open_stream, [].append)
            self.assertEqual(harness.open_offsets[start:], [(Y, 0)])

            # A failed open is a transfer failure, not a content fault, so the
            # same reader still tries that address first next time.
            fail = Endpoint("127.0.0.1", 9103)
            harness.add(_Replica(fail, DATA, open_error=True))
            directory.replace(SERVICE, (fail, Y))
            view.advance()
            harness.replicas[Y].data = DATA
            start = len(harness.open_offsets)
            fetcher.fetch(view, SERVICE, harness.open_stream, [].append)
            self.assertEqual(harness.open_offsets[start:], [(Y, 0)])
            harness.replicas[fail].open_error = False
            start = len(harness.open_offsets)
            fetcher.fetch(view, SERVICE, harness.open_stream, [].append)
            self.assertEqual(harness.open_offsets[start:], [(fail, 0)])
        finally:
            view.close()
            directory.close()
        self.assertEqual(os.listdir(scratch), [])

    def test_short_reads_are_reassembled_into_full_blocks(self):
        fetcher, scratch = self.fetcher()
        harness = Harness()
        harness.add(_DribbleReplica(X, DATA))
        directory = Directory(capacity=4)
        view = make_view(directory, (X,))
        try:
            emitted = []
            written = fetcher.fetch(view, SERVICE, harness.open_stream, emitted.append)
            self.assertEqual(written, len(DATA))
            self.assertEqual(emitted, list(BLOCKS))
            self.assertTrue(harness.all_closed())
        finally:
            view.close()
            directory.close()
        self.assertEqual(os.listdir(scratch), [])

    def test_fault_record_evicts_oldest_beyond_64(self):
        fetcher = self.fetcher()[0]
        for i in range(70):
            fetcher._record_fault(Endpoint("127.0.0.1", 10000 + i))
        self.assertEqual(len(fetcher._faults), 64)
        # The first six are gone; the newest are retained.
        oldest = Endpoint("127.0.0.1", 10000)
        self.assertNotIn(oldest, fetcher._faults)
        self.assertIn(Endpoint("127.0.0.1", 10006), fetcher._faults)
        self.assertIn(Endpoint("127.0.0.1", 10069), fetcher._faults)


    # -- 2. incomplete, recovery, downstream rejection ----------------------
    def test_interrupted_replicas_report_prefix_then_recover(self):
        fetcher, scratch = self.fetcher()
        harness = Harness()
        harness.add(_Replica(X, DATA, error_at=BLOCK))
        harness.add(_Replica(Y, DATA, error_at=BLOCK))
        directory = Directory(capacity=8)
        view = make_view(directory, (X, Y))
        try:
            emitted = []
            with self.assertRaises(FetchIncomplete) as caught:
                fetcher.fetch(view, SERVICE, harness.open_stream, emitted.append)
            self.assertEqual(caught.exception.bytes_written, BLOCK)
            self.assertEqual(emitted, [BLOCKS[0]])     # verified contiguous prefix
            self.assertEqual(sum(len(block) for block in emitted),
                             caught.exception.bytes_written)
            self.assertTrue(harness.all_closed())
            self.assertEqual(os.listdir(scratch), [])

            # Y recovers; X still breaks.  A new delivery must be complete and
            # must still try X first before falling back to Y.
            harness.replicas[Y].error_at = None
            start = len(harness.open_offsets)
            emitted_again = []
            written = fetcher.fetch(view, SERVICE, harness.open_stream, emitted_again.append)
            self.assertEqual(written, len(DATA))
            self.assertEqual(emitted_again, list(BLOCKS))
            self.assertEqual(harness.open_offsets[start:], [(X, 0), (Y, BLOCK)])

            # A downstream rejection propagates unchanged, is not misread as a
            # content fault, and never triggers another replica request.
            class Rejected(Exception):
                pass

            def reject(block):
                raise Rejected("downstream closed")

            harness.replicas[Y].error_at = None
            start = len(harness.open_offsets)
            streams_before = len(harness.streams)
            with self.assertRaises(Rejected):
                fetcher.fetch(view, SERVICE, harness.open_stream, reject)
            self.assertEqual(harness.open_offsets[start:], [(X, 0)])
            self.assertEqual(len(harness.streams), streams_before + 1)
            self.assertTrue(harness.all_closed())
            self.assertEqual(os.listdir(scratch), [])

            # X was not blamed for the downstream rejection: the next delivery
            # opens X again (still first) before falling back to Y.
            start = len(harness.open_offsets)
            emitted_final = []
            fetcher.fetch(view, SERVICE, harness.open_stream, emitted_final.append)
            self.assertEqual(
                harness.open_offsets[start:], [(X, 0), (Y, BLOCK)])
            self.assertEqual(emitted_final, list(BLOCKS))
        finally:
            view.close()
            directory.close()

    # -- 3. a real loopback HTTP backend ------------------------------------
    def test_real_loopback_http_backend(self):
        fetcher, scratch = self.fetcher()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(DATA)))
                self.end_headers()
                self.wfile.write(DATA)

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
        worker.start()
        endpoint = Endpoint(*server.server_address)
        directory = Directory(capacity=4)
        view = make_view(directory, (endpoint,))

        opened = []

        def open_http(target, offset):
            opened.append((target, offset))
            connection = HTTPConnection(target.host, target.port, timeout=2)
            connection.request("GET", "/asset")
            response = connection.getresponse()
            if offset:
                remaining = offset
                while remaining > 0:
                    chunk = response.read(min(BLOCK, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
            return _HTTPStream(connection, response)

        try:
            self.assertEqual(view.resolve(SERVICE), (endpoint,))
            emitted = []
            written = fetcher.fetch(view, SERVICE, open_http, emitted.append)
            self.assertEqual(written, len(DATA))
            self.assertEqual(emitted, list(BLOCKS))
            self.assertEqual(opened, [(endpoint, 0)])
        finally:
            view.close()
            directory.close()
            server.shutdown()
            worker.join()
            server.server_close()
        self.assertEqual(os.listdir(scratch), [])


class _HTTPStream:
    def __init__(self, connection, response):
        self._connection = connection
        self._response = response
        self._closed = False

    def read(self, n):
        return self._response.read(n)

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._response.close()
        finally:
            self._connection.close()


class ValidationTest(unittest.TestCase):
    def make(self, *args, **kwargs):
        kwargs.setdefault("scratch_dir", tempfile.gettempdir())
        return VerifiedFetcher(*args, **kwargs)

    def test_rejects_invalid_manifests(self):
        good = DIGESTS
        for size, block_size, digests in (
            (0, BLOCK, ()),                       # empty body
            (-1, BLOCK, good),                    # negative
            (True, BLOCK, good),                  # bool is not a size
            (len(DATA), 0, good),                 # block too small
            (len(DATA), 65537, good),             # block too large
            (len(DATA), BLOCK, good[:2]),         # digest count mismatch
            (len(DATA), BLOCK, "x" * 64),         # not a tuple
            (len(DATA), BLOCK, (good[0].upper(),) + good[1:]),  # not lowercase
            (len(DATA), BLOCK, (good[0][:-1] + "z",) + good[1:]),  # not hex
        ):
            with self.assertRaises(ValueError):
                self.make(size, block_size, digests)
        # More than 128 blocks derived from size.
        with self.assertRaises(ValueError):
            self.make(129, 1, tuple(hashlib.sha256(bytes([i])).hexdigest()
                                    for i in range(129)))

    def test_empty_and_oversized_views_are_handled_without_reads(self):
        fetcher = self.make(len(DATA), BLOCK, DIGESTS)
        opened = []

        class EmptyView:
            def resolve(self, service):
                return ()

        emitted = []
        with self.assertRaises(FetchIncomplete) as caught:
            fetcher.fetch(EmptyView(), SERVICE, lambda e, o: opened.append((e, o)), emitted.append)
        self.assertEqual(caught.exception.bytes_written, 0)
        self.assertEqual(opened, [])
        self.assertEqual(emitted, [])

        many = tuple(Endpoint("127.0.0.1", 9200 + i) for i in range(9))

        class ManyView:
            def resolve(self, service):
                return many

        with self.assertRaises(ValueError):
            fetcher.fetch(ManyView(), SERVICE, lambda e, o: opened.append((e, o)), emitted.append)
        self.assertEqual(opened, [])


if __name__ == "__main__":
    unittest.main()
