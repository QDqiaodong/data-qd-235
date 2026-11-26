"""Process-local broadcast ring with explicit per-subscriber lag."""
from dataclasses import dataclass
from threading import Condition


class Closed(RuntimeError):
    pass


class Lagged(RuntimeError):
    def __init__(self, expected, oldest):
        self.expected, self.oldest = expected, oldest
        super().__init__(f"positions [{expected}, {oldest}) were overwritten")


@dataclass(frozen=True)
class Event:
    sequence: int
    data: bytes


class Broadcast:
    def __init__(self, *, capacity, max_payload=65536):
        if any(type(val) is not int or val < 1 for val in (capacity, max_payload)):
            raise ValueError("capacity and max_payload must be positive")
        self._ring = [None] * capacity
        self._capacity = capacity
        self._max_payload = max_payload
        self._next = 0
        self._closed = False
        self._cond = Condition()

    @property
    def max_payload(self):
        return self._max_payload

    def _oldest(self):
        return max(0, self._next - self._capacity)

    def publish(self, payload):
        if not isinstance(payload, bytes) or len(payload) > self._max_payload:
            raise ValueError("event must be bytes within the payload bound")
        with self._cond:
            if self._closed:
                raise Closed("bus closed")
            sequence = self._next
            self._ring[sequence % self._capacity] = Event(sequence, payload)
            self._next += 1
            self._cond.notify_all()
            return sequence

    def subscribe(self, *, start=None):
        """Start at next publication by default, or supply a sequence to replay."""
        with self._cond:
            if self._closed:
                raise Closed("bus closed")
            cursor = self._next if start is None else start
            if type(cursor) is not int or not 0 <= cursor <= self._next:
                raise ValueError("start must be between zero and the next sequence")
            return Subscription(self, cursor)

    def close(self):
        with self._cond:
            self._closed = True
            self._cond.notify_all()


class Subscription:
    def __init__(self, bus, cursor):
        self._bus, self._cursor = bus, cursor
        self._closed = False

    def recv(self, timeout=None):
        """Read exactly the next sequence. Lag or timeout never advances it."""
        bus = self._bus
        with bus._cond:
            ready = bus._cond.wait_for(
                lambda: self._closed or bus._closed or self._cursor < bus._next, timeout)
            if self._closed:
                raise Closed("subscription closed")
            if not ready:
                raise TimeoutError("no event available")
            oldest = bus._oldest()
            if self._cursor < oldest:
                raise Lagged(self._cursor, oldest)
            if self._cursor == bus._next:
                raise Closed("bus closed and retained events consumed")
            event = bus._ring[self._cursor % bus._capacity]
            self._cursor += 1
            return event

    def skip_lost(self):
        """Explicitly skip overwritten sequences. Return skipped [start, end)."""
        with self._bus._cond:
            if self._closed:
                raise Closed("subscription closed")
            start = self._cursor
            self._cursor = max(start, self._bus._oldest())
            return start, self._cursor

    def close(self):
        with self._bus._cond:
            self._closed = True
            self._bus._cond.notify_all()
