"""Process-local endpoint discovery with rebuildable client views."""
import json
import re
from dataclasses import dataclass
from threading import Lock

from broadcast import Broadcast, Closed, Lagged


_SERVICE = re.compile(r"[a-z][a-z0-9-]{0,62}")
_HOST = re.compile(r"[A-Za-z0-9.:-]{1,253}")


def _service(name):
    if not isinstance(name, str) or not _SERVICE.fullmatch(name):
        raise ValueError("invalid service name")


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int

    def __post_init__(self):
        if (not isinstance(self.host, str) or not _HOST.fullmatch(self.host)
                or type(self.port) is not int or not 1 <= self.port <= 65535):
            raise ValueError("invalid endpoint")


@dataclass(frozen=True)
class Progress:
    next_sequence: int
    resynced: bool


class Directory:
    """Authoritative endpoint sets and a bounded stream of replacements."""

    def __init__(self, *, capacity, max_payload=65536):
        self._bus = Broadcast(capacity=capacity, max_payload=max_payload)
        self._lock = Lock()
        self._entries = {}
        self._next = 0
        self._closed = False

    def replace(self, service, endpoints):
        """Replace one service atomically; empty removes it, unchanged is a no-op."""
        _service(service)
        if (not isinstance(endpoints, tuple)
                or any(not isinstance(endpoint, Endpoint) for endpoint in endpoints)
                or len(set(endpoints)) != len(endpoints)):
            raise ValueError("endpoints must be a tuple of distinct Endpoint values")
        with self._lock:
            if self._closed:
                raise Closed("directory closed")
            if self._entries.get(service, ()) == endpoints:
                return None
            payload = json.dumps(
                [service, [(ep.host, ep.port) for ep in endpoints]],
                separators=(",", ":"),
            ).encode("ascii")
            sequence = self._bus.publish(payload)
            _replace(self._entries, service, endpoints)
            self._next = sequence + 1
            return sequence

    def _attach(self):
        with self._lock:
            # No replacement can split the copied state from its subscription cut.
            return self._entries.copy(), self._next, self._bus.subscribe(start=self._next)

    def watch(self):
        """Open a client view at the current state, not at the oldest event."""
        return View(self)

    def close(self):
        with self._lock:
            self._closed = True
            self._bus.close()


def _replace(entries, service, endpoints):
    if endpoints:
        entries[service] = endpoints
    else:
        entries.pop(service, None)


class View:
    """Single-owner lookup view; apply changes explicitly before resolving."""

    def __init__(self, directory):
        self._directory = directory
        self._entries, self._next, self._sub = directory._attach()
        self._closed = False

    @property
    def next_sequence(self):
        return self._next

    def resolve(self, service):
        """Return the ordered endpoints at this client's last applied cut."""
        if self._closed:
            raise Closed("view closed")
        _service(service)
        return self._entries.get(service, ())

    def advance(self, timeout=0):
        """Apply one retained replacement, or rebuild the view after a gap."""
        if self._closed:
            raise Closed("view closed")
        try:
            try:
                event = self._sub.recv(timeout)
            except Lagged:
                entries, next_sequence, sub = self._directory._attach()
                self._sub.close()
                self._entries, self._next, self._sub = entries, next_sequence, sub
                return Progress(self._next, True)
        except Closed:
            self.close()
            raise
        service, addresses = json.loads(event.data)
        _replace(self._entries, service, tuple(Endpoint(*address) for address in addresses))
        self._next = event.sequence + 1
        return Progress(self._next, False)

    def close(self):
        self._closed = True
        self._sub.close()
