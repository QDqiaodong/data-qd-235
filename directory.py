"""Process-local endpoint discovery with rebuildable client views."""
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from threading import Lock

from broadcast import Broadcast, Closed, Lagged


_SERVICE = re.compile(r"[a-z][a-z0-9-]{0,62}")
_HOST = re.compile(r"[A-Za-z0-9.:-]{1,253}")

_MAX_SLOT_ADDRESSES = 64
_MAX_RESERVE_SERVICES = 32
_MAX_SLOTS_PER_ADDRESS = 8
_MAX_RESERVE_DEMAND = 64


class SlotUnavailable(RuntimeError):
    """No slot can be granted without violating capacity or the reserve plan."""


class NoReserve(RuntimeError):
    """take_reserve() was called while no emergency plan was registered."""


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


def _validated_slot_limits(slot_limits):
    if slot_limits is None:
        return {}
    if not isinstance(slot_limits, Mapping):
        raise ValueError("slot_limits must be a mapping of Endpoint to slot count")
    if len(slot_limits) > _MAX_SLOT_ADDRESSES:
        raise ValueError("too many slot addresses")
    limits = {}
    for endpoint, slots in slot_limits.items():
        if not isinstance(endpoint, Endpoint):
            raise ValueError("slot_limits keys must be Endpoint values")
        if type(slots) is not int or not 1 <= slots <= _MAX_SLOTS_PER_ADDRESS:
            raise ValueError("slot count must be an integer from 1 to 8")
        limits[endpoint] = slots
    return limits


def _validated_reserve(demand):
    """Turn a demand mapping into an ordered tuple of (service, count) pairs."""
    if not isinstance(demand, Mapping):
        raise ValueError("demand must be a mapping of service name to count")
    if len(demand) > _MAX_RESERVE_SERVICES:
        raise ValueError("too many services in reserve")
    plan = []
    total = 0
    for service, count in demand.items():
        _service(service)
        if type(count) is not int or not 1 <= count <= _MAX_SLOTS_PER_ADDRESS:
            raise ValueError("demand count must be an integer from 1 to 8")
        total += count
        plan.append((service, count))
    if total > _MAX_RESERVE_DEMAND:
        raise ValueError("reserve demand exceeds 64 slots")
    return tuple(plan)


class _Flow:
    """Small integer max-flow network used to test and materialize allocations."""

    def __init__(self, size):
        self._graph = [[] for _ in range(size)]

    def add_edge(self, source, target, capacity):
        forward = [target, capacity, len(self._graph[target])]
        reverse = [source, 0, len(self._graph[source])]
        self._graph[source].append(forward)
        self._graph[target].append(reverse)
        return forward

    def max_flow(self, source, sink):
        graph = self._graph
        size = len(graph)
        total = 0
        while True:
            level = [-1] * size
            level[source] = 0
            queue = [source]
            for node in queue:
                for edge in graph[node]:
                    if edge[1] > 0 and level[edge[0]] < 0:
                        level[edge[0]] = level[node] + 1
                        queue.append(edge[0])
            if level[sink] < 0:
                return total
            cursor = [0] * size

            def augment(node, available):
                if node == sink:
                    return available
                edges = graph[node]
                while cursor[node] < len(edges):
                    edge = edges[cursor[node]]
                    target = edge[0]
                    if edge[1] > 0 and level[target] == level[node] + 1:
                        pushed = augment(target, min(available, edge[1]))
                        if pushed:
                            edge[1] -= pushed
                            graph[target][edge[2]][1] += pushed
                            return pushed
                    cursor[node] += 1
                return 0

            while True:
                pushed = augment(source, _MAX_RESERVE_DEMAND)
                if not pushed:
                    break
                total += pushed


def _allocate(entries, limits, used, plan):
    """Allocate every plan entry at once, or return None if that is impossible.

    Endpoint capacity is keyed by address, so two services that publish the same
    host/port draw from one shared pool.  A max-flow assignment is used because a
    greedy per-service choice can strand capacity that only a different split
    could use; neither the directory nor leases are modified here.
    """
    if not plan:
        return []
    total = sum(count for _, count in plan)
    candidate_lists = []
    endpoint_order = []
    endpoint_index = {}
    for service, _count in plan:
        candidates = []
        for endpoint in entries.get(service, ()):
            limit = limits.get(endpoint, 0)
            if limit <= 0 or limit - used.get(endpoint, 0) <= 0:
                continue
            if endpoint not in endpoint_index:
                endpoint_index[endpoint] = len(endpoint_order)
                endpoint_order.append(endpoint)
            candidates.append(endpoint)
        candidate_lists.append(candidates)

    services = len(plan)
    addresses = len(endpoint_order)
    source = 0
    sink = services + addresses + 1
    flow = _Flow(sink + 1)
    service_nodes = [1 + i for i in range(services)]
    address_nodes = {
        endpoint: services + 1 + i for endpoint, i in endpoint_index.items()
    }
    for index, (_service_name, count) in enumerate(plan):
        flow.add_edge(source, service_nodes[index], count)

    edges = []
    for index, _plan_entry in enumerate(plan):
        service_edges = []
        for endpoint in candidate_lists[index]:
            capacity = limits[endpoint] - used.get(endpoint, 0)
            edge = flow.add_edge(service_nodes[index], address_nodes[endpoint], capacity)
            service_edges.append((endpoint, edge, capacity))
        edges.append(service_edges)
    for endpoint in endpoint_order:
        capacity = limits[endpoint] - used.get(endpoint, 0)
        flow.add_edge(address_nodes[endpoint], sink, capacity)

    if flow.max_flow(source, sink) != total:
        return None

    allocation = []
    for index, (service, _count) in enumerate(plan):
        for endpoint, edge, capacity in edges[index]:
            for _ in range(capacity - edge[1]):
                allocation.append((service, endpoint))
    return allocation


class Lease:
    """A group of granted slots; closing returns each of them exactly once."""

    def __init__(self, directory, entries):
        self._directory = directory
        self._entries = tuple(entries)
        self._closed = False

    @property
    def entries(self):
        return self._entries

    def close(self):
        directory = self._directory
        with directory._lock:
            if self._closed:
                return
            self._closed = True
            for _service_name, endpoint in self._entries:
                remaining = directory._used.get(endpoint, 0) - 1
                if remaining > 0:
                    directory._used[endpoint] = remaining
                else:
                    directory._used.pop(endpoint, None)


class Directory:
    """Authoritative endpoint sets and a bounded stream of replacements."""

    def __init__(self, *, capacity, max_payload=65536, slot_limits=None):
        self._bus = Broadcast(capacity=capacity, max_payload=max_payload)
        self._lock = Lock()
        self._entries = {}
        self._next = 0
        self._closed = False
        self._slot_limits = _validated_slot_limits(slot_limits)
        self._used = {}
        self._reserve = None

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

    def set_reserve(self, demand):
        """Register or replace the next emergency demand.

        The new plan is accepted only when the current authoritative endpoints
        and currently free slots can satisfy all of it at once.  A rejected plan
        leaves the previous one untouched; an empty mapping withdraws it.
        """
        with self._lock:
            if self._closed:
                raise Closed("directory closed")
            plan = _validated_reserve(demand)
            if not plan:
                self._reserve = None
                return None
            if _allocate(self._entries, self._slot_limits, self._used, plan) is None:
                raise SlotUnavailable("reserve cannot be satisfied")
            self._reserve = plan
            return None

    def take_reserve(self):
        """Grant every slot in the registered plan from the authoritative state."""
        with self._lock:
            if self._closed:
                raise Closed("directory closed")
            plan = self._reserve
            if plan is None:
                raise NoReserve("no emergency reserve registered")
            allocation = _allocate(self._entries, self._slot_limits, self._used, plan)
            if allocation is None:
                raise SlotUnavailable("reserve cannot be satisfied")
            for _service_name, endpoint in allocation:
                self._used[endpoint] = self._used.get(endpoint, 0) + 1
            self._reserve = None
            return Lease(self, allocation)

    def _acquire(self, view, service):
        with self._lock:
            if self._closed:
                raise Closed("directory closed")
            _service(service)
            authoritative = self._entries.get(service, ())
            for endpoint in view._entries.get(service, ()):
                # A lagging view may still resolve a withdrawn address; it must
                # not borrow from one.  Only addresses visible in this view are
                # candidates, so newer addresses are never used early.
                if endpoint not in authoritative:
                    continue
                limit = self._slot_limits.get(endpoint, 0)
                if limit <= 0:
                    continue
                if limit - self._used.get(endpoint, 0) <= 0:
                    continue
                if self._reserve is not None:
                    hypothetical = dict(self._used)
                    hypothetical[endpoint] = hypothetical.get(endpoint, 0) + 1
                    if _allocate(self._entries, self._slot_limits,
                                 hypothetical, self._reserve) is None:
                        continue
                self._used[endpoint] = self._used.get(endpoint, 0) + 1
                return Lease(self, ((service, endpoint),))
            raise SlotUnavailable("no slot available for service")

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

    def acquire(self, service):
        """Borrow one slot from the first usable address in this view's order."""
        if self._closed:
            raise Closed("view closed")
        return self._directory._acquire(self, service)

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
