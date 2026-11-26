"""Process-local endpoint discovery with rebuildable client views."""
import json
import re
from collections import deque
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

_MAX_ROLLOUT_SERVICES = 8
_MAX_ROLLOUT_VERSIONS = 4
_MAX_ROLLOUT_ENDPOINTS = 4
_MAX_ROLLOUT_PROTOCOLS = 4
_MAX_ROLLOUT_DEPENDENCIES = 16
_MAX_ROLLOUT_BRIDGES = 4
_MAX_PLAN_TARGETS = 8


class SlotUnavailable(RuntimeError):
    """No slot can be granted without violating capacity or the reserve plan."""


class NoReserve(RuntimeError):
    """take_reserve() was called while no emergency plan was registered."""


class IncompatibleRollout(RuntimeError):
    """A known version would make the authoritative rollout cut incompatible."""


class NoRolloutPath(RuntimeError):
    """No compatible sequence of single-service steps reaches the targets."""


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


class _Rollout:
    """Immutable protocol catalog plus the authoritative per-service cut."""

    def __init__(self, catalog, cut, service_order):
        self.catalog = catalog
        self.cut = cut
        self.service_order = service_order

    def valid(self):
        for service in self.service_order:
            for dependency in self.catalog[service]["dependencies"]:
                consumer = self.catalog[service]["versions"][self.cut[service]][2]
                provider = self.catalog[dependency]["versions"][self.cut[dependency]][1]
                if not consumer.get(dependency, frozenset()) & provider:
                    return False
        return True


def _protocol_set(names, what):
    if not isinstance(names, tuple) or not 1 <= len(names) <= _MAX_ROLLOUT_PROTOCOLS:
        raise ValueError(f"{what} must be a tuple of 1-4 protocol names")
    for name in names:
        _service(name)
    if len(set(names)) != len(names):
        raise ValueError(f"{what} must contain distinct protocol names")
    return frozenset(names)


def _validated_rollout_catalog(catalog):
    """Validate a catalog and materialize it as plain, immutable-by-convention data."""
    if not isinstance(catalog, Mapping):
        raise ValueError("catalog must be a mapping of service name to versions")
    if not 1 <= len(catalog) <= _MAX_ROLLOUT_SERVICES:
        raise ValueError("catalog must manage 1-8 services")
    materialized = {}
    dependency_count = 0
    for service, versions in catalog.items():
        _service(service)
        if not isinstance(versions, Mapping) or not 1 <= len(versions) <= _MAX_ROLLOUT_VERSIONS:
            raise ValueError("each service must declare 1-4 versions")
        parsed = []
        seen_endpoints = set()
        dependency_keys = None
        for version, spec in versions.items():
            _service(version)
            if not isinstance(spec, tuple) or len(spec) != 3:
                raise ValueError("a version spec must be an (endpoints, protocols, dependencies) tuple")
            endpoints, protocols, dependencies = spec
            if (not isinstance(endpoints, tuple)
                    or not 1 <= len(endpoints) <= _MAX_ROLLOUT_ENDPOINTS
                    or any(not isinstance(endpoint, Endpoint) for endpoint in endpoints)
                    or len(set(endpoints)) != len(endpoints)):
                raise ValueError("a version needs 1-4 distinct Endpoint values")
            if endpoints in seen_endpoints:
                raise ValueError("versions of one service must have distinct endpoint tuples")
            seen_endpoints.add(endpoints)
            offered = _protocol_set(protocols, "offered protocols")
            if not isinstance(dependencies, Mapping):
                raise ValueError("dependencies must be a mapping")
            if len(dependencies) > _MAX_ROLLOUT_DEPENDENCIES:
                raise ValueError("too many dependencies")
            accepted = {}
            for dependency, names in dependencies.items():
                _service(dependency)
                if dependency == service:
                    raise ValueError("a service cannot depend on itself")
                if dependency not in catalog:
                    raise ValueError("dependencies must all be in the catalog")
                accepted[dependency] = _protocol_set(names, "accepted protocols")
            keys = frozenset(accepted)
            if dependency_keys is None:
                dependency_keys = keys
            elif keys != dependency_keys:
                raise ValueError("all versions of a service must declare the same dependencies")
            parsed.append((endpoints, offered, accepted))
        dependency_count += len(dependency_keys or ())
        materialized[service] = {
            "versions": tuple(parsed),
            "order": tuple(versions),
            "dependencies": dependency_keys,
        }
    if dependency_count > _MAX_ROLLOUT_DEPENDENCIES:
        raise ValueError("catalog has more than 16 directed dependencies")
    return materialized


def _replace_payload(service, endpoints):
    return json.dumps(
        [service, [(endpoint.host, endpoint.port) for endpoint in endpoints]],
        separators=(",", ":"),
    ).encode("ascii")


def _validated_targets(targets):
    if not isinstance(targets, Mapping) or len(targets) > _MAX_PLAN_TARGETS:
        raise ValueError("targets must be a mapping of at most 8 services")
    validated = {}
    for service, version in targets.items():
        _service(service)
        _service(version)
        validated[service] = version
    return validated


def _validated_bridges(bridges, targets):
    if bridges is None:
        return {}
    if not isinstance(bridges, Mapping) or len(bridges) > _MAX_PLAN_TARGETS:
        raise ValueError("bridges must be a mapping of at most 8 services")
    validated = {}
    for service, versions in bridges.items():
        _service(service)
        if service not in targets:
            raise ValueError("bridges may only name services in targets")
        if (not isinstance(versions, tuple)
                or len(versions) > _MAX_ROLLOUT_BRIDGES):
            raise ValueError("a bridge must list at most 4 version names")
        for version in versions:
            _service(version)
        if len(set(versions)) != len(versions):
            raise ValueError("bridge versions must be distinct")
        validated[service] = frozenset(versions)
    return validated


def _plan_rollout(rollout, targets, bridges):
    """Breadth-first search over reachable per-service versions.

    A state assigns every managed service a version index.  Unlisted services
    stay where the plan starts; listed services may use their start, target, or
    explicitly bridged versions.  Every transition changes exactly one service
    and is kept only while the whole cut remains compatible.  Breadth-first
    expansion over services in catalog order then version order returns a
    shortest plan deterministically; ties between equally short plans are
    resolved by that order.

    Compatibility is precomputed per dependency edge, and a transition only
    re-checks the edges touching the service that changed, so the search stays
    small at the documented limits (8 services, 4 versions each).
    """
    catalog = rollout.catalog
    services = rollout.service_order
    position = {service: offset for offset, service in enumerate(services)}
    start = tuple(rollout.cut[service] for service in services)
    order_of = {service: catalog[service]["order"] for service in services}

    desired = list(start)
    for service, version in targets.items():
        desired[position[service]] = order_of[service].index(version)

    allowed = {}
    for offset, service in enumerate(services):
        if service not in targets:
            allowed[offset] = (start[offset],)
            continue
        indices = {start[offset], desired[offset]}
        for version in bridges.get(service, ()):
            indices.add(order_of[service].index(version))
        allowed[offset] = tuple(sorted(indices))

    goal = tuple(desired)
    if start == goal:
        return ()

    # edge_ok[(consumer_offset, dependency_offset)][ci][di] is True when the
    # consuming version accepts at least one protocol the provider offers.
    edges = []
    edge_ok = {}
    touching = [[] for _ in services]
    for offset, service in enumerate(services):
        entry = catalog[service]
        versions = entry["versions"]
        for dependency in entry["dependencies"]:
            dep_offset = position[dependency]
            provided = [frozenset(v[1]) for v in catalog[dependency]["versions"]]
            table = [
                tuple(bool(v[2].get(dependency, frozenset()) & provided[di])
                      for di in range(len(provided)))
                for v in versions
            ]
            edge_ok[(offset, dep_offset)] = table
            edges.append((offset, dep_offset))
            touching[offset].append((offset, dep_offset))
            touching[dep_offset].append((offset, dep_offset))

    def state_ok(state):
        for consumer, dependency in edges:
            if not edge_ok[(consumer, dependency)][state[consumer]][state[dependency]]:
                return False
        return True

    if not state_ok(start):
        return None

    parents = {start: None}
    steps = {start: None}
    queue = deque([start])
    while queue:
        state = queue.popleft()
        for offset in range(len(services)):
            for index in allowed[offset]:
                if index == state[offset]:
                    continue
                successor = state[:offset] + (index,) + state[offset + 1:]
                if successor in parents:
                    continue
                if not all(
                        edge_ok[(consumer, dependency)][successor[consumer]][successor[dependency]]
                        for consumer, dependency in touching[offset]):
                    continue
                parents[successor] = state
                steps[successor] = (
                    services[offset], catalog[services[offset]]["versions"][index][0])
                if successor == goal:
                    plan = []
                    node = successor
                    while parents[node] is not None:
                        plan.append(steps[node])
                        node = parents[node]
                    plan.reverse()
                    return tuple(plan)
                queue.append(successor)
    return None


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
        self._rollout = None

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
            rollout = self._rollout
            next_cut = None
            if rollout is not None and service in rollout.catalog:
                next_cut = self._rollout_transition(rollout, service, endpoints)
            payload = _replace_payload(service, endpoints)
            sequence = self._bus.publish(payload)
            _replace(self._entries, service, endpoints)
            if next_cut is not None:
                rollout.cut = next_cut
            self._next = sequence + 1
            return sequence

    def configure_protocol_rollout(self, catalog):
        """Enable protocol-compatible rolling changes for 1-8 managed services.

        All validation happens before any state changes: the catalog must be
        consistent, every managed service must already publish a legal version,
        the starting cut must be compatible, and every version's replace event
        must fit the directory's payload bound.  A rejected call enables nothing.
        """
        with self._lock:
            if self._closed:
                raise Closed("directory closed")
            if self._rollout is not None:
                raise ValueError("protocol rollout already configured")
            materialized = _validated_rollout_catalog(catalog)
            cut = {}
            for service in materialized:
                current = self._entries.get(service)
                if not current:
                    raise ValueError("managed service has no published endpoints")
                cut[service] = self._locate_version_under_lock(materialized, service, current)
            projected = _Rollout(materialized, cut, tuple(materialized))
            if not projected.valid():
                raise ValueError("the starting cut is not protocol compatible")
            bound = self._bus.max_payload
            for service, entry in materialized.items():
                for endpoints, _offered, _accepted in entry["versions"]:
                    if len(_replace_payload(service, endpoints)) > bound:
                        raise ValueError("a version exceeds the event payload bound")
            self._rollout = projected
            return None

    @staticmethod
    def _locate_version_under_lock(catalog, service, endpoints):
        entry = catalog[service]
        for index, (version_endpoints, _offered, _accepted) in enumerate(entry["versions"]):
            if version_endpoints == endpoints:
                return index
        raise ValueError("endpoints do not match any declared version")

    def _rollout_transition(self, rollout, service, endpoints):
        """Return the next cut for a known version, or raise without side effects.

        The current cut, the catalog and the published endpoint tuple are all
        read under the caller's lock, so a compatibility decision and the
        publication it guards cannot be split by another replacement.
        """
        if not endpoints:
            raise ValueError("cannot withdraw a managed service")
        try:
            index = self._locate_version_under_lock(rollout.catalog, service, endpoints)
        except ValueError:
            raise ValueError("endpoints do not match any declared version") from None
        current = rollout.cut[service]
        if current == index:
            return None
        next_cut = dict(rollout.cut)
        next_cut[service] = index
        candidate = _Rollout(rollout.catalog, next_cut, rollout.service_order)
        if not candidate.valid():
            raise IncompatibleRollout("the new cut would be protocol incompatible")
        return next_cut

    def plan_protocol_rollout(self, targets, bridges=None):
        """Return a minimal sequence of single-service steps to reach ``targets``.

        The plan is computed from the authoritative cut under the lock but does
        not reserve anything; the caller must still publish each step through
        ``replace``, which re-checks compatibility at that moment.
        """
        with self._lock:
            rollout = self._rollout
            if rollout is None:
                if self._closed:
                    raise Closed("directory closed")
                raise ValueError("protocol rollout is not configured")
            if self._closed:
                raise Closed("directory closed")
            validated_targets = _validated_targets(targets)
            validated_bridges = _validated_bridges(bridges, validated_targets)
            for service, version in validated_targets.items():
                if service not in rollout.catalog:
                    raise ValueError("targets must name managed services")
                if version not in rollout.catalog[service]["order"]:
                    raise ValueError("target version is not declared")
            for service, versions in validated_bridges.items():
                order = rollout.catalog[service]["order"]
                for version in versions:
                    if version not in order:
                        raise ValueError("bridge version is not declared")
            # Snapshot the authoritative cut so the search itself holds no lock.
            snapshot = _Rollout(rollout.catalog, dict(rollout.cut), rollout.service_order)
        plan = _plan_rollout(snapshot, validated_targets, validated_bridges)
        if plan is None:
            raise NoRolloutPath("no compatible rollout path reaches the targets")
        return plan

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
