"""Caller-side query batching over caller-owned directory :class:`View` objects.

A :class:`BatchClient` serves one service.  Several callers keep their own
``View`` and enqueue small string queries; the client coalesces the head query
of every caller that can currently be sent into one backend batch per
endpoint, bounded by the per-endpoint cap on unsettled queries.

Callers own their views: the client only calls ``View.resolve(service)`` at
the start of each :meth:`BatchClient.dispatch`, never advances a view and
never reroutes a request after its batch has been accepted.  A batch whose
acceptance is unknown keeps its full endpoint capacity and its callers'
ordering until the transport proves it was not accepted, or until it settles.
"""

from collections import deque
from collections.abc import Mapping

from directory import Endpoint, View

_MAX_CALLERS = 8
_MAX_ADDRESSES = 8
_MAX_REQUESTS = 64
_MIN_LIMIT = 1
_MAX_LIMIT = 8

# Request lifecycle.
_QUEUED = "queued"                       # never submitted, waits in caller order
_FLYING = "flying"                       # member of a live batch
_CANCELLED_QUEUED = "cancelled-queued"   # terminal available, order already released
_CANCELLED_FLYING = "cancelled-flying"   # terminal available, still blocks and bills capacity
_DONE = "done"                           # settled, terminal available

# Batch lifecycle.  Rejected batches are removed immediately; settled batch
# ids are remembered so duplicate/late settlements stay rejected.
_UNKNOWN = "unknown"
_ACCEPTED = "accepted"


class OutcomeUnknown(RuntimeError):
    """The transport cannot tell whether a batch was accepted."""


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


class _Request:
    __slots__ = ("id", "caller", "payload", "state", "batch", "terminal")

    def __init__(self, request_id, caller, payload):
        self.id = request_id
        self.caller = caller
        self.payload = payload
        self.state = _QUEUED
        self.batch = None
        self.terminal = None


class _Batch:
    __slots__ = ("id", "endpoint", "items", "members", "state")

    def __init__(self, batch_id, endpoint, items):
        self.id = batch_id
        self.endpoint = endpoint
        # The exact tuple handed to transport.submit; reuse verbatim on every
        # idempotent re-drive so the attempt identity never changes.
        self.items = items
        self.members = frozenset(request_id for request_id, _payload in items)
        self.state = _UNKNOWN


class BatchClient:
    """Merge per-caller query streams into bounded backend batches."""

    def __init__(self, service, views, limits, transport):
        if not isinstance(service, str) or not service:
            raise ValueError("service must be a non-empty string")
        if not isinstance(views, Mapping) or not 1 <= len(views) <= _MAX_CALLERS:
            raise ValueError("views must map 1 to 8 callers to View objects")
        if not isinstance(limits, Mapping) or not 1 <= len(limits) <= _MAX_ADDRESSES:
            raise ValueError("limits must map 1 to 8 Endpoints to a query cap")
        if any(not isinstance(view, View) for view in views.values()):
            raise ValueError("views values must be View instances")
        validated_limits = {}
        for endpoint, cap in limits.items():
            if not isinstance(endpoint, Endpoint):
                raise ValueError("limits keys must be Endpoint values")
            if not _is_int(cap) or not _MIN_LIMIT <= cap <= _MAX_LIMIT:
                raise ValueError("per-address limit must be an integer from 1 to 8")
            validated_limits[endpoint] = cap
        if not all(callable(getattr(transport, name, None))
                   for name in ("submit", "lookup")):
            raise ValueError("transport must provide submit() and lookup()")

        self._service = service
        self._views = dict(views)
        self._limits = validated_limits
        self._transport = transport
        self._callers = tuple(views)
        self._queues = {caller: deque() for caller in self._callers}
        self._requests = {}                 # request id -> _Request, live records only
        self._batches = {}                  # batch id -> _Batch, unknown/accepted only
        self._settled_ids = set()           # ids whose settlement was accepted
        self._outstanding = {endpoint: 0 for endpoint in validated_limits}
        self._next_request_id = 1
        self._next_batch_id = 1

    # ------------------------------------------------------------------ enqueue

    def enqueue(self, caller, payload):
        """Queue one string query; return a request id never reused here."""
        if caller not in self._views:
            raise ValueError("unknown caller")
        if not isinstance(payload, str):
            raise ValueError("payload must be a string")
        if len(self._requests) >= _MAX_REQUESTS:
            raise OverflowError("at most 64 requests may be outstanding")
        request_id = self._next_request_id
        self._next_request_id += 1
        request = _Request(request_id, caller, payload)
        self._requests[request_id] = request
        self._queues[caller].append(request_id)
        return request_id

    def cancel(self, request_id):
        """Cancel a request that has not delivered a terminal yet.

        Cancelling a queued request releases its caller's ordering immediately.
        Cancelling a request already submitted while acceptance is not proven
        makes the cancelled terminal available to :meth:`take` at once, but the
        request still bills the original endpoint capacity and blocks the
        caller's next request until the batch is rejected or settles.
        """
        request = self._requests.get(request_id)
        if request is None:
            return False
        if request.state == _QUEUED:
            request.state = _CANCELLED_QUEUED
            request.terminal = ("cancelled", None)
            self._release_order(request_id)
            return True
        if request.state == _FLYING:
            request.state = _CANCELLED_FLYING
            request.terminal = ("cancelled", None)
            return True
        return False

    def take(self, request_id):
        """Remove and return the terminal, or ``None`` if none is ready."""
        request = self._requests.get(request_id)
        if request is None or request.terminal is None:
            return None
        terminal = request.terminal
        del self._requests[request_id]
        return terminal

    # ----------------------------------------------------------------- dispatch

    def dispatch(self):
        """Run one planning round plus one recovery pass; returns nothing.

        New batches are planned first: their view of remaining capacity
        already excludes every unknown/accepted batch, so they cannot conflict
        with a pending one.  Recovery runs afterwards and, when it proves a
        batch was not accepted, its members keep their queue positions for the
        next round rather than being rushed out mid-round.
        """
        self._send_new_batches()
        self._recover_pending()

    def _recover_pending(self):
        """Drive every unknown batch to a conclusion using lookup first.

        A still-unknown batch is then re-driven through ``submit`` with the very
        same batch id, endpoint and items; the transport answers idempotently.
        """
        for batch in list(self._batches.values()):
            if batch.state != _UNKNOWN:
                continue
            try:
                conclusion = self._transport.lookup(batch.id)
            except OutcomeUnknown:
                conclusion = None
            if conclusion is None:
                try:
                    conclusion = self._transport.submit(
                        batch.id, batch.endpoint, batch.items)
                except OutcomeUnknown:
                    continue
            if conclusion is True:
                batch.state = _ACCEPTED
            elif conclusion is False:
                self._reject_batch(batch)
            # Any other value violates the transport contract; leave the batch
            # unknown rather than guessing.

    def _reject_batch(self, batch):
        """Release a proven-not-accepted batch and requeue its live members."""
        self._outstanding[batch.endpoint] -= len(batch.members)
        del self._batches[batch.id]
        for request_id in batch.members:
            request = self._requests.get(request_id)
            if request is None or request.state == _CANCELLED_FLYING:
                # The caller may already have taken the cancelled terminal;
                # either way the request must not be re-scheduled.
                self._release_order(request_id)
                continue
            request.state = _QUEUED
            request.batch = None
            # It keeps its place in the caller deque, so submission order is
            # preserved without re-enqueuing anything.

    def _send_new_batches(self):
        remaining = {
            endpoint: self._limits[endpoint] - self._outstanding[endpoint]
            for endpoint in self._limits
        }
        heads = []
        candidates = {}
        for caller in self._callers:
            queue = self._queues[caller]
            if not queue:
                continue
            request = self._requests.get(queue[0])
            # A missing record (cancelled terminal already taken) or any
            # non-queued head blocks the caller until its batch resolves.
            if request is None or request.state != _QUEUED:
                continue
            visible = self._views[caller].resolve(self._service)
            options = [
                endpoint for endpoint in visible
                if endpoint in self._limits and remaining.get(endpoint, 0) > 0
            ]
            if not options:
                continue
            heads.append((caller, request))
            candidates[request.id] = options

        if not heads:
            return
        assignment = self._assign(heads, candidates, remaining)

        planned = {}
        endpoint_order = []
        for caller, request in heads:
            endpoint = assignment.get(request.id)
            if endpoint is None:
                continue
            if endpoint not in planned:
                planned[endpoint] = []
                endpoint_order.append(endpoint)
            planned[endpoint].append((request.id, request.payload))

        for endpoint in endpoint_order:
            items = tuple(planned[endpoint])
            batch_id = self._next_batch_id
            self._next_batch_id += 1
            batch = _Batch(batch_id, endpoint, items)
            self._batches[batch_id] = batch
            for request_id, _payload in items:
                request = self._requests[request_id]
                request.state = _FLYING
                request.batch = batch_id
            # Reserve the whole batch before the call so an unknown receipt
            # still bills every member against the address.
            self._outstanding[endpoint] += len(items)
            try:
                accepted = self._transport.submit(batch_id, endpoint, items)
            except OutcomeUnknown:
                # The receipt is already lost: stay unknown, keep the full
                # reservation and let later dispatches recover the verdict.
                batch.state = _UNKNOWN
                continue
            if accepted:
                batch.state = _ACCEPTED
            else:
                self._reject_batch(batch)

    def _assign(self, heads, candidates, remaining):
        """Return a maximum-cardinality head -> endpoint assignment.

        A flexible head must never consume the only slot a single-option head
        can use.  Two independent bipartite matchings implement that priority.
        Constrained heads are matched first against all endpoint capacity; the
        slots they take are then removed and the flexible heads run a fresh
        matching on the remainder.  Every overall maximum matching can be
        rearranged to seat the constrained heads on their sole endpoint first,
        so this still reaches the global maximum while maximizing constrained
        coverage.  Each matching is a standard augmenting-path search.
        """
        assignment = {}
        left = dict(remaining)

        def match(group):
            # endpoint -> request id occupying one of its (unit) slots; the
            # table holds at most ``left[endpoint]`` entries per endpoint.
            seats = {}

            def augment(request_id, options, seen):
                for endpoint in options:
                    if endpoint in seen or left[endpoint] <= 0:
                        continue
                    seen.add(endpoint)
                    holders = seats.setdefault(endpoint, [])
                    if len(holders) < left[endpoint]:
                        holders.append(request_id)
                        return True
                    for incumbent in list(holders):
                        if augment(incumbent, candidates[incumbent], seen):
                            holders.remove(incumbent)
                            holders.append(request_id)
                            return True
                return False

            for request_id in group:
                if augment(request_id, candidates[request_id], set()):
                    for endpoint, holders in seats.items():
                        if request_id in holders:
                            assignment[request_id] = endpoint
                            break
            for endpoint, holders in seats.items():
                left[endpoint] -= len(holders)

        forced = [
            request.id for _caller, request in heads
            if len(candidates[request.id]) == 1
        ]
        flexible = [
            request.id for _caller, request in heads
            if len(candidates[request.id]) > 1
        ]
        match(forced)
        match(flexible)
        return assignment

    # ------------------------------------------------------------------ settle

    def settle(self, batch_id, values):
        """Deliver a whole backend settlement; reject mismatches atomically."""
        batch = self._batches.get(batch_id)
        if batch is None or batch_id in self._settled_ids:
            # Unknown id, or a duplicate/late settlement of an old batch: it
            # must never release a newer batch's capacity or reach new requests.
            return False
        if not isinstance(values, Mapping):
            return False
        if set(values.keys()) != batch.members:
            return False
        if not all(isinstance(value, str) for value in values.values()):
            return False

        # A valid settlement also proves an unresolved batch was accepted.
        batch.state = _ACCEPTED
        self._settled_ids.add(batch_id)
        self._outstanding[batch.endpoint] -= len(batch.members)
        del self._batches[batch_id]

        for request_id in batch.members:
            request = self._requests.get(request_id)
            if request is None:
                # Its cancelled terminal was already taken; just drop the
                # ordering marker so the caller can continue.
                self._release_order(request_id)
                continue
            if request.state == _CANCELLED_FLYING:
                # The cancellation terminal already left; ignore the value.
                pass
            else:
                request.state = _DONE
                request.terminal = ("ok", values[request_id])
            request.batch = None
            self._release_order(request_id)
        return True

    # ------------------------------------------------------------------ helpers

    def _release_order(self, request_id):
        """Drop an id from whichever caller queue still holds it."""
        for queue in self._queues.values():
            try:
                queue.remove(request_id)
                return
            except ValueError:
                pass
