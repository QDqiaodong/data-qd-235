"""Plan and execute a staged service chain that survives one lost fault domain.

A :class:`WorkflowRouter` binds a caller-supplied order of services, a mapping
from published addresses to fault-domain names, and the directed receipt
handoffs that a later step may consume.  It answers two different resilience
questions before anything is dialed:

* can the whole chain be built from the currently visible addresses
  (``route_exists`` and ``cold_blocked``), and
* can a chain that was already partly executed continue if one step first
  reports that its *entire* fault domain is unavailable (``protected``).

The second promise fixes the completed prefix: a retry only changes the failed
step and the steps that have not run yet, and it reuses the receipt that the
failed step's predecessor already produced.  A receipt never becomes invalid
because the domain that issued it later disappears.

``run`` executes at most one such in-call recovery.  The transport is allowed to
raise :class:`DomainUnavailable` only when it has established that the whole
fault domain of its current target is down *and* the attempt produced no side
effect.  Any other failure -- including a broken connection whose side effects
are unknown -- is propagated unchanged.  No fault is remembered between calls,
and nothing is probed or sent in parallel.
"""

import re
from collections.abc import Mapping
from typing import NamedTuple

from directory import Endpoint


_SERVICE = re.compile(r"[a-z][a-z0-9-]{0,62}")

_MAX_SERVICES = 6
_MAX_DOMAINS = 24
_MAX_DISTINCT_DOMAINS = 6
_MAX_ENDPOINTS_PER_SERVICE = 4
_MAX_LINKS = 80
_MAX_RECEIPT = 4096


class NoProtectedWorkflow(RuntimeError):
    """No normal chain can keep its completed prefix across a domain loss."""


class DomainUnavailable(RuntimeError):
    """A whole fault domain is confirmed down and the attempt had no side effect."""


class PreflightResult(NamedTuple):
    """Read-only outcome of :meth:`WorkflowRouter.preflight`."""

    route_exists: bool
    cold_blocked: tuple
    protected: bool


def _service(name):
    if not isinstance(name, str) or not _SERVICE.fullmatch(name):
        raise ValueError("invalid service or domain name")


def _validated_services(services):
    if not isinstance(services, tuple):
        raise ValueError("services must be a tuple")
    if not 2 <= len(services) <= _MAX_SERVICES:
        raise ValueError("services must contain 2-6 names")
    for service in services:
        _service(service)
    if len(set(services)) != len(services):
        raise ValueError("services must be distinct")
    return tuple(services)


def _validated_domains(domains):
    if not isinstance(domains, Mapping):
        raise ValueError("domains must be a mapping of Endpoint to domain name")
    if len(domains) > _MAX_DOMAINS:
        raise ValueError("too many address-to-domain entries")
    materialized = {}
    for endpoint, name in domains.items():
        if not isinstance(endpoint, Endpoint):
            raise ValueError("domains keys must be Endpoint values")
        _service(name)
        materialized[endpoint] = name
    if len(set(materialized.values())) > _MAX_DISTINCT_DOMAINS:
        raise ValueError("too many distinct fault domains")
    return materialized


def _validated_links(links, services, domains):
    if not isinstance(links, tuple):
        raise ValueError("links must be a tuple")
    if len(links) > _MAX_LINKS:
        raise ValueError("too many links")
    materialized = set()
    for link in links:
        if not isinstance(link, tuple) or len(link) != 4:
            raise ValueError(
                "each link must be (service, Endpoint, service, Endpoint)")
        previous_service, previous_endpoint, next_service, next_endpoint = link
        _service(previous_service)
        _service(next_service)
        if previous_service not in services:
            raise ValueError("a link must start at a configured service")
        position = services.index(previous_service)
        if position + 1 >= len(services) or services[position + 1] != next_service:
            raise ValueError("a link must join adjacent steps")
        if (not isinstance(previous_endpoint, Endpoint)
                or not isinstance(next_endpoint, Endpoint)):
            raise ValueError("link addresses must be Endpoint values")
        if previous_endpoint not in domains or next_endpoint not in domains:
            raise ValueError("link addresses must be registered in domains")
        if link in materialized:
            raise ValueError("links must be distinct")
        materialized.add(link)
    return frozenset(materialized)


class _RouteGraph:
    """A snapshot of usable candidates plus memoized suffix searches.

    ``banned`` is either a domain name that the whole suffix must avoid, or
    ``None`` for an unrestricted search.  Suffixes are found in view order and
    only use handoffs that are explicitly configured, so a reroute never dials
    an instance that could not consume the predecessor's receipt.
    """

    def __init__(self, services, candidates, domains, links):
        self.services = services
        self.candidates = candidates
        self.domains = domains
        self.links = links
        self._cache = {}

    def suffix(self, index, previous, banned):
        """Return endpoints for ``index`` onward, or ``None`` if impossible.

        ``previous`` is the endpoint that actually produced the receipt for
        this step (``None`` for the first step).  Every returned endpoint lies
        outside ``banned`` and is joined to its predecessor by a configured
        handoff.
        """
        key = (index, previous, banned)
        cache = self._cache
        if key in cache:
            return cache[key]

        if index == len(self.services):
            result = ()
        else:
            result = None
            for endpoint in self.candidates[index]:
                if banned is not None and self.domains[endpoint] == banned:
                    continue
                if index and (self.services[index - 1], previous,
                              self.services[index], endpoint) not in self.links:
                    continue
                tail = self.suffix(index + 1, endpoint, banned)
                if tail is not None:
                    result = (endpoint,) + tail
                    break

        cache[key] = result
        return result

    def chain_exists(self, banned=None):
        return self.suffix(0, None, banned) is not None

    def protected_chain(self):
        """Return the first normal chain meeting the per-step promise, if any.

        For every step of the chain, the search also asks whether the suffix
        starting at that step can be rebuilt outside that step's own domain,
        using the endpoint that actually completed the preceding step as the
        only fixed handoff source.
        """
        length = len(self.services)

        def protected(chain):
            for index, endpoint in enumerate(chain):
                previous = chain[index - 1] if index else None
                if self.suffix(index, previous, self.domains[endpoint]) is None:
                    return False
            return True

        def search(index, chain):
            if index == length:
                return chain if protected(chain) else None
            for endpoint in self.candidates[index]:
                if index and (self.services[index - 1], chain[-1],
                              self.services[index], endpoint) not in self.links:
                    continue
                found = search(index + 1, chain + (endpoint,))
                if found is not None:
                    return found
            return None

        return search(0, ())

    def involved_domains(self):
        names = {self.domains[endpoint]
                 for endpoints in self.candidates
                 for endpoint in endpoints}
        return tuple(sorted(names))


class WorkflowRouter:
    """Staged handoff routing over one caller-owned :class:`directory.View`."""

    def __init__(self, services, domains, links):
        self._services = _validated_services(services)
        self._domains = _validated_domains(domains)
        self._links = _validated_links(links, self._services, self._domains)

    # -- view snapshot ------------------------------------------------------
    def _graph(self, view):
        """Snapshot the view once; no address outside this snapshot is used."""
        candidates = []
        for service in self._services:
            try:
                visible = tuple(view.resolve(service))
            except TypeError:
                raise ValueError(
                    "view must resolve to an iterable of Endpoint values") from None
            for endpoint in visible:
                if not isinstance(endpoint, Endpoint):
                    raise ValueError("view resolved a non-Endpoint value")
            if len(visible) > _MAX_ENDPOINTS_PER_SERVICE:
                raise ValueError(
                    "a service resolves more than 4 endpoints in this view")
            endpoints = []
            for endpoint in visible:
                # An address that is not marked with a fault domain can never
                # be part of this chain.
                if endpoint not in self._domains or endpoint in endpoints:
                    continue
                endpoints.append(endpoint)
            candidates.append(tuple(endpoints))
        return _RouteGraph(self._services, tuple(candidates),
                           self._domains, self._links)

    # -- operator preflight ------------------------------------------------
    def preflight(self, view):
        """Inspect the current view without dialing or advancing anything."""
        graph = self._graph(view)
        return PreflightResult(
            route_exists=graph.chain_exists(),
            cold_blocked=tuple(
                domain for domain in graph.involved_domains()
                if not graph.chain_exists(domain)),
            protected=graph.protected_chain() is not None,
        )

    # -- protected execution ----------------------------------------------
    def run(self, view, invoke):
        """Execute one chain, continuing across at most one domain loss.

        ``invoke(service, endpoint, previous_receipt)`` performs the real work
        and returns a ``bytes`` receipt of at most 4096 bytes.  The first step
        receives ``None`` as its predecessor receipt.  When a step raises
        :class:`DomainUnavailable`, the successful prefix is kept, the failed
        step is retried on another linked instance outside that domain with the
        same predecessor receipt, and only unexecuted steps may be replanned.
        """
        graph = self._graph(view)
        chain = graph.protected_chain()
        if chain is None:
            raise NoProtectedWorkflow(
                "no workflow can continue after a domain loss")

        services = self._services
        plan = list(chain)
        results = []
        handled = False
        index = 0

        while index < len(services):
            service = services[index]
            endpoint = plan[index]
            receipt = results[-1][2] if index else None
            try:
                value = invoke(service, endpoint, receipt)
            except DomainUnavailable:
                if handled:
                    # This call promised to absorb only the first domain loss.
                    raise
                banned = self._domains[endpoint]
                previous = results[-1][1] if index else None
                suffix = graph.suffix(index, previous, banned)
                if suffix is None:
                    # A protected normal chain can always continue, but never
                    # silently retry without such a suffix.
                    raise
                handled = True
                plan[index:] = suffix
                continue

            if not isinstance(value, bytes) or len(value) > _MAX_RECEIPT:
                raise ValueError(
                    "a step receipt must be bytes of at most 4096 bytes")
            results.append((service, endpoint, value))
            index += 1

        return tuple(results)


__all__ = [
    "DomainUnavailable",
    "NoProtectedWorkflow",
    "PreflightResult",
    "WorkflowRouter",
]
