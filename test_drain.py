"""Drain handoff: caller-protected routes and operator-confirmed shutdown."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from http.client import HTTPConnection
from threading import Barrier
from unittest import TestCase

from broadcast import Closed
from demo import backend
from directory import (
    Directory,
    Drain,
    DrainConflict,
    DrainPending,
    Endpoint,
    RouteLimit,
    RouteUnavailable,
)


def dial(endpoint):
    with closing(HTTPConnection(endpoint.host, endpoint.port, timeout=2)) as client:
        client.request("GET", "/")
        response = client.getresponse()
        return response.status, response.read().decode("ascii")


def request_via(view, service, route):
    """Select the protected address, hold the protection across the request."""
    endpoints = view.resolve(service)
    assert endpoints and endpoints[0] == route.endpoint
    return dial(route.endpoint)


def request_via_route(endpoint, route):
    """Dial a held protection directly, as when its originating view is gone."""
    assert route.endpoint == endpoint
    return dial(endpoint)


class HandoffBasicsTest(TestCase):
    def test_constructor_validates_drain_endpoints(self):
        a = Endpoint("127.0.0.1", 7001)
        b = Endpoint("127.0.0.1", 7002)
        self.assertEqual(Directory(capacity=1).watch().resolve("x"), ())
        # Default is empty: nothing is managed.
        with self.assertRaises(ValueError):
            Directory(capacity=1).begin_drain(a)
        for bad in ([a], (a, a), (("h", 1),), (a,) * 33):
            with self.assertRaises(ValueError):
                Directory(capacity=1, drain_endpoints=bad)
        Directory(capacity=1, drain_endpoints=(a, b))

    def test_open_route_requires_managed_visible_address(self):
        a = Endpoint("127.0.0.1", 7001)
        a2 = Endpoint("127.0.0.1", 7002)
        other = Endpoint("127.0.0.1", 7099)
        directory = Directory(capacity=4, drain_endpoints=(a, a2))
        directory.replace("api", (a,))
        tracked = directory.watch(tracked=True)
        plain = directory.watch()

        with self.assertRaises(RouteUnavailable):  # plain view cannot protect
            plain.open_route("api", a)
        with self.assertRaises(ValueError):  # unmanaged address
            tracked.open_route("api", other)
        with self.assertRaises(ValueError):  # not an Endpoint
            tracked.open_route("api", ("127.0.0.1", 7001))
        with self.assertRaises(ValueError):  # bad service name
            tracked.open_route("Bad Name", a)
        with self.assertRaises(RouteUnavailable):  # not in this view's cut
            tracked.open_route("api", a2)
        with self.assertRaises(RouteUnavailable):  # wrong service
            tracked.open_route("db", a)

        directory.replace("api", (a2,))
        # open_route never advances the view to a publication it has not seen.
        with self.assertRaises(RouteUnavailable):
            tracked.open_route("api", a2)
        # The address withdrawn from the authoritative directory is still in
        # the lagging view's cut, so that view may protect it until it advances.
        route = tracked.open_route("api", a)
        self.assertEqual(route.endpoint, a)
        route.close()
        route.close()

    def test_route_and_view_limits(self):
        a = Endpoint("127.0.0.1", 7001)
        directory = Directory(capacity=1, drain_endpoints=(a,))
        directory.replace("api", (a,))
        views = [directory.watch(tracked=True) for _ in range(64)]
        with self.assertRaises(RouteLimit):
            directory.watch(tracked=True)
        routes = [views[0].open_route("api", a) for _ in range(256)]
        with self.assertRaises(RouteLimit):
            views[1].open_route("api", a)
        # Reaching a limit does not disturb the protections already held.
        for route in routes:
            route.close()
        again = views[1].open_route("api", a)
        again.close()
        views[0].close()
        # A closed tracked view frees its slot for a new tracked view.
        directory.watch(tracked=True)

    def test_closed_directory_and_view_boundaries(self):
        a = Endpoint("127.0.0.1", 7001)
        directory = Directory(capacity=1, drain_endpoints=(a,))
        directory.replace("api", (a,))
        view = directory.watch(tracked=True)
        route = view.open_route("api", a)
        directory.replace("api", ())
        drain = directory.begin_drain(a)
        view.close()
        with self.assertRaises(Closed):
            view.open_route("api", a)
        directory.close()
        with self.assertRaises(Closed):
            directory.watch(tracked=True)
        with self.assertRaises(Closed):
            directory.begin_drain(a)
        other = Endpoint("127.0.0.1", 7002)
        d2 = Directory(capacity=1, drain_endpoints=(other,))
        d2.replace("api", (other,))
        v2 = d2.watch(tracked=True)
        d2.replace("api", ())
        drain2 = d2.begin_drain(other)
        d2.close()
        with self.assertRaises(Closed):
            v2.open_route("api", other)
        # A held protection may still be released, and the drain still works.
        route.close()
        self.assertEqual(drain.state, "ready")
        drain.finish()
        self.assertEqual(drain2.state, "ready")
        drain2.finish()

    def test_directory_close_waits_only_on_in_flight_routes(self):
        a = Endpoint("127.0.0.1", 7001)
        directory = Directory(capacity=4, drain_endpoints=(a,))
        directory.replace("api", (a,))
        viewer = directory.watch(tracked=True)   # still shows the address
        route = viewer.open_route("api", a)
        directory.replace("api", ())
        drain = directory.begin_drain(a)
        self.assertEqual(drain.state, "pending")  # view + route
        directory.close()
        # No view can start a request anymore, so only the held route blocks.
        self.assertEqual(drain.state, "pending")
        route.close()
        self.assertEqual(drain.state, "ready")
        drain.finish()

    def test_finish_is_idempotent_and_does_not_release_the_next_drain(self):
        a = Endpoint("127.0.0.1", 7001)
        directory = Directory(capacity=2, drain_endpoints=(a,))
        directory.replace("api", (a,))
        directory.replace("api", ())
        first = directory.begin_drain(a)
        self.assertEqual(first.state, "ready")
        first.finish()
        self.assertEqual(first.state, "finished")
        first.finish()  # no-op, and must not touch the next handoff
        directory.replace("api", (a,))
        held = directory.watch(tracked=True)   # sees the new-round publication
        route = held.open_route("api", a)
        directory.replace("api", ())
        second = directory.begin_drain(a)
        self.assertIsNot(second, first)
        first.finish()  # retry of the finished one releases nothing
        self.assertEqual(second.state, "pending")
        self.assertFalse(second._finished)
        route.close()   # the view still shows the address, so still pending
        self.assertEqual(second.state, "pending")
        held.advance()  # read the withdrawal
        self.assertEqual(second.state, "ready")
        second.finish()
        self.assertEqual(second.state, "finished")
        self.assertEqual(first.state, "finished")


class ScenarioOneTest(TestCase):
    """Two services on the old address, a held old request, and a lagged view."""

    def _setup(self, capacity):
        old = backend("old")
        new = backend("new")
        old_endpoint = old.__enter__()
        new_endpoint = new.__enter__()
        directory = Directory(
            capacity=capacity, drain_endpoints=(old_endpoint, new_endpoint))
        directory.replace("render", (old_endpoint,))
        directory.replace("alias", (old_endpoint,))
        return directory, old, new, old_endpoint, new_endpoint

    def _teardown(self, old, new):
        for manager in (old, new):
            manager.__exit__(None, None, None)

    def test_handoff_waits_for_alias_then_held_request_then_lagging_view(self):
        directory, old, new, old_ep, new_ep = self._setup(capacity=1)
        holder = directory.watch(tracked=True)
        idle = directory.watch(tracked=True)
        try:
            # Only render is switched; the address stays published via alias,
            # so the handoff cannot begin yet.
            directory.replace("render", (new_ep,))
            self.assertIn(old_ep, directory._entries["alias"])
            with self.assertRaises(DrainConflict):
                directory.begin_drain(old_ep)

            directory.replace("alias", ())
            drain = directory.begin_drain(old_ep)
            self.assertEqual(drain.state, "pending")

            # The caller takes an old-address protection, then closes its view;
            # the protection survives and still reads the old backend.  The
            # operator still cannot stop: the request and ``idle`` block.
            route = holder.open_route("render", old_ep)
            holder.close()
            self.assertEqual(dial(route.endpoint), (200, "old"))
            self.assertEqual(drain.state, "pending")
            with self.assertRaises(DrainPending):
                drain.finish()

            # After the request is released only ``idle`` still shows the old
            # address.  Because the handoff is pending, that view may still
            # obtain a protection and reach the old backend until it catches up.
            route.close()
            self.assertEqual(idle.resolve("render"), (old_ep,))
            self.assertEqual(drain.state, "pending")
            pending_route = idle.open_route("render", old_ep)
            self.assertEqual(dial(pending_route.endpoint), (200, "old"))
            pending_route.close()

            # Advancing past the withdrawn events (capacity 1 forces a rebuild)
            # removes the address and makes the handoff ready; then that same
            # view may no longer start an old request.
            idle.advance()
            self.assertEqual(idle.resolve("render"), (new_ep,))
            self.assertEqual(idle.resolve("alias"), ())
            self.assertEqual(drain.state, "ready")
            with self.assertRaises(RouteUnavailable):
                idle.open_route("render", old_ep)
            drain.finish()
            idle.close()
        finally:
            self._teardown(old, new)
            directory.close()

    def test_protection_survives_view_close_and_still_reaches_backend(self):
        directory, old, new, old_ep, new_ep = self._setup(capacity=2)
        view = directory.watch(tracked=True)
        try:
            route = view.open_route("render", old_ep)
            directory.replace("render", (new_ep,))
            directory.replace("alias", ())
            drain = directory.begin_drain(old_ep)
            # Closing the view does not release the caller's protection.
            view.close()
            self.assertEqual(drain.state, "pending")
            self.assertEqual(request_via_route(old_ep, route), (200, "old"))
            self.assertEqual(drain.state, "pending")   # still in flight
            route.close()
            self.assertEqual(drain.state, "ready")
            drain.finish()
        finally:
            self._teardown(old, new)
            directory.close()

    def test_withdrawn_view_must_advance_before_ready(self):
        directory, old, new, old_ep, new_ep = self._setup(capacity=1)
        view = directory.watch(tracked=True)
        try:
            directory.replace("render", (new_ep,))
            directory.replace("alias", ())
            drain = directory.begin_drain(old_ep)
            # Protect an old request from the stale but still-open view.
            route = view.open_route("render", old_ep)
            self.assertEqual(request_via(view, "render", route), (200, "old"))
            self.assertEqual(drain.state, "pending")
            route.close()
            # The view is idle and still shows the old address?  Its cut is
            # after removal because begin_drain required no live event, but the
            # view has not read the events; it still resolves old.
            self.assertEqual(view.resolve("render"), (old_ep,))
            # Because the view is at a cut before the drain start, it blocks.
            self.assertEqual(drain.state, "pending")
            with self.assertRaises(DrainPending):
                drain.finish()
            # Advancing (here a full rebuild, since the ring is size 1) is what
            # makes the handoff ready, with no timers or new publications.
            view.advance()
            self.assertEqual(view.resolve("render"), (new_ep,))
            self.assertEqual(view.resolve("alias"), ())
            self.assertEqual(drain.state, "ready")
            drain.finish()
            self.assertEqual(drain.state, "finished")
        finally:
            self._teardown(old, new)
            directory.close()

    def test_lagged_rebuild_wakes_ready_without_a_new_publication(self):
        directory, old, new, old_ep, new_ep = self._setup(capacity=1)
        view = directory.watch(tracked=True)
        try:
            # view misses the render change and the alias removal (ring of 1).
            directory.replace("render", (new_ep,))   # seq1
            directory.replace("alias", ())           # seq2 overwrites seq1
            drain = directory.begin_drain(old_ep)
            self.assertEqual(drain.state, "pending")
            route = view.open_route("render", old_ep)
            self.assertEqual(request_via(view, "render", route), (200, "old"))
            route.close()
            self.assertEqual(drain.state, "pending")
            # A lagged advance triggers a full rebuild that is past the cut.
            progress = view.advance()
            self.assertTrue(progress.resynced)
            self.assertEqual(drain.state, "ready")
            self.assertEqual(view.resolve("render"), (new_ep,))
            with self.assertRaises(RouteUnavailable):
                view.open_route("render", old_ep)
            drain.finish()
        finally:
            self._teardown(old, new)
            directory.close()


class ScenarioTwoTest(TestCase):
    """A view that has not yet consumed an old join event from the ring."""

    def test_old_join_vs_new_publication_of_same_address(self):
        with backend("old") as old_ep, backend("new") as new_ep:
            directory = Directory(capacity=4, drain_endpoints=(old_ep, new_ep))
            # Created before the old join: it initially sees no address, but
            # its cursor is at the old join and the ring still holds it.
            late = directory.watch(tracked=True)
            self.assertEqual(late.resolve("render"), ())
            self.assertEqual(late.next_sequence, 0)
            directory.replace("render", (old_ep,))       # seq0 (old join)
            directory.replace("render", (new_ep,))       # seq1
            directory.replace("render", ())              # seq2
            # ``late`` still shows nothing, so it does not block: the handoff
            # can be ready and finished before it ever catches up.
            drain = directory.begin_drain(old_ep)
            self.assertEqual(drain.state, "ready")
            drain.finish()
            # Publish the same address again as a new round.
            directory.replace("render", (old_ep,))       # seq3 (new round)
            # Let ``late`` replay the retained old join first.  resolve now
            # shows the old address, but its origin is the old round.
            late.advance()                               # seq0: old join
            self.assertEqual(late.resolve("render"), (old_ep,))
            with self.assertRaises(RouteUnavailable):
                late.open_route("render", old_ep)        # old round rejected
            late.advance()                               # seq1: new
            late.advance()                               # seq2: removed
            with self.assertRaises(RouteUnavailable):
                late.open_route("render", old_ep)
            # Only after genuinely consuming the new-round publication (seq3)
            # may this view protect the address and reach the live backend.
            late.advance()                               # seq3: new round
            route = late.open_route("render", old_ep)
            self.assertEqual(request_via(late, "render", route), (200, "old"))
            route.close()
            fresh = directory.watch(tracked=True)
            fresh_route = fresh.open_route("render", old_ep)
            self.assertEqual(request_via(fresh, "render", fresh_route), (200, "old"))
            fresh_route.close()
            fresh.close()
            late.close()
            directory.close()

    def test_pending_branch_advances_past_the_withdrawal(self):
        with backend("old") as old_ep, backend("new") as new_ep:
            directory = Directory(capacity=4, drain_endpoints=(old_ep, new_ep))
            directory.replace("render", (old_ep,))       # seq0
            seen = directory.watch(tracked=True)         # shows old
            directory.replace("render", (new_ep,))       # seq1
            directory.replace("render", ())              # seq2
            drain = directory.begin_drain(old_ep)
            # ``seen`` still displays the old address, so the handoff is
            # pending and may let it keep serving until it advances.
            self.assertEqual(drain.state, "pending")
            old_route = seen.open_route("render", old_ep)
            self.assertEqual(request_via(seen, "render", old_route), (200, "old"))
            old_route.close()
            # Advance until the view no longer shows the old address.  Once it
            # stops displaying it and no retained event reintroduces it, the
            # handoff becomes ready with no timer or further publication.
            while old_ep in seen.resolve("render"):
                self.assertEqual(drain.state, "pending")
                seen.advance()
            self.assertEqual(drain.state, "ready")
            drain.finish()
            with self.assertRaises(RouteUnavailable):
                seen.open_route("render", old_ep)
            seen.close()
            directory.close()


class ScenarioThreeTest(TestCase):
    """Republishing while draining, protocol compatibility, and races."""

    def test_republish_by_other_name_and_protocol_is_refused_until_finish(self):
        with backend("old") as old_ep, backend("new") as new_ep:
            directory = Directory(capacity=4, drain_endpoints=(old_ep,))
            directory.replace("app", (old_ep,))
            directory.replace("store", (new_ep,))
            directory.configure_protocol_rollout({
                "app": {
                    "v1": ((old_ep,), ("v1",), {"store": ("v1",)}),
                    "v2": ((new_ep,), ("v1",), {"store": ("v1",)}),
                },
                "store": {"v1": ((new_ep,), ("v1",), {})},
            })
            view = directory.watch()
            # Move the managed service to its compatible v2, withdrawing the
            # old address from the authoritative directory.
            self.assertIsNotNone(directory.replace("app", (new_ep,)))
            self.assertEqual(view.advance().resynced, False)
            drain = directory.begin_drain(old_ep)
            self.assertEqual(drain.state, "ready")
            next_before = directory._next
            # A different service name is refused while the handoff is open.
            with self.assertRaises(DrainConflict):
                directory.replace("alias", (old_ep,))
            # The originally compatible rollout version is refused too.
            with self.assertRaises(DrainConflict):
                directory.replace("app", (old_ep,))
            self.assertEqual(directory._next, next_before)
            self.assertEqual(directory._entries["app"], (new_ep,))
            self.assertEqual(directory._entries["store"], (new_ep,))
            # No event reached existing views for either rejected replace.
            with self.assertRaises(TimeoutError):
                view.advance()
            drain.finish()
            # After finish the compatible rollout publication succeeds.
            self.assertIsNotNone(directory.replace("app", (old_ep,)))
            self.assertEqual(view.advance().resynced, False)
            self.assertEqual(view.resolve("app"), (old_ep,))
            view.close()
            directory.close()

    def test_route_and_ready_never_cross_the_safety_boundary(self):
        # Half the views still show the address (they keep the handoff
        # pending), the others have already advanced past it.  Concurrently
        # some of those views grant/hold protections while the operator calls
        # finish(): the shared lock means finish can only succeed once nothing
        # may start an old request, and afterwards no view may start one.
        for _attempt in range(20):
            a = Endpoint("127.0.0.1", 7001)
            directory = Directory(capacity=8, drain_endpoints=(a,))
            directory.replace("api", (a,))
            stale = [directory.watch(tracked=True) for _ in range(4)]
            advanced = [directory.watch(tracked=True) for _ in range(4)]
            directory.replace("api", ())
            for view in advanced:
                view.advance()  # consume the withdrawal; no longer shows a
            drain = directory.begin_drain(a)
            self.assertEqual(drain.state, "pending")  # ``stale`` blocks
            ready = Barrier(5)

            def grab(view):
                ready.wait(5)
                try:
                    return view.open_route("api", a)
                except RouteUnavailable:
                    return None

            def operate():
                ready.wait(5)
                try:
                    drain.finish()
                    return "finished"
                except DrainPending:
                    return "pending"

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(grab, v) for v in stale]
                operator = pool.submit(operate)
                routes = [f.result(5) for f in futures]
                outcome = operator.result(5)
            held = [r for r in routes if r is not None]
            # Either finish could not proceed (stale views still show a), or it
            # finished after the grants were released -- never both.
            self.assertEqual(outcome, "pending")   # stale views block finish
            self.assertEqual(drain.state, "pending")
            for route in held:
                route.close()
            # Now advancing the stale views past the withdrawal removes the
            # last obstacle; readiness appears and only then finish works.
            for view in stale:
                view.advance()
            self.assertEqual(drain.state, "ready")
            for view in stale + advanced:
                with self.assertRaises(RouteUnavailable):
                    view.open_route("api", a)
            drain.finish()
            self.assertEqual(drain.state, "finished")
            for view in stale + advanced:
                view.close()
            directory.close()

    def test_publisher_and_operator_race_has_one_winner(self):
        for attempt in range(20):
            directory = Directory(capacity=8, drain_endpoints=(
                Endpoint("127.0.0.1", 7101),))
            old_ep = Endpoint("127.0.0.1", 7101)
            new_ep = Endpoint("127.0.0.1", 7102)
            directory.replace("app", (old_ep,))
            directory.replace("app", ())
            ready = Barrier(2)

            def publish():
                ready.wait(5)
                try:
                    return ("published", directory.replace("other", (old_ep,)))
                except DrainConflict:
                    return ("blocked", None)

            def drain():
                ready.wait(5)
                try:
                    return ("drained", directory.begin_drain(old_ep))
                except DrainConflict:
                    return ("blocked", None)

            with ThreadPoolExecutor(max_workers=2) as pool:
                pf = pool.submit(publish)
                df = pool.submit(drain)
                p_outcome = pf.result(5)
                d_outcome = df.result(5)

            published = p_outcome[0] == "published"
            drained = d_outcome[0] == "drained"
            # Exactly one wins (a publisher that lands first blocks the drain,
            # and a drain that begins first blocks the publisher).
            self.assertNotEqual(published, drained)
            if published:
                self.assertIn(old_ep, directory._entries["other"])
                with self.assertRaises(DrainConflict):
                    directory.begin_drain(old_ep)
            else:
                self.assertNotIn("other", directory._entries)
                handoff = d_outcome[1]
                self.assertEqual(handoff.state, "ready")
                handoff.finish()
                self.assertNotIn("other", directory._entries)
            directory.close()


class RealRequestTest(TestCase):
    """An actual protected request reaches the loopback backend that is held."""

    def test_held_protection_dials_old_backend_while_new_publishes(self):
        with backend("old") as old_ep, backend("new") as new_ep:
            directory = Directory(
                capacity=4, drain_endpoints=(old_ep, new_ep))
            directory.replace("render", (old_ep,))
            view = directory.watch(tracked=True)
            route = view.open_route("render", old_ep)
            directory.replace("render", (new_ep,))
            # The protected request still reaches the old listener.
            self.assertEqual(request_via(view, "render", route), (200, "old"))
            # A fresh view reaches the new listener.
            fresh = directory.watch(tracked=True)
            new_route = fresh.open_route("render", new_ep)
            self.assertEqual(request_via(fresh, "render", new_route), (200, "new"))
            new_route.close()
            route.close()
            view.close()
            fresh.close()
            directory.close()
