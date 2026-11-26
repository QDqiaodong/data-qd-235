from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from http.client import HTTPConnection
from threading import Barrier
from unittest import TestCase

from broadcast import Closed
from demo import backend
from directory import Directory, Endpoint, Lease, NoReserve, SlotUnavailable


X = Endpoint("127.0.0.1", 7101)
Y = Endpoint("127.0.0.1", 7102)
Z = Endpoint("127.0.0.1", 7103)
A = Endpoint("127.0.0.1", 7201)
B = Endpoint("127.0.0.1", 7202)


def dial(entry):
    """Request a loopback backend through a granted (service, endpoint) entry."""
    _, endpoint = entry
    with closing(HTTPConnection(endpoint.host, endpoint.port, timeout=2)) as client:
        client.request("GET", "/")
        response = client.getresponse()
        return response.status, response.read().decode("ascii")


class ReserveTest(TestCase):
    def test_render_order_follows_reserve_feasibility(self):
        directory = Directory(capacity=4, slot_limits={X: 1, Y: 1, Z: 1})
        directory.replace("api", (X, Y))
        directory.replace("db", (X,))
        directory.replace("render", (Y, Z))
        view = directory.watch()

        directory.set_reserve({"api": 1, "db": 1})
        z_lease = view.acquire("render")
        self.assertEqual(z_lease.entries, (("render", Z),))
        z_lease.close()

        directory.set_reserve({"api": 1})
        y_lease = view.acquire("render")
        self.assertEqual(y_lease.entries, (("render", Y),))

        # Y is held, so api must take X and db has nowhere left.
        with self.assertRaises(SlotUnavailable):
            directory.set_reserve({"api": 1, "db": 1})
        # The rejected plan leaves the original api-only reserve in place.
        x_lease = directory.take_reserve()
        self.assertEqual(x_lease.entries, (("api", X),))
        for lease in (z_lease, y_lease, x_lease):
            lease.close()

    def test_take_reserve_reaches_live_backends_and_survives_an_outage(self):
        with backend("x") as x, backend("y") as y, backend("z") as z:
            directory = Directory(capacity=4, slot_limits={x: 1, y: 1, z: 1})
            directory.replace("api", (x, y))
            directory.replace("db", (x,))
            directory.replace("render", (y, z))
            view = directory.watch()

            directory.set_reserve({"api": 1, "db": 1})
            z_lease = view.acquire("render")
            self.assertEqual(dial(z_lease.entries[0]), (200, "z"))
            directory.set_reserve({"api": 1})
            y_lease = view.acquire("render")
            self.assertEqual(dial(y_lease.entries[0]), (200, "y"))
            # Holding y blocks the two-service plan while db only has x.
            with self.assertRaises(SlotUnavailable):
                directory.set_reserve({"api": 1, "db": 1})
            api_lease = directory.take_reserve()
            self.assertEqual(api_lease.entries, (("api", x),))
            self.assertEqual(dial(api_lease.entries[0]), (200, "x"))
            for lease in (z_lease, y_lease, api_lease):
                lease.close()

            # Adding z to db makes the two-service plan fit while y is held.
            directory.replace("db", (x, z))
            directory.set_reserve({"api": 1, "db": 1})
            held = view.acquire("render")
            self.assertEqual(held.entries, (("render", y),))
            group = directory.take_reserve()
            self.assertEqual(
                {dial(entry) for entry in group.entries},
                {(200, "x"), (200, "z")},
            )
            self.assertEqual([service for service, _ in group.entries], ["api", "db"])
            for service, endpoint in group.entries:
                expected = "x" if service == "api" else "z"
                self.assertEqual(dial((service, endpoint)), (200, expected))
            held.close()
            group.close()

            # A fresh plan that a temporary outage makes impossible.
            directory.set_reserve({"api": 1, "db": 1})
            directory.replace("db", ())
            with self.assertRaises(SlotUnavailable):
                view.acquire("render")
            with self.assertRaises(SlotUnavailable):
                directory.take_reserve()
            # Restoring db revives the very same plan.
            directory.replace("db", (x, z))
            recovered = directory.take_reserve()
            self.assertEqual(recovered.entries[0][0], "api")
            self.assertEqual(recovered.entries[1][0], "db")
            self.assertEqual(dial(recovered.entries[0]), (200, "x"))
            self.assertEqual(dial(recovered.entries[1]), (200, "z"))
            recovered.close()

    def test_stale_view_cannot_borrow_a_withdrawn_address(self):
        directory = Directory(capacity=1, slot_limits={A: 1, B: 1})
        directory.replace("render", (A,))
        slow = directory.watch()
        directory.replace("render", (B,))
        directory.replace("assets", (A,))

        self.assertEqual(slow.resolve("render"), (A,))
        with self.assertRaises(SlotUnavailable):
            slow.acquire("render")

        self.assertTrue(slow.advance().resynced)
        self.assertEqual(slow.resolve("render"), (B,))
        lease = slow.acquire("render")
        self.assertEqual(lease.entries, (("render", B),))
        lease.close()

    def test_view_never_borrows_an_address_it_has_not_seen(self):
        directory = Directory(capacity=2, slot_limits={A: 1, B: 1})
        directory.replace("api", (A,))
        view = directory.watch()
        directory.replace("api", (A, B))

        first = view.acquire("api")
        self.assertEqual(first.entries, (("api", A),))
        with self.assertRaises(SlotUnavailable):
            view.acquire("api")
        self.assertEqual(view.advance().resynced, False)
        second = view.acquire("api")
        self.assertEqual(second.entries, (("api", B),))
        first.close()
        second.close()

    def test_contention_for_the_last_slot_and_idempotent_return(self):
        directory = Directory(capacity=2, slot_limits={A: 1})
        directory.replace("api", (A,))
        left, right = directory.watch(), directory.watch()
        ready = Barrier(2)

        def grab(view):
            ready.wait(3)
            try:
                return view.acquire("api")
            except SlotUnavailable:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(grab, view) for view in (left, right)]
            outcomes = [future.result(5) for future in futures]
        grants = [outcome for outcome in outcomes if outcome is not None]
        self.assertEqual(len(grants), 1)
        lease = grants[0]
        self.assertEqual(lease.entries, (("api", A),))

        directory.replace("api", ())
        directory.replace("api", (A,))
        fresh = directory.watch()
        with self.assertRaises(SlotUnavailable):
            fresh.acquire("api")
        lease.close()
        lease.close()
        replacement = fresh.acquire("api")
        self.assertEqual(replacement.entries, (("api", A),))
        replacement.close()

    def test_services_sharing_one_address_share_its_slots(self):
        directory = Directory(capacity=2, slot_limits={A: 1})
        directory.replace("api", (A,))
        directory.replace("db", (A,))
        view = directory.watch()
        lease = view.acquire("api")
        with self.assertRaises(SlotUnavailable):
            view.acquire("db")
        with self.assertRaises(SlotUnavailable):
            directory.set_reserve({"api": 1, "db": 1})
        lease.close()
        shared = directory.watch().acquire("db")
        self.assertEqual(shared.entries, (("db", A),))
        shared.close()

    def test_multi_slot_addresses_may_repeat_in_a_group(self):
        directory = Directory(capacity=2, slot_limits={A: 3, B: 1})
        directory.replace("api", (A, B))
        directory.set_reserve({"api": 3})
        lease = directory.take_reserve()
        self.assertEqual(lease.entries, (("api", A),) * 3)
        with self.assertRaises(SlotUnavailable):
            directory.set_reserve({"api": 2})
        lease.close()
        directory.replace("db", (B,))
        directory.set_reserve({"api": 3, "db": 1})
        both = directory.take_reserve()
        self.assertEqual(len(both.entries), 4)
        self.assertEqual({service for service, _ in both.entries}, {"api", "db"})
        self.assertEqual(
            sum(1 for service, _ in both.entries if service == "api"), 3
        )
        both.close()

    def test_unconfigured_addresses_resolve_but_never_grant(self):
        plain = Endpoint("127.0.0.1", 7301)
        directory = Directory(capacity=2)
        directory.replace("api", (plain,))
        view = directory.watch()
        self.assertEqual(view.resolve("api"), (plain,))
        with self.assertRaises(SlotUnavailable):
            view.acquire("api")
        with self.assertRaises(SlotUnavailable):
            directory.set_reserve({"api": 1})

        mixed = Directory(capacity=2, slot_limits={A: 1})
        mixed.replace("api", (plain, A))
        mixed_view = mixed.watch()
        self.assertEqual(mixed_view.resolve("api"), (plain, A))
        lease = mixed_view.acquire("api")
        self.assertEqual(lease.entries, (("api", A),))
        lease.close()

    def test_no_reserve_and_withdrawal(self):
        directory = Directory(capacity=2, slot_limits={A: 1})
        directory.replace("api", (A,))
        with self.assertRaises(NoReserve):
            directory.take_reserve()
        directory.set_reserve({"api": 1})
        directory.set_reserve({})
        with self.assertRaises(NoReserve):
            directory.take_reserve()

    def test_invalid_slot_configuration(self):
        for limits in ([A], {("h", 1): 1}, {A: 0}, {A: 9}, {A: True},
                       {A: 1.0}, {Endpoint("h", port): 1 for port in range(1, 66)}):
            with self.assertRaises(ValueError):
                Directory(capacity=1, slot_limits=limits)

    def test_invalid_reserve_demand(self):
        directory = Directory(capacity=2, slot_limits={A: 8})
        directory.replace("api", (A,))
        for demand in ([("api", 1)], {"api": 0}, {"api": 9}, {"api": True},
                       {"api": 1.0}, {"Bad Name": 1},
                       {f"s{i}": 1 for i in range(33)},
                       {f"s{i}": 8 for i in range(9)}):
            with self.assertRaises(ValueError):
                directory.set_reserve(demand)
        with self.assertRaises(NoReserve):
            directory.take_reserve()

    def test_concurrent_grants_never_expose_partial_state(self):
        directory = Directory(capacity=2, slot_limits={A: 4})
        directory.replace("api", (A,))
        views = [directory.watch() for _ in range(8)]
        ready = Barrier(8)

        def grab(view):
            ready.wait(5)
            try:
                return view.acquire("api")
            except SlotUnavailable:
                return None

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(grab, view) for view in views]
            leases = [f.result(5) for f in futures]
        granted = [lease for lease in leases if lease is not None]
        self.assertEqual(len(granted), 4)
        self.assertEqual(directory._used, {A: 4})
        with self.assertRaises(SlotUnavailable):
            directory.watch().acquire("api")
        for lease in granted:
            lease.close()
        self.assertEqual(directory._used, {})

    def test_closed_view_and_directory(self):
        directory = Directory(capacity=2, slot_limits={A: 2})
        directory.replace("api", (A,))
        view = directory.watch()
        self.assertIsNone(directory.set_reserve({"api": 1}))
        lease = view.acquire("api")

        view.close()
        with self.assertRaises(Closed):
            view.acquire("api")
        other = directory.watch()
        directory.close()
        with self.assertRaises(Closed):
            other.acquire("api")
        with self.assertRaises(Closed):
            directory.set_reserve({"api": 1})
        with self.assertRaises(Closed):
            directory.take_reserve()

        # An already issued lease still returns its slot after every close.
        lease.close()
        lease.close()
        self.assertIsInstance(lease, Lease)
