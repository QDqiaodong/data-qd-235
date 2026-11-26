from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from http.client import HTTPConnection
from threading import Barrier
from unittest import TestCase

from broadcast import Closed
from demo import backend
from directory import Directory, Endpoint, IncompatibleRollout, NoRolloutPath, Progress


# Managed direct endpoints (application and storage families).
APP_V1 = Endpoint("127.0.0.1", 7401)
APP_BRIDGE = Endpoint("127.0.0.1", 7402)
APP_V2 = Endpoint("127.0.0.1", 7403)
STORE_V1 = Endpoint("127.0.0.1", 7501)
STORE_BRIDGE = Endpoint("127.0.0.1", 7502)
STORE_V2 = Endpoint("127.0.0.1", 7503)


def dial(endpoint):
    with closing(HTTPConnection(endpoint.host, endpoint.port, timeout=2)) as client:
        client.request("GET", "/")
        response = client.getresponse()
        return response.status, response.read().decode("ascii")


def catalog():
    """App-only accepts v1 -> v2; storage-only offers v1 -> v2.

    The bridge versions accept/offer both, which is what makes a three-step
    route possible without ever landing on an incompatible pairing.
    """
    return {
        "app": {
            "v1": ((APP_V1,), ("v1",), {"store": ("v1",)}),
            "bridge": ((APP_BRIDGE,), ("v1", "v2"), {"store": ("v1", "v2")}),
            "v2": ((APP_V2,), ("v2",), {"store": ("v2",)}),
        },
        "store": {
            "v1": ((STORE_V1,), ("v1",), {}),
            "bridge": ((STORE_BRIDGE,), ("v1", "v2"), {}),
            "v2": ((STORE_V2,), ("v2",), {}),
        },
    }


def fresh_directory(cap=8):
    directory = Directory(capacity=cap)
    directory.replace("app", (APP_V1,))
    directory.replace("store", (STORE_V1,))
    directory.configure_protocol_rollout(catalog())
    return directory


class ConfigureTest(TestCase):
    def test_enable_validates_then_does_nothing(self):
        directory = Directory(capacity=4)
        directory.replace("app", (APP_V1,))
        directory.replace("store", (STORE_V1,))
        view = directory.watch()
        self.assertIsNone(directory.configure_protocol_rollout(catalog()))
        with self.assertRaises(TimeoutError):
            view.advance()
        self.assertEqual(view.next_sequence, 2)
        # Re-enabling is refused and changes nothing.
        with self.assertRaises(ValueError):
            directory.configure_protocol_rollout(catalog())

    def test_invalid_configurations_are_rejected_atomically(self):
        def base():
            directory = Directory(capacity=4)
            directory.replace("app", (APP_V1,))
            directory.replace("store", (STORE_V1,))
            return directory

        deref = Endpoint("h", 1)
        invalid = [
            {},
            {f"s{i}": {"v": ((Endpoint("h", i + 1),), ("p",), {})} for i in range(9)},
            {"app": {}},
            {"app": {f"v{i}": ((Endpoint("h", i + 1),), ("p",), {}) for i in range(5)}},
            {"app": {"v": ((), ("p",), {})}},
            {"app": {"v": ((deref, deref), ("p",), {})}},
            {"app": {"v": (tuple(Endpoint("h", i + 1) for i in range(5)), ("p",), {})}},
            # Same ordered endpoints across versions cannot identify a version.
            {"app": {"v1": ((deref,), ("p",), {}), "v2": ((deref,), ("q",), {})}},
            {"app": {"v": ((Endpoint("h", 1),), (), {})}},
            {"app": {"v": ((Endpoint("h", 1),), ("p", "P"), {})}},
            {"app": {"v": ((Endpoint("h", 1),), ("p", "p"), {})}},
            {"app": {"v": ((Endpoint("h", 1),), ("p", "a", "b", "c", "d"), {})}},
            {"app": {"v": ((Endpoint("h", 1),), ("p",), {"app": ("p",)})}},
            {"app": {"v": ((Endpoint("h", 1),), ("p",), {"other": ("p",)})}},
            {"app": {"v1": ((Endpoint("h", 1),), ("p",), {"store": ("p",)}),
                     "v2": ((Endpoint("h", 2),), ("p",), {})},
             "store": {"v": ((Endpoint("h", 3),), ("p",), {})}},
            # More than 16 directed dependencies across the catalog.
            {**{f"d{i}": {"v": ((Endpoint("h", i + 1),), ("p",), {})} for i in range(8)},
             "app": {"v": ((Endpoint("h", 99),), ("p",),
                           {f"d{i}": ("p",) for i in range(8)})}},
        ]
        for bad in invalid:
            directory = base()
            view = directory.watch()
            with self.assertRaises(ValueError):
                directory.configure_protocol_rollout(bad)
            with self.assertRaises(TimeoutError):
                view.advance()
            self.assertEqual(view.next_sequence, 2)

    def test_requires_published_legal_and_compatible_start(self):
        directory = Directory(capacity=4)
        directory.replace("app", (APP_V1,))
        with self.assertRaises(ValueError):  # store not published
            directory.configure_protocol_rollout(catalog())

        directory.replace("store", (Endpoint("127.0.0.1", 7599),))
        with self.assertRaises(ValueError):  # not a declared version
            directory.configure_protocol_rollout(catalog())

        directory.replace("store", (STORE_V2,))
        with self.assertRaises(ValueError):  # incompatible start
            directory.configure_protocol_rollout(catalog())

        with self.assertRaises(TimeoutError):
            directory.watch().advance()

    def test_version_must_fit_the_event_payload_bound(self):
        big = {f"s{i}": {"v": ((Endpoint("127.0.0.1", 7600 + i),), ("p",), {})} for i in range(3)}
        initial = max(
            (len(f'["{name}",[["127.0.0.1",7600]]]'.encode("ascii")) for name in big),
            default=0,
        )
        directory = Directory(capacity=4, max_payload=initial)
        for name in big:
            directory.replace(name, (Endpoint("127.0.0.1", 7600),))
        # A version carrying two endpoints cannot fit the same event budget.
        big["s0"]["v2"] = ((Endpoint("127.0.0.1", 7600), Endpoint("127.0.0.1", 7699)), ("p",), {})
        with self.assertRaises(ValueError):
            directory.configure_protocol_rollout(big)
        with self.assertRaises(TimeoutError):
            directory.watch().advance()

    def test_later_catalog_mutation_does_not_change_rules(self):
        source = catalog()
        directory = Directory(capacity=4)
        directory.replace("app", (APP_V1,))
        directory.replace("store", (STORE_V1,))
        directory.configure_protocol_rollout(source)
        source["app"]["v2"] = ((APP_V1,), ("v9",), {"store": ("v9",)})
        source["store"]["v2"] = ((STORE_V1,), ("v9",), {})
        plan = directory.plan_protocol_rollout(
            {"app": "v2", "store": "v2"}, {"app": ("bridge",)})
        for service, endpoints in plan:
            directory.replace(service, endpoints)
        self.assertEqual(directory.watch().resolve("app"), (APP_V2,))

    def test_unknown_version_and_withdrawal_are_value_errors(self):
        directory = fresh_directory()
        view = directory.watch()
        with self.assertRaises(ValueError):
            directory.replace("app", ())
        with self.assertRaises(ValueError):
            directory.replace("app", (Endpoint("127.0.0.1", 7499),))
        with self.assertRaises(TimeoutError):
            view.advance()
        self.assertEqual(view.next_sequence, 2)


class PlanningTest(TestCase):
    def test_no_bridge_means_no_path(self):
        directory = fresh_directory()
        view = directory.watch()
        with self.assertRaises(NoRolloutPath):
            directory.plan_protocol_rollout({"app": "v2", "store": "v2"})
        with self.assertRaises(TimeoutError):
            view.advance()

    def test_each_direction_and_both(self):
        app_first = (("app", (APP_BRIDGE,)), ("store", (STORE_V2,)), ("app", (APP_V2,)))
        store_first = (("store", (STORE_BRIDGE,)), ("app", (APP_V2,)), ("store", (STORE_V2,)))
        targets = {"app": "v2", "store": "v2"}
        directory = fresh_directory()
        self.assertEqual(
            directory.plan_protocol_rollout(targets, {"app": ("bridge",)}), app_first)
        self.assertEqual(
            directory.plan_protocol_rollout(targets, {"store": ("bridge",)}), store_first)
        both = directory.plan_protocol_rollout(
            targets, {"app": ("bridge",), "store": ("bridge",)})
        self.assertEqual(len(both), 3)
        self.assertEqual(both.count(("app", (APP_BRIDGE,))), 1)
        # Planning never touched the directory.
        self.assertEqual(directory.watch().resolve("app"), (APP_V1,))

    def test_plan_is_empty_at_target_and_rejects_bad_input(self):
        directory = fresh_directory()
        self.assertEqual(directory.plan_protocol_rollout({}), ())
        self.assertEqual(directory.plan_protocol_rollout({"app": "v1"}), ())
        for bad_targets in ({"app": "nope"}, {"ghost": "v1"}, {"app": "bad name"}):
            with self.assertRaises(ValueError):
                directory.plan_protocol_rollout(bad_targets)
        with self.assertRaises(ValueError):
            directory.plan_protocol_rollout({"app": "v2"}, {"store": ("bridge",)})
        with self.assertRaises(ValueError):
            directory.plan_protocol_rollout({"app": "v2"}, {"app": ("a", "b", "c", "d", "e")})
        with self.assertRaises(ValueError):
            directory.plan_protocol_rollout([("app", "v2")])

        plain = Directory(capacity=4)
        plain.replace("app", (APP_V1,))
        with self.assertRaises(ValueError):
            plain.plan_protocol_rollout({"app": "v1"})

    def test_old_plan_cannot_bypass_a_later_check(self):
        directory = fresh_directory()
        plan = directory.plan_protocol_rollout({"app": "v2", "store": "v2"},
                                               {"app": ("bridge",)})
        # Someone republishes the target directly while still on v1.
        with self.assertRaises(IncompatibleRollout):
            directory.replace("store", (STORE_V2,))
        # The stale plan's first step still applies; later steps re-check.
        directory.replace(*plan[0])
        self.assertEqual(directory.watch().resolve("app"), (APP_BRIDGE,))


# A longer rollout that needs two app transitions unless storage bridges.
LONG_APP_V1 = Endpoint("127.0.0.1", 7701)
LONG_APP_T1 = Endpoint("127.0.0.1", 7702)
LONG_APP_T2 = Endpoint("127.0.0.1", 7703)
LONG_APP_V2 = Endpoint("127.0.0.1", 7704)
LONG_STORE_OLD = Endpoint("127.0.0.1", 7801)
LONG_STORE_MID = Endpoint("127.0.0.1", 7802)
LONG_STORE_NEW = Endpoint("127.0.0.1", 7803)
LONG_STORE_BRIDGE = Endpoint("127.0.0.1", 7804)
LONG_WORKER_V1 = Endpoint("127.0.0.1", 7901)


def long_catalog(with_worker=False):
    cat = {
        "app": {
            "a1": ((LONG_APP_V1,), ("p1",), {"store": ("old",)}),
            "t1": ((LONG_APP_T1,), ("p1",), {"store": ("old", "mid")}),
            "t2": ((LONG_APP_T2,), ("p1",), {"store": ("mid", "new")}),
            "a4": ((LONG_APP_V2,), ("p1",), {"store": ("new",)}),
        },
        "store": {
            "s1": ((LONG_STORE_OLD,), ("old",), {}),
            "s2": ((LONG_STORE_MID,), ("mid",), {}),
            "s3": ((LONG_STORE_NEW,), ("new",), {}),
            "ss": ((LONG_STORE_BRIDGE,), ("old", "new"), {}),
        },
    }
    if with_worker:
        # Fixed consumer that refuses the middle protocol entirely.
        cat["worker"] = {"w1": ((LONG_WORKER_V1,), ("p1",), {"store": ("old", "new")})}
    return cat


def long_directory(with_worker=False):
    directory = Directory(capacity=16)
    directory.replace("app", (LONG_APP_V1,))
    directory.replace("store", (LONG_STORE_OLD,))
    if with_worker:
        directory.replace("worker", (LONG_WORKER_V1,))
    directory.configure_protocol_rollout(long_catalog(with_worker))
    return directory


LONG_TARGETS = {"app": "a4", "store": "s3"}


class LongRouteTest(TestCase):
    def test_five_steps_then_three_with_bridge(self):
        directory = long_directory()
        long_plan = directory.plan_protocol_rollout(
            LONG_TARGETS, {"app": ("t1", "t2"), "store": ("s2",)})
        self.assertEqual(len(long_plan), 5)
        self.assertEqual([svc for svc, _ in long_plan], ["app", "store", "app", "store", "app"])
        short_plan = directory.plan_protocol_rollout(
            LONG_TARGETS, {"app": ("t1", "t2"), "store": ("s2", "ss")})
        self.assertEqual(len(short_plan), 3)
        self.assertEqual(short_plan[0], ("store", (LONG_STORE_BRIDGE,)))

    def test_fixed_consumer_blocks_long_route_but_not_shortcut(self):
        directory = long_directory(with_worker=True)
        with self.assertRaises(NoRolloutPath):
            directory.plan_protocol_rollout(LONG_TARGETS, {"app": ("t1", "t2"), "store": ("s2",)})
        shortcut = directory.plan_protocol_rollout(
            LONG_TARGETS, {"app": ("t1", "t2"), "store": ("s2", "ss")})
        self.assertEqual(len(shortcut), 3)

    def test_planning_long_and_impossible_publishes_nothing(self):
        directory = long_directory(with_worker=True)
        view = directory.watch()
        directory.plan_protocol_rollout(LONG_TARGETS, {"app": ("t1", "t2"), "store": ("s2", "ss")})
        with self.assertRaises(NoRolloutPath):
            directory.plan_protocol_rollout(LONG_TARGETS, {"app": ("t1", "t2"), "store": ("s2",)})
        with self.assertRaises(TimeoutError):
            view.advance()
        self.assertEqual(view.next_sequence, directory._next)

    def test_long_route_publishes_and_keeps_compatibility(self):
        directory = long_directory()
        view = directory.watch()
        plan = directory.plan_protocol_rollout(
            LONG_TARGETS, {"app": ("t1", "t2"), "store": ("s2",)})
        for service, endpoints in plan:
            directory.replace(service, endpoints)
            self.assertFalse(view.advance().resynced)
        self.assertEqual(view.resolve("app"), (LONG_APP_V2,))
        self.assertEqual(view.resolve("store"), (LONG_STORE_NEW,))


class ActualRolloutTest(TestCase):
    def _live_catalog(self):
        servers = {}

        def add(name):
            manager = backend(name)
            endpoint = manager.__enter__()
            servers[name] = (manager, endpoint)
            return endpoint

        av1, abridge, av2 = add("app-v1"), add("app-bridge"), add("app-v2")
        sv1, sbridge, sv2 = add("store-v1"), add("store-bridge"), add("store-v2")
        cat = {
            "app": {
                "v1": ((av1,), ("v1",), {"store": ("v1",)}),
                "bridge": ((abridge,), ("v1", "v2"), {"store": ("v1", "v2")}),
                "v2": ((av2,), ("v2",), {"store": ("v2",)}),
            },
            "store": {
                "v1": ((sv1,), ("v1",), {}),
                "bridge": ((sbridge,), ("v1", "v2"), {}),
                "v2": ((sv2,), ("v2",), {}),
            },
        }
        named = {av1: "app-v1", abridge: "app-bridge", av2: "app-v2",
                 sv1: "store-v1", sbridge: "store-bridge", sv2: "store-v2"}
        return servers, cat, named, (av1, sv1, av2, sv2)

    def test_both_bridge_routes_through_live_views(self):
        servers, cat, named, (av1, sv1, av2, sv2) = self._live_catalog()
        try:
            for bridge_first in ("app", "store"):
                directory = Directory(capacity=8)
                directory.replace("app", (av1,))
                directory.replace("store", (sv1,))
                directory.configure_protocol_rollout(cat)
                view = directory.watch()
                plan = directory.plan_protocol_rollout(
                    {"app": "v2", "store": "v2"}, {bridge_first: ("bridge",)})
                self.assertEqual(len(plan), 3)
                for service, endpoints in plan:
                    self.assertIsNotNone(directory.replace(service, endpoints))
                    self.assertEqual(view.advance(), Progress(view.next_sequence, False))
                    app_ep = view.resolve("app")[0]
                    store_ep = view.resolve("store")[0]
                    self.assertEqual(dial(app_ep), (200, named[app_ep]))
                    self.assertEqual(dial(store_ep), (200, named[store_ep]))
                self.assertEqual(view.resolve("app"), (av2,))
                self.assertEqual(view.resolve("store"), (sv2,))
                view.close()
                directory.close()
        finally:
            for manager, _endpoint in servers.values():
                manager.__exit__(None, None, None)

    def test_store_direct_v2_outcome_depends_on_current_app(self):
        directory = fresh_directory()
        with self.assertRaises(IncompatibleRollout):
            directory.replace("store", (STORE_V2,))
        self.assertEqual(directory.watch().resolve("store"), (STORE_V1,))
        directory.replace("app", (APP_BRIDGE,))
        self.assertIsNotNone(directory.replace("store", (STORE_V2,)))
        self.assertEqual(directory.watch().resolve("store"), (STORE_V2,))

    def test_rejected_replacement_leaves_cut_sequence_and_events_intact(self):
        directory = fresh_directory()
        view = directory.watch()
        next_before = directory._next
        with self.assertRaises(IncompatibleRollout):
            directory.replace("store", (STORE_V2,))
        self.assertEqual(directory._next, next_before)
        self.assertEqual(view.resolve("store"), (STORE_V1,))
        with self.assertRaises(TimeoutError):
            view.advance()
        self.assertEqual(view.next_sequence, 2)

    def test_unmanaged_service_keeps_old_behaviour(self):
        directory = fresh_directory()
        self.assertEqual(directory.replace("assets", (APP_V1,)), 2)
        self.assertEqual(directory.replace("assets", ()), 3)

    def test_unchanged_known_version_returns_none_without_an_event(self):
        directory = fresh_directory()
        view = directory.watch()
        self.assertIsNone(directory.replace("app", (APP_V1,)))
        with self.assertRaises(TimeoutError):
            view.advance()

    def test_new_interfaces_raise_closed_after_shutdown(self):
        directory = fresh_directory()
        directory.close()
        with self.assertRaises(Closed):
            directory.configure_protocol_rollout(catalog())
        with self.assertRaises(Closed):
            directory.plan_protocol_rollout({"app": "v2"})
        with self.assertRaises(Closed):
            directory.replace("app", (APP_V1,))


class ConcurrencyTest(TestCase):
    def test_opposing_publishers_never_land_an_incompatible_pair(self):
        # Start on compatible bridges: each side may leave alone, but not both.
        for _attempt in range(20):
            directory = Directory(capacity=8)
            directory.replace("app", (APP_BRIDGE,))
            directory.replace("store", (STORE_BRIDGE,))
            directory.configure_protocol_rollout(catalog())
            start = directory.watch()
            ready = Barrier(2)

            def publish(service, endpoints):
                ready.wait(5)
                try:
                    return directory.replace(service, endpoints)
                except IncompatibleRollout:
                    return None

            with ThreadPoolExecutor(max_workers=2) as pool:
                app_future = pool.submit(publish, "app", (APP_V1,))
                store_future = pool.submit(publish, "store", (STORE_V2,))
                outcomes = {"app": app_future.result(5), "store": store_future.result(5)}
            winners = [name for name, result in outcomes.items() if result is not None]
            self.assertEqual(len(winners), 1)
            winner = winners[0]
            expected_app = (APP_V1,) if winner == "app" else (APP_BRIDGE,)
            expected_store = (STORE_V2,) if winner == "store" else (STORE_BRIDGE,)
            self.assertEqual(start.resolve("app"), (APP_BRIDGE,))
            self.assertEqual(start.resolve("store"), (STORE_BRIDGE,))
            # Both views see exactly one new event and the same final pairing.
            self.assertEqual(start.advance(), Progress(3, False))
            with self.assertRaises(TimeoutError):
                start.advance()
            self.assertEqual(start.next_sequence, 3)
            self.assertEqual(start.resolve("app"), expected_app)
            self.assertEqual(start.resolve("store"), expected_store)
            fresh = directory.watch()
            self.assertEqual(fresh.next_sequence, 3)
            self.assertEqual(fresh.resolve("app"), expected_app)
            self.assertEqual(fresh.resolve("store"), expected_store)
            # The final pairing remains protocol compatible.
            app_name = "v1" if winner == "app" else "bridge"
            store_name = "v2" if winner == "store" else "bridge"
            protocols = {"v1": {"v1"}, "bridge": {"v1", "v2"}, "v2": {"v2"}}
            self.assertTrue(protocols[app_name] & protocols[store_name])
            start.close()
            fresh.close()
            directory.close()
