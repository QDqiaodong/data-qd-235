"""Acceptance tests for the protected staged workflow router.

Scenarios 1 and 2 use a fixed three-service topology and scripted callbacks.
Scenario 3 drives the same chain against real loopback HTTP servers.  No test
depends on real faults, health checks or sleeping to choose an address.
"""

import unittest
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from directory import Directory, Endpoint
from workflow_router import (
    DomainUnavailable,
    NoProtectedWorkflow,
    PreflightResult,
    WorkflowRouter,
)


# Two fault domains; one address per service in each.
ALPHA = "alpha"
BETA = "beta"

SRC_A = Endpoint("alpha-source", 9101)
SRC_B = Endpoint("beta-source", 9101)
REN_A = Endpoint("alpha-render", 9102)
REN_B = Endpoint("beta-render", 9102)
REG_A = Endpoint("alpha-record", 9103)
REG_B = Endpoint("beta-record", 9103)

DOMAINS = {
    SRC_A: ALPHA, SRC_B: BETA,
    REN_A: ALPHA, REN_B: BETA,
    REG_A: ALPHA, REG_B: BETA,
}

SERVICES = ("source", "render", "record")

# The topology before the extra handoff: each render instance records only to
# the registration instance of its own rack.
BASE_LINKS = (
    ("source", SRC_A, "render", REN_A),
    ("source", SRC_B, "render", REN_A),
    ("source", SRC_B, "render", REN_B),
    ("render", REN_A, "record", REG_A),
    ("render", REN_B, "record", REG_B),
)

# The one added handoff that makes the already-started prefix resumable.
NEW_LINK = ("render", REN_B, "record", REG_A)


def _directory(endpoints_by_service):
    directory = Directory(capacity=8)
    for service, endpoints in endpoints_by_service.items():
        directory.replace(service, endpoints)
    return directory


def _full_directory():
    return _directory({
        "source": (SRC_A, SRC_B),
        "render": (REN_A, REN_B),
        "record": (REG_A, REG_B),
    })


def _surviving_directory(domain):
    addresses = {
        "source": (SRC_A, SRC_B),
        "render": (REN_A, REN_B),
        "record": (REG_A, REG_B),
    }
    kept = {
        service: tuple(endpoint for endpoint in endpoints
                       if DOMAINS[endpoint] != domain)
        for service, endpoints in addresses.items()
    }
    return _directory(kept)


class Harness:
    """Records attempted steps and optionally scripts failures."""

    def __init__(self, fail=None):
        self.calls = []
        self._fail = fail

    def __call__(self, service, endpoint, previous_receipt):
        self.calls.append((service, endpoint, previous_receipt))
        if self._fail is not None:
            self._fail(service, endpoint, previous_receipt)
        return ("%s@%s" % (service, endpoint.host)).encode("ascii")


class TopologyTest(unittest.TestCase):
    def setUp(self):
        self.directory = _full_directory()
        self.view = self.directory.watch()

    def tearDown(self):
        self.view.close()
        self.directory.close()

    def _call_pairs(self, harness):
        return [(service, endpoint) for service, endpoint, _receipt
                in harness.calls]

    def _report(self, router, harness, result=None):
        """Print the operator-visible preflight and actual attempts."""
        print("preflight:", router.preflight(self.view))
        print("attempted:", [(service, DOMAINS[endpoint], endpoint)
                             for service, endpoint, _receipt in harness.calls])
        if result is not None:
            print("succeeded:", [(service, endpoint)
                                 for service, endpoint, _receipt in result])

    # -- 1. cold resilience vs in-flight resilience -------------------------
    def test_domain_loss_without_the_extra_link_blocks_the_start(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS)
        start_cut = self.view.next_sequence
        self.assertEqual(router.preflight(self.view),
                         PreflightResult(True, (), False))
        # Preflight neither dials nor advances the caller's view.
        self.assertEqual(self.view.next_sequence, start_cut)

        # A fresh chain can still be built after either single domain is lost,
        # so neither domain is cold-blocking.
        for domain in (ALPHA, BETA):
            directory = _surviving_directory(domain)
            try:
                view = directory.watch()
                try:
                    cold = router.preflight(view)
                    self.assertTrue(cold.route_exists)
                    # With one domain's addresses gone, that surviving domain
                    # is the one that cold-blocks the chain.
                    self.assertEqual(cold.cold_blocked,
                                     (ALPHA if domain == BETA else BETA,))
                    self.assertFalse(cold.protected)
                finally:
                    view.close()
            finally:
                directory.close()

        # No normal chain preserves an already-completed prefix, so running
        # must refuse before the first call.
        harness = Harness()
        with self.assertRaises(NoProtectedWorkflow):
            router.run(self.view, harness)
        self.assertEqual(harness.calls, [])

    def test_adding_one_link_changes_only_in_flight_resilience(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))
        after = router.preflight(self.view)
        self.assertTrue(after.route_exists)
        self.assertEqual(after.cold_blocked, ())
        self.assertTrue(after.protected)

        # The cold conclusion is unchanged when the new link is present.
        for domain in (ALPHA, BETA):
            directory = _surviving_directory(domain)
            try:
                view = directory.watch()
                try:
                    cold = router.preflight(view)
                    self.assertTrue(cold.route_exists)
                    self.assertEqual(cold.cold_blocked,
                                     (ALPHA if domain == BETA else BETA,))
                    self.assertFalse(cold.protected)
                finally:
                    view.close()
            finally:
                directory.close()

        harness = Harness()
        result = router.run(self.view, harness)
        self._report(router, harness, result)
        self.assertEqual(len(result), 3)
        self.assertEqual([step[0] for step in result], list(SERVICES))
        # The only protected normal chain begins at the beta source.
        self.assertEqual(result[0][1], SRC_B)
        self.assertEqual(harness.calls[0][1], SRC_B)

    # -- 2. controlled domain failures --------------------------------------
    def test_record_domain_loss_swaps_only_the_record_address(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))

        # The protected normal chain records to the alpha instance first; that
        # is the first registration attempt, and its whole domain is lost.
        def fail(service, endpoint, previous_receipt):
            if service == "record" and endpoint == REG_A:
                raise DomainUnavailable("alpha rack down")

        harness = Harness(fail)
        result = router.run(self.view, harness)
        self._report(router, harness, result)

        self.assertEqual(len(result), 3)
        self.assertEqual([endpoint for _s, endpoint, _r in result],
                         [SRC_B, REN_B, REG_B])
        # The first two steps ran exactly once each; only record was retried.
        self.assertEqual(
            self._call_pairs(harness),
            [("source", SRC_B), ("render", REN_B),
             ("record", REG_A), ("record", REG_B)],
        )
        # The successful retry received exactly the same render receipt that
        # the failed attempt saw.
        failed_attempt = harness.calls[2]
        retried = harness.calls[3]
        self.assertIs(retried[2], failed_attempt[2])
        self.assertEqual(retried[2], result[1][2])
        # Every successful receipt belongs to a legal link chain.
        self.assertIn(("source", SRC_B, "render", REN_B), BASE_LINKS)
        self.assertIn(("render", REN_B, "record", REG_B), BASE_LINKS)

    def test_render_domain_loss_keeps_source_receipt_and_moves_to_alpha(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))

        def fail(service, endpoint, previous_receipt):
            if service == "render" and endpoint == REN_B:
                raise DomainUnavailable("beta rack down")

        harness = Harness(fail)
        result = router.run(self.view, harness)
        self._report(router, harness, result)

        self.assertEqual(len(result), 3)
        self.assertEqual([endpoint for _s, endpoint, _r in result],
                         [SRC_B, REN_A, REG_A])
        self.assertEqual(
            self._call_pairs(harness),
            [("source", SRC_B), ("render", REN_B),
             ("render", REN_A), ("record", REG_A)],
        )
        # The source receipt stays with the caller; both render attempts and
        # the final record see that same receipt (or its successor).
        source_receipt = result[0][2]
        self.assertIs(harness.calls[0][2], None)
        self.assertIs(harness.calls[1][2], source_receipt)
        self.assertIs(harness.calls[2][2], source_receipt)
        self.assertIs(harness.calls[3][2], result[1][2])

    def test_unconfirmed_side_effect_is_raised_without_a_second_attempt(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))

        class Ambiguous(Exception):
            pass

        def fail(service, endpoint, previous_receipt):
            if service == "record" and endpoint == REG_A:
                raise Ambiguous("connection broke; outcome unknown")

        harness = Harness(fail)
        with self.assertRaises(Ambiguous) as caught:
            router.run(self.view, harness)
        self._report(router, harness)
        self.assertIs(caught.exception.__class__, Ambiguous)
        # Only one registration attempt happened.
        self.assertEqual(
            self._call_pairs(harness),
            [("source", SRC_B), ("render", REN_B), ("record", REG_A)],
        )

    def test_a_second_domain_loss_is_not_absorbed(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))

        def fail(service, endpoint, previous_receipt):
            if service == "render" and endpoint == REN_B:
                raise DomainUnavailable("beta rack down")
            if service == "record" and endpoint == REG_A:
                raise DomainUnavailable("alpha rack down")

        harness = Harness(fail)
        with self.assertRaises(DomainUnavailable):
            router.run(self.view, harness)
        self._report(router, harness)
        # Source ran once, render was rerouted once, then the second loss
        # surfaced instead of being retried again.
        self.assertEqual(
            self._call_pairs(harness),
            [("source", SRC_B), ("render", REN_B),
             ("render", REN_A), ("record", REG_A)],
        )

    def test_invalid_receipt_stops_without_retry_or_later_steps(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))
        calls = []

        def invoke(service, endpoint, previous_receipt):
            calls.append((service, endpoint))
            return b"x" * 4097

        with self.assertRaises(ValueError):
            router.run(self.view, invoke)
        self.assertEqual(len(calls), 1)

    def test_receipt_that_is_not_bytes_is_rejected(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))
        calls = []

        def invoke(service, endpoint, previous_receipt):
            calls.append((service, endpoint))
            return "not-bytes"

        with self.assertRaises(ValueError):
            router.run(self.view, invoke)
        self.assertEqual(len(calls), 1)

    def test_empty_and_maximum_receipts_are_accepted_verbatim(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))
        produced = []

        def invoke(service, endpoint, previous_receipt):
            receipt = b"" if service == "source" else b"x" * 4096
            produced.append(receipt)
            return receipt

        result = router.run(self.view, invoke)
        self.assertEqual(len(result), 3)
        self.assertEqual([receipt for _s, _e, receipt in result], produced)
        # The caller-visible receipts are the exact objects invoke returned.
        for step, receipt in zip(result, produced):
            self.assertIs(step[2], receipt)

    def test_first_step_domain_loss_keeps_nothing_and_avoids_that_domain(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))

        def fail(service, endpoint, previous_receipt):
            if service == "source" and endpoint == SRC_B:
                raise DomainUnavailable("beta rack down")

        harness = Harness(fail)
        result = router.run(self.view, harness)
        self._report(router, harness, result)
        self.assertEqual([endpoint for _s, endpoint, _r in result],
                         [SRC_A, REN_A, REG_A])
        # No step after the loss touches beta, and the first step's receipt
        # input is still None.
        self.assertEqual(
            self._call_pairs(harness),
            [("source", SRC_B), ("source", SRC_A),
             ("render", REN_A), ("record", REG_A)],
        )
        self.assertIs(harness.calls[0][2], None)
        self.assertIs(harness.calls[1][2], None)

    def test_other_exception_at_an_early_step_stops_the_chain(self):
        router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))

        class Casual(Exception):
            pass

        def fail(service, endpoint, previous_receipt):
            if service == "render":
                raise Casual("no retry")

        harness = Harness(fail)
        with self.assertRaises(Casual) as caught:
            router.run(self.view, harness)
        self._report(router, harness)
        self.assertIs(caught.exception.__class__, Casual)
        self.assertEqual(
            self._call_pairs(harness),
            [("source", SRC_B), ("render", REN_B)])

    def test_unmarked_visible_addresses_are_not_part_of_the_chain(self):
        extra_source = Endpoint("unmarked-source", 9101)
        directory = Directory(capacity=8)
        directory.replace("source", (SRC_A, SRC_B, extra_source))
        directory.replace("render", (REN_A, REN_B))
        directory.replace("record", (REG_A, REG_B))
        view = directory.watch()
        try:
            router = WorkflowRouter(SERVICES, DOMAINS, BASE_LINKS + (NEW_LINK,))
            self.assertTrue(router.preflight(view).route_exists)
            harness = Harness()
            router.run(view, harness)
            self.assertNotIn(extra_source, [e for _s, e in self._call_pairs(harness)])

            # Only the unmarked source is left: no marked first step remains.
            directory.replace("source", (extra_source,))
            view.advance()
            cold = router.preflight(view)
            self.assertFalse(cold.route_exists)
            self.assertFalse(cold.protected)
        finally:
            view.close()
            directory.close()


class RealLoopbackTest(unittest.TestCase):
    """Scenario 3: the protected chain really dials three live backends."""

    def _server(self, name):
        payload = name.encode("ascii")

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/step":
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
        worker.start()
        return server, worker, Endpoint(*server.server_address)

    def setUp(self):
        self.servers = []
        self.real = {}
        for domain, prefix in ((ALPHA, "alpha"), (BETA, "beta")):
            for service in SERVICES:
                server, worker, endpoint = self._server("%s-%s" % (prefix, service))
                self.servers.append((server, worker))
                self.real[(domain, service)] = endpoint

        self.directory = Directory(capacity=8)
        for service in SERVICES:
            self.directory.replace(
                service,
                (self.real[(ALPHA, service)], self.real[(BETA, service)]))
        self.view = self.directory.watch()

        self.domains = {
            self.real[(domain, service)]: domain
            for domain in (ALPHA, BETA) for service in SERVICES
        }

        def addr(domain, service):
            return self.real[(domain, service)]

        self.links = (
            ("source", addr(ALPHA, "source"),
             "render", addr(ALPHA, "render")),
            ("source", addr(BETA, "source"),
             "render", addr(ALPHA, "render")),
            ("source", addr(BETA, "source"),
             "render", addr(BETA, "render")),
            ("render", addr(ALPHA, "render"),
             "record", addr(ALPHA, "record")),
            ("render", addr(BETA, "render"),
             "record", addr(BETA, "record")),
            ("render", addr(BETA, "render"),
             "record", addr(ALPHA, "record")),
        )

    def tearDown(self):
        self.view.close()
        self.directory.close()
        for server, worker in self.servers:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_real_views_replies_and_unchanged_cut(self):
        router = WorkflowRouter(SERVICES, self.domains, self.links)
        preflight = router.preflight(self.view)
        self.assertTrue(preflight.route_exists)
        self.assertEqual(preflight.cold_blocked, ())
        self.assertTrue(preflight.protected)

        start_cut = self.view.next_sequence
        requested = []

        def invoke(service, endpoint, previous_receipt):
            requested.append((service, endpoint))
            connection = HTTPConnection(endpoint.host, endpoint.port, timeout=2)
            try:
                connection.request("GET", "/step")
                response = connection.getresponse()
                body = response.read()
            finally:
                connection.close()
            if response.status != 200:
                raise RuntimeError("backend failed")
            return body

        result = router.run(self.view, invoke)
        self.assertEqual(self.view.next_sequence, start_cut)
        self.assertEqual(len(result), 3)
        self.assertEqual(
            [endpoint for _s, endpoint, _r in result],
            [self.real[(BETA, "source")], self.real[(BETA, "render")],
             self.real[(ALPHA, "record")]])
        self.assertEqual(
            [receipt.decode("ascii") for _s, _e, receipt in result],
            ["beta-source", "beta-render", "alpha-record"])
        self.assertEqual([endpoint for _s, endpoint in requested], [
            self.real[(BETA, "source")], self.real[(BETA, "render")],
            self.real[(ALPHA, "record")]])

    def test_an_address_outside_the_view_is_never_called(self):
        # Configure a handoff to an address that is registered and linked but
        # not visible at this view's cut: it must not become callable.
        late = Endpoint("127.0.0.1", 1)   # never served by any fixture
        domains = dict(self.domains)
        domains[late] = BETA
        router = WorkflowRouter(
            SERVICES,
            domains,
            self.links + (("render", self.real[(BETA, "render")],
                           "record", late),),
        )
        # Publish the extra address after the view's cut; the view has not
        # advanced and still cannot see it.
        self.directory.replace("record", (
            self.real[(ALPHA, "record")], self.real[(BETA, "record")], late))
        self.assertNotIn(late, self.view.resolve("record"))

        start_cut = self.view.next_sequence
        requested = []

        def invoke(service, endpoint, previous_receipt):
            requested.append((service, endpoint))
            return b"receipt"

        router.run(self.view, invoke)
        self.assertNotIn(late, requested)
        self.assertEqual(self.view.next_sequence, start_cut)


class ValidationTest(unittest.TestCase):
    def test_rejects_invalid_service_lists(self):
        for services in (
            ("a",),
            tuple("abcdefg"),
            ("a", "a"),
            ("a", "B"),
            ("a", "bad name"),
            ["a", "b"],
        ):
            with self.assertRaises(ValueError):
                WorkflowRouter(services, {}, ())

    def test_rejects_bad_domain_configuration(self):
        for domains in (
            # seven distinct domains would exceed the cap
            {Endpoint("h%d" % i, 9000): "d%d" % i for i in range(7)},
            # twenty-five addresses would exceed the cap
            {Endpoint("h%d" % i, 9000): "d0" for i in range(25)},
            {SRC_A: "bad domain"},
            {"not-an-endpoint": ALPHA},
            [(SRC_A, ALPHA)],
        ):
            with self.assertRaises(ValueError):
                WorkflowRouter(SERVICES, domains, ())

        # At the cap itself configuration is accepted.
        WorkflowRouter(SERVICES, {Endpoint("ok", 9000): ALPHA}, ())

    def test_rejects_bad_links(self):
        for links in (
            {"a": 1},
            [NEW_LINK],
            (("source", SRC_A, "record", REG_A),),        # not adjacent
            (("source", SRC_A, "render", Endpoint("x", 1)),),  # unregistered
            (("source", SRC_A, "render", REN_A),) * 2,    # duplicate
            (("source", SRC_A, "render"),),               # wrong shape
            (("nope", SRC_A, "render", REN_A),),          # service unknown
            ((REN_A, SRC_A, "render", REN_A),),           # endpoints swapped
        ):
            with self.assertRaises(ValueError):
                WorkflowRouter(SERVICES, DOMAINS, links)

    def test_configuration_is_snapshotted(self):
        domains = dict(DOMAINS)
        router = WorkflowRouter(SERVICES, domains, BASE_LINKS + (NEW_LINK,))
        domains.clear()
        directory = _full_directory()
        view = directory.watch()
        try:
            result = router.run(view, Harness())
            self.assertEqual(len(result), 3)
        finally:
            view.close()
            directory.close()

    def test_more_than_four_visible_endpoints_is_refused_before_invoking(self):
        directory = Directory(capacity=8)
        many = tuple(Endpoint("extra%d" % i, 9300 + i) for i in range(5))
        directory.replace("source", many)
        directory.replace("render", (REN_A,))
        directory.replace("record", (REG_A,))
        view = directory.watch()
        domains = dict(DOMAINS)
        for endpoint in many:
            domains[endpoint] = ALPHA
        router = WorkflowRouter(
            SERVICES, domains,
            (("source", many[0], "render", REN_A),
             ("render", REN_A, "record", REG_A)))
        calls = []
        try:
            with self.assertRaises(ValueError):
                router.run(view, lambda s, e, r: calls.append((s, e)) or b"x")
            self.assertEqual(calls, [])
        finally:
            view.close()
            directory.close()


if __name__ == "__main__":
    unittest.main()
