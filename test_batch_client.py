"""Acceptance and regression tests for the caller-side BatchClient."""
from unittest import TestCase

from batch_client import BatchClient, OutcomeUnknown
from directory import Directory, Endpoint


A = Endpoint("127.0.0.1", 8001)
B = Endpoint("127.0.0.1", 8002)


class RecordingTransport:
    """Deterministic, in-memory transport with scriptable uncertainty.

    Every real submission appends (batch_id, endpoint, items) to ``attempts``.
    Ids placed in ``unknown_on_submit`` make the first (and every) submit raise
    OutcomeUnknown and lookup return None until the test flips them via
    ``make_known``.  Idempotent re-drives of a decided batch return the recorded
    conclusion with the identical arguments and never execute twice.
    """

    def __init__(self):
        self.attempts = []
        self._known = {}              # batch id -> True/False
        self._unknown = set()         # batch ids currently undecidable
        self._unknown_on_submit = set()  # ids that go unknown on first submit
        self._fail_ids = set()        # ids whose next submit returns False
        self._last_seen = {}          # batch id -> (endpoint, items)
        self._fail_new = False

    def fail_next_new(self):
        self._fail_new = True

    def fail_new_id(self, batch_id):
        """The next submission of this id is a definite reject (False)."""
        self._fail_ids.add(batch_id)

    def lose_receipt(self, batch_id):
        """The next submission of this id is accepted-or-not but undecidable."""
        self._unknown_on_submit.add(batch_id)

    def submit(self, batch_id, endpoint, items):
        if batch_id in self._known:
            assert self._last_seen[batch_id] == (endpoint, items), \
                "re-drive must keep the identical address and items"
            return self._known[batch_id]
        if batch_id in self._unknown or batch_id in self._unknown_on_submit:
            self._unknown_on_submit.discard(batch_id)
            self._unknown.add(batch_id)
            self._last_seen[batch_id] = (endpoint, items)
            raise OutcomeUnknown("receipt lost")
        if batch_id in self._fail_ids or self._fail_new:
            self._fail_ids.discard(batch_id)
            self._fail_new = False
            self._known[batch_id] = False
            self._last_seen[batch_id] = (endpoint, items)
            return False
        self.attempts.append((batch_id, endpoint, items))
        self._known[batch_id] = True
        self._last_seen[batch_id] = (endpoint, items)
        return True

    def lookup(self, batch_id):
        if batch_id in self._unknown:
            return None
        return self._known.get(batch_id)

    # ------------------------------------------------------------- test hooks

    def make_known(self, batch_id, accepted):
        self._unknown.discard(batch_id)
        self._unknown_on_submit.discard(batch_id)
        self._known[batch_id] = accepted

    def batch_for(self, endpoint):
        matches = [record for record in self.attempts if record[1] == endpoint]
        assert matches, f"no executed batch was ever sent to {endpoint}"
        return matches[-1]

    def last_seen(self, batch_id):
        """The (endpoint, items) of a batch, including an undecided one."""
        return self._last_seen[batch_id]


def ids_of(items):
    return tuple(request_id for request_id, _payload in items)


def advance_until(view, service, expected):
    while view.resolve(service) != expected:
        view.advance()


class PublishedCutTest(TestCase):
    """The end-to-end cut scenario driven through a real Directory.watch()."""

    def _build(self):
        directory = Directory(capacity=8)
        # Publish A first so jia's slow view contains only A.
        directory.replace("svc", (A,))
        jia = directory.watch()
        directory.replace("svc", (A, B))
        yi = directory.watch()
        bing = directory.watch()
        transport = RecordingTransport()
        client = BatchClient(
            "svc",
            {"jia": jia, "yi": yi, "bing": bing},
            {A: 1, B: 2},
            transport,
        )
        return directory, jia, yi, bing, transport, client

    def test_first_round_matches_the_published_cuts(self):
        _directory, jia, yi, bing, transport, client = self._build()

        yi_q = client.enqueue("yi", "yi-1")
        bing_q = client.enqueue("bing", "bing-1")
        jia_q1 = client.enqueue("jia", "jia-1")
        jia_q2 = client.enqueue("jia", "jia-2")

        client.dispatch()

        a_id, _a_ep, a_items = transport.batch_for(A)
        b_id, _b_ep, b_items = transport.batch_for(B)
        self.assertEqual(ids_of(a_items), (jia_q1,))
        self.assertEqual(set(ids_of(b_items)), {yi_q, bing_q})
        self.assertEqual({ep for _bid, ep, _ in transport.attempts}, {A, B})
        # jia's second query stays queued behind the first unsettled one.
        self.assertIsNone(client.take(jia_q2))

    def test_cancel_before_send_lets_the_next_query_go(self):
        _directory, jia, yi, bing, transport, client = self._build()

        jia_q1 = client.enqueue("jia", "jia-1")
        jia_q2 = client.enqueue("jia", "jia-2")
        self.assertTrue(client.cancel(jia_q1))
        self.assertFalse(client.cancel(jia_q1))   # already cancelled
        self.assertFalse(client.cancel(999))      # unknown id
        self.assertEqual(client.take(jia_q1), ("cancelled", None))
        self.assertIsNone(client.take(jia_q1))    # already taken

        client.dispatch()
        _id, _ep, a_items = transport.batch_for(A)
        self.assertEqual(ids_of(a_items), (jia_q2,))

    def test_cancel_after_unknown_receipt_blocks_until_the_batch_resolves(self):
        _directory, jia, yi, bing, transport, client = self._build()

        yi_q = client.enqueue("yi", "yi-1")
        bing_q = client.enqueue("bing", "bing-1")
        jia_q1 = client.enqueue("jia", "jia-1")
        jia_q2 = client.enqueue("jia", "jia-2")
        # Batch id 1 is allocated to the first planned endpoint (A, limit 1);
        # make its receipt lost before the round is sent.
        transport.lose_receipt(1)
        client.dispatch()

        a_id = 1
        a_endpoint, a_items = transport.last_seen(a_id)
        self.assertEqual(a_endpoint, A)
        self.assertEqual(ids_of(a_items), (jia_q1,))
        b_id, _ep, _items = transport.batch_for(B)

        # Cancelling after the attempt: the terminal is available at once,
        # but capacity and caller ordering stay held.
        self.assertTrue(client.cancel(jia_q1))
        self.assertEqual(client.take(jia_q1), ("cancelled", None))

        # Nothing new can go to A while the batch is unresolved.  The unknown
        # batch was attempted once (last_seen) but never confirmed executed.
        client.dispatch()
        self.assertEqual(len(transport.attempts), 1)  # only B executed
        self.assertIsNone(client.take(jia_q2))

        # yi and bing still receive their own B results correctly.
        self.assertTrue(client.settle(
            b_id, {yi_q: "yi-result", bing_q: "bing-result"}))
        self.assertEqual(client.take(yi_q), ("ok", "yi-result"))
        self.assertEqual(client.take(bing_q), ("ok", "bing-result"))
        self.assertIsNone(client.take(yi_q))  # terminals deliver at most once

        # With B free, yi and bing send their next batches while A is stuck.
        yi_q2 = client.enqueue("yi", "yi-2")
        bing_q2 = client.enqueue("bing", "bing-2")
        client.dispatch()
        b_attempts = [record for record in transport.attempts
                      if record[1] == B and record[0] != b_id]
        self.assertEqual(len(b_attempts), 1)
        self.assertEqual(set(ids_of(b_attempts[0][2])), {yi_q2, bing_q2})

        # A becomes queryable as accepted: still no second jia query early,
        # and no second attempt was ever made against A.
        transport.make_known(a_id, True)
        client.dispatch()
        self.assertEqual(
            [ep for _bid, ep, _ in transport.attempts].count(A), 0)

        # The original batch's settlement must include the cancelled member.
        self.assertTrue(client.settle(a_id, {jia_q1: "late"}))
        self.assertIsNone(client.take(jia_q1))  # already taken as cancelled
        client.dispatch()
        # jia's second query now goes to A as a brand-new executed batch.
        a_attempts = [record for record in transport.attempts if record[1] == A]
        self.assertEqual(len(a_attempts), 1)
        self.assertEqual(ids_of(a_attempts[0][2]), (jia_q2,))
        self.assertNotEqual(a_attempts[0][0], a_id)

        # The late/duplicate settlement cannot touch the new batch.
        self.assertFalse(client.settle(a_id, {jia_q1: "late"}))

    def test_views_keep_their_original_cuts_throughout(self):
        directory, jia, yi, bing, transport, client = self._build()
        client.enqueue("jia", "q")
        client.enqueue("yi", "q")
        client.enqueue("bing", "q")
        client.dispatch()
        self.assertEqual(jia.resolve("svc"), (A,))
        self.assertEqual(yi.resolve("svc"), (A, B))
        self.assertEqual(bing.resolve("svc"), (A, B))
        # The client never advances caller views itself.
        directory.replace("svc", (B,))
        self.assertEqual(jia.resolve("svc"), (A,))
        self.assertEqual(yi.resolve("svc"), (A, B))
        self.assertEqual(bing.resolve("svc"), (A, B))
        # The directory cursor each view reads from is untouched.
        before = {name: view.next_sequence for name, view in
                  (("jia", jia), ("yi", yi), ("bing", bing))}
        client.dispatch()
        after = {name: view.next_sequence for name, view in
                 (("jia", jia), ("yi", yi), ("bing", bing))}
        self.assertEqual(before, after)


class RecoveryTest(TestCase):
    def _client_with_cut(self, endpoints, limits=None):
        directory = Directory(capacity=8)
        views = {name: directory.watch()
                 for name in ("jia", "yi", "bing")}
        transport = RecordingTransport()
        client = BatchClient(
            "svc", views,
            limits if limits is not None else {A: 1, B: 2},
            transport,
        )
        directory.replace("svc", endpoints)
        for view in views.values():
            advance_until(view, "svc", endpoints)
        return directory, views, transport, client

    def test_explicit_reject_releases_capacity_and_requeues_in_order(self):
        _dir, views, transport, client = self._client_with_cut((A, B))
        q1 = client.enqueue("jia", "one")
        q2 = client.enqueue("jia", "two")
        transport.fail_next_new()
        client.dispatch()
        self.assertEqual(transport.attempts, [])  # the reject was never executed
        self.assertIsNone(client.take(q1))

        client.dispatch()
        self.assertEqual(len(transport.attempts), 1)
        self.assertEqual(ids_of(transport.attempts[0][2]), (q1,))
        batch_id = transport.attempts[0][0]
        self.assertTrue(client.settle(batch_id, {q1: "r1"}))
        self.assertEqual(client.take(q1), ("ok", "r1"))
        client.dispatch()
        self.assertEqual(ids_of(transport.attempts[1][2]), (q2,))

    def test_unknown_then_proven_rejected_frees_the_slot(self):
        _dir, views, transport, client = self._client_with_cut((A,))
        q1 = client.enqueue("jia", "one")
        q2 = client.enqueue("jia", "two")
        transport.lose_receipt(1)
        client.dispatch()
        batch_id = 1
        self.assertEqual(transport.attempts, [])  # undecided: nothing executed

        # Repeated dispatches cannot re-send under a new id while undecided.
        client.dispatch()
        client.dispatch()
        self.assertEqual(len(transport.attempts), 0)
        self.assertIsNone(client.take(q2))

        transport.make_known(batch_id, False)
        client.dispatch()
        # The reject frees the slot; this round cannot re-send, so the new
        # attempt appears on the next dispatch with a fresh batch id.
        client.dispatch()
        self.assertEqual(len(transport.attempts), 1)
        new_id = transport.attempts[0][0]
        self.assertNotEqual(new_id, batch_id)
        self.assertEqual(ids_of(transport.attempts[0][2]), (q1,))
        self.assertTrue(client.settle(new_id, {q1: "r1"}))
        self.assertEqual(client.take(q1), ("ok", "r1"))

    def test_idempotent_redrive_returns_recorded_conclusion_without_reexecute(self):
        _dir, views, transport, client = self._client_with_cut((A,))
        q1 = client.enqueue("jia", "one")
        transport.lose_receipt(1)
        client.dispatch()
        batch_id = 1
        endpoint, items = A, ((q1, "one"),)
        self.assertIsNone(transport.lookup(batch_id))
        client.dispatch()  # must not raise, must not create another attempt
        self.assertEqual(len(transport.attempts), 0)

        transport.make_known(batch_id, True)
        # The idempotent re-drive returns the old verdict, no new execution.
        self.assertIs(transport.submit(batch_id, endpoint, items), True)
        self.assertEqual(len(transport.attempts), 0)
        client.dispatch()
        self.assertTrue(client.settle(batch_id, {q1: "r"}))
        self.assertEqual(client.take(q1), ("ok", "r"))

    def test_cancelled_flying_request_dropped_when_batch_is_rejected(self):
        _dir, views, transport, client = self._client_with_cut((A,))
        q1 = client.enqueue("jia", "one")
        q2 = client.enqueue("jia", "two")
        transport.lose_receipt(1)
        client.dispatch()
        self.assertTrue(client.cancel(q1))
        self.assertEqual(client.take(q1), ("cancelled", None))
        # While unresolved the cancelled request still bills A and blocks q2.
        client.dispatch()
        self.assertEqual(len(transport.attempts), 0)
        self.assertIsNone(client.take(q2))

        transport.make_known(1, False)
        client.dispatch()  # proves the reject, releases the slot
        client.dispatch()  # q2 is now the head and goes out
        self.assertEqual(len(transport.attempts), 1)
        self.assertEqual(ids_of(transport.attempts[0][2]), (q2,))
        # The cancelled request is never resurrected.
        self.assertIsNone(client.take(q1))

    def test_round_sends_the_maximum_feasible_number_of_requests(self):
        directory = Directory(capacity=8)
        views = {f"j{i}": directory.watch() for i in range(4)}
        transport = RecordingTransport()
        # Three addresses with capacities 1, 1, 2: four heads can all go.
        client = BatchClient(
            "svc", views, {A: 1, B: 1, Endpoint("127.0.0.1", 8003): 2},
            transport,
        )
        directory.replace("svc", (A, B, Endpoint("127.0.0.1", 8003)))
        for view in views.values():
            advance_until(view, "svc",
                          (A, B, Endpoint("127.0.0.1", 8003)))
        ids = [client.enqueue(f"j{i}", f"q{i}") for i in range(4)]
        client.dispatch()
        sent = {rid for _bid, _ep, items in transport.attempts
                for rid, _payload in items}
        self.assertEqual(sent, set(ids))

        # Once an address saturates, a head with no feasible endpoint waits
        # rather than displacing an already-planned request.
        q5 = client.enqueue("j0", "q5")
        client.dispatch()
        # j0 still has an unsettled flying request, so its next one is blocked.
        self.assertIsNone(client.take(q5))


class SettlementValidationTest(TestCase):
    def _one_batch(self):
        directory = Directory(capacity=8)
        views = {"jia": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 1}, transport)
        directory.replace("svc", (A,))
        for view in views.values():
            advance_until(view, "svc", (A,))
        q1 = client.enqueue("jia", "one")
        q2 = client.enqueue("jia", "two")
        client.dispatch()
        batch_id, _ep, items = transport.batch_for(A)
        self_local = (q1,)
        assert ids_of(items) == self_local
        return client, transport, batch_id, q1, q2

    def test_incomplete_or_extra_settlement_changes_nothing(self):
        client, transport, batch_id, q1, q2 = self._one_batch()
        self.assertFalse(client.settle(batch_id, {q2: "wrong-member"}))
        self.assertFalse(client.settle(batch_id,
                                       {q1: "x", 999: "z"}))
        self.assertFalse(client.settle(123456, {q1: "x"}))
        self.assertFalse(client.settle(batch_id, {q1: 1}))
        # Nothing delivered, capacity still held.
        self.assertIsNone(client.take(q1))
        client.dispatch()
        self.assertEqual(len(transport.attempts), 1)
        # The valid settlement now succeeds and frees everything.
        self.assertTrue(client.settle(batch_id, {q1: "x"}))
        self.assertEqual(client.take(q1), ("ok", "x"))

    def test_settlement_must_include_cancelled_members(self):
        client, transport, batch_id, q1, q2 = self._one_batch()
        self.assertTrue(client.cancel(q1))
        self.assertEqual(client.take(q1), ("cancelled", None))
        # A settlement omitting the cancelled member is rejected wholesale.
        self.assertFalse(client.settle(batch_id, {}))
        self.assertFalse(client.settle(batch_id, {q2: "not-a-member"}))
        self.assertTrue(client.settle(batch_id, {q1: "ignored"}))
        self.assertIsNone(client.take(q1))
        # The caller's next request is now unblocked.
        client.dispatch()
        self.assertEqual(ids_of(transport.attempts[1][2]), (q2,))

    def test_capacity_accounting_returns_to_zero_after_full_lifecycle(self):
        directory = Directory(capacity=8)
        views = {"j1": directory.watch(), "j2": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 2}, transport)
        directory.replace("svc", (A,))
        for view in views.values():
            advance_until(view, "svc", (A,))
        ids = [client.enqueue(name, "q") for name in ("j1", "j2")]
        client.dispatch()
        self.assertEqual(client._outstanding[A], 2)
        batch_id = transport.attempts[0][0]
        self.assertTrue(client.settle(
            batch_id, {request_id: f"v{request_id}" for request_id in ids}))
        self.assertEqual(client._outstanding[A], 0)
        for request_id in ids:
            self.assertEqual(client.take(request_id),
                             ("ok", f"v{request_id}"))
        # The endpoint can immediately serve a fresh batch at full capacity.
        more = [client.enqueue(name, "q2") for name in ("j1", "j2")]
        client.dispatch()
        self.assertEqual(set(ids_of(transport.attempts[1][2])), set(more))


class AllocationTest(TestCase):
    def test_flexible_head_cannot_starve_a_single_endpoint_head(self):
        directory = Directory(capacity=8)
        views = {name: directory.watch() for name in ("jia", "yi", "bing")}
        transport = RecordingTransport()
        # A holds one, B holds two: all three requests can be served.
        client = BatchClient("svc", views, {A: 1, B: 2}, transport)
        directory.replace("svc", (A, B))
        for view in views.values():
            advance_until(view, "svc", (A, B))
        # jia drops B and keeps only A; yi/bing retain both addresses.
        directory.replace("svc", (A,))
        views["jia"].advance()
        self.assertEqual(views["jia"].resolve("svc"), (A,))
        self.assertEqual(views["yi"].resolve("svc"), (A, B))

        q_jia = client.enqueue("jia", "only-a")
        q_yi = client.enqueue("yi", "flex")
        q_bing = client.enqueue("bing", "flex")
        client.dispatch()

        sent = {record[1]: ids_of(record[2]) for record in transport.attempts}
        # The constrained head wins its only endpoint; the flexible ones
        # settle on B rather than starving jia.
        self.assertEqual(sent[A], (q_jia,))
        self.assertEqual(set(sent[B]), {q_yi, q_bing})
        self.assertEqual(len(transport.attempts), 2)

    def test_flexible_head_yields_the_last_forced_slot(self):
        directory = Directory(capacity=8)
        views = {name: directory.watch() for name in ("jia", "yi")}
        transport = RecordingTransport()
        # Only one slot per address: at most two requests can go out.
        client = BatchClient("svc", views, {A: 1, B: 1}, transport)
        directory.replace("svc", (A, B))
        for view in views.values():
            advance_until(view, "svc", (A, B))
        directory.replace("svc", (A,))
        views["jia"].advance()

        q_jia = client.enqueue("jia", "only-a")
        q_yi = client.enqueue("yi", "flex")
        client.dispatch()
        sent = {record[1]: ids_of(record[2]) for record in transport.attempts}
        self.assertEqual(sent[A], (q_jia,))
        self.assertEqual(sent[B], (q_yi,))

    def test_address_requires_both_view_membership_and_limit(self):
        directory = Directory(capacity=8)
        views = {"jia": directory.watch(), "yi": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 1}, transport)
        directory.replace("svc", (A, B))
        for view in views.values():
            advance_until(view, "svc", (A, B))
        q1 = client.enqueue("jia", "x")
        client.dispatch()
        # B is visible but has no configured limit, so only A is used.
        self.assertEqual([ep for _bid, ep, _ in transport.attempts], [A])
        self.assertTrue(client.settle(transport.attempts[0][0], {q1: "r"}))
        self.assertEqual(client.take(q1), ("ok", "r"))

        # An address in limits but gone from the view cannot serve.
        directory.replace("svc", (B,))
        for view in views.values():
            advance_until(view, "svc", (B,))
        q2 = client.enqueue("yi", "y")
        client.dispatch()
        self.assertEqual(len(transport.attempts), 1)  # nothing new sent
        self.assertIsNone(client.take(q2))

    def test_one_batch_per_address_per_round_even_with_spare_capacity(self):
        directory = Directory(capacity=8)
        views = {name: directory.watch() for name in ("j1", "j2", "j3")}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 4}, transport)
        directory.replace("svc", (A,))
        for view in views.values():
            advance_until(view, "svc", (A,))
        qs = [client.enqueue(name, "q") for name in ("j1", "j2", "j3")]
        client.dispatch()
        self.assertEqual(len(transport.attempts), 1)
        self.assertEqual(set(ids_of(transport.attempts[0][2])), set(qs))

    def test_maximal_round_when_backend_rejects_part_of_the_plan(self):
        directory = Directory(capacity=8)
        views = {"j1": directory.watch(), "j2": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 1, B: 1}, transport)
        # Each caller resolves a different single endpoint, so the round must
        # produce two independent batches rather than one merged batch.
        directory.replace("svc", (A,))
        views["j1"].advance()
        directory.replace("svc", (A, B))
        directory.replace("svc", (B,))
        advance_until(views["j2"], "svc", (B,))
        self.assertEqual(views["j1"].resolve("svc"), (A,))
        self.assertEqual(views["j2"].resolve("svc"), (B,))

        q1 = client.enqueue("j1", "a")
        q2 = client.enqueue("j2", "b")
        # Reject the first batch the round sends; the other endpoint's batch
        # is planned independently and must still be accepted this round.
        transport.fail_next_new()
        client.dispatch()
        self.assertEqual(len(transport.attempts), 1)
        accepted_endpoint = transport.attempts[0][1]
        rejected_endpoint = A if accepted_endpoint == B else B
        sent_id = ids_of(transport.attempts[0][2])[0]
        blocked = q2 if sent_id == q1 else q1
        self.assertIsNone(client.take(blocked))
        # Next round the rejected head goes out to its original endpoint
        # without changing submission order.
        client.dispatch()
        self.assertEqual(len(transport.attempts), 2)
        self.assertEqual(transport.attempts[1][1], rejected_endpoint)
        self.assertEqual(ids_of(transport.attempts[1][2]), (blocked,))


class BoundsAndSemanticsTest(TestCase):
    def test_constructor_validation(self):
        directory = Directory(capacity=4)
        view = directory.watch()
        transport = RecordingTransport()
        with self.assertRaises(ValueError):
            BatchClient("", {"jia": view}, {A: 1}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {}, {A: 1}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {f"c{i}": view for i in range(9)},
                        {A: 1}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {"jia": "not-a-view"}, {A: 1}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {"jia": view}, {A: 0}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {"jia": view}, {A: 9}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {"jia": view}, {A: True}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {"jia": view},
                        {Endpoint("h", p): 1 for p in range(1, 10)}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {"jia": view}, {("x",): 1}, transport)
        with self.assertRaises(ValueError):
            BatchClient("svc", {"jia": view}, {A: 1}, object())

    def test_enqueue_bounds_and_unknown_caller(self):
        directory = Directory(capacity=4)
        views = {"jia": directory.watch()}
        client = BatchClient("svc", views, {A: 1}, RecordingTransport())
        with self.assertRaises(ValueError):
            client.enqueue("nobody", "q")
        with self.assertRaises(ValueError):
            client.enqueue("jia", 123)
        ids = [client.enqueue("jia", str(i)) for i in range(64)]
        self.assertEqual(len(set(ids)), 64)
        with self.assertRaises(OverflowError):
            client.enqueue("jia", "65th")
        # Taking a cancelled terminal frees the caller-side mailbox slot.
        self.assertTrue(client.cancel(ids[0]))
        self.assertEqual(client.take(ids[0]), ("cancelled", None))
        new_id = client.enqueue("jia", "replacement")
        self.assertNotIn(new_id, ids)

    def test_cancel_and_take_state_machine(self):
        directory = Directory(capacity=4)
        views = {"jia": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 1}, transport)
        directory.replace("svc", (A,))
        advance_until(views["jia"], "svc", (A,))
        q1 = client.enqueue("jia", "one")
        q2 = client.enqueue("jia", "two")
        self.assertIsNone(client.take(q1))
        transport.lose_receipt(1)
        client.dispatch()
        batch_id = 1
        # While acceptance is unknown the flying request can still be
        # cancelled; once the verdict is accepted it cannot.
        self.assertTrue(client.cancel(q1))
        self.assertEqual(client.take(q1), ("cancelled", None))
        transport.make_known(batch_id, True)
        client.dispatch()
        self.assertFalse(client.cancel(q1))
        self.assertTrue(client.settle(batch_id, {q1: "r1"}))
        # The next queued request is now the head.
        client.dispatch()
        self.assertEqual(ids_of(transport.attempts[0][2]), (q2,))

    def test_cancelled_flying_request_still_settles_and_releases_once(self):
        directory = Directory(capacity=4)
        views = {"jia": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 1}, transport)
        directory.replace("svc", (A,))
        advance_until(views["jia"], "svc", (A,))
        q1 = client.enqueue("jia", "one")
        q2 = client.enqueue("jia", "two")
        transport.lose_receipt(1)
        client.dispatch()
        batch_id = 1
        self.assertTrue(client.cancel(q1))
        self.assertEqual(client.take(q1), ("cancelled", None))
        # Capacity held: q2 cannot be sent while the batch is unresolved.
        client.dispatch()
        self.assertEqual(len(transport.attempts), 0)
        transport.make_known(batch_id, True)
        client.dispatch()
        self.assertTrue(client.settle(batch_id, {q1: "v"}))
        client.dispatch()
        self.assertEqual(ids_of(transport.attempts[0][2]), (q2,))

    def test_terminal_delivery_is_at_most_once_under_duplicate_settlement(self):
        directory = Directory(capacity=4)
        views = {"j1": directory.watch(), "j2": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 2}, transport)
        directory.replace("svc", (A,))
        for view in views.values():
            advance_until(view, "svc", (A,))
        q1 = client.enqueue("j1", "one")
        q2 = client.enqueue("j2", "two")
        client.dispatch()
        # Both share endpoint A and therefore share one batch.
        first_batch, _ep, first_items = transport.batch_for(A)
        self.assertEqual(set(ids_of(first_items)), {q1, q2})
        values = {q1: "x", q2: "y"}
        self.assertTrue(client.settle(first_batch, values))
        self.assertFalse(client.settle(first_batch, values))
        self.assertEqual(client.take(q1), ("ok", "x"))
        self.assertEqual(client.take(q2), ("ok", "y"))

        # A fresh batch on the same address cannot be touched by the old settle.
        q3 = client.enqueue("j1", "three")
        client.dispatch()
        second_batch, _ep, _items = transport.batch_for(A)
        self.assertNotEqual(second_batch, first_batch)
        self.assertFalse(client.settle(first_batch, values))
        self.assertIsNone(client.take(q3))
        self.assertTrue(client.settle(second_batch, {q3: "z"}))
        self.assertEqual(client.take(q3), ("ok", "z"))

    def test_batch_ids_are_unique_on_this_transport(self):
        directory = Directory(capacity=4)
        views = {"jia": directory.watch(), "yi": directory.watch()}
        transport = RecordingTransport()
        client = BatchClient("svc", views, {A: 1, B: 2}, transport)
        directory.replace("svc", (A, B))
        for view in views.values():
            advance_until(view, "svc", (A, B))
        client.enqueue("jia", "a")
        client.enqueue("yi", "b")
        client.dispatch()
        ids = [bid for bid, _ep, _items in transport.attempts]
        self.assertEqual(len(ids), len(set(ids)))
