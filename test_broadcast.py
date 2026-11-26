from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest import TestCase

from broadcast import Broadcast, Closed, Event as Published, Lagged


class BroadcastTest(TestCase):
    def test_shared_ring_independent_cursors_with_explicit_gap(self):
        bus = Broadcast(capacity=2)
        fast, slow = bus.subscribe(), bus.subscribe()
        for i, data in enumerate((b"a", b"b", b"c", b"d")):
            self.assertEqual(bus.publish(data), i)
            self.assertEqual(fast.recv(0), Published(i, data))
        for _ in range(2):
            with self.assertRaises(Lagged) as lag:
                slow.recv(0)
            self.assertEqual((lag.exception.expected, lag.exception.oldest), (0, 2))
        self.assertEqual(slow.skip_lost(), (0, 2))
        self.assertEqual([slow.recv(0), slow.recv(0)], [Published(2, b"c"), Published(3, b"d")])

    def test_resume_and_close_drains_retained_events(self):
        bus = Broadcast(capacity=3)
        bus.publish(b"old")
        latest, replay = bus.subscribe(), bus.subscribe(start=0)
        self.assertEqual(replay.recv(0), Published(0, b"old"))
        with self.assertRaises(TimeoutError):
            latest.recv(0)
        bus.publish(b"new")
        bus.close()
        for sub in (latest, replay):
            self.assertEqual(sub.recv(0), Published(1, b"new"))
            with self.assertRaises(Closed):
                sub.recv(0)
        with self.assertRaises(Closed):
            bus.publish(b"late")
        with self.assertRaises(Closed):
            bus.subscribe()

    def test_publication_and_subscription_close_cross_thread(self):
        bus = Broadcast(capacity=1)
        left, right = bus.subscribe(), bus.subscribe()
        ready = [Event(), Event()]

        def receive(sub, signal):
            signal.set()
            return sub.recv(3)

        with ThreadPoolExecutor(max_workers=2) as pool:
            readers = [pool.submit(receive, s, e) for s, e in zip((left, right), ready)]
            self.assertTrue(all(e.wait(2) for e in ready))
            left.close()
            bus.publish(b"broadcast")
            with self.assertRaises(Closed):
                readers[0].result(4)
            self.assertEqual(readers[1].result(4), Published(0, b"broadcast"))

    def test_refuses_invalid_sequence_and_payload_without_advancing(self):
        bus = Broadcast(capacity=1, max_payload=2)
        for bad in (-1, 1, True):
            with self.assertRaises(ValueError):
                bus.subscribe(start=bad)
        for bad in (b"too long", "str"):
            with self.assertRaises(ValueError):
                bus.publish(bad)
        self.assertEqual(bus.publish(b"ok"), 0)
