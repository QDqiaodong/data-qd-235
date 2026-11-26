from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest import TestCase

from broadcast import Closed
from demo import run_demo
from directory import Directory, Endpoint, Progress


OLD = Endpoint("127.0.0.1", 7001)
NEW = Endpoint("127.0.0.1", 7002)


class DirectoryTest(TestCase):
    def test_exact_snapshot_cut_and_normal_updates(self):
        directory = Directory(capacity=4)
        self.assertEqual(directory.replace("api", (OLD, NEW)), 0)
        view = directory.watch()
        self.assertEqual(view.next_sequence, 1)
        self.assertEqual(view.resolve("api"), (OLD, NEW))
        self.assertEqual(view.resolve("missing"), ())
        self.assertIsNone(directory.replace("api", (OLD, NEW)))
        with self.assertRaises(TimeoutError):
            view.advance()
        self.assertEqual(view.next_sequence, 1)
        self.assertEqual(directory.replace("api", (NEW, OLD)), 1)
        self.assertEqual(view.resolve("api"), (OLD, NEW))
        self.assertEqual(view.advance(), Progress(2, False))
        self.assertEqual(view.resolve("api"), (NEW, OLD))
        self.assertEqual(directory.replace("api", ()), 2)
        self.assertEqual(view.advance(), Progress(3, False))
        self.assertEqual(view.resolve("api"), ())

    def test_overwritten_removal_is_recovered_without_reapplying_suffix(self):
        directory = Directory(capacity=2)
        directory.replace("api", (OLD,))
        fast, slow = directory.watch(), directory.watch()
        directory.replace("api", ())
        self.assertEqual(fast.advance(), Progress(2, False))
        directory.replace("search", (NEW,))
        directory.replace("assets", (OLD,))
        self.assertEqual(slow.advance(), Progress(4, True))
        self.assertEqual(slow.resolve("api"), ())
        self.assertEqual(slow.resolve("search"), (NEW,))
        self.assertEqual(slow.resolve("assets"), (OLD,))
        self.assertEqual(fast.resolve("search"), ())
        self.assertEqual(fast.advance(), Progress(3, False))
        with self.assertRaises(TimeoutError):
            slow.advance()
        self.assertEqual(directory.replace("search", (OLD, NEW)), 4)
        self.assertEqual(slow.advance(), Progress(5, False))
        self.assertEqual(slow.resolve("search"), (OLD, NEW))

    def test_rejected_replacements_do_not_mutate_or_publish(self):
        directory = Directory(capacity=2, max_payload=45)
        directory.replace("api", (OLD,))
        view = directory.watch()
        with self.assertRaises(ValueError):
            directory.replace("Bad Name", (NEW,))
        with self.assertRaises(ValueError):
            directory.replace("api", (OLD, OLD))
        with self.assertRaises(ValueError):
            directory.replace("api", (("localhost", 80),))
        with self.assertRaises(ValueError):
            directory.replace("api", (Endpoint("a" * 50, 80),))
        with self.assertRaises(ValueError):
            Endpoint("localhost", True)
        with self.assertRaises(TimeoutError):
            view.advance()
        self.assertEqual(directory.watch().resolve("api"), (OLD,))
        self.assertEqual(directory.replace("api", (NEW,)), 1)
        self.assertEqual(view.advance(), Progress(2, False))

    def test_concurrent_attachment_and_replacement_share_a_cut(self):
        directory = Directory(capacity=4)
        # Either side may acquire the directory lock first; both cuts are valid.
        with ThreadPoolExecutor(max_workers=1) as pool:
            for i in range(24):
                before = OLD if i % 2 else NEW
                after = NEW if i % 2 else OLD
                barrier = Barrier(2)

                def open_view():
                    barrier.wait(3)
                    return directory.watch()

                reader = pool.submit(open_view)
                barrier.wait(3)
                self.assertEqual(directory.replace("api", (after,)), i)
                view = reader.result(3)
                if view.next_sequence == i:
                    self.assertEqual(view.resolve("api"), () if i == 0 else (before,))
                    self.assertEqual(view.advance(), Progress(i + 1, False))
                else:
                    self.assertEqual(view.next_sequence, i + 1)
                    with self.assertRaises(TimeoutError):
                        view.advance()
                self.assertEqual(view.resolve("api"), (after,))
                view.close()

    def test_closed_source_drains_or_refuses_unrecoverable_view(self):
        directory = Directory(capacity=1)
        behind, current, canceled = directory.watch(), directory.watch(), directory.watch()
        canceled.close()
        with self.assertRaises(Closed):
            canceled.advance()
        directory.replace("api", (OLD,))
        current.advance()
        directory.replace("api", (NEW,))
        directory.close()
        self.assertEqual(current.advance(), Progress(2, False))
        self.assertEqual(current.resolve("api"), (NEW,))
        for view in (current, behind):
            with self.assertRaises(Closed):
                view.advance()
            with self.assertRaises(Closed):
                view.resolve("api")
        with self.assertRaises(Closed):
            directory.watch()
        with self.assertRaises(Closed):
            directory.replace("api", (NEW,))

    def test_real_clients_use_changed_membership_after_wrap(self):
        self.assertEqual(
            run_demo(),
            ((200, "old-renderer"), (200, "new-renderer"), (200, "new-renderer"), None),
        )
