"""Route demo using two real loopback HTTP servers and a tiny update ring."""
from contextlib import closing, contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from directory import Directory, Endpoint


@contextmanager
def backend(name):
    payload = name.encode("ascii")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
        worker.start()
        try:
            yield Endpoint(*server.server_address)
        finally:
            server.shutdown()
            worker.join()


def request_render(view):
    endpoints = view.resolve("render")
    if not endpoints:
        return None
    target = endpoints[0]
    with closing(HTTPConnection(target.host, target.port, timeout=2)) as client:
        client.request("GET", "/render")
        response = client.getresponse()
        return response.status, response.read().decode("ascii")


def run_demo():
    with backend("old-renderer") as old, backend("new-renderer") as new:
        directory = Directory(capacity=2)
        directory.replace("render", (old,))
        fast, slow = directory.watch(), directory.watch()
        try:
            before = request_render(slow)
            directory.replace("render", (new,))
            fast_cut = fast.advance()
            fast_result = request_render(fast)
            # The slow client misses the render change; the old server is still live.
            directory.replace("assets", (old,))
            directory.replace("search", (new,))
            slow_cut = slow.advance()
            slow_result = request_render(slow)
            directory.replace("render", ())
            removal = slow.advance()
            removed_result = request_render(slow)
            assert before == (200, "old-renderer")
            assert not fast_cut.resynced and fast_result == (200, "new-renderer")
            assert slow_cut.resynced and slow_result == (200, "new-renderer")
            assert not removal.resynced and removed_result is None
        finally:
            fast.close()
            slow.close()
            directory.close()
    return before, fast_result, slow_result, removed_result


if __name__ == "__main__":
    before, fast, slow, removed = run_demo()
    print("before:", before)
    print("retained replacement:", fast)
    print("after lag and resnapshot:", slow)
    print("after removal:", "no endpoint" if removed is None else removed)
