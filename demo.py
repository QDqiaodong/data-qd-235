from broadcast import Broadcast, Closed, Lagged


if __name__ == "__main__":
    bus = Broadcast(capacity=2)
    fast, slow = bus.subscribe(), bus.subscribe()
    observed = []
    for payload in (b"start", b"checkpoint", b"finish"):
        assert bus.publish(payload) == len(observed)
        observed.append(fast.recv(0).data.decode())
    try:
        slow.recv(0)
    except Lagged as err:
        assert (err.expected, err.oldest) == (0, 1)
    else:
        raise AssertionError("lost event was not detected")
    skipped = slow.skip_lost()
    bus.close()
    retained = [slow.recv(0).data.decode(), slow.recv(0).data.decode()]
    try:
        slow.recv(0)
    except Closed:
        pass
    else:
        raise AssertionError("expected end of stream")
    assert skipped == (0, 1) and observed == ["start", "checkpoint", "finish"]
    assert retained == ["checkpoint", "finish"]
    print("fast:", observed)
    print("slow skipped:", skipped, "then read:", retained)
