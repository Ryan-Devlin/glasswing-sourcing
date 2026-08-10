from ai_search import worker


class _Connection:
    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


def test_deadlocked_batch_rolls_back_before_recording_error(monkeypatch):
    conn = _Connection()
    writes = []
    monkeypatch.setattr(
        worker,
        "set_worker_state",
        lambda connection, key, value: writes.append((connection.rollbacks, key, value)),
    )

    worker.recover_crawl_transaction(conn, RuntimeError("deadlock detected"))

    assert conn.rollbacks == 1
    assert writes == [
        (1, "crawler_status", "error_retrying"),
        (1, "last_crawl_error", "deadlock detected"),
    ]
