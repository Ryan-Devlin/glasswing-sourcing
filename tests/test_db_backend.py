import db_backend


def test_using_postgres_reflects_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert db_backend.using_postgres() is False
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@host/db")
    assert db_backend.using_postgres() is True


def test_placeholder_translation_is_positional_and_order_preserving():
    assert (
        db_backend.PostgresConnection._translate("SELECT * FROM t WHERE a=? AND b=?")
        == "SELECT * FROM t WHERE a=%s AND b=%s"
    )
    assert db_backend.PostgresConnection._translate("SELECT 1") == "SELECT 1"


class _FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params):
        self.calls.append(("execute", sql, params))

    def executemany(self, sql, rows):
        self.calls.append(("executemany", sql, rows))


class _FakeRawConnection:
    def __init__(self):
        self.cursor_instance = _FakeCursor()

    def cursor(self):
        return self.cursor_instance


def test_postgres_adapter_strips_nul_from_text_but_preserves_binary():
    raw = _FakeRawConnection()
    connection = db_backend.PostgresConnection(raw)

    connection.execute(
        "INSERT INTO pages(title,raw) VALUES (?,?)",
        ("bad\x00title", b"binary\x00payload"),
    )
    connection.executemany(
        "INSERT INTO pages(title,score) VALUES (?,?)",
        [("one\x00", 1), ("\x00two", 2)],
    )

    assert raw.cursor_instance.calls[0] == (
        "execute",
        "INSERT INTO pages(title,raw) VALUES (%s,%s)",
        ("badtitle", b"binary\x00payload"),
    )
    assert raw.cursor_instance.calls[1] == (
        "executemany",
        "INSERT INTO pages(title,score) VALUES (%s,%s)",
        [("one", 1), ("two", 2)],
    )
