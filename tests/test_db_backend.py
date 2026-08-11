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
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_instance

    def rollback(self):
        self.rollbacks += 1


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


def test_pooled_adapter_releases_connection_only_once():
    raw = _FakeRawConnection()
    released = []
    connection = db_backend.PostgresConnection(raw, release=released.append)

    connection.close()
    connection.close()

    assert released == [raw]
    assert raw.rollbacks == 1


class _FakeLockCursor:
    def __init__(self, locked):
        self._locked = locked

    def fetchone(self):
        return {"locked": self._locked}


class _FakeLockConnection:
    """Simulates pg_try_advisory_lock() returning a scripted sequence of
    True/False results, one per call, without touching a real database."""

    def __init__(self, results):
        self._results = list(results)
        self.calls = 0

    def execute(self, sql, params=()):
        assert "pg_try_advisory_lock" in sql
        self.calls += 1
        locked = self._results.pop(0) if self._results else False
        return _FakeLockCursor(locked)


def test_acquire_advisory_lock_succeeds_immediately_when_free():
    conn = _FakeLockConnection([True])
    assert db_backend.acquire_advisory_lock_with_timeout(
        conn, "some_key", timeout_seconds=1, poll_seconds=0.01
    ) is True
    assert conn.calls == 1


def test_acquire_advisory_lock_retries_until_released():
    conn = _FakeLockConnection([False, False, True])
    assert db_backend.acquire_advisory_lock_with_timeout(
        conn, "some_key", timeout_seconds=1, poll_seconds=0.01
    ) is True
    assert conn.calls == 3


def test_acquire_advisory_lock_gives_up_after_timeout_instead_of_hanging():
    # A stuck lock (held by a killed process that never released it) used to
    # mean pg_advisory_lock() blocked forever -- every future restart of that
    # service hung silently at startup. Confirmed live across three
    # different workers after routine redeploys.
    conn = _FakeLockConnection([])  # never available
    result = db_backend.acquire_advisory_lock_with_timeout(
        conn, "some_key", timeout_seconds=0.05, poll_seconds=0.01
    )
    assert result is False
    assert conn.calls >= 1


class _FakeCatalogConnection:
    """Answers the schema-object catalog query with a fixed object list."""

    def __init__(self, present):
        self.present = list(present)
        self.scripts = []

    def execute(self, sql, params=()):
        self.scripts.append(sql)

        class Cursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        return Cursor([{"name": name} for name in self.present])


def test_schema_objects_in_script_parses_tables_and_indexes():
    script = """
        CREATE TABLE IF NOT EXISTS pages (id SERIAL PRIMARY KEY);
        CREATE TABLE domains (id SERIAL PRIMARY KEY);
        CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain_id);
        CREATE UNIQUE INDEX idx_domains_name ON domains(name);
    """
    assert db_backend.schema_objects_in_script(script) == {
        "pages",
        "domains",
        "idx_pages_domain",
        "idx_domains_name",
    }


def test_schema_gate_skips_ddl_when_every_object_exists():
    script = (
        "CREATE TABLE IF NOT EXISTS pages (id SERIAL);"
        "CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain_id);"
    )
    conn = _FakeCatalogConnection(["pages", "idx_pages_domain", "extra_table"])
    assert db_backend.postgres_schema_is_current(conn, "ai_search", script) is True


def test_schema_gate_runs_ddl_when_an_object_is_missing():
    script = (
        "CREATE TABLE IF NOT EXISTS pages (id SERIAL);"
        "CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain_id);"
    )
    conn = _FakeCatalogConnection(["pages"])
    assert db_backend.postgres_schema_is_current(conn, "ai_search", script) is False


def test_schema_gate_is_case_insensitive_about_catalog_names():
    script = "CREATE TABLE IF NOT EXISTS Pages (id SERIAL);"
    conn = _FakeCatalogConnection(["pages"])
    assert db_backend.postgres_schema_is_current(conn, "ai_search", script) is True


def test_schema_gate_runs_ddl_when_a_create_name_cannot_be_parsed():
    # Fail safe: an unparseable CREATE must never let the gate claim the
    # schema is complete, or a genuinely missing object would never be built.
    script = (
        "CREATE TABLE IF NOT EXISTS pages (id SERIAL);"
        'CREATE TABLE "quoted name" (id SERIAL);'
    )
    conn = _FakeCatalogConnection(["pages"])
    assert db_backend.postgres_schema_is_current(conn, "ai_search", script) is False


def test_real_subsystem_schemas_are_fully_parseable():
    """Every CREATE in the shipped schemas must be name-parseable.

    If a future edit adds a form the parser misses, the gate degrades to
    always running the DDL (safe but slow) -- catch that here instead.
    """
    import importlib

    for module_name, schema in (
        ("ai_search.database", "ai_search"),
        ("zone_universe.database", "zone_universe"),
    ):
        module = importlib.import_module(module_name)
        script = module._POSTGRES_SCHEMA
        parsed = db_backend.schema_objects_in_script(script)
        creates = db_backend._DDL_CREATE_COUNT_PATTERN.findall(script)
        assert len(parsed) == len(creates), (
            f"{module_name}: parsed {len(parsed)} names from "
            f"{len(creates)} CREATE statements"
        )
