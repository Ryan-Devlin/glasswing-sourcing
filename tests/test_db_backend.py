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
