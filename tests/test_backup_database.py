import csv
import hashlib
import io
import os
import zipfile

import pandas as pd
import pytest

from classification_store import connect_store, record_manual_move, upsert_classifications


def _live_postgres_url() -> str | None:
    """Same opt-in pattern as test_classification_store.py: skip unless a
    reachable TEST_DATABASE_URL is set, so the normal run never depends on a
    live database."""
    url = os.getenv("TEST_DATABASE_URL", "").strip()
    if not url:
        return None
    try:
        import psycopg

        with psycopg.connect(url, connect_timeout=5):
            pass
    except Exception:
        return None
    return url


def _clear_other_database_env(monkeypatch):
    for var in ("PIPELINE_DATABASE_URL", "AI_SEARCH_DATABASE_URL", "ZONE_UNIVERSE_DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_backup_contains_every_table_with_matching_checksum(tmp_path, monkeypatch):
    url = _live_postgres_url()
    monkeypatch.setenv("DATABASE_URL", url)
    _clear_other_database_env(monkeypatch)
    monkeypatch.setenv("GLASSWING_DB_BACKUP_ROOT", str(tmp_path))
    test_domain = "pg-backup-test.ai"

    with connect_store() as conn:
        conn.execute("DELETE FROM classifications WHERE domain=?", (test_domain,))
        conn.commit()

    try:
        frame = pd.DataFrame([{
            "domain": test_domain,
            "page_title": "Backup Test Co",
            "homepage_excerpt": "Enterprise platform " * 40,
            "word_count": 120,
            "routing_bucket": "review",
            "rule_score": 60,
        }])
        assert upsert_classifications(frame) == 1

        import backup_database

        result = backup_database.create_database_backup()
        assert set(result.keys()) == {"classification_store"}
        key = result["classification_store"]
        payload = open(key, "rb").read()

        with open(f"{key}.sha256") as sidecar:
            assert sidecar.read().strip() == hashlib.sha256(payload).hexdigest()

        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = set(archive.namelist())
            assert names == {
                "classifications.csv", "manual_decisions.csv",
                "movement_log.csv", "store_meta.csv", "manifest.json",
            }
            rows = list(csv.DictReader(
                io.StringIO(archive.read("classifications.csv").decode("utf-8"))
            ))
            assert any(row["domain"] == test_domain for row in rows)
    finally:
        with connect_store() as conn:
            conn.execute("DELETE FROM classifications WHERE domain=?", (test_domain,))
            conn.commit()


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_backup_only_covers_subsystems_actually_on_postgres(tmp_path, monkeypatch):
    url = _live_postgres_url()
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("PIPELINE_DATABASE_URL", url)
    monkeypatch.delenv("AI_SEARCH_DATABASE_URL", raising=False)
    monkeypatch.delenv("ZONE_UNIVERSE_DATABASE_URL", raising=False)
    monkeypatch.setenv("GLASSWING_DB_BACKUP_ROOT", str(tmp_path))

    # Ensure the pipeline schema/tables exist regardless of test execution
    # order -- this test shouldn't depend on test_continuous_pipeline_postgres.py
    # having already run first in the same suite.
    import continuous_pipeline
    continuous_pipeline.connect_db().close()

    import backup_database

    result = backup_database.create_database_backup()
    assert set(result.keys()) == {"classification_store", "pipeline"}


def test_create_database_backup_refuses_local_mode(monkeypatch, tmp_path):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    _clear_other_database_env(monkeypatch)
    monkeypatch.setenv("GLASSWING_DB_BACKUP_ROOT", str(tmp_path))

    import backup_database

    with pytest.raises(RuntimeError, match="nothing to back up"):
        backup_database.create_database_backup()
