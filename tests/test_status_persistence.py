import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import continuous_pipeline


class StatusPersistenceTests(unittest.TestCase):
    def test_postgres_claim_prioritizes_manual_then_active_certstream_run(self):
        class Result:
            def fetchall(self):
                return [
                    {"domain": "manual.com", "first_seen": "3", "last_seen_run_id": "manual_domain_intake"},
                    {"domain": "current.ai", "first_seen": "2", "last_seen_run_id": "run-now"},
                    {"domain": "old-cached.ai", "first_seen": "1", "last_seen_run_id": "ai_search_cache_reuse_v1"},
                ]

        class Connection:
            def __init__(self):
                self.claimed = []

            def execute(self, sql, params=()):
                self.params = params
                return Result()

            def executemany(self, _sql, rows):
                self.claimed = [row[0] for row in rows]

            def commit(self):
                pass

        conn = Connection()
        with patch.object(continuous_pipeline, "using_postgres", return_value=True), patch.object(
            continuous_pipeline,
            "_corpus_priority_for_domains",
            return_value={"old-cached.ai": 0, "current.ai": 2},
        ):
            claimed = continuous_pipeline.claim_batch(
                conn,
                size=3,
                allowed_extensions={".ai"},
                preferred_run_id="run-now",
            )

        self.assertEqual(claimed, ["manual.com", "current.ai", "old-cached.ai"])
        self.assertEqual(conn.claimed, claimed)
        self.assertEqual(conn.params[1], "run-now")

    def test_listener_connectivity_does_not_clobber_active_screening_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            status = Path(directory) / "continuous_status.json"
            with patch.object(
                continuous_pipeline, "STREAM_STATUS", status
            ), patch.object(
                continuous_pipeline,
                "shared_pipeline_backend_available",
                return_value=False,
            ):
                continuous_pipeline.write_status(
                    running=True,
                    stage="screening",
                    current_batch=["active.ai"],
                )
                continuous_pipeline.write_status(
                    connected=False,
                    stream_connection_state="reconnecting",
                )

            snapshot = json.loads(status.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["stage"], "screening")
            self.assertFalse(snapshot["connected"])
            self.assertEqual(
                snapshot["stream_connection_state"], "reconnecting"
            )

    def test_idle_stage_reflects_connection_without_mutating_status(self):
        self.assertEqual(
            continuous_pipeline.pipeline_idle_stage({"connected": True}),
            "listening",
        )
        self.assertEqual(
            continuous_pipeline.pipeline_idle_stage({"connected": False}),
            "reconnecting",
        )

    def test_status_snapshot_is_published_to_shared_cloud_store(self):
        with tempfile.TemporaryDirectory() as directory:
            status = Path(directory) / "continuous_status.json"
            published = []
            with patch.object(continuous_pipeline, "STREAM_STATUS", status), patch.object(
                continuous_pipeline,
                "shared_pipeline_backend_available",
                return_value=True,
            ), patch.object(
                continuous_pipeline,
                "publish_pipeline_status",
                side_effect=lambda payload: published.append(dict(payload)),
            ):
                continuous_pipeline.write_status(
                    running=True,
                    stage="screening",
                    screened=12,
                )

            self.assertEqual(len(published), 1)
            self.assertTrue(published[0]["running"])
            self.assertEqual(published[0]["screened"], 12)

    def test_pipeline_prefers_shared_cloud_config(self):
        shared = {"selected_extensions": [".ai", ".io"], "scraper_workers": 31}
        with patch.object(
            continuous_pipeline,
            "shared_pipeline_backend_available",
            return_value=True,
        ), patch.object(
            continuous_pipeline,
            "read_pipeline_config",
            return_value=shared,
        ), patch.object(
            continuous_pipeline,
            "_shared_config_checked_at",
            0.0,
        ), patch.object(
            continuous_pipeline,
            "_shared_config_cache",
            {},
        ):
            loaded = continuous_pipeline.load_run_config()

        self.assertEqual(loaded, shared)

    def test_concurrent_status_writes_never_share_temp_file(self):
        with tempfile.TemporaryDirectory() as directory:
            status = Path(directory) / "continuous_status.json"
            errors = []

            def writer(index):
                try:
                    continuous_pipeline.write_status(writer=index)
                except Exception as exc:
                    errors.append(exc)

            with patch.object(continuous_pipeline, "STREAM_STATUS", status), patch.object(
                continuous_pipeline,
                "shared_pipeline_backend_available",
                return_value=False,
            ):
                threads = [threading.Thread(target=writer, args=(i,)) for i in range(30)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

            self.assertEqual(errors, [])
            self.assertIn("writer", json.loads(status.read_text()))
            self.assertEqual(list(Path(directory).glob("*.tmp")), [])

    def test_ghost_is_final_after_immediate_scrape_retry(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE domains (
                domain TEXT PRIMARY KEY, status TEXT, next_attempt_at TEXT,
                last_processed_at TEXT, classification TEXT, last_error TEXT
            )
        """)
        conn.execute(
            "INSERT INTO domains(domain,status) VALUES ('ghost.ai','processing')"
        )
        with patch.object(
            continuous_pipeline, "classification_map", return_value={"ghost.ai": "ghost"}
        ):
            continuous_pipeline.mark_success(conn, ["ghost.ai"])
        row = conn.execute("SELECT * FROM domains WHERE domain='ghost.ai'").fetchone()
        self.assertEqual(row["status"], "complete")
        self.assertIsNone(row["next_attempt_at"])
        self.assertEqual(
            row["last_error"],
            "website_unreachable_after_immediate_retry",
        )

    def test_discard_pending_by_extension_only_touches_that_tld(self):
        # Confirmed live: an accidental Extensions misconfiguration queued
        # 1700+ .co domains before anyone noticed, and claim_batch() (no
        # extension awareness, oldest first) would have kept drawing from
        # that stale backlog for over an hour after the config was fixed.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE domains (
                domain TEXT PRIMARY KEY, status TEXT, next_attempt_at TEXT,
                last_error TEXT
            )
        """)
        conn.executemany(
            "INSERT INTO domains(domain, status) VALUES (?, 'pending')",
            [("junk1.co",), ("junk2.co",), ("keep.ai",)],
        )
        conn.execute(
            "INSERT INTO domains(domain, status) VALUES ('inflight.co', 'processing')"
        )
        conn.execute(
            "INSERT INTO domains(domain, status) VALUES ('done.co', 'complete')"
        )
        conn.commit()

        discarded = continuous_pipeline.discard_pending_by_extension(conn, ".co")

        self.assertEqual(discarded, 3)
        rows = {
            row["domain"]: row["status"]
            for row in conn.execute("SELECT domain, status FROM domains").fetchall()
        }
        self.assertEqual(rows["junk1.co"], "discarded")
        self.assertEqual(rows["junk2.co"], "discarded")
        self.assertEqual(rows["inflight.co"], "discarded")
        self.assertEqual(rows["keep.ai"], "pending")
        self.assertEqual(rows["done.co"], "complete")

    def test_discard_pending_by_extension_none_wipes_every_tld(self):
        # Confirmed live: a 121,000+ historical backlog (many TLDs, not
        # just the one misconfiguration that started the investigation)
        # dominated every claim_batch() call regardless of which specific
        # TLD's backlog got cleared -- extension=None wipes all of it.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE domains (
                domain TEXT PRIMARY KEY, status TEXT, next_attempt_at TEXT,
                last_error TEXT
            )
        """)
        conn.executemany(
            "INSERT INTO domains(domain, status) VALUES (?, 'pending')",
            [("junk1.co",), ("junk2.ai",), ("junk3.app",)],
        )
        conn.execute(
            "INSERT INTO domains(domain, status) VALUES ('done.ai', 'complete')"
        )
        conn.commit()

        discarded = continuous_pipeline.discard_pending_by_extension(conn, None)

        self.assertEqual(discarded, 3)
        rows = {
            row["domain"]: row["status"]
            for row in conn.execute("SELECT domain, status FROM domains").fetchall()
        }
        self.assertEqual(rows["junk1.co"], "discarded")
        self.assertEqual(rows["junk2.ai"], "discarded")
        self.assertEqual(rows["junk3.app"], "discarded")
        self.assertEqual(rows["done.ai"], "complete")

    def test_reset_certstream_queue_deletes_every_status_and_claude_log(self):
        # "Clear the entire CertStream system of data" -- unlike
        # discard_pending_by_extension (which only ever marks not-yet-
        # attempted rows as discarded, preserving history), this is a real
        # DELETE across every status, for a genuinely fresh start.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE domains (
                domain TEXT PRIMARY KEY, status TEXT
            )
        """)
        conn.execute("CREATE TABLE claude_calls (called_at TEXT, domain TEXT)")
        conn.executemany(
            "INSERT INTO domains(domain, status) VALUES (?, ?)",
            [
                ("pending.ai", "pending"),
                ("done.ai", "complete"),
                ("dead.ai", "failed"),
                ("old.ai", "discarded"),
            ],
        )
        conn.execute(
            "INSERT INTO claude_calls(called_at, domain) VALUES ('t', 'done.ai')"
        )
        conn.commit()

        counts = continuous_pipeline.reset_certstream_queue(conn)

        self.assertEqual(counts["domains_deleted"], 4)
        self.assertEqual(counts["claude_calls_deleted"], 1)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM domains").fetchone()[0], 0
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM claude_calls").fetchone()[0], 0
        )

    def test_mark_failure_gives_up_domain_a_real_bucket_not_silence(self):
        # Confirmed live: a domain that times out on every one of
        # max_attempts tries never reaches process()/route_company at all
        # -- screen_records excludes it from `output` entirely, and
        # mark_failure only ever updated the pipeline's own queue table.
        # It just vanished after ~3 attempts with zero classification
        # record, forever, not "not yet bucketed" but never bucketed.
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE domains (
                domain TEXT PRIMARY KEY, status TEXT, attempts INTEGER,
                next_attempt_at TEXT, last_processed_at TEXT, last_error TEXT
            )
        """)
        conn.execute(
            "INSERT INTO domains(domain, status, attempts) "
            "VALUES ('dead.ai', 'processing', 3)"
        )
        conn.commit()

        with patch(
            "classification_store.upsert_classifications"
        ) as mock_upsert:
            continuous_pipeline.mark_failure(
                conn, ["dead.ai"], "TimeoutError: fetch timed out", 3
            )

        mock_upsert.assert_called_once()
        persisted = mock_upsert.call_args[0][0]
        self.assertEqual(list(persisted["domain"]), ["dead.ai"])
        self.assertEqual(list(persisted["routing_bucket"]), ["insufficient"])
        self.assertEqual(
            list(persisted["routing_reason"]),
            ["website_never_reachable_after_max_attempts"],
        )
        row = conn.execute(
            "SELECT status FROM domains WHERE domain='dead.ai'"
        ).fetchone()
        self.assertEqual(row["status"], "failed")

    def test_mark_failure_retry_path_does_not_persist_a_classification(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE domains (
                domain TEXT PRIMARY KEY, status TEXT, attempts INTEGER,
                next_attempt_at TEXT, last_processed_at TEXT, last_error TEXT
            )
        """)
        conn.execute(
            "INSERT INTO domains(domain, status, attempts) "
            "VALUES ('retry-me.ai', 'processing', 1)"
        )
        conn.commit()

        with patch(
            "classification_store.upsert_classifications"
        ) as mock_upsert:
            continuous_pipeline.mark_failure(
                conn, ["retry-me.ai"], "TimeoutError: fetch timed out", 3
            )

        mock_upsert.assert_not_called()
        row = conn.execute(
            "SELECT status FROM domains WHERE domain='retry-me.ai'"
        ).fetchone()
        self.assertEqual(row["status"], "pending")

    def test_claim_batch_prefers_newest_domains_over_stale_backlog(self):
        # Confirmed live: a 121,000+ historical backlog meant claim_batch's
        # old oldest-first ordering would have starved freshly arriving
        # domains for days, even after fixing the actual bugs that caused
        # the backlog and the extension misconfiguration. Fresh, likely-
        # live domains must be tried before ancient, likely-dead ones.
        with tempfile.TemporaryDirectory() as directory:
            queue_db = Path(directory) / "queue.sqlite3"
            # Nonexistent paths so _connect_db_sqlite() takes the bare
            # fallback query (no ai_search/zone_universe ATTACH) -- isolates
            # the first_seen ordering fix from the separate, legitimate
            # already-crawled priority tier.
            missing_ai_search = Path(directory) / "no_ai_search.sqlite3"
            missing_zone_universe = Path(directory) / "no_zone_universe.sqlite3"
            with patch.object(
                continuous_pipeline, "QUEUE_DB", queue_db
            ), patch.object(
                continuous_pipeline, "AI_SEARCH_DB", missing_ai_search
            ), patch.object(
                continuous_pipeline, "ZONE_UNIVERSE_DB", missing_zone_universe
            ):
                conn = continuous_pipeline.connect_db()
                conn.execute(
                    "INSERT INTO domains(domain, first_seen, last_seen) "
                    "VALUES ('stale-backlog-item.ai', '2020-01-01T00:00:00Z', "
                    "'2020-01-01T00:00:00Z')"
                )
                conn.execute(
                    "INSERT INTO domains(domain, first_seen, last_seen) "
                    "VALUES ('just-discovered.ai', '2026-08-06T00:00:00Z', "
                    "'2026-08-06T00:00:00Z')"
                )
                conn.execute(
                    "INSERT INTO domains(domain, first_seen, last_seen) "
                    "VALUES ('unselected.app', '2026-08-07T00:00:00Z', "
                    "'2026-08-07T00:00:00Z')"
                )
                conn.execute(
                    "INSERT INTO domains(domain, first_seen, last_seen, last_seen_run_id) "
                    "VALUES ('manual-intake.com', '2026-08-08T00:00:00Z', "
                    "'2026-08-08T00:00:00Z', 'manual_domain_intake')"
                )
                conn.commit()

                claimed = continuous_pipeline.claim_batch(
                    conn, size=10, allowed_extensions={".ai"}
                )
                unselected_status = conn.execute(
                    "SELECT status FROM domains WHERE domain='unselected.app'"
                ).fetchone()["status"]
                conn.close()

        self.assertEqual(claimed[0], "manual-intake.com")
        self.assertEqual(claimed[1], "just-discovered.ai")
        self.assertEqual(claimed[2], "stale-backlog-item.ai")
        self.assertEqual(unselected_status, "pending")

    def test_duplicate_domain_registration_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.sqlite3"
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                CREATE TABLE domains (
                    domain TEXT PRIMARY KEY,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    last_processed_at TEXT,
                    classification TEXT,
                    last_error TEXT DEFAULT '',
                    seen_count INTEGER NOT NULL DEFAULT 1,
                    first_seen_run_id TEXT NOT NULL DEFAULT '',
                    last_seen_run_id TEXT NOT NULL DEFAULT ''
                )
                """
            )
            first = continuous_pipeline.register_domain_event(
                conn, "duplicate.ai", {".ai"}, run_id="run-1"
            )
            second = continuous_pipeline.register_domain_event(
                conn, "duplicate.ai", {".ai"}, run_id="run-1"
            )
            row = conn.execute(
                "SELECT seen_count FROM domains WHERE domain='duplicate.ai'"
            ).fetchone()
            conn.close()

        self.assertTrue(first["is_new_domain"])
        self.assertTrue(second["is_repeat_event"])
        self.assertEqual(row["seen_count"], 2)
