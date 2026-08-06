import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import continuous_pipeline


class StatusPersistenceTests(unittest.TestCase):
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
