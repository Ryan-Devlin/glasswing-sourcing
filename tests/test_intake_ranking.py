"""Intake score storage and queue ordering, against a real SQLite queue.

intake_score.py covers the ranking policy; this covers that the score reaches
the database, only ever moves up, and actually changes what gets claimed.
"""

import sqlite3
import tempfile
import unittest
from pathlib import Path

import continuous_pipeline
from intake_score import read_certificate

ALL = {".ai", ".io", ".com", ".dev", ".app"}


def queue_db(directory):
    conn = sqlite3.connect(Path(directory) / "queue.sqlite3")
    conn.row_factory = sqlite3.Row
    conn.execute("""
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
    """)
    continuous_pipeline.ensure_domain_registry_schema(conn)
    return conn


def stored(conn, domain):
    return conn.execute(
        "SELECT intake_score, intake_evidence, seen_count FROM domains WHERE domain=?",
        (domain,),
    ).fetchone()


class ScoreStorageTests(unittest.TestCase):
    def test_the_score_and_its_evidence_reach_the_row(self):
        with tempfile.TemporaryDirectory() as d:
            conn = queue_db(d)
            continuous_pipeline.register_domain_event(
                conn, "foo.ai", ALL, intake_score=64,
                intake_evidence="tld:30 product[api,app]:+16",
            )
            row = stored(conn, "foo.ai")
            self.assertEqual(row["intake_score"], 64)
            self.assertIn("product", row["intake_evidence"])

    def test_a_richer_later_certificate_raises_the_score(self):
        # The property that removes the need for a revisit scheduler: a bare
        # registration climbs the queue by itself the moment its founders
        # ship and that provisions app/api subdomains.
        with tempfile.TemporaryDirectory() as d:
            conn = queue_db(d)
            day_one = read_certificate(["foo.ai", "www.foo.ai"], ALL).scored[0]
            continuous_pipeline.register_domain_event(
                conn, "foo.ai", ALL,
                intake_score=day_one.score, intake_evidence=day_one.evidence,
            )
            launched = read_certificate(
                ["foo.ai", "app.foo.ai", "api.foo.ai", "dashboard.foo.ai"], ALL
            ).scored[0]
            continuous_pipeline.register_domain_event(
                conn, "foo.ai", ALL,
                intake_score=launched.score, intake_evidence=launched.evidence,
            )
            row = stored(conn, "foo.ai")
            self.assertEqual(row["intake_score"], launched.score)
            self.assertGreater(launched.score, day_one.score)
            self.assertIn("product", row["intake_evidence"])
            self.assertEqual(row["seen_count"], 2)

    def test_a_weaker_later_certificate_never_lowers_the_score(self):
        # A renewal that happens to carry only the apex must not undo what an
        # earlier, richer certificate already proved about the domain.
        with tempfile.TemporaryDirectory() as d:
            conn = queue_db(d)
            continuous_pipeline.register_domain_event(
                conn, "foo.ai", ALL, intake_score=64, intake_evidence="rich",
            )
            continuous_pipeline.register_domain_event(
                conn, "foo.ai", ALL, intake_score=30, intake_evidence="bare",
            )
            row = stored(conn, "foo.ai")
            self.assertEqual(row["intake_score"], 64)
            self.assertEqual(row["intake_evidence"], "rich")

    def test_an_unscored_repeat_leaves_the_stored_score_alone(self):
        # Other producers (CSV upload, manual intake) register without a
        # certificate. Missing evidence must not be read as bad evidence.
        with tempfile.TemporaryDirectory() as d:
            conn = queue_db(d)
            continuous_pipeline.register_domain_event(
                conn, "foo.ai", ALL, intake_score=64, intake_evidence="rich",
            )
            continuous_pipeline.register_domain_event(conn, "foo.ai", ALL)
            self.assertEqual(stored(conn, "foo.ai")["intake_score"], 64)

    def test_an_unscored_first_sighting_is_null_not_zero(self):
        # NULL sorts last under COALESCE(intake_score, -1) and reads as
        # "never scored", which is not the same claim as "scored zero".
        with tempfile.TemporaryDirectory() as d:
            conn = queue_db(d)
            continuous_pipeline.register_domain_event(conn, "foo.ai", ALL)
            self.assertIsNone(stored(conn, "foo.ai")["intake_score"])


class ClaimOrderingTests(unittest.TestCase):
    def test_the_queue_orders_by_evidence_before_recency(self):
        # Arrival runs ~4x screening capacity, so this ordering decides which
        # quarter of the feed is ever looked at. Recency is uncorrelated with
        # whether a domain is a company; the certificate is not.
        with tempfile.TemporaryDirectory() as d:
            conn = queue_db(d)
            for domain, score, seen in [
                ("old-but-built.ai", 64, "2026-01-01T00:00:00Z"),
                ("new-but-bare.ai", 30, "2026-08-18T00:00:00Z"),
                ("never-scored.ai", None, "2026-08-17T00:00:00Z"),
            ]:
                conn.execute(
                    "INSERT INTO domains(domain, first_seen, last_seen, "
                    "intake_score) VALUES(?,?,?,?)",
                    (domain, seen, seen, score),
                )
            conn.commit()
            order = [
                row["domain"]
                for row in conn.execute(
                    "SELECT domain FROM domains WHERE status='pending' "
                    "ORDER BY COALESCE(intake_score, -1) DESC, first_seen DESC"
                )
            ]
            self.assertEqual(
                order, ["old-but-built.ai", "new-but-bare.ai", "never-scored.ai"]
            )


if __name__ == "__main__":
    unittest.main()
