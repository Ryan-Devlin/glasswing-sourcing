import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from classification_store import connect_store, upsert_classifications
from rejection_cleanup import (
    automated_rejected_domains,
    purge_automated_rejections,
    rebuild_full_text_indexes,
)


class RejectionCleanupTests(unittest.TestCase):
    def test_historical_selector_matches_live_storage_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "classifications.sqlite3"
            upsert_classifications(
                pd.DataFrame([
                    {
                        "domain": "denied.ai",
                        "routing_bucket": "reject",
                        "glasswing_rating": 90,
                    },
                    {
                        "domain": "ghost.ai",
                        "routing_bucket": "ghost",
                        "glasswing_rating": 90,
                    },
                    {
                        "domain": "weak.ai",
                        "routing_bucket": "insufficient",
                        "glasswing_rating": 24,
                    },
                    {
                        "domain": "borderline.ai",
                        "routing_bucket": "insufficient",
                        "glasswing_rating": 25,
                    },
                    {
                        "domain": "review.ai",
                        "routing_bucket": "review",
                        "glasswing_rating": 1,
                    },
                    {
                        "domain": "slop.ai",
                        "routing_bucket": "priority",
                        "glasswing_rating": 80,
                        "venture_slop_risk": True,
                    },
                ]),
                database,
            )
            with patch(
                "rejection_cleanup.protected_training_domains",
                return_value=set(),
            ):
                selected = automated_rejected_domains(database)
            self.assertEqual(
                selected,
                {"denied.ai", "ghost.ai", "weak.ai", "slop.ai"},
            )

    def test_full_text_rebuild_drops_orphans(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ai_db = root / "ai.sqlite3"
            zone_db = root / "zone.sqlite3"
            with sqlite3.connect(ai_db) as conn:
                conn.executescript(
                    """
                    CREATE TABLE domains(id INTEGER PRIMARY KEY,domain TEXT);
                    CREATE TABLE pages(
                        id INTEGER PRIMARY KEY,domain_id INTEGER,title TEXT,
                        meta_description TEXT,headings TEXT,cleaned_text TEXT
                    );
                    CREATE VIRTUAL TABLE page_search USING fts5(
                        page_id UNINDEXED,domain,title,description,headings,body
                    );
                    INSERT INTO domains VALUES (1,'live.ai');
                    INSERT INTO pages VALUES (1,1,'Live','Desc','Head','Body');
                    INSERT INTO page_search VALUES
                        (1,'live.ai','Old','','',''),
                        (2,'deleted.ai','Deleted','','','Secret');
                    """
                )
            with sqlite3.connect(zone_db) as conn:
                conn.executescript(
                    """
                    CREATE TABLE pages(
                        id INTEGER PRIMARY KEY,domain TEXT,title TEXT,
                        meta_description TEXT,headings TEXT,cleaned_text TEXT
                    );
                    CREATE VIRTUAL TABLE page_search USING fts5(
                        page_id UNINDEXED,domain,title,description,headings,body
                    );
                    INSERT INTO pages VALUES
                        (1,'live.com','Live','Desc','Head','Body');
                    INSERT INTO page_search VALUES
                        (1,'live.com','Old','','',''),
                        (2,'deleted.com','Deleted','','','Secret');
                    """
                )

            result = rebuild_full_text_indexes(
                ai_search_db=ai_db,
                zone_db=zone_db,
            )
            self.assertEqual(result["ai_indexed_pages"], 1)
            self.assertEqual(result["zone_indexed_pages"], 1)
            with sqlite3.connect(ai_db) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT domain,title FROM page_search"
                    ).fetchall(),
                    [("live.ai", "Live")],
                )
            with sqlite3.connect(zone_db) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT domain,title FROM page_search"
                    ).fetchall(),
                    [("live.com", "Live")],
                )

    def test_purge_removes_payloads_but_protects_human_domain(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            classification_db = root / "classifications.sqlite3"
            queue_db = root / "queue.sqlite3"
            ai_db = root / "ai.sqlite3"
            zone_db = root / "zone.sqlite3"
            data = root / "data"
            (data / "filtered").mkdir(parents=True)

            upsert_classifications(
                pd.DataFrame([
                    {"domain": "auto.ai", "routing_bucket": "reject"},
                    {"domain": "human.ai", "routing_bucket": "reject"},
                ]),
                classification_db,
            )
            with sqlite3.connect(queue_db) as conn:
                conn.execute(
                    """
                    CREATE TABLE domains(
                        domain TEXT PRIMARY KEY,status TEXT,classification TEXT,
                        last_error TEXT,next_attempt_at TEXT
                    )
                    """
                )
                conn.executemany(
                    "INSERT INTO domains VALUES (?,'complete','reject','x','1')",
                    [("auto.ai",), ("human.ai",)],
                )
            with sqlite3.connect(ai_db) as conn:
                conn.executescript(
                    """
                    PRAGMA foreign_keys=ON;
                    CREATE TABLE domains(id INTEGER PRIMARY KEY,domain TEXT UNIQUE);
                    CREATE TABLE pages(
                        id INTEGER PRIMARY KEY,
                        domain_id INTEGER REFERENCES domains(id) ON DELETE CASCADE
                    );
                    CREATE VIRTUAL TABLE page_search USING fts5(
                        page_id UNINDEXED,domain,title,description,headings,body
                    );
                    """
                )
                conn.executemany(
                    "INSERT INTO domains(domain) VALUES (?)",
                    [("auto.ai",), ("human.ai",)],
                )
                conn.execute(
                    "INSERT INTO pages(domain_id) SELECT id FROM domains"
                )
                conn.execute(
                    """
                    INSERT INTO page_search(
                        page_id,domain,title,description,headings,body
                    )
                    SELECT p.id,d.domain,'','','',''
                    FROM pages p JOIN domains d ON d.id=p.domain_id
                    """
                )
            with sqlite3.connect(zone_db) as conn:
                conn.executescript(
                    """
                    CREATE TABLE pages(id INTEGER PRIMARY KEY,domain TEXT UNIQUE);
                    CREATE TABLE page_search(page_id INTEGER);
                    CREATE TABLE crawl_retries(domain TEXT PRIMARY KEY);
                    CREATE TABLE registration_queue(domain TEXT PRIMARY KEY);
                    CREATE TABLE domain_registration(domain TEXT PRIMARY KEY);
                    INSERT INTO pages(domain) VALUES ('auto.ai'),('human.ai');
                    INSERT INTO page_search VALUES (1),(2);
                    INSERT INTO crawl_retries VALUES ('auto.ai'),('human.ai');
                    INSERT INTO registration_queue VALUES ('auto.ai'),('human.ai');
                    INSERT INTO domain_registration VALUES ('auto.ai'),('human.ai');
                    """
                )
            pd.DataFrame([
                {"domain": "auto.ai", "description": "delete"},
                {"domain": "human.ai", "description": "keep"},
            ]).to_csv(data / "filtered" / "all_screened.csv", index=False)

            with patch(
                "rejection_cleanup.protected_training_domains",
                return_value={"human.ai"},
            ):
                result = purge_automated_rejections(
                    ["auto.ai", "human.ai"],
                    classification_db=classification_db,
                    queue_db=queue_db,
                    ai_search_db=ai_db,
                    zone_db=zone_db,
                    data_root=data,
                )

            self.assertEqual(result["purged_domains"], 1)
            self.assertEqual(result["protected"], 1)
            with connect_store(classification_db, read_only=True) as conn:
                self.assertEqual(
                    [row["domain"] for row in conn.execute(
                        "SELECT domain FROM classifications"
                    )],
                    ["human.ai"],
                )
            with sqlite3.connect(queue_db) as conn:
                auto = conn.execute(
                    "SELECT status,classification,last_error "
                    "FROM domains WHERE domain='auto.ai'"
                ).fetchone()
            self.assertEqual(auto, ("discarded", "", ""))
            with sqlite3.connect(ai_db) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT domain FROM domains ORDER BY domain"
                    ).fetchall(),
                    [("human.ai",)],
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT domain FROM page_search"
                    ).fetchall(),
                    [("human.ai",)],
                )
            with sqlite3.connect(zone_db) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT domain FROM pages ORDER BY domain"
                    ).fetchall(),
                    [("human.ai",)],
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT page_id FROM page_search"
                    ).fetchall(),
                    [(2,)],
                )
            remaining = pd.read_csv(data / "filtered" / "all_screened.csv")
            self.assertEqual(remaining["domain"].tolist(), ["human.ai"])


if __name__ == "__main__":
    unittest.main()
