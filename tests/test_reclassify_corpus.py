import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import reclassify_corpus


class CachedCorpusRescoreTests(unittest.TestCase):
    def test_corpus_domains_combines_and_deduplicates_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ai_path = root / "ai.sqlite3"
            zone_path = root / "zone.sqlite3"
            with sqlite3.connect(ai_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE domains(id INTEGER PRIMARY KEY, domain TEXT);
                    CREATE TABLE pages(domain_id INTEGER);
                    INSERT INTO domains VALUES
                        (1, 'alpha.ai'), (2, 'overlap.app');
                    INSERT INTO pages VALUES (1), (2);
                    """
                )
            with sqlite3.connect(zone_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE pages(domain TEXT);
                    INSERT INTO pages VALUES
                        ('overlap.app'), ('beta.com');
                    """
                )

            self.assertEqual(
                reclassify_corpus.corpus_domains(
                    "all",
                    ai_search_db=ai_path,
                    zone_db=zone_path,
                ),
                ["alpha.ai", "beta.com", "overlap.app"],
            )

    def test_record_fallback_keeps_saved_content(self):
        fetch = reclassify_corpus.cached_fetch_from_record(
            "example.ai",
            {
                "page_title": "Example",
                "meta_description": "Enterprise robotics",
                "homepage_excerpt": "A real cached product description.",
                "http_status": "200",
                "pages_scraped": 2,
            },
        )
        self.assertIsNotNone(fetch)
        self.assertEqual(fetch.source, "classification_cache_fallback")
        self.assertEqual(fetch.status, 200)
        self.assertEqual(fetch.pages_scraped, 2)

    def test_state_round_trip_is_atomic_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            reclassify_corpus.atomic_write_json(
                path,
                {"status": "running", "processed": 25},
            )
            self.assertEqual(
                json.loads(path.read_text())["processed"],
                25,
            )
            self.assertFalse(path.with_suffix(".json.tmp").exists())

    def test_zone_import_detection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "zone.sqlite3"
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "CREATE TABLE zones(tld TEXT, import_status TEXT)"
                )
                conn.execute(
                    "INSERT INTO zones VALUES ('com', 'importing')"
                )
            self.assertTrue(reclassify_corpus.zone_import_active(path))
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "UPDATE zones SET import_status='ready'"
                )
            self.assertFalse(reclassify_corpus.zone_import_active(path))


if __name__ == "__main__":
    unittest.main()
