import sqlite3
import tempfile
import unittest
import zlib
from pathlib import Path

from archive_gc import compact_html_archive


class ArchiveGarbageCollectionTests(unittest.TestCase):
    def test_only_live_archive_blocks_survive_rewrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "pages.sqlite3"
            archive = root / "html"
            archive.mkdir()
            pack = archive / "old.htmlpack"
            live = zlib.compress(b"live page")
            orphan = zlib.compress(b"deleted rejection")
            pack.write_bytes(live + orphan)
            raw_hash = "a" * 64
            with sqlite3.connect(database) as conn:
                conn.executescript(
                    """
                    CREATE TABLE pages(
                        id INTEGER PRIMARY KEY,
                        raw_html_hash TEXT,
                        html_archive_path TEXT,
                        html_archive_offset INTEGER,
                        html_archive_length INTEGER,
                        html_archive_codec TEXT,
                        raw_html_bytes INTEGER,
                        compressed_html_bytes INTEGER
                    );
                    CREATE TABLE html_blobs(
                        raw_html_hash TEXT PRIMARY KEY,
                        archive_path TEXT,
                        archive_offset INTEGER,
                        archive_length INTEGER,
                        archive_codec TEXT,
                        raw_bytes INTEGER,
                        created_at TEXT
                    );
                    """
                )
                conn.execute(
                    "INSERT INTO pages VALUES (1,?,?,?,?,?,?,?)",
                    (raw_hash, str(pack), 0, len(live), "zlib", 9, len(live)),
                )
                conn.execute(
                    "INSERT INTO html_blobs VALUES (?,?,?,?,?,?,?)",
                    (raw_hash, str(pack), 0, len(live), "zlib", 9, "1"),
                )
                conn.execute(
                    "INSERT INTO html_blobs VALUES (?,?,?,?,?,?,?)",
                    ("b" * 64, str(pack), len(live), len(orphan),
                     "zlib", 17, "1"),
                )

            result = compact_html_archive(database, archive, shard_count=2)
            self.assertEqual(result["live_blobs"], 1)
            self.assertGreater(result["bytes_reclaimed"], 0)
            with sqlite3.connect(database) as conn:
                pointer = conn.execute(
                    "SELECT html_archive_path,html_archive_offset,"
                    "html_archive_length FROM pages"
                ).fetchone()
                self.assertEqual(
                    conn.execute("SELECT count(*) FROM html_blobs").fetchone()[0],
                    1,
                )
            rebuilt = Path(pointer[0]).read_bytes()[
                pointer[1] : pointer[1] + pointer[2]
            ]
            self.assertEqual(zlib.decompress(rebuilt), b"live page")


if __name__ == "__main__":
    unittest.main()

