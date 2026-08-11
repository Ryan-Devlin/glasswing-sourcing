import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace

from zone_universe.crawler import (
    crawl_once,
    quarantine_low_priority_domains,
)
from zone_universe.database import connect, utc_now
from zone_universe.domain_quality import assess_domain, crawl_priority
from zone_universe.zone_parser import remove_obsolete_queue_versions


class EarlyDomainQuarantineTests(unittest.TestCase):
    def test_known_and_invented_brands_remain_crawlable(self):
        domains = [
            "openai.com",
            "qodo.com",
            "x.com",
            "a5c.app",
            "0x88.app",
            "1password.com",
            "3dscanner.app",
        ]
        kept, quarantined = quarantine_low_priority_domains(
            domains,
            maximum_priority=2,
        )
        self.assertEqual(kept, domains)
        self.assertEqual(quarantined, [])

    def test_low_value_names_are_removed_before_dns_and_http(self):
        domains = [
            "openai.com",
            "localareashoppingnetworks.com",
            "sfgrt2ht99ar0-xef8a.com",
            "v1x2sdb9ieyecxwd.app",
        ]
        kept, quarantined = quarantine_low_priority_domains(
            domains,
            maximum_priority=2,
        )
        self.assertEqual(kept, ["openai.com"])
        self.assertEqual(
            quarantined,
            domains[1:],
        )

    def test_high_confidence_dga_and_hyphen_noise_are_rejected(self):
        for domain in (
            "pqlkjnmrtvwxz.com",
            "ab-ab-ab-ab-ab.com",
            "a-b-c-d-e-f-g-h-i.com",
        ):
            with self.subTest(domain=domain):
                self.assertFalse(assess_domain(domain).accepted)

    def test_priority_contract(self):
        self.assertEqual(crawl_priority("quantumsecurity.com"), 0)
        self.assertLessEqual(crawl_priority("qodo.com"), 2)
        self.assertEqual(
            crawl_priority("localareashoppingnetworks.com"),
            3,
        )

    def test_obsolete_queue_versions_are_removed_after_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_dir = root / "old"
            current_dir = root / "current"
            old_dir.mkdir()
            current_dir.mkdir()
            old_path = old_dir / "000.txt"
            current_path = current_dir / "000.txt"
            old_path.write_text("old.app\n", encoding="ascii")
            current_path.write_text("current.app\n", encoding="ascii")
            conn = connect(root / "universe.sqlite3")
            for version, path in (
                ("old", old_path),
                ("current", current_path),
            ):
                conn.execute(
                    """
                    INSERT INTO shards(
                        tld,queue_version,shard,path,total_bytes,
                        queued_domains,updated_at
                    ) VALUES ('app',?,0,?,?,1,?)
                    """,
                    (
                        version,
                        str(path),
                        path.stat().st_size,
                        utc_now(),
                    ),
                )
            conn.commit()

            removed = remove_obsolete_queue_versions(
                conn,
                tld="app",
                active_version="current",
            )

            self.assertEqual(removed, 1)
            self.assertFalse(old_dir.exists())
            self.assertTrue(current_dir.exists())
            self.assertEqual(
                conn.execute(
                    "SELECT queue_version FROM shards"
                ).fetchone()[0],
                "current",
            )
            conn.close()


class EarlyDomainQuarantineIntegrationTests(
    unittest.IsolatedAsyncioTestCase
):
    async def test_cold_domain_advances_queue_without_network_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "universe.sqlite3"
            shard_path = root / "000.txt"
            shard_path.write_text(
                "localareashoppingnetworks.com\n",
                encoding="ascii",
            )
            conn = connect(database)
            conn.execute(
                """
                INSERT INTO zones(
                    tld,import_status,active_queue_version,
                    discovered_domains
                ) VALUES ('com','ready','v1',1)
                """
            )
            conn.execute(
                """
                INSERT INTO shards(
                    tld,queue_version,shard,priority,path,total_bytes,
                    queued_domains,updated_at
                ) VALUES ('com','v1',0,1,?,?,1,?)
                """,
                (
                    str(shard_path),
                    shard_path.stat().st_size,
                    utc_now(),
                ),
            )
            conn.commit()
            settings = SimpleNamespace(
                database=database,
                minimum_free_gb=0,
                batch_size=10,
                max_crawl_priority=2,
            )

            result = await crawl_once(
                conn,
                settings,
                tld="com",
            )

            self.assertEqual(
                result,
                {
                    "skipped_existing": 0,
                    "skipped_low_priority": 1,
                },
            )
            shard = conn.execute(
                """
                SELECT byte_offset,attempted,skipped_low_priority
                FROM shards
                """
            ).fetchone()
            self.assertEqual(
                int(shard["byte_offset"]),
                shard_path.stat().st_size,
            )
            self.assertEqual(int(shard["attempted"]), 0)
            self.assertEqual(int(shard["skipped_low_priority"]), 1)
            conn.close()


if __name__ == "__main__":
    unittest.main()
