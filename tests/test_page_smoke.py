"""Smoke tests: each Streamlit page must render without raising.

Runs each page in a fresh subprocess with every data/log path env var
pointed at a throwaway tmp directory, so a page render can never read or
write this machine's real data/ folder.
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

_PAGE_RUNNER = """
import sys
from streamlit.testing.v1 import AppTest

at = AppTest.from_file(sys.argv[1], default_timeout=60)
at.run()
print("SMOKE_EXCEPTION:", repr(at.exception))
"""


class PageSmokeTests(unittest.TestCase):
    def _assert_page_renders_cleanly(self, page_relpath: str, tmp_path: Path) -> None:
        # Start from the real environment (so PATH/HOME/etc. behave normally)
        # but force every data/log path onto a throwaway tmp dir, and force
        # the local SQLite/local-file backends regardless of this machine's
        # ambient DATABASE_URL/S3_BUCKET, so a page render can never read or
        # write real production data.
        env = dict(os.environ)
        env.pop("DATABASE_URL", None)
        env.pop("S3_BUCKET", None)
        env["PYTHONPATH"] = ":".join(
            [
                str(ROOT / "ai-domain-search-engine" / "src"),
                str(ROOT / "zone-universe" / "src"),
                str(ROOT),
            ]
        )
        env.update(
            {
                "GLASSWING_RUNTIME_MODE": "local",
                "GLASSWING_DATA_ROOT": str(tmp_path / "data"),
                "GLASSWING_LOG_ROOT": str(tmp_path / "logs"),
                "GLASSWING_CLASSIFICATION_DB": str(tmp_path / "data" / "classifications.sqlite3"),
                "GLASSWING_MANUAL_SNAPSHOT_ROOT": str(tmp_path / "data" / "manual" / "snapshots"),
                "AI_SEARCH_DB": str(tmp_path / "ai-search.sqlite3"),
                "AI_SEARCH_HTML_ARCHIVE": str(tmp_path / "ai-html"),
                "ZONE_UNIVERSE_DB": str(tmp_path / "zone-universe.sqlite3"),
                "ZONE_UNIVERSE_DATA_ROOT": str(tmp_path / "zone-data"),
                "ZONE_UNIVERSE_LOG_ROOT": str(tmp_path / "zone-logs"),
                "ZONE_UNIVERSE_HTML_ARCHIVE": str(tmp_path / "zone-html"),
            }
        )

        result = subprocess.run(
            [sys.executable, "-c", _PAGE_RUNNER, page_relpath],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )

        self.assertEqual(
            result.returncode,
            0,
            msg=f"subprocess failed for {page_relpath}:\n{result.stdout}\n{result.stderr}",
        )
        self.assertIn(
            "SMOKE_EXCEPTION: ElementList()",
            result.stdout,
            msg=f"{page_relpath} raised during render:\n{result.stdout}\n{result.stderr}",
        )

    def test_overview_renders_without_exception(self):
        # Loaded through main.py, not directly -- overview.py uses
        # st.page_link(), which needs the page registry st.navigation()
        # builds. AppTest.from_file() on a subpage skips that registry
        # entirely (a harness limitation, not a real navigation issue: this
        # is also the actual path a real visitor takes, since Overview is
        # the default landing page).
        with tempfile.TemporaryDirectory() as tmp_path:
            self._assert_page_renders_cleanly("main.py", Path(tmp_path))

    def test_dashboard_renders_without_exception(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            self._assert_page_renders_cleanly("dashboard.py", Path(tmp_path))

    def test_ai_search_renders_without_exception(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            self._assert_page_renders_cleanly("pages/ai_search.py", Path(tmp_path))

    def test_zone_universe_renders_without_exception(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            self._assert_page_renders_cleanly("pages/zone_universe.py", Path(tmp_path))

    def test_system_health_renders_without_exception(self):
        with tempfile.TemporaryDirectory() as tmp_path:
            self._assert_page_renders_cleanly("pages/system_health.py", Path(tmp_path))


if __name__ == "__main__":
    unittest.main()
