import unittest
from pathlib import Path


class SmoothRefreshTests(unittest.TestCase):
    def setUp(self):
        self.source = Path("dashboard.py").read_text(encoding="utf-8")

    def test_automatic_refresh_is_slow_and_pipeline_scoped(self):
        self.assertIn("st_autorefresh", self.source)
        self.assertIn("interval=30_000", self.source)
        self.assertIn("if running:", self.source)

    def test_manual_refresh_button(self):
        self.assertIn('"Refresh now"', self.source)
        self.assertIn("manual_dashboard_refresh", self.source)

    def test_updated_refresh_copy(self):
        self.assertIn("Automatically refreshes every 30 seconds", self.source)
        self.assertIn("Refresh now", self.source)


if __name__ == "__main__":
    unittest.main()
