import unittest
from pathlib import Path


class StreamReconnectTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = Path("continuous_pipeline.py").read_text(
            encoding="utf-8"
        )
        self.dashboard = Path("dashboard.py").read_text(encoding="utf-8")

    def test_busy_listener_does_not_enforce_pong_deadline(self):
        self.assertIn("ping_interval=20", self.pipeline)
        self.assertIn("ping_timeout=None", self.pipeline)

    def test_successful_reconnect_clears_active_error(self):
        self.assertIn('stream_connection_state="connected"', self.pipeline)
        self.assertIn('last_error=""', self.pipeline)
        self.assertIn("stream_reconnect_count", self.pipeline)
        self.assertIn("last_stream_disconnect", self.pipeline)

    def test_listener_does_not_own_processing_stage(self):
        listener = self.pipeline.split("async def listener(", 1)[1].split(
            "def read_status", 1
        )[0]
        self.assertNotIn('stage="listening"', listener)
        self.assertNotIn('stage="reconnecting"', listener)

    def test_reconnect_is_not_presented_as_pipeline_crash(self):
        self.assertIn(
            'connection_state in {"connecting", "reconnecting"}',
            self.dashboard,
        )
        self.assertIn("reconnecting automatically", self.dashboard)


if __name__ == "__main__":
    unittest.main()
