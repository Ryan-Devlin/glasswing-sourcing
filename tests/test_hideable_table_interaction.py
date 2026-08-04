"""Exercise the real Streamlit widget-state cycle for Hide and Restore."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
_RUNNER = r'''
import pandas as pd
from streamlit.testing.v1 import AppTest

source = """
import pandas as pd
from ui_theme import render_hideable_table
render_hideable_table(
    pd.DataFrame([{"Domain": "probe.ai", "Title": "Probe"}]),
    domain_column="Domain",
    key="probe_editor",
)
"""
app = AppTest.from_string(source, default_timeout=20).run()
assert not app.exception
app.session_state["probe_editor"] = {
    "edited_rows": {0: {"Hide": True}},
    "added_rows": [],
    "deleted_rows": [],
}
app.run()
assert not app.exception

from classification_store import hidden_domain_set
assert hidden_domain_set(["probe.ai"]) == {"probe.ai"}

# A later rerun still omits the row; it must not silently clear persistence.
app.run()
assert hidden_domain_set(["probe.ai"]) == {"probe.ai"}

# Reveal hidden rows, uncheck Hide, and verify the domain is restored.
app.session_state["probe_editor_show_hidden"] = True
app.run()
assert not app.exception
app.session_state["probe_editor"] = {
    "edited_rows": {0: {"Hide": False}},
    "added_rows": [],
    "deleted_rows": [],
}
app.run()
assert not app.exception
assert hidden_domain_set(["probe.ai"]) == set()
'''


def test_hide_survives_rerun_and_unhide_restores_visibility():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        env = dict(os.environ)
        env.pop("DATABASE_URL", None)
        env["PYTHONPATH"] = str(ROOT)
        env["GLASSWING_DATA_ROOT"] = str(root / "data")
        env["GLASSWING_CLASSIFICATION_DB"] = str(
            root / "classifications.sqlite3"
        )
        result = subprocess.run(
            [sys.executable, "-c", _RUNNER],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
    assert result.returncode == 0, result.stdout + result.stderr
