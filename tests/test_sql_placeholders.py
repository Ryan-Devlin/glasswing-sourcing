"""No literal LIKE pattern inside a SQL string.

psycopg reads a bare % in a statement as a placeholder and rejects the whole
query: "only '%s', '%b', '%t' are allowed as placeholders". Every one of these
is silent in the wrong place -- three separate times in one day a LIKE pattern
written inline threw at runtime, and twice it was inside a try/except whose
job is to keep a page rendering, so the feature simply produced nothing and
said nothing. The pattern has to be a parameter.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Every module that talks to the store through the sqlite-style shim.
MODULES = (
    "web_api.py",
    "classification_store.py",
    "continuous_pipeline.py",
    "founder_signals.py",
    "form_d.py",
    "funnel.py",
)

# A LIKE (or ILIKE, or NOT LIKE) followed by a quoted literal containing %.
INLINE_LIKE = re.compile(
    r"(?:I?LIKE)\s+(['\"])(?:(?!\1).)*%(?:(?!\1).)*\1",
    re.IGNORECASE,
)


def test_no_module_inlines_a_like_pattern():
    offenders = []
    for name in MODULES:
        path = ROOT / name
        if not path.exists():
            continue
        for number, line in enumerate(
            path.read_text().splitlines(), start=1
        ):
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith("--"):
                continue
            if INLINE_LIKE.search(line):
                offenders.append(f"{name}:{number}: {stripped[:88]}")
    assert not offenders, (
        "LIKE patterns must be passed as parameters, not written into the "
        "SQL -- psycopg reads the % as a placeholder:\n  "
        + "\n  ".join(offenders)
    )
