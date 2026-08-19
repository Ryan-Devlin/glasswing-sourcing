"""Every table that lists records shows how long ago the record arrived.

The column has to be built on the timestamp that means "when we found this".
updated_at is refreshed by every re-screen, so a column built on it dates
every long-standing record as today -- which is worse than no column.
"""

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

import web_api

client = TestClient(web_api.app)
JSX = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
DOMAIN_JS = Path("webapp/src/domain.js").read_text(encoding="utf-8")


def test_one_shared_column_definition():
    """Defined once so the column cannot mean different things per page."""
    assert JSX.count("const ADDED_COLUMN = {") == 1
    assert 'label: "Date added"' in JSX


def test_the_column_is_on_every_record_listing_table():
    # companyCols (classifications + both corpus pages), both training
    # baskets, and Form D.
    assert JSX.count("ADDED_COLUMN,") >= 4


def test_updated_at_is_the_last_resort_not_the_first():
    """The ordering is the whole correctness of this column."""
    order = [
        DOMAIN_JS.index("row.first_seen"),
        DOMAIN_JS.index("row.classified_at"),
        DOMAIN_JS.index("row.updated_at"),
    ]
    assert order == sorted(order)


def test_the_corpus_query_selects_the_date_it_needs():
    """The corpus pages carried only updated_at, so the column would have
    read "Today" for every row on them."""
    source = Path("classification_store.py").read_text(encoding="utf-8")
    start = source.index("def query_source_classifications")
    body = source[start:start + 3000]
    assert "classified_at" in body


def test_corpus_rows_actually_carry_a_first_seen_date():
    rows = client.get("/api/corpus/zone?bucket=investable&page_size=25").json()
    for row in rows.get("rows") or []:
        assert "classified_at" in row


def test_classification_rows_carry_a_first_seen_date():
    rows = client.get("/api/classifications?bucket=all&page_size=25").json()
    for row in rows.get("rows") or []:
        assert "classified_at" in row


def test_calendar_days_not_elapsed_hours():
    """Something added at 11pm yesterday must read "Yesterday" this morning,
    not "Today" -- the column is compared against dates, not durations."""
    assert "startOfToday" in DOMAIN_JS and "startOfThen" in DOMAIN_JS
    assert '"Yesterday"' in DOMAIN_JS
    assert '"Today"' in DOMAIN_JS


def test_a_missing_date_renders_a_dash_not_today():
    """No date is not the same claim as "arrived today"."""
    window = DOMAIN_JS[DOMAIN_JS.index("export function daysAgoLabel"):]
    assert 'if (!value) return "—";' in window
    assert 'Number.isNaN(observed.getTime())) return "—"' in window
