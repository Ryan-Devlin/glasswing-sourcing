"""Shared test fixtures.

Database-backend selection throughout the app is driven by the presence of
`DATABASE_URL` in the environment (see db_backend.py). If a developer's shell
happens to have that variable exported (e.g., left over from Postgres work),
every test that touches classification_store would silently redirect from its
intended isolated tmp_path SQLite database to a real, possibly production,
Postgres instance -- exactly the "never combine test data with live data"
failure this project explicitly guards against. This autouse fixture removes
`DATABASE_URL` for the duration of every test; a test that specifically wants
to exercise the Postgres path can still set it back with monkeypatch inside
that test.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _no_ambient_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
