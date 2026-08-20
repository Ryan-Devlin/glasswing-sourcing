#!/usr/bin/env python3
"""Look again at domains that had no website yet the first time we checked.

A TLS certificate is issued when a domain is *provisioned*, which is days or
weeks before anything is deployed at it. That is the whole reason CertStream
is worth watching -- it shows a company at registration -- and it is also why
most of what it delivers is an empty address.

Measured live 2026-08-19: of 107 genuinely new domains in one listener run, 68
produced no usable verdict, and across 24 hours 853 screening outcomes were
"parked", "registrar placeholder" or "never reachable". The retry policy in
the queue gives a domain ``min(60, 2**attempts)`` minutes -- about an hour --
and parked pages are not retried at all. So a company whose site went up two
weeks after its certificate was simply never seen again, and its absence shows
up nowhere: a domain nobody looked at twice is not a rejection anyone can
count.

The distinction this module rests on is between two things the old code
treated identically:

* **"Nothing there yet"** -- parked, placeholder, unreachable. We learned
  nothing about the company because there was no company page to read. Worth
  another look.
* **"Not a fit"** -- consumer product, agency, non-US, no enterprise angle.
  The site was there and was read. Looking again in a month buys nothing,
  because the answer will not have changed.

Only the first is rescheduled. Revisiting the second would spend the crawl
budget re-deciding questions that are already settled.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Screening reasons that mean "we could not see a company", not "this is not
# a company we want". Kept as an explicit set rather than a prefix match: a
# new reason should have to be considered and added deliberately, because
# guessing wrong in the permissive direction spends real crawl budget on
# domains whose answer is already known.
NOT_BUILT_YET_REASONS = frozenset({
    "parked_or_for_sale",
    "generic_hosting_or_registrar_placeholder",
    "website_never_reachable_after_max_attempts",
    "website_unavailable",
    "website_unavailable_or_no_usable_company_evidence",
    "inaccessible_site",
    "ghost_or_inaccessible_site",
    "coming_soon",
    "unreadable_no_retry_left",
})

# Two further looks, then stop. Chosen to bracket the plausible window for a
# site going up rather than to sample it finely -- a domain still parked at 30
# days is overwhelmingly a domain that was bought and never used, and a third
# visit would cost more than it returns. These are days from the *first*
# screening, not from the previous visit.
REVISIT_SCHEDULE_DAYS = (14, 30)

SCHEMA = """
CREATE TABLE IF NOT EXISTS revisit_queue (
    domain TEXT PRIMARY KEY,
    first_reason TEXT NOT NULL DEFAULT '',
    first_seen_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_visit_at TEXT NOT NULL,
    last_outcome TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_revisit_due
    ON revisit_queue(next_visit_at, attempts);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _visit_at(first_seen: str, attempts: int) -> str:
    """When the ``attempts``-th revisit of a domain first seen then is due."""
    if attempts >= len(REVISIT_SCHEDULE_DAYS):
        return ""
    try:
        base = datetime.fromisoformat(str(first_seen).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        base = datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    due = base + timedelta(days=REVISIT_SCHEDULE_DAYS[attempts])
    return due.isoformat().replace("+00:00", "Z")


def ensure_schema(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA.strip().split(";"):
        if statement.strip():
            conn.execute(statement)
    conn.commit()


def worth_revisiting(reason: object) -> bool:
    return str(reason or "").strip().lower() in NOT_BUILT_YET_REASONS


def schedule_revisits(
    conn: sqlite3.Connection,
    rows: list[dict],
) -> int:
    """Queue a second look at every row whose verdict was "nothing there".

    Idempotent per domain: re-screening a still-parked domain must not reset
    its attempt count, or a domain would be revisited forever on a fortnightly
    loop and never retire.
    """
    now = utc_now()
    scheduled = 0
    for row in rows:
        domain = str(row.get("domain") or "").strip().lower()
        reason = str(row.get("routing_reason") or "")
        if not domain or not worth_revisiting(reason):
            continue
        existing = conn.execute(
            "SELECT attempts FROM revisit_queue WHERE domain=?", (domain,)
        ).fetchone()
        if existing is not None:
            continue
        due = _visit_at(now, 0)
        if not due:
            continue
        conn.execute(
            """
            INSERT INTO revisit_queue(
                domain, first_reason, first_seen_at, attempts,
                next_visit_at, last_outcome, updated_at
            ) VALUES (?, ?, ?, 0, ?, '', ?)
            ON CONFLICT(domain) DO NOTHING
            """,
            (domain, reason, now, due, now),
        )
        scheduled += 1
    if scheduled:
        conn.commit()
    return scheduled


def claim_due_revisits(
    conn: sqlite3.Connection,
    *,
    limit: int = 500,
    now: str = "",
) -> list[str]:
    """Domains due for another look, oldest first.

    Claiming advances the schedule immediately rather than after the crawl
    reports back. A revisit that dies mid-flight should cost the domain one of
    its two looks, not put it in a loop that re-claims it on every pass.
    """
    moment = now or utc_now()
    rows = conn.execute(
        """
        SELECT domain, first_seen_at, attempts FROM revisit_queue
        WHERE next_visit_at <= ? AND attempts < ?
        ORDER BY next_visit_at, domain
        LIMIT ?
        """,
        (moment, len(REVISIT_SCHEDULE_DAYS), max(0, int(limit))),
    ).fetchall()
    claimed: list[str] = []
    for row in rows:
        domain = str(row["domain"])
        attempts = int(row["attempts"]) + 1
        due = _visit_at(str(row["first_seen_at"]), attempts)
        if due:
            conn.execute(
                "UPDATE revisit_queue SET attempts=?, next_visit_at=?, "
                "updated_at=? WHERE domain=?",
                (attempts, due, moment, domain),
            )
        else:
            # Out of looks. Dropped rather than kept with a far-future date,
            # so the table stays the size of the work outstanding.
            conn.execute("DELETE FROM revisit_queue WHERE domain=?", (domain,))
        claimed.append(domain)
    if claimed:
        conn.commit()
    return claimed


def retire(conn: sqlite3.Connection, domains: list[str]) -> int:
    """Stop revisiting domains that have since produced a real verdict."""
    values = [str(d or "").strip().lower() for d in domains]
    values = [d for d in values if d]
    if not values:
        return 0
    removed = 0
    for start in range(0, len(values), 500):
        batch = values[start : start + 500]
        marks = ",".join("?" for _ in batch)
        cursor = conn.execute(
            f"DELETE FROM revisit_queue WHERE domain IN ({marks})", batch
        )
        removed += int(cursor.rowcount or 0)
    conn.commit()
    return removed


def pending_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT count(*) AS n FROM revisit_queue").fetchone()
    return int(row["n"] or 0) if row else 0
