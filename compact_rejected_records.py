#!/usr/bin/env python3
"""Shrink stored JSON for decided rejections, in small committed batches.

compact_classification_storage refuses to run on Postgres, for a good reason
recorded there: rewriting the whole table writes a WAL record per row, which on
a bounded volume can consume the remaining disk before checkpoints reclaim it
and leave the database unable to finish recovery. Its comment asks instead for
"an online, disk-capacity-aware maintenance operation". This is that.

What it changes: each row's record_json is rewritten to exactly what
compact_classification_record would produce today, which is precisely what a
*new* write of the same row already produces. The only thing dropped is keys
outside COMPACT_CLASSIFICATION_FIELDS, which no current code path reads or
writes -- every row screened since that whitelist landed is already in this
shape.

Scope is deliberately the reject bucket alone. Those are decided on a specific
factual disqualifier, are never re-read, and are already treated as disposable
elsewhere (discard_uninvestable_classifications).

RUN 2026-08-25, with the volume at 74% of 5 GB. Reclaimed 98 MB over 36,236
rows before a dropped connection ended the pass at row 98,500 of 224,094; the
committed work survived, and the rest was finished by a second run.

Measure with a *random* sample, never a prefix. Two earlier estimates were
both wrong for opposite reasons: 69% came from sampling the largest rows, and
0% came from scanning in domain order, where the first 13,500 rejected rows
happen to be short junk-domain records (3,588 B average, none shrinkable)
against 4,892 B and 36% shrinkable across the bucket as a whole. A random
sample of 500 gives 28.9%, and 28.9% of the 618 MB that record_json actually
occupies is roughly 180 MB -- not the 280 MB an earlier note claimed, which
was taken against a 1,105 MB figure that included the review bucket.

So the cost driver is growth, not history: 180 MB is about two days of intake
at the August peak. This buys time; it does not solve capacity. If the volume
gets tight again the honest options are a bigger volume or dropping the
rejected rows outright.

Safety: batches commit separately with a pause between them so checkpoints keep
up, rows that would not shrink are skipped rather than rewritten for nothing,
and the run aborts if the database grows instead of shrinking.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

REPO = Path(os.environ["BACKFILL_REPO"])
sys.path.insert(0, str(REPO))
os.chdir(REPO)
os.environ["CLASSIFICATION_DATABASE_URL"] = os.environ["DATABASE_PUBLIC_URL"]

from classification_store import (  # noqa: E402
    compact_classification_record,
    connect_store,
)

DRY = os.environ.get("DRY_RUN", "1") == "1"
BATCH = max(20, int(os.environ.get("BATCH", "200")))
PAUSE = float(os.environ.get("PAUSE", "0.4"))
LIMIT = int(os.environ.get("LIMIT", "0"))
BUCKET = os.environ.get("BUCKET", "reject")
# A row has to save at least this much to be worth a WAL record.
MIN_SAVING = max(0, int(os.environ.get("MIN_SAVING", "512")))


# A full pass is tens of minutes of small queries, and on 2026-08-25 one died
# at row 98,500 of 224,094 with "SSL error: unexpected eof while reading" --
# a dropped connection, nothing to do with the work. The 98 MB it had already
# reclaimed was safe, because batches commit separately, but the run was over.
#
# So: transient connection failures are retried rather than fatal, and START
# lets a run pick up where one left off. Resuming is an optimisation, not a
# correctness requirement -- rows already compacted no longer clear MIN_SAVING,
# so a plain restart skips them anyway; it just re-reads them first.
START = os.environ.get("START", "")
RETRIES = max(0, int(os.environ.get("RETRIES", "5")))
RETRY_PAUSE = float(os.environ.get("RETRY_PAUSE", "3"))


def with_retries(what, action):
    """Retry an action that failed for reasons unrelated to its own work."""
    for attempt in range(RETRIES + 1):
        try:
            return action()
        except Exception as exc:
            transient = any(
                token in str(exc).lower()
                for token in (
                    "ssl", "eof", "connection", "timeout", "terminating",
                    "server closed", "broken pipe",
                )
            )
            if not transient or attempt == RETRIES:
                raise
            print(
                f"  {what} failed ({type(exc).__name__}), retrying in "
                f"{RETRY_PAUSE:.0f}s: {str(exc)[:90]}",
                file=sys.stderr, flush=True,
            )
            time.sleep(RETRY_PAUSE)


def database_bytes(conn) -> int:
    return int(
        conn.execute(
            "SELECT pg_database_size(current_database()) AS n"
        ).fetchone()["n"]
    )


def _opening():
    with connect_store(read_only=True) as conn:
        return database_bytes(conn), int(conn.execute(
            "SELECT count(*) AS n FROM classifications WHERE routing_bucket=?",
            (BUCKET,),
        ).fetchone()["n"])


start_bytes, total = with_retries("opening read", _opening)
print(f"bucket={BUCKET}  rows={total:,}  database={start_bytes/1024**2:,.0f} MB")
print(f"dry run: {DRY}   batch={BATCH}   min saving/row={MIN_SAVING}B")

cursor = START
seen = examined = rewritten = 0
saved = 0
while True:
    def read_batch(at=None):
        with connect_store(read_only=True) as conn:
            return conn.execute(
                "SELECT domain, record_json FROM classifications "
                "WHERE routing_bucket=? AND domain > ? ORDER BY domain LIMIT ?",
                (BUCKET, at, BATCH),
            ).fetchall()

    rows = with_retries("read", lambda: read_batch(cursor))
    if not rows:
        break

    pending: list[tuple[str, str]] = []
    for row in rows:
        seen += 1
        cursor = str(row["domain"])
        raw = str(row["record_json"] or "")
        if not raw or raw == "{}":
            continue
        try:
            record = json.loads(raw)
        except (TypeError, ValueError):
            continue
        examined += 1
        compacted = json.dumps(
            compact_classification_record(record),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        if len(raw) - len(compacted) < MIN_SAVING:
            continue
        saved += len(raw) - len(compacted)
        pending.append((compacted, cursor))

    if pending and not DRY:
        def write(batch=None):
            with connect_store() as conn:
                conn.executemany(
                    "UPDATE classifications SET record_json=? WHERE domain=?",
                    batch,
                )
                conn.commit()

        with_retries("write", lambda: write(pending))
        rewritten += len(pending)
        time.sleep(PAUSE)
        # Abort rather than fill the volume: if this is making the database
        # bigger, the assumption behind it is wrong.
        def measure():
            with connect_store(read_only=True) as conn:
                return database_bytes(conn)

        now_bytes = with_retries("size check", measure)
        if now_bytes > start_bytes * 1.15:
            print(
                f"\nSTOPPING: database grew from {start_bytes/1024**2:,.0f} MB "
                f"to {now_bytes/1024**2:,.0f} MB",
                file=sys.stderr,
            )
            break
    elif pending:
        rewritten += len(pending)

    print(
        f"  {seen:>7,}/{total:,}  rewritten {rewritten:>7,}  "
        f"reclaimed {saved/1024**2:>7,.0f} MB",
        flush=True,
    )
    if LIMIT and seen >= LIMIT:
        print(f"stopping at the {LIMIT:,}-row cap")
        break

def _closing():
    with connect_store(read_only=True) as conn:
        return database_bytes(conn)


end_bytes = with_retries("closing read", _closing)
print(
    f"\nexamined {examined:,}  rewritten {rewritten:,}  "
    f"json reclaimed {saved/1024**2:,.0f} MB"
)
print(f"database {start_bytes/1024**2:,.0f} MB -> {end_bytes/1024**2:,.0f} MB")
if DRY:
    print("dry run -- nothing written")
