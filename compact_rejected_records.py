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

MEASURED 2026-08-25, AND NOT CURRENTLY WORTH RUNNING. 222,478 rejected rows
hold 1,105 MB. On a *random* sample of 400 of them, 183 would shrink and the
total saving is 25.4% -- roughly 280 MB, or 7.7% of a 3,649 MB database, in
exchange for rewriting a hundred thousand rows and taking on precisely the WAL
risk the guard in compact_classification_storage exists to avoid.

An earlier estimate of 69% was wrong because it sampled the *largest* rows.
The bloat is concentrated in old rows that predate the field whitelist: the
first 4,000 rejected rows in domain order yielded nothing at all, because
everything written recently is already in this shape, and the corpus grew from
314k to 449k rows in five days.

So the cost driver is growth, not history, and the 8am-8pm intake window
already halves growth. Run this only if the picture changes -- if the ratio
moves, or the volume gets tight -- and re-measure with a random sample first.

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


def database_bytes(conn) -> int:
    return int(
        conn.execute(
            "SELECT pg_database_size(current_database()) AS n"
        ).fetchone()["n"]
    )


with connect_store(read_only=True) as conn:
    start_bytes = database_bytes(conn)
    total = int(conn.execute(
        "SELECT count(*) AS n FROM classifications WHERE routing_bucket=?",
        (BUCKET,),
    ).fetchone()["n"])
print(f"bucket={BUCKET}  rows={total:,}  database={start_bytes/1024**2:,.0f} MB")
print(f"dry run: {DRY}   batch={BATCH}   min saving/row={MIN_SAVING}B")

cursor = ""
seen = examined = rewritten = 0
saved = 0
while True:
    with connect_store(read_only=True) as conn:
        rows = conn.execute(
            "SELECT domain, record_json FROM classifications "
            "WHERE routing_bucket=? AND domain > ? ORDER BY domain LIMIT ?",
            (BUCKET, cursor, BATCH),
        ).fetchall()
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
        with connect_store() as conn:
            conn.executemany(
                "UPDATE classifications SET record_json=? WHERE domain=?",
                pending,
            )
            conn.commit()
        rewritten += len(pending)
        time.sleep(PAUSE)
        # Abort rather than fill the volume: if this is making the database
        # bigger, the assumption behind it is wrong.
        with connect_store(read_only=True) as conn:
            now_bytes = database_bytes(conn)
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

with connect_store(read_only=True) as conn:
    end_bytes = database_bytes(conn)
print(
    f"\nexamined {examined:,}  rewritten {rewritten:,}  "
    f"json reclaimed {saved/1024**2:,.0f} MB"
)
print(f"database {start_bytes/1024**2:,.0f} MB -> {end_bytes/1024**2:,.0f} MB")
if DRY:
    print("dry run -- nothing written")
