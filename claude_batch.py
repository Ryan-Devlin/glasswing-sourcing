#!/usr/bin/env python3
"""Submit and collect Claude diligence through the Batch API.

The Batch API bills at half the standard rate and answers within the hour
rather than in the same call. The diligence worker is a one-shot process on a
timer, so a batch cannot be submitted and awaited in one run without holding
the worker open for an hour. Instead each run does two things in order:

1. **Collect** — if a batch from an earlier run has ended, ingest its results.
2. **Submit** — send this run's batch tier and record the id for next time.

The id lives in a small JSON file next to the other worker state rather than
in a new table: this is worker bookkeeping, not corpus data, and it must not
require a schema migration to land.

**At most one batch is in flight at a time.** A second submission while one is
outstanding would double the day's spend without the caller's accounting ever
seeing it, since a batch is only counted against the budget once its results
come back. :func:`submit` refuses rather than silently overwriting the id --
losing the id would strand a batch that is already paid for.

Generic over the request body: callers hand in ``(custom_id, params)`` pairs,
so this module never imports the classifier and can be exercised with a fake.
"""

from __future__ import annotations

import inspect
import json
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common import LOGS, atomic_write_json

BATCH_STATE = LOGS / "claude_batch_state.json"

# The API accepts a restricted character set for custom_id; a domain's dots
# are not guaranteed to survive it, so ids are sanitised and the mapping back
# to the domain is persisted with the batch rather than re-derived.
_SAFE_ID = re.compile(r"[^A-Za-z0-9_-]")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def custom_id_for(domain: str) -> str:
    return _SAFE_ID.sub("_", str(domain or "").strip().lower())[:60] or "unknown"


@dataclass(frozen=True)
class BatchState:
    batch_id: str = ""
    submitted_at: str = ""
    domains: dict[str, str] = None  # custom_id -> domain
    # The candidate row for each id, carried across the run boundary. The
    # persistence path merges the whole row into what it stores, so a batched
    # result arriving an hour later needs the same row the synchronous tier
    # would have had -- re-deriving it from the corpus next run would let the
    # two tiers store different columns for the same kind of verdict.
    rows: dict[str, dict] = None

    @property
    def in_flight(self) -> bool:
        return bool(self.batch_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "submitted_at": self.submitted_at,
            "domains": dict(self.domains or {}),
            "rows": dict(self.rows or {}),
        }


def read_state(path: Path = BATCH_STATE) -> BatchState:
    """Load the in-flight batch, or an empty state.

    A corrupt or unreadable file reads as empty rather than raising: the file
    is worker bookkeeping, and a parse error must not take diligence down. The
    cost of being wrong is one duplicate batch, not lost corpus data.
    """
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return BatchState(domains={}, rows={})
    if not isinstance(raw, dict):
        return BatchState(domains={}, rows={})
    domains = raw.get("domains")
    rows = raw.get("rows")
    return BatchState(
        batch_id=str(raw.get("batch_id", "") or ""),
        submitted_at=str(raw.get("submitted_at", "") or ""),
        domains=dict(domains) if isinstance(domains, dict) else {},
        rows=dict(rows) if isinstance(rows, dict) else {},
    )


def write_state(state: BatchState, path: Path = BATCH_STATE) -> None:
    atomic_write_json(Path(path), state.to_dict())


def clear_state(path: Path = BATCH_STATE) -> None:
    write_state(BatchState(domains={}, rows={}), path)


async def submit(
    client: Any,
    entries: list[tuple[str, dict]],
    *,
    rows: dict[str, dict] | None = None,
    path: Path = BATCH_STATE,
    now: str = "",
) -> str:
    """Submit one batch and persist its id. Returns "" when nothing was sent.

    Refuses while another batch is outstanding — see the module docstring.
    """
    if not entries:
        return ""
    existing = read_state(path)
    if existing.in_flight:
        return ""

    batch = await client.messages.batches.create(
        requests=[
            {"custom_id": custom_id, "params": params}
            for custom_id, params in entries
        ]
    )
    batch_id = str(getattr(batch, "id", "") or "")
    if not batch_id:
        return ""
    write_state(
        BatchState(
            batch_id=batch_id,
            submitted_at=now or _utc_now(),
            domains={custom_id: custom_id for custom_id, _ in entries},
            rows=dict(rows or {}),
        ),
        path,
    )
    return batch_id


async def collect(
    client: Any,
    *,
    path: Path = BATCH_STATE,
) -> tuple[dict[str, Any], list[tuple[str, str]], str, dict[str, dict]]:
    """Ingest a finished batch.

    Returns ``(responses_by_custom_id, failures, status, rows_by_custom_id)``.
    The rows are the ones stashed at submit time, so the caller can persist a
    batched verdict exactly as it would a synchronous one. ``status`` is one
    of ``"idle"`` (nothing in flight), ``"running"`` (still processing, state
    left intact so the next run tries again), or ``"ended"``.

    The state file is cleared **only** on ``ended``. A transient error while
    reading results leaves the id in place so the next run can retry rather
    than abandoning a batch that has already been paid for.
    """
    state = read_state(path)
    if not state.in_flight:
        return {}, [], "idle", {}

    batch = await client.messages.batches.retrieve(state.batch_id)
    if str(getattr(batch, "processing_status", "") or "") != "ended":
        return {}, [], "running", {}

    responses: dict[str, Any] = {}
    failures: list[tuple[str, str]] = []
    # The async SDK returns an awaitable that resolves to the result stream,
    # while the sync one returns the stream directly. Accept either rather
    # than pinning a shape that a client-library bump could change under us.
    stream = client.messages.batches.results(state.batch_id)
    if inspect.isawaitable(stream):
        stream = await stream
    async for entry in stream:
        custom_id = str(getattr(entry, "custom_id", "") or "")
        result = getattr(entry, "result", None)
        kind = str(getattr(result, "type", "") or "")
        if kind == "succeeded":
            responses[custom_id] = getattr(result, "message", None)
        else:
            # errored / canceled / expired all mean no verdict for this row.
            # Reported rather than dropped so the caller can requeue it.
            failures.append((custom_id, kind or "unknown"))

    clear_state(path)
    return responses, failures, "ended", dict(state.rows or {})
