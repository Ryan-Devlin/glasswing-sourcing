from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
import stat
import uuid
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from db_backend import PostgresConnection

from .config import QUEUES, ZONEFILES
from .database import connect, log_event, utc_now
from .domain_quality import assess_domain


DOMAIN = re.compile(rb"^[a-z0-9.-]+$", re.I)


SNAPSHOT_TIMEZONE = ZoneInfo("America/New_York")


def snapshot_day_of(timestamp: str) -> str:
    """The Eastern calendar day an import belongs to.

    The daily schedule opens at 6 AM America/New_York, so a "day" of intake
    is an Eastern day, not a UTC one -- a 22:00 ET import would otherwise be
    filed under tomorrow.
    """
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(SNAPSHOT_TIMEZONE).date().isoformat()


def normalize_tld(value: str) -> str:
    tld = str(value).strip().lower().strip(".")
    if not tld or not re.fullmatch(r"[a-z0-9-]{2,63}", tld):
        raise ValueError("TLD must look like 'com' or 'app'.")
    return tld


def source_fingerprint(path: Path) -> str:
    stat = path.stat()
    payload = f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}"
    return hashlib.sha256(payload.encode()).hexdigest()


def _open_zone(path: Path):
    # Named pipes are used by the production CZDS streaming importer. Avoid a
    # separate magic-byte probe: opening a FIFO twice would consume bytes from
    # the one-shot network stream and can deadlock the producer.
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rb")
    with path.open("rb") as handle:
        magic = handle.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rb")
    return path.open("rb")


def _snapshot_domains(path: Path):
    with gzip.open(path, "rt", encoding="ascii") as handle:
        for line in handle:
            domain = line.strip()
            if domain:
                yield domain


def extract_delegated_domain(raw_line: bytes, tld: str) -> str:
    """Extract one delegation owner from an RFC 1035 zone-file line."""
    if not raw_line or raw_line[:1].isspace():
        return ""
    line = raw_line.split(b";", 1)[0].strip()
    if not line or line.startswith(b"$"):
        return ""
    parts = line.split()
    upper = [part.upper() for part in parts]
    if b"NS" not in upper:
        return ""
    ns_index = upper.index(b"NS")
    if ns_index < 1:
        return ""
    owner = parts[0].lower().rstrip(b".")
    suffix = b"." + tld.encode()
    if owner == tld.encode() or not owner.endswith(suffix):
        return ""
    if not DOMAIN.fullmatch(owner):
        return ""
    try:
        domain = owner.decode("ascii")
        domain.encode("idna")
    except (UnicodeDecodeError, UnicodeError):
        return ""
    if len(domain) > 253 or any(
        not label or len(label) > 63
        for label in domain.split(".")
    ):
        return ""
    return domain


def queue_shard(domain: str, shard_count: int) -> int:
    """Spread lexical prefixes across shards so crawling is not A-to-Z."""
    label = domain.split(".", 1)[0]
    prefix = (label[:2] or label).encode("ascii", errors="ignore")
    digest = hashlib.blake2b(prefix, digest_size=2).digest()
    return int.from_bytes(digest, "big") % shard_count


def remove_obsolete_queue_versions(
    conn,
    *,
    tld: str,
    active_version: str,
) -> int:
    """Keep only the active recoverable queue after an atomic zone refresh."""
    rows = conn.execute(
        """
        SELECT DISTINCT queue_version,path
        FROM shards
        WHERE tld=? AND queue_version<>?
        """,
        (tld, active_version),
    ).fetchall()
    versions = {str(row["queue_version"]) for row in rows}
    directories = {
        Path(str(row["path"])).parent
        for row in rows
        if str(row["path"])
    }
    conn.execute(
        "DELETE FROM shards WHERE tld=? AND queue_version<>?",
        (tld, active_version),
    )
    conn.commit()
    for directory in directories:
        shutil.rmtree(directory, ignore_errors=True)
    return len(versions)


def import_zone(
    zone_path: Path | str,
    *,
    tld: str,
    shard_count: int = 64,
    limit: int = 0,
    database: Path | str | None = None,
    queue_root: Path = QUEUES,
    reject_gibberish: bool = True,
) -> dict[str, object]:
    """Snapshot a zone and queue only domains added since the prior snapshot.

    The first import is deliberately a baseline: it records the current zone
    without scheduling millions of already-existing domains.  Every later
    import performs a streaming merge diff and writes only ``today - prior``
    to the crawl queue.
    """
    path = Path(zone_path).expanduser().resolve()
    if not path.exists() or not (
        path.is_file() or stat.S_ISFIFO(path.stat().st_mode)
    ):
        raise FileNotFoundError(path)
    tld = normalize_tld(tld)
    shard_count = max(4, min(int(shard_count), 128))
    if shard_count & (shard_count - 1):
        raise ValueError("Shard count must be a power of two.")
    limit = max(0, int(limit))
    fingerprint = source_fingerprint(path)
    version = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + fingerprint[:10]
    )
    tld_root = queue_root / tld
    build_dir = tld_root / f".building-{version}-{uuid.uuid4().hex[:8]}"
    final_dir = tld_root / version
    build_dir.mkdir(parents=True, exist_ok=False)

    conn = connect(database) if database else connect()
    previous = conn.execute(
        "SELECT snapshot_path FROM zones WHERE tld=?",
        (tld,),
    ).fetchone()
    previous_snapshot = (
        Path(str(previous["snapshot_path"]))
        if previous is not None and str(previous["snapshot_path"])
        else None
    )
    if previous_snapshot is not None and not previous_snapshot.exists():
        previous_snapshot = None
    conn.execute(
        """
        INSERT INTO zones(
            tld,zone_path,source_size,source_modified_at,
            source_fingerprint,import_status,import_error
        ) VALUES (?,?,?,?,?,'importing','')
        ON CONFLICT(tld) DO UPDATE SET
            zone_path=excluded.zone_path,
            source_size=excluded.source_size,
            source_modified_at=excluded.source_modified_at,
            source_fingerprint=excluded.source_fingerprint,
            import_status='importing',
            import_error=''
        """,
        (
            tld,
            str(path),
            path.stat().st_size,
            datetime.fromtimestamp(
                path.stat().st_mtime,
                timezone.utc,
            ).isoformat(),
            fingerprint,
        ),
    )
    conn.commit()

    lines = 0
    delegated = 0
    repeated_records = 0
    invalid_or_irrelevant = 0
    rejected_gibberish = 0
    total_shards = shard_count
    shard_counts = [0] * total_shards
    added_domains = 0
    removed_domains = 0
    unchanged_domains = 0
    last_domain = ""
    rejection_root = queue_root.parent / "rejected"
    rejection_path = rejection_root / f"{tld}-{version}.summary.json"
    rejection_reasons: Counter[str] = Counter()
    rejection_examples: dict[str, list[str]] = {}
    try:
        with ExitStack() as stack:
            outputs = [
                stack.enter_context(
                    (build_dir / f"{index:03d}.txt").open(
                        "wb",
                        buffering=1024 * 1024,
                    )
                )
                for index in range(total_shards)
            ]
            snapshot_handle = stack.enter_context(
                gzip.open(build_dir / "domains.txt.gz", "wt", encoding="ascii")
            )
            added_handle = stack.enter_context(
                gzip.open(build_dir / "added.txt.gz", "wt", encoding="ascii")
            )
            removed_handle = stack.enter_context(
                gzip.open(build_dir / "removed.txt.gz", "wt", encoding="ascii")
            )
            previous_iterator = (
                iter(_snapshot_domains(previous_snapshot))
                if previous_snapshot is not None
                else iter(())
            )
            previous_domain = next(previous_iterator, "")
            if reject_gibberish:
                rejection_root.mkdir(parents=True, exist_ok=True)
            zone = stack.enter_context(_open_zone(path))
            for raw_line in zone:
                lines += 1
                domain = extract_delegated_domain(raw_line, tld)
                if not domain:
                    invalid_or_irrelevant += 1
                    continue
                if domain == last_domain:
                    repeated_records += 1
                    continue
                last_domain = domain
                if reject_gibberish:
                    quality = assess_domain(domain)
                    if not quality.accepted:
                        rejected_gibberish += 1
                        reason = str(quality.reason or "unspecified")
                        rejection_reasons[reason] += 1
                        examples = rejection_examples.setdefault(reason, [])
                        if len(examples) < 25:
                            examples.append(domain)
                        continue
                snapshot_handle.write(domain + "\n")
                is_new = False
                if previous_snapshot is not None:
                    while previous_domain and previous_domain < domain:
                        removed_handle.write(previous_domain + "\n")
                        removed_domains += 1
                        previous_domain = next(previous_iterator, "")
                    if previous_domain == domain:
                        unchanged_domains += 1
                        previous_domain = next(previous_iterator, "")
                    else:
                        added_handle.write(domain + "\n")
                        added_domains += 1
                        is_new = True
                # A baseline snapshot queues nothing.  On refreshes, only
                # domains absent from yesterday's snapshot are crawl work.
                if is_new:
                    shard = queue_shard(domain, shard_count)
                    outputs[shard].write(domain.encode() + b"\n")
                    shard_counts[shard] += 1
                delegated += 1
                if limit and delegated >= limit:
                    break
            if previous_snapshot is not None:
                while previous_domain:
                    removed_handle.write(previous_domain + "\n")
                    removed_domains += 1
                    previous_domain = next(previous_iterator, "")

        if reject_gibberish:
            temporary_summary = rejection_path.with_suffix(".json.tmp")
            temporary_summary.write_text(
                json.dumps(
                    {
                        "tld": tld,
                        "queue_version": version,
                        "rejected_domains": rejected_gibberish,
                        "by_reason": dict(rejection_reasons.most_common()),
                        "examples": rejection_examples,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            temporary_summary.replace(rejection_path)

        tld_root.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            raise FileExistsError(final_dir)
        build_dir.rename(final_dir)
        now = utc_now()
        snapshot_path = final_dir / "domains.txt.gz"
        added_path = final_dir / "added.txt.gz"
        removed_path = final_dir / "removed.txt.gz"
        if not isinstance(conn, PostgresConnection):
            conn.execute("BEGIN IMMEDIATE")
        for shard, count in enumerate(shard_counts):
            shard_path = final_dir / f"{shard:03d}.txt"
            conn.execute(
                """
                INSERT INTO shards(
                    tld,queue_version,shard,priority,path,
                    byte_offset,total_bytes,
                    queued_domains,updated_at
                ) VALUES (?,?,?,?,?,0,?,?,?)
                """,
                (
                    tld,
                    version,
                    shard,
                    0,
                    str(shard_path),
                    shard_path.stat().st_size,
                    count,
                    now,
                ),
            )
        conn.execute(
            """
            UPDATE zones SET
                import_status='ready',
                imported_at=?,
                discovered_domains=?,
                shard_count=?,
                active_queue_version=?,
                rejected_gibberish=?,
                snapshot_path=?,
                added_domains=?,
                removed_domains=?,
                unchanged_domains=?,
                delta_added_path=?,
                delta_removed_path=?,
                import_error=''
            WHERE tld=?
            """,
            (
                now,
                delegated,
                total_shards,
                version,
                rejected_gibberish,
                str(snapshot_path),
                added_domains,
                removed_domains,
                unchanged_domains,
                str(added_path),
                str(removed_path),
                tld,
            ),
        )
        # One row per zone per Eastern day, so the dashboard can show what
        # yesterday pulled in next to today. The zones table only ever holds
        # the newest import, which makes any day-over-day view impossible.
        conn.execute(
            """
            INSERT INTO zone_daily_deltas(
                tld,snapshot_day,imported_at,added_domains,removed_domains,
                unchanged_domains,discovered_domains,delta_added_path
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(tld,snapshot_day) DO UPDATE SET
                imported_at=excluded.imported_at,
                added_domains=excluded.added_domains,
                removed_domains=excluded.removed_domains,
                unchanged_domains=excluded.unchanged_domains,
                discovered_domains=excluded.discovered_domains,
                delta_added_path=excluded.delta_added_path
            """,
            (
                tld,
                snapshot_day_of(now),
                now,
                added_domains,
                removed_domains,
                unchanged_domains,
                delegated,
                str(added_path),
            ),
        )
        conn.commit()
        log_event(
            conn,
            "zone_imported",
            f"Snapshotted {delegated:,} delegated domains and queued "
            f"{added_domains:,} daily additions in {total_shards} shards; excluded "
            f"{rejected_gibberish:,} obvious generated labels; "
            f"delta +{added_domains:,}/-{removed_domains:,}.",
            tld=tld,
        )
        removed_queue_versions = remove_obsolete_queue_versions(
            conn,
            tld=tld,
            active_version=version,
        )
    except Exception as exc:
        # Roll back first: on Postgres every later statement in an aborted
        # transaction fails, including the orphan check below.
        conn.rollback()
        shutil.rmtree(build_dir, ignore_errors=True)
        # build_dir is renamed to final_dir before any of the database work,
        # so a failure after the rename used to strand a complete queue
        # directory -- snapshot plus 64 shards, hundreds of MB for .com --
        # that no shards row references and no later import ever reclaims.
        # Keep it only once its shard rows are actually committed.
        durable = conn.execute(
            "SELECT 1 FROM shards WHERE tld=? AND queue_version=? LIMIT 1",
            (tld, version),
        ).fetchone()
        if durable is None:
            shutil.rmtree(final_dir, ignore_errors=True)
        conn.execute(
            """
            UPDATE zones SET import_status='error',import_error=?
            WHERE tld=?
            """,
            (f"{type(exc).__name__}: {exc}"[:1000], tld),
        )
        conn.commit()
        raise
    finally:
        conn.close()

    return {
        "tld": tld,
        "zone_path": str(path),
        "queue_version": version,
        "lines_read": lines,
        "delegated_domains": delegated,
        "repeated_records": repeated_records,
        "irrelevant_records": invalid_or_irrelevant,
        "rejected_gibberish": rejected_gibberish,
        "added_domains": added_domains,
        "removed_domains": removed_domains,
        "unchanged_domains": unchanged_domains,
        "snapshot_path": str(final_dir / "domains.txt.gz"),
        "delta_added_path": str(final_dir / "added.txt.gz"),
        "delta_removed_path": str(final_dir / "removed.txt.gz"),
        "rejection_audit": (
            str(rejection_path) if reject_gibberish else ""
        ),
        "shard_count": total_shards,
        "queue_directory": str(final_dir),
        "obsolete_queue_versions_removed": removed_queue_versions,
    }


def discover_zonefiles(root: Path = ZONEFILES) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.iterdir()
        if path.is_file()
        and (
            path.name.endswith(".zone")
            or path.name.endswith(".zone.gz")
            or path.suffix.lower() == ".gz"
        )
    )
