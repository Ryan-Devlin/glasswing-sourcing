from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from .config import DATABASE
from .database import PostgresConnection, connect, utc_now


DOMAIN = re.compile(
    r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
RDAP_BASES = {
    "app": "https://pubapi.registry.google/rdap/",
    "com": "https://rdap.verisign.com/com/v1/",
}


@dataclass(frozen=True)
class RegistrationRecord:
    domain: str
    tld: str
    registration_at: str = ""
    expiration_at: str = ""
    changed_at: str = ""
    registrar: str = ""
    rdap_status: str = ""
    lookup_status: str = "ok"
    lookup_error: str = ""
    rdap_url: str = ""
    checked_at: str = ""


def normalize_domain(value: str) -> str:
    domain = str(value).strip().lower().rstrip(".")
    if "://" in domain:
        domain = urllib.parse.urlsplit(domain).hostname or ""
    domain = domain.encode("idna").decode("ascii")
    if not DOMAIN.fullmatch(domain):
        raise ValueError(f"Invalid domain: {value}")
    return domain


def rdap_url(domain: str) -> str:
    normalized = normalize_domain(domain)
    tld = normalized.rsplit(".", 1)[-1]
    try:
        base = RDAP_BASES[tld]
    except KeyError as exc:
        raise ValueError(f"RDAP lookup is not configured for .{tld}") from exc
    return f"{base}domain/{urllib.parse.quote(normalized, safe='.-')}"


def _event(payload: dict, action: str) -> str:
    for event in payload.get("events", []):
        if str(event.get("eventAction", "")).lower() == action:
            return str(event.get("eventDate", ""))
    return ""


def _registrar(payload: dict) -> str:
    for entity in payload.get("entities", []):
        roles = {str(role).lower() for role in entity.get("roles", [])}
        if "registrar" not in roles:
            continue
        vcard = entity.get("vcardArray", [])
        if len(vcard) != 2 or not isinstance(vcard[1], list):
            return str(entity.get("handle", ""))
        for item in vcard[1]:
            if (
                isinstance(item, list)
                and len(item) >= 4
                and str(item[0]).lower() == "fn"
            ):
                return str(item[3])
        return str(entity.get("handle", ""))
    return ""


def parse_rdap(
    domain: str,
    payload: dict,
    *,
    source_url: str = "",
    checked_at: str = "",
) -> RegistrationRecord:
    normalized = normalize_domain(domain)
    statuses = payload.get("status", [])
    if not isinstance(statuses, list):
        statuses = [statuses]
    return RegistrationRecord(
        domain=normalized,
        tld=normalized.rsplit(".", 1)[-1],
        registration_at=_event(payload, "registration"),
        expiration_at=_event(payload, "expiration"),
        changed_at=(
            _event(payload, "last changed")
            or _event(payload, "last update of rdap database")
        ),
        registrar=_registrar(payload),
        rdap_status=", ".join(str(value) for value in statuses if value),
        lookup_status="ok",
        rdap_url=source_url,
        checked_at=checked_at or utc_now(),
    )


def lookup_registration(
    domain: str,
    *,
    timeout: float = 10,
    opener: Callable[..., object] = urllib.request.urlopen,
) -> RegistrationRecord:
    normalized = normalize_domain(domain)
    url = rdap_url(normalized)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/rdap+json, application/json",
            "User-Agent": (
                "Glasswing-Zone-Research/0.1 "
                "(low-volume registration-date research)"
            ),
        },
    )
    checked_at = utc_now()
    try:
        with opener(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return parse_rdap(
            normalized,
            payload,
            source_url=url,
            checked_at=checked_at,
        )
    except urllib.error.HTTPError as exc:
        status = (
            "not_found" if exc.code == 404
            else "rate_limited" if exc.code == 429
            else "error"
        )
        error = f"HTTP {exc.code}"
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        status = "error"
        error = str(exc)[:300]
    return RegistrationRecord(
        domain=normalized,
        tld=normalized.rsplit(".", 1)[-1],
        lookup_status=status,
        lookup_error=error,
        rdap_url=url,
        checked_at=checked_at,
    )


def save_registration(
    record: RegistrationRecord,
    *,
    database: Path | str = DATABASE,
) -> None:
    values = asdict(record)
    with connect(database) as conn:
        conn.execute(
            """
            INSERT INTO domain_registration(
                domain,tld,registration_at,expiration_at,changed_at,
                registrar,rdap_status,lookup_status,lookup_error,
                rdap_url,checked_at
            ) VALUES (
                :domain,:tld,:registration_at,:expiration_at,:changed_at,
                :registrar,:rdap_status,:lookup_status,:lookup_error,
                :rdap_url,:checked_at
            )
            ON CONFLICT(domain) DO UPDATE SET
                tld=excluded.tld,
                registration_at=excluded.registration_at,
                expiration_at=excluded.expiration_at,
                changed_at=excluded.changed_at,
                registrar=excluded.registrar,
                rdap_status=excluded.rdap_status,
                lookup_status=excluded.lookup_status,
                lookup_error=excluded.lookup_error,
                rdap_url=excluded.rdap_url,
                checked_at=excluded.checked_at
            """,
            values,
        )


def lookup_and_save(
    domain: str,
    *,
    database: Path | str = DATABASE,
    timeout: float = 10,
) -> RegistrationRecord:
    record = lookup_registration(domain, timeout=timeout)
    save_registration(record, database=database)
    return record


def queue_registrations(
    domains: list[str],
    *,
    database: Path | str = DATABASE,
    priority: int = 50,
) -> int:
    """Queue uncached domains for conservative background RDAP enrichment."""
    normalized = list(
        dict.fromkeys(normalize_domain(domain) for domain in domains)
    )
    if not normalized:
        return 0
    now = utc_now()
    queued = 0
    with connect(database) as conn:
        # SQLite's max() is a scalar function when given 2+ args; Postgres's
        # max() is aggregate-only and has no such overload -- GREATEST() is
        # its scalar equivalent. Never reached until now: this path only
        # runs once the crawl loop's registration backfill actually fires.
        greatest = "GREATEST" if isinstance(conn, PostgresConnection) else "max"
        upsert_sql = f"""
            INSERT INTO registration_queue(
                domain,tld,priority,status,attempts,next_attempt_at,
                last_error,queued_at,updated_at
            ) VALUES (?,?,?,'pending',0,'','',?,?)
            ON CONFLICT(domain) DO UPDATE SET
                priority={greatest}(registration_queue.priority,excluded.priority),
                status=CASE
                    WHEN registration_queue.status='complete'
                    THEN registration_queue.status
                    ELSE 'pending'
                END,
                updated_at=excluded.updated_at
            """
        for domain in normalized:
            cached = conn.execute(
                """
                SELECT 1 FROM domain_registration
                WHERE domain=? AND lookup_status='ok'
                """,
                (domain,),
            ).fetchone()
            if cached is not None:
                continue
            existing = conn.execute(
                "SELECT status FROM registration_queue WHERE domain=?",
                (domain,),
            ).fetchone()
            cursor = conn.execute(
                upsert_sql,
                (
                    domain,
                    domain.rsplit(".", 1)[-1],
                    int(priority),
                    now,
                    now,
                ),
            )
            # cursor.rowcount (not conn.total_changes, a SQLite-only
            # cumulative counter that doesn't exist on PostgresConnection)
            # -- portable across both backends since both return a cursor
            # exposing rowcount from conn.execute().
            queued += int(existing is None and cursor.rowcount > 0)
    return queued


def recover_registration_queue(
    *,
    database: Path | str = DATABASE,
) -> int:
    """Return interrupted lookups to the pending queue after a restart."""
    with connect(database) as conn:
        cursor = conn.execute(
            """
            UPDATE registration_queue
            SET status='pending',updated_at=?
            WHERE status='processing'
            """,
            (utc_now(),),
        )
        return max(0, int(cursor.rowcount))


def claim_registration(
    *,
    database: Path | str = DATABASE,
) -> str:
    now = utc_now()
    with connect(database) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT domain FROM registration_queue
            WHERE status='pending'
              AND (next_attempt_at='' OR next_attempt_at<=?)
            ORDER BY priority DESC,queued_at,domain
            LIMIT 1
            """,
            (now,),
        ).fetchone()
        if row is None:
            conn.commit()
            return ""
        domain = str(row["domain"])
        conn.execute(
            """
            UPDATE registration_queue
            SET status='processing',attempts=attempts+1,updated_at=?
            WHERE domain=?
            """,
            (now, domain),
        )
        conn.commit()
        return domain


def finish_registration(
    record: RegistrationRecord,
    *,
    database: Path | str = DATABASE,
) -> None:
    now = datetime.now(timezone.utc)
    with connect(database) as conn:
        row = conn.execute(
            "SELECT attempts FROM registration_queue WHERE domain=?",
            (record.domain,),
        ).fetchone()
        attempts = int(row["attempts"]) if row is not None else 1
        if record.lookup_status in {"ok", "not_found"}:
            status = "complete"
            retry_at = ""
        elif attempts >= 3:
            status = "failed"
            retry_at = ""
        else:
            status = "pending"
            delay = 60 if record.lookup_status == "rate_limited" else 24
            retry_at = (
                now + timedelta(hours=delay)
            ).isoformat().replace("+00:00", "Z")
        conn.execute(
            """
            UPDATE registration_queue
            SET status=?,next_attempt_at=?,last_error=?,updated_at=?
            WHERE domain=?
            """,
            (
                status,
                retry_at,
                record.lookup_error[:300],
                now.isoformat().replace("+00:00", "Z"),
                record.domain,
            ),
        )


def automatic_lookups_today(
    *,
    database: Path | str = DATABASE,
) -> int:
    start = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
    with connect(database, read_only=True) as conn:
        return int(
            conn.execute(
                """
                SELECT count(*) AS count FROM registration_queue
                WHERE attempts>0 AND updated_at>=?
                """,
                (start,),
            ).fetchone()["count"]
        )


def cached_registrations(
    domains: list[str],
    *,
    database: Path | str = DATABASE,
) -> dict[str, dict]:
    normalized = list(
        dict.fromkeys(normalize_domain(domain) for domain in domains)
    )
    if not normalized:
        return {}
    placeholders = ",".join("?" for _ in normalized)
    with connect(database, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT domain,registration_at,expiration_at,changed_at,
                   registrar,rdap_status,lookup_status,lookup_error,
                   rdap_url,checked_at
            FROM domain_registration
            WHERE domain IN ({placeholders})
            """,
            normalized,
        ).fetchall()
    return {str(row["domain"]): dict(row) for row in rows}
