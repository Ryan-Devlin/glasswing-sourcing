"""Read-only search plus explicit handoff into the parent screening queue."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .search import SearchOptions, search


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def search_connection(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def search_companies(
    search_db: Path,
    query: str,
    *,
    limit: int = 100,
    offset: int = 0,
    minimum_words: int = 0,
    site_type: str = "All active sites",
    exclude_parked: bool = True,
) -> list[dict]:
    conn = search_connection(search_db)
    try:
        return search(conn, query, SearchOptions(
            limit=limit,
            offset=offset,
            minimum_words=minimum_words,
            site_type=site_type,
            exclude_parked=exclude_parked,
        ))
    finally:
        conn.close()


def search_stats(search_db: Path) -> dict[str, int]:
    conn = search_connection(search_db)
    try:
        queries = {
            "domains": "SELECT count(*) FROM domains",
            "crawled": (
                "SELECT count(*) FROM domains "
                "WHERE last_crawled_at IS NOT NULL"
            ),
            "indexed_pages": "SELECT count(*) FROM page_search",
            "parked": "SELECT count(*) FROM domains WHERE is_parked=1",
            "coming_soon": (
                "SELECT count(*) FROM domains WHERE is_coming_soon=1"
            ),
            "homepage_only": (
                "SELECT count(*) FROM ("
                "SELECT domain_id FROM pages "
                "GROUP BY domain_id HAVING count(*)=1"
                ")"
            ),
            "evidence_enriched": (
                "SELECT count(*) FROM ("
                "SELECT domain_id FROM pages "
                "GROUP BY domain_id HAVING count(*)>1"
                ")"
            ),
        }
        return {
            name: int(conn.execute(sql).fetchone()[0])
            for name, sql in queries.items()
        }
    finally:
        conn.close()


def screenable_domains(
    search_db: Path,
    *,
    limit: int = 0,
    minimum_words: int = 1,
    include_parked: bool = False,
    exclude_queue_db: Path | None = None,
    include_stale: bool = False,
) -> list[str]:
    conn = search_connection(search_db)
    try:
        if exclude_queue_db is not None:
            conn.execute(
                "ATTACH DATABASE ? AS screening_queue",
                (str(exclude_queue_db),),
            )
        where = [
            "d.active=1",
            "d.last_crawled_at IS NOT NULL",
            "EXISTS (SELECT 1 FROM pages p "
            "WHERE p.domain_id=d.id AND p.word_count>=?)",
        ]
        params: list[object] = [minimum_words]
        if not include_parked:
            where.append("d.is_parked=0")
        if exclude_queue_db is not None:
            unknown = (
                "NOT EXISTS (SELECT 1 FROM screening_queue.domains q "
                "WHERE q.domain=d.domain)"
            )
            if include_stale:
                stale = """
                    EXISTS (
                        SELECT 1 FROM screening_queue.domains q
                        WHERE q.domain=d.domain
                          AND q.status='complete'
                          AND q.classification IN (
                              'ghost','insufficient','screened_out',''
                          )
                          AND (
                              q.last_processed_at IS NULL
                              OR d.last_crawled_at > q.last_processed_at
                              OR (
                                  q.classification='ghost'
                                  AND coalesce(q.last_seen_run_id, '') !=
                                      'ai_search_cache_reuse_v1'
                              )
                          )
                    )
                """
                where.append(f"({unknown} OR {stale})")
            else:
                where.append(unknown)
        limit_sql = " LIMIT ?" if limit > 0 else ""
        if limit > 0:
            params.append(limit)
        return [
            str(row[0])
            for row in conn.execute(
                f"SELECT d.domain FROM domains d "
                f"WHERE {' AND '.join(where)} "
                f"ORDER BY d.last_crawled_at{limit_sql}",
                params,
            )
        ]
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, schema: str, table: str) -> bool:
    prefix = f"{schema}." if schema != "main" else ""
    return (
        conn.execute(
            f"SELECT 1 FROM {prefix}sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is not None
    )


def pipeline_status_for_corpus(
    search_db: Path,
    queue_db: Path,
    *,
    crawled_only: bool = False,
) -> dict[str, int]:
    # A brand-new deployment can query this before the pipeline queue (or even
    # the search corpus) has ever been created -- both are ordinary
    # `CREATE TABLE IF NOT EXISTS` databases owned elsewhere (continuous_pipeline.py,
    # ai_search/database.py), so an absent table here means "nothing yet", not
    # an error to surface to the dashboard.
    conn = sqlite3.connect(queue_db, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("ATTACH DATABASE ? AS search_corpus", (str(search_db),))
        if not _table_exists(conn, "search_corpus", "domains"):
            return {"in_screening_registry": 0, "not_queued": 0}

        crawl_filter = (
            "AND s.last_crawled_at IS NOT NULL" if crawled_only else ""
        )
        queue_ready = _table_exists(conn, "main", "domains")
        result: dict[str, int] = {}
        if queue_ready:
            rows = conn.execute(f"""
                SELECT q.status, count(*) AS count
                FROM domains q
                JOIN search_corpus.domains s ON s.domain=q.domain
                WHERE 1=1 {crawl_filter}
                GROUP BY q.status
            """).fetchall()
            result = {str(row["status"]): int(row["count"]) for row in rows}
        result["in_screening_registry"] = sum(result.values())
        if queue_ready:
            result["not_queued"] = int(conn.execute(f"""
                SELECT count(*) FROM search_corpus.domains s
                WHERE 1=1 {crawl_filter}
                AND
                NOT EXISTS (
                    SELECT 1 FROM domains q WHERE q.domain=s.domain
                )
            """).fetchone()[0])
        else:
            result["not_queued"] = int(conn.execute(f"""
                SELECT count(*) FROM search_corpus.domains s WHERE 1=1 {crawl_filter}
            """).fetchone()[0])
        return result
    finally:
        conn.close()


def all_discovered_domains(search_db: Path) -> list[str]:
    conn = search_connection(search_db)
    try:
        return [
            str(row[0])
            for row in conn.execute("""
                SELECT domain FROM domains
                WHERE active=1
                ORDER BY first_seen_at, domain
            """)
        ]
    finally:
        conn.close()


def enqueue_for_screening(
    queue_db: Path,
    domains: list[str],
    *,
    source_run_id: str = "ai_search_import",
    requeue_existing: bool = False,
) -> dict[str, int]:
    """Insert only unknown domains; preserve every existing queue record."""
    if not domains:
        return {"requested": 0, "queued": 0, "already_known": 0}
    conn = sqlite3.connect(queue_db, timeout=30)
    now = utc_now()
    queued = 0
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN IMMEDIATE")
        for domain in dict.fromkeys(domains):
            if requeue_existing:
                cursor = conn.execute("""
                    INSERT INTO domains(
                        domain, first_seen, last_seen, status, attempts,
                        next_attempt_at, seen_count, first_seen_run_id,
                        last_seen_run_id
                    )
                    VALUES (?, ?, ?, 'pending', 0, ?, 1, ?, ?)
                    ON CONFLICT(domain) DO UPDATE SET
                        last_seen=excluded.last_seen,
                        status='pending',
                        attempts=0,
                        next_attempt_at=excluded.next_attempt_at,
                        last_error='',
                        seen_count=domains.seen_count+1,
                        last_seen_run_id=excluded.last_seen_run_id
                """, (
                    domain, now, now, now, source_run_id, source_run_id,
                ))
            else:
                cursor = conn.execute("""
                    INSERT INTO domains(
                        domain, first_seen, last_seen, status, attempts,
                        next_attempt_at, seen_count, first_seen_run_id,
                        last_seen_run_id
                    )
                    VALUES (?, ?, ?, 'pending', 0, ?, 1, ?, ?)
                    ON CONFLICT(domain) DO NOTHING
                """, (
                    domain, now, now, now, source_run_id, source_run_id,
                ))
            queued += int(cursor.rowcount == 1)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "requested": len(set(domains)),
        "queued": queued,
        "already_known": len(set(domains)) - queued,
    }
