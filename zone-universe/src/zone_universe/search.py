from __future__ import annotations

import re
import sqlite3
import zlib
from pathlib import Path

from db_backend import PostgresConnection

from .config import DATABASE
from .database import connect
from .html_archive import read_html


TOKEN = re.compile(r'(-?)"([^"]+)"|(-?)([\w.-]+)', re.UNICODE)


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def compile_query(raw: str) -> str:
    positives: list[tuple[str, bool]] = []
    negatives: list[str] = []
    for match in TOKEN.finditer(str(raw).strip()):
        negative = bool(match.group(1) or match.group(3))
        value = (match.group(2) or match.group(4) or "").strip()
        if not value or value == "-":
            continue
        if negative:
            negatives.append(_quote(value))
        else:
            positives.append((value, match.group(2) is not None))
    if not positives:
        raise ValueError("Enter at least one search word or phrase.")
    if len(positives) > 1 and not any(quoted for _, quoted in positives):
        terms = [_quote(" ".join(value for value, _ in positives))]
    else:
        terms = [_quote(value) for value, _ in positives]
    expression = " AND ".join(terms)
    for term in negatives:
        expression += f" NOT {term}"
    return expression


def _parse_terms(raw: str) -> tuple[list[tuple[str, bool]], list[str]]:
    """Shared token parsing behind both compile_query() and its Postgres
    counterpart: same phrase/word/negation grammar either way."""
    positives: list[tuple[str, bool]] = []
    negatives: list[str] = []
    for match in TOKEN.finditer(str(raw).strip()):
        negative = bool(match.group(1) or match.group(3))
        value = (match.group(2) or match.group(4) or "").strip()
        if not value or value == "-":
            continue
        if negative:
            negatives.append(value)
        else:
            positives.append((value, match.group(2) is not None))
    if not positives:
        raise ValueError("Enter at least one search word or phrase.")
    return positives, negatives


def _tsquery_fragment(value: str) -> str:
    """phraseto_tsquery for multi-word terms (word-adjacency, matching
    compile_query()'s "unquoted multi-word == exact phrase" rule below);
    plainto_tsquery for single words. The term is always a bind parameter,
    never spliced into the SQL."""
    return (
        "phraseto_tsquery('english', ?)"
        if " " in value.strip()
        else "plainto_tsquery('english', ?)"
    )


def compile_query_postgres(raw: str) -> tuple[str, list[str]]:
    """Postgres equivalent of compile_query(): same phrase/AND/NOT semantics
    (including "unquoted multi-word input is one exact phrase"), expressed
    with tsquery's `&&`/`!!` operators over parameterized phraseto_tsquery/
    plainto_tsquery calls instead of a hand-built FTS5 MATCH string."""
    positives, negatives = _parse_terms(raw)
    if len(positives) > 1 and not any(quoted for _, quoted in positives):
        terms = [" ".join(value for value, _ in positives)]
    else:
        terms = [value for value, _ in positives]

    params = list(terms)
    expression = " && ".join(_tsquery_fragment(term) for term in terms)
    for term in negatives:
        expression += f" && !!({_tsquery_fragment(term)})"
        params.append(term)
    return expression, params


def _search_pages_postgres(
    conn: PostgresConnection,
    query: str,
    *,
    tld: str,
    limit: int,
    offset: int,
    minimum_words: int,
) -> list[dict]:
    expression, tsquery_params = compile_query_postgres(query)
    where = [f"p.search_vector @@ ({expression})"]
    params: list[object] = list(tsquery_params)
    if tld:
        where.append("p.tld=?")
        params.append(tld.strip().lower().lstrip("."))
    if minimum_words:
        where.append("p.word_count>=?")
        params.append(max(0, int(minimum_words)))
    params.extend([max(1, min(int(limit), 250)), max(0, int(offset))])

    # SQL text order: ts_headline's (expression), ts_rank_cd's (expression),
    # then `params` (which itself starts with the WHERE clause's own copy).
    full_params = tsquery_params + tsquery_params + params
    rows = conn.execute(f"""
        SELECT p.domain,p.tld,p.final_url,p.title,
               p.meta_description AS description,
               ts_headline('english', p.cleaned_text, ({expression}),
                   'StartSel=<mark>, StopSel=</mark>, MaxFragments=1, '
                   'MaxWords=32, MinWords=15, ShortWord=3'
               ) AS snippet,
               p.word_count,p.crawled_at,p.http_status,
               p.is_parked,p.is_coming_soon,
               coalesce(r.registration_at,'') AS registration_at,
               coalesce(r.registrar,'') AS registrar,
               coalesce(r.lookup_status,'') AS registration_lookup_status,
               ts_rank_cd(p.search_vector, ({expression})) AS rank
        FROM pages p
        LEFT JOIN domain_registration r ON r.domain=p.domain
        WHERE {' AND '.join(where)}
        ORDER BY rank DESC
        LIMIT ? OFFSET ?
    """, full_params).fetchall()
    return [dict(row) for row in rows]


def search_pages(
    query: str,
    *,
    tld: str = "",
    limit: int = 100,
    offset: int = 0,
    minimum_words: int = 0,
    database: Path | str = DATABASE,
) -> list[dict]:
    with connect(database, read_only=True) as conn:
        if isinstance(conn, PostgresConnection):
            return _search_pages_postgres(
                conn, query, tld=tld, limit=limit, offset=offset,
                minimum_words=minimum_words,
            )
        expression = compile_query(query)
        where = ["page_search MATCH ?"]
        params: list[object] = [expression]
        if tld:
            where.append("p.tld=?")
            params.append(tld.strip().lower().lstrip("."))
        if minimum_words:
            where.append("p.word_count>=?")
            params.append(max(0, int(minimum_words)))
        params.extend(
            [
                max(1, min(int(limit), 250)),
                max(0, int(offset)),
            ]
        )
        rows = conn.execute(
            f"""
            SELECT p.domain,p.tld,p.final_url,p.title,
                   p.meta_description AS description,
                   snippet(
                       page_search,5,'<mark>','</mark>',' … ',32
                   ) AS snippet,
                   p.word_count,p.crawled_at,p.http_status,
                   p.is_parked,p.is_coming_soon,
                   coalesce(r.registration_at,'') AS registration_at,
                   coalesce(r.registrar,'') AS registrar,
                   coalesce(r.lookup_status,'') AS registration_lookup_status,
                   bm25(page_search,0.0,8.0,5.0,4.0,1.0) AS rank
            FROM page_search
            JOIN pages p
              ON p.id=CAST(page_search.page_id AS INTEGER)
            LEFT JOIN domain_registration r ON r.domain=p.domain
            WHERE {' AND '.join(where)}
            ORDER BY rank
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def raw_html(
    domain: str,
    *,
    database: Path | str = DATABASE,
) -> str:
    normalized = str(domain).strip().lower()
    with connect(database, read_only=True) as conn:
        row = conn.execute(
            """
            SELECT raw_html_zlib,html_archive_path,
                   html_archive_offset,html_archive_length,
                   html_archive_codec
            FROM pages WHERE domain=?
            """,
            (normalized,),
        ).fetchone()
    if row is None:
        return ""
    if row["raw_html_zlib"] is not None:
        return zlib.decompress(row["raw_html_zlib"]).decode(
            "utf-8",
            errors="replace",
        )
    return read_html(
        row["html_archive_path"],
        offset=int(row["html_archive_offset"] or 0),
        length=int(row["html_archive_length"] or 0),
        codec=str(row["html_archive_codec"] or ""),
    )
