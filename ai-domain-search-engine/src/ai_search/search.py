from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass

from db_backend import PostgresConnection

TOKEN = re.compile(r'(-?)"([^"]+)"|(-?)([\w.-]+)', re.UNICODE)


@dataclass(frozen=True)
class SearchOptions:
    limit: int = 25
    offset: int = 0
    title_only: bool = False
    active_only: bool = True
    exclude_parked: bool = True
    minimum_words: int = 0
    source: str = ""
    sort: str = "relevance"
    crawl_status: str = ""
    site_type: str = "All active sites"
    domain_contains: str = ""


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def compile_query(raw: str, *, title_only: bool = False) -> str:
    positives: list[tuple[str, bool]] = []
    negatives: list[str] = []
    for match in TOKEN.finditer(raw.strip()):
        negative = bool(match.group(1) or match.group(3))
        value = (match.group(2) or match.group(4) or "").strip()
        if not value or value == "-":
            continue
        if negative:
            term = _quote(value)
            if title_only:
                term = f"title:{term}"
            negatives.append(term)
        else:
            positives.append((value, match.group(2) is not None))
    if not positives:
        raise ValueError("Enter at least one search word or phrase.")

    # Treat ordinary multi-word input as an exact phrase. Explicit quotes retain
    # advanced grouping, e.g. `"quantum data" security` means phrase AND word.
    if len(positives) > 1 and not any(quoted for _, quoted in positives):
        positive_terms = [_quote(" ".join(value for value, _ in positives))]
    else:
        positive_terms = [_quote(value) for value, _ in positives]
    if title_only:
        positive_terms = [f"title:{term}" for term in positive_terms]

    expression = " AND ".join(positive_terms)
    for term in negatives:
        expression += f" NOT {term}"
    return expression


def _parse_terms(raw: str) -> tuple[list[tuple[str, bool]], list[str]]:
    """Shared token parsing behind both compile_query() and its Postgres
    counterpart: same phrase/word/negation grammar either way."""
    positives: list[tuple[str, bool]] = []
    negatives: list[str] = []
    for match in TOKEN.finditer(raw.strip()):
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
    plainto_tsquery for single words. The term itself is always a bind
    parameter, never spliced into the SQL -- Postgres's own parser handles
    tokenization/stemming, so arbitrary search text can't affect tsquery
    syntax."""
    return (
        "phraseto_tsquery('english', ?)"
        if " " in value.strip()
        else "plainto_tsquery('english', ?)"
    )


def compile_query_postgres(raw: str) -> tuple[str, list[str]]:
    """Postgres equivalent of compile_query(): same phrase/AND/NOT semantics
    (including "unquoted multi-word input is one exact phrase"), expressed
    with tsquery's `&&`/`!!` operators over parameterized phraseto_tsquery/
    plainto_tsquery calls instead of a hand-built FTS5 MATCH string.

    Does not support title_only -- unused by the live app (pages/ai_search.py
    and og_bridge.search_companies never set it), and a single combined
    search_vector has no per-column MATCH restriction the way FTS5's
    `title:"..."` prefix does.
    """
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


def _search_postgres(
    conn: PostgresConnection, query: str, options: SearchOptions
) -> list[dict]:
    expression, tsquery_params = compile_query_postgres(query)
    where = [f"p.search_vector @@ ({expression})"]
    params: list[object] = list(tsquery_params)
    if options.active_only:
        where.append("d.active=1")
    if options.exclude_parked:
        where.append("d.is_parked=0")
    if options.minimum_words:
        where.append("p.word_count>=?")
        params.append(options.minimum_words)
    if options.source:
        where.append("d.discovery_source=?")
        params.append(options.source)
    if options.crawl_status:
        where.append("d.latest_crawl_status=?")
        params.append(options.crawl_status)
    if options.site_type == "Coming soon":
        where.append("d.is_coming_soon=1")
    elif options.site_type == "Established content":
        where.append("d.is_coming_soon=0")
        where.append("p.word_count>=50")
    elif options.site_type == "Parked":
        where.append("d.is_parked=1")
    if options.domain_contains.strip():
        where.append("strpos(d.domain, ?) > 0")
        params.append(options.domain_contains.strip().lower())
    order = {
        "newest": "d.first_seen_at DESC",
        "recently crawled": "p.crawled_at DESC",
        # ts_rank_cd is the opposite convention from bm25: higher = more
        # relevant, so this is DESC where the SQLite path below is ASC.
    }.get(options.sort.lower(), "rank DESC")
    params.extend([max(1, min(options.limit, 200)), max(0, options.offset)])

    # SQL text order is: ts_headline's (expression), then ts_rank_cd's
    # (expression), then everything in `params` (which itself starts with
    # the WHERE clause's own (expression) copy).
    full_params = tsquery_params + tsquery_params + params
    rows = conn.execute(f"""
        SELECT d.domain, p.final_url, p.title,
               p.meta_description AS description,
               ts_headline('english', p.cleaned_text, ({expression}),
                   'StartSel=<mark>, StopSel=</mark>, MaxFragments=1, '
                   'MaxWords=28, MinWords=15, ShortWord=3'
               ) AS snippet,
               p.word_count, p.crawled_at, d.latest_crawl_status,
               d.discovery_source, d.is_coming_soon,
               ts_rank_cd(p.search_vector, ({expression})) AS rank
        FROM pages p
        JOIN domains d ON d.id=p.domain_id
        WHERE {' AND '.join(where)}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """, full_params).fetchall()
    return [dict(row) for row in rows]


def search(
    conn: sqlite3.Connection | PostgresConnection,
    query: str,
    options: SearchOptions | None = None,
) -> list[dict]:
    options = options or SearchOptions()
    if isinstance(conn, PostgresConnection):
        return _search_postgres(conn, query, options)
    expression = compile_query(query, title_only=options.title_only)
    where = ["page_search MATCH ?"]
    params: list[object] = [expression]
    if options.active_only:
        where.append("d.active=1")
    if options.exclude_parked:
        where.append("d.is_parked=0")
    if options.minimum_words:
        where.append("p.word_count>=?")
        params.append(options.minimum_words)
    if options.source:
        where.append("d.discovery_source=?")
        params.append(options.source)
    if options.crawl_status:
        where.append("d.latest_crawl_status=?")
        params.append(options.crawl_status)
    if options.site_type == "Coming soon":
        where.append("d.is_coming_soon=1")
    elif options.site_type == "Established content":
        where.append("d.is_coming_soon=0")
        where.append("p.word_count>=50")
    elif options.site_type == "Parked":
        where.append("d.is_parked=1")
    if options.domain_contains.strip():
        where.append("instr(d.domain, ?) > 0")
        params.append(options.domain_contains.strip().lower())
    order = {
        "newest": "d.first_seen_at DESC",
        "recently crawled": "p.crawled_at DESC",
    }.get(options.sort.lower(), "rank ASC")
    params.extend([max(1, min(options.limit, 200)), max(0, options.offset)])
    rows = conn.execute(f"""
        SELECT d.domain, p.final_url, p.title,
               p.meta_description AS description,
               snippet(page_search, 5, '<mark>', '</mark>', ' … ', 28) AS snippet,
               p.word_count, p.crawled_at, d.latest_crawl_status,
               d.discovery_source, d.is_coming_soon, bm25(
                   page_search, 0.0, 8.0, 5.0, 4.0, 1.0
               ) AS rank
        FROM page_search
        JOIN pages p ON p.id=CAST(page_search.page_id AS INTEGER)
        JOIN domains d ON d.id=p.domain_id
        WHERE {' AND '.join(where)}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """, params).fetchall()
    return [dict(row) for row in rows]
