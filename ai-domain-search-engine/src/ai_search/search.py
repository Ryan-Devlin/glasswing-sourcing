from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass

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


def search(
    conn: sqlite3.Connection,
    query: str,
    options: SearchOptions | None = None,
) -> list[dict]:
    options = options or SearchOptions()
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
