from __future__ import annotations

import re
import sqlite3
import zlib
from pathlib import Path

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


def search_pages(
    query: str,
    *,
    tld: str = "",
    limit: int = 100,
    offset: int = 0,
    minimum_words: int = 0,
    database: Path | str = DATABASE,
) -> list[dict]:
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
    with connect(database, read_only=True) as conn:
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
