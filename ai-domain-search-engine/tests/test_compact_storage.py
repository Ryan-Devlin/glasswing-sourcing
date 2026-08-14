from dataclasses import replace
from types import SimpleNamespace

from ai_search.crawler import _save_page
from ai_search.database import add_domain, connect, index_page, utc_now
from ai_search.extract import extract_page
from ai_search.html_archive import read_html
from ai_search.maintenance import compact_existing_pages
from ai_search.search import search


def seed_page(conn, domain: str, html: str, *, parked: bool = False) -> int:
    add_domain(conn, domain, "fixture")
    domain_id = conn.execute(
        "SELECT id FROM domains WHERE domain=?",
        (domain,),
    ).fetchone()[0]
    conn.execute(
        "UPDATE domains SET is_parked=? WHERE id=?",
        (int(parked), domain_id),
    )
    cursor = conn.execute(
        """
        INSERT INTO pages(
            domain_id,url,final_url,title,meta_description,headings,
            visible_text,cleaned_text,raw_html,crawled_at,word_count
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            domain_id,
            f"https://{domain}",
            f"https://{domain}",
            "Quantum Data",
            "Infrastructure for scientific teams",
            "Platform",
            "Quantum data infrastructure",
            "Quantum data infrastructure for scientific teams",
            html,
            utc_now(),
            7,
        ),
    )
    index_page(conn, cursor.lastrowid)
    conn.commit()
    return int(cursor.lastrowid)


def test_compaction_preserves_search_and_archives_live_html(tmp_path):
    database = tmp_path / "search.sqlite3"
    archive = tmp_path / "html"
    conn = connect(database)
    page_id = seed_page(
        conn,
        "example.ai",
        "<html><body>Quantum data infrastructure</body></html>",
    )

    result = compact_existing_pages(
        conn,
        database=database,
        archive_root=archive,
        make_backup=False,
        vacuum=False,
    )

    row = conn.execute(
        """
        SELECT raw_html,visible_text,cleaned_text,html_archive_path,
               html_archive_offset,html_archive_length,html_archive_codec
        FROM pages WHERE id=?
        """,
        (page_id,),
    ).fetchone()
    assert result["pages_processed"] == 1
    assert row["raw_html"] == ""
    assert row["visible_text"] == ""
    assert "Quantum data" in row["cleaned_text"]
    assert "Quantum data" in read_html(
        row["html_archive_path"],
        offset=row["html_archive_offset"],
        length=row["html_archive_length"],
        codec=row["html_archive_codec"],
    )
    assert [item["domain"] for item in search(conn, "quantum data")] == [
        "example.ai"
    ]


def test_compaction_discards_parked_html_but_keeps_search_evidence(tmp_path):
    database = tmp_path / "search.sqlite3"
    conn = connect(database)
    page_id = seed_page(
        conn,
        "forsale.ai",
        "<html><body>This domain is for sale</body></html>",
        parked=True,
    )

    result = compact_existing_pages(
        conn,
        database=database,
        archive_root=tmp_path / "html",
        make_backup=False,
        vacuum=False,
    )

    row = conn.execute(
        "SELECT raw_html,html_archive_path FROM pages WHERE id=?",
        (page_id,),
    ).fetchone()
    assert result["templates_discarded"] == 1
    assert row["raw_html"] == ""
    assert row["html_archive_path"] == ""


def test_future_crawls_never_retain_database_or_archived_html(tmp_path):
    database = tmp_path / "search.sqlite3"
    archive = tmp_path / "html"
    conn = connect(database)
    add_domain(conn, "new.ai", "fixture")
    domain_id = conn.execute(
        "SELECT id FROM domains WHERE domain='new.ai'"
    ).fetchone()[0]
    html = """
        <html><head><title>Cyber Platform</title></head>
        <body><h1>Enterprise identity security infrastructure</h1>
        <p>Our production platform protects workforce identities, cloud
        applications, and critical access workflows for security teams.</p>
        <p>Customers deploy policy automation and continuous threat detection
        across their enterprise infrastructure.</p></body></html>
    """
    page = replace(
        extract_page(html, "https://new.ai"),
        is_coming_soon=False,
    )
    page_id = _save_page(
        conn,
        domain_id,
        "https://new.ai",
        "https://new.ai",
        200,
        "text/html",
        html,
        page,
        SimpleNamespace(
            retain_raw_html=True,
            html_archive=archive,
        ),
        "new.ai",
    )
    index_page(conn, page_id)
    conn.commit()

    row = conn.execute(
        """
        SELECT raw_html,visible_text,cleaned_text,html_archive_path,
               compressed_html_bytes
        FROM pages WHERE id=?
        """,
        (page_id,),
    ).fetchone()
    assert row["raw_html"] == ""
    assert row["visible_text"] == ""
    assert "identity security" in row["cleaned_text"]
    assert row["html_archive_path"] == ""
    assert row["compressed_html_bytes"] == 0
    assert [item["domain"] for item in search(conn, "identity security")] == [
        "new.ai"
    ]
