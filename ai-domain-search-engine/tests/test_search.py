from ai_search.database import add_domain, connect, index_page, utc_now
from ai_search.search import SearchOptions, compile_query, search


def seed(conn, domain, title, body, parked=0):
    add_domain(conn, domain, "fixture")
    domain_id = conn.execute(
        "SELECT id FROM domains WHERE domain=?", (domain,)
    ).fetchone()[0]
    conn.execute(
        "UPDATE domains SET latest_crawl_status='success', is_parked=? WHERE id=?",
        (parked, domain_id),
    )
    cursor = conn.execute("""
        INSERT INTO pages(
            domain_id,url,final_url,title,meta_description,headings,
            visible_text,cleaned_text,crawled_at,word_count
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        domain_id, f"https://{domain}", f"https://{domain}", title, "", "",
        body, f"{title} {body}", utc_now(), len(body.split()),
    ))
    index_page(conn, cursor.lastrowid)
    conn.commit()


def test_arbitrary_keyword_phrase_and_exclusion_search(tmp_path):
    conn = connect(tmp_path / "test.sqlite3")
    seed(conn, "quantum-labs.ai", "Compute", "quantum computing for medicine")
    seed(conn, "robots.ai", "Robotics", "robotics for supply chain")
    seed(conn, "school.ai", "Education", "quantum education courses")
    seed(conn, "exact.ai", "Infrastructure", "quantum data infrastructure")
    seed(conn, "separate.ai", "Systems", "quantum systems use data")

    assert [r["domain"] for r in search(conn, "robotics")] == ["robots.ai"]
    assert [r["domain"] for r in search(
        conn, '"quantum computing"'
    )] == ["quantum-labs.ai"]
    assert [r["domain"] for r in search(
        conn, "quantum data"
    )] == ["exact.ai"]
    assert [r["domain"] for r in search(
        conn, "quantum -education"
    )] == ["quantum-labs.ai", "exact.ai", "separate.ai"]
    assert search(conn, "cybersecurity") == []


def test_title_only_and_parked_default(tmp_path):
    conn = connect(tmp_path / "test.sqlite3")
    seed(conn, "body.ai", "Other", "robotics platform")
    seed(conn, "title.ai", "Robotics Company", "software")
    seed(conn, "parked.ai", "Robotics Domain", "domain", parked=1)
    result = search(conn, "robotics", SearchOptions(title_only=True))
    assert [row["domain"] for row in result] == ["title.ai"]
    all_results = search(
        conn, "robotics", SearchOptions(exclude_parked=False)
    )
    assert {row["domain"] for row in all_results} == {
        "body.ai", "title.ai", "parked.ai"
    }


def test_query_compiler_requires_positive_term():
    assert compile_query('supply chain -"job board"') == (
        '"supply chain" NOT "job board"'
    )
    assert compile_query('"supply chain" logistics') == (
        '"supply chain" AND "logistics"'
    )
    assert compile_query("quantum data", title_only=True) == (
        'title:"quantum data"'
    )
