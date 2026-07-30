from unittest.mock import patch

import filter as screening
from ai_search.database import add_domain, connect, index_page, utc_now


def test_screening_reuses_saved_crawler_content(tmp_path):
    project_root = tmp_path
    database_path = (
        project_root
        / "ai-domain-search-engine"
        / "data"
        / "search.sqlite3"
    )
    conn = connect(database_path)
    add_domain(conn, "cached.ai", "fixture")
    domain_id = conn.execute(
        "SELECT id FROM domains WHERE domain='cached.ai'"
    ).fetchone()[0]
    cursor = conn.execute(
        """
        INSERT INTO pages(
            domain_id,url,final_url,title,meta_description,headings,
            visible_text,cleaned_text,raw_html,crawled_at,word_count,http_status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            domain_id,
            "https://cached.ai",
            "https://cached.ai",
            "Cached Robotics",
            "Enterprise automation",
            "",
            "Warehouse robotics platform",
            "Cached Robotics Enterprise automation Warehouse robotics platform",
            "<html><body>Warehouse robotics platform</body></html>",
            utc_now(),
            5,
            200,
        ),
    )
    index_page(conn, cursor.lastrowid)
    conn.execute(
        """
        UPDATE domains
        SET latest_crawl_status='success',last_crawled_at=?
        WHERE id=?
        """,
        (utc_now(), domain_id),
    )
    conn.commit()
    conn.close()

    with patch.object(screening, "ROOT", project_root):
        cached = screening.load_crawler_fetches(["cached.ai"])

    assert cached["cached.ai"].source == "crawler_cache"
    assert cached["cached.ai"].title == "Cached Robotics"
    assert "Warehouse robotics" in cached["cached.ai"].text
