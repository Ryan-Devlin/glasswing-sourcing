from pathlib import Path


def test_page_is_retitled_domain_intake():
    main = Path("main.py").read_text(encoding="utf-8")
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    assert 'title="Domain Intake"' in main
    assert 'title="Domain Crawler (.ai)"' not in main
    assert 'st.title("Domain Intake")' in ai_search


def test_upload_adds_to_the_shared_queue_and_this_pages_own_discovered_count():
    # Must go through the same shared queue every other source uses
    # (enqueue_for_screening), so uploaded domains get scored by the same
    # Python pipeline and appear in the same Investable/Needs Review/Hard
    # Denied buckets. Also must register in this corpus's own
    # Discovered/Coverage count (add_domain) -- that's the number this
    # page actually displays, not a separate "total in system" figure.
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    assert "st.file_uploader(" in ai_search
    assert "enqueue_for_screening(" in ai_search
    assert 'source_run_id="csv_upload"' in ai_search
    assert "add_domain(discover_conn, domain" in ai_search
    assert "Total domains in system" not in ai_search


def test_upload_normalizes_and_dedupes_before_enqueueing():
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    upload_section = ai_search.split("def domain_column_in(", 1)[1]
    assert "stable_domain(value)" in upload_section
    assert "seen_domains" in upload_section


def test_upload_accepts_plain_txt_domain_lists_too():
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    assert '"csv", "txt"' in ai_search
    upload_section = ai_search.split("def domain_column_in(", 1)[1]
    assert 'endswith(".txt")' in upload_section


def test_domain_column_detection_accepts_common_header_names():
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    body = ai_search.split("def domain_column_in(", 1)[1].split(
        "\n\n\n", 1
    )[0]
    for candidate in ("domain", "domains", "url", "website", "site"):
        assert f'"{candidate}"' in body
