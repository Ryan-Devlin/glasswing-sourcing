from pathlib import Path


def test_page_is_retitled_csv_scraper():
    main = Path("main.py").read_text(encoding="utf-8")
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    assert 'title="CSV Scraper"' in main
    assert 'title="Domain Crawler (.ai)"' not in main
    assert 'st.title("CSV Scraper")' in ai_search


def test_upload_adds_to_the_shared_queue_not_a_separate_store():
    # "If we had 15k domains from a previous dataset and add 20k domains
    # it should show 35k" -- must go through the same shared queue every
    # other source uses (enqueue_for_screening), not a new local table,
    # so uploaded domains get scored by the same Python pipeline and
    # appear in the same Investable/Needs Review/Hard Denied buckets.
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    assert "st.file_uploader(" in ai_search
    assert "enqueue_for_screening(" in ai_search
    assert 'source_run_id="csv_upload"' in ai_search
    assert "screening_queue_connection" in ai_search
    assert "Total domains in system" in ai_search


def test_upload_normalizes_and_dedupes_before_enqueueing():
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    upload_section = ai_search.split("def domain_column_in(", 1)[1]
    assert "stable_domain(value)" in upload_section
    assert "seen_domains" in upload_section


def test_domain_column_detection_accepts_common_header_names():
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    body = ai_search.split("def domain_column_in(", 1)[1].split(
        "\n\n\n", 1
    )[0]
    for candidate in ("domain", "domains", "url", "website", "site"):
        assert f'"{candidate}"' in body
