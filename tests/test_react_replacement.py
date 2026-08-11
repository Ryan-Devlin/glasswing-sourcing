from pathlib import Path


def test_react_app_has_every_primary_page_and_no_streamlit_dependency():
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    for label in (
        "Home",
        "CertStream Scraper",
        "Form D Deal Flow",
        "Domain Intake",
        "Domain Crawler (.com, .app)",
        "System Health",
    ):
        assert label in source
    assert "streamlit" not in source.lower()
    for bucket in ("Investable", "Needs Review", "Hard Denied", "YES", "NO"):
        assert bucket in source
    assert 'aria-label="Classification bucket"' not in source
    assert 'aria-label="Crawler bucket"' not in source
    assert "Array.isArray(result.data?.rows)" in source
    assert '"/api/ai/domain"' in source
    assert "Run through platform" in source
    for metric in ("Domains available", "Waiting to scrape", "Searchable pages"):
        assert metric in source
    for zone_metric in ("Investable retained", "Discarded", "Fetch failures"):
        # Legacy corpus counters were replaced with deal-funnel metrics.
        assert zone_metric not in source
    for zone_metric in (
        "Domains screened",
        "Live websites",
        "Relevant companies",
        "Investable deals",
        "Qualified candidates",
            "Average screening speed",
    ):
        assert zone_metric in source
    assert "Deal-first mode · qualified-candidate storage only" not in source
    assert "controlCrawler" in source
    assert "function GlobalWorkerControls" in source
    assert "<GlobalWorkerControls />" in source
    assert "function FormDPage" in source
    assert "/api/form-d/control" in source
    assert '["formd", "Form D Deal Flow"' not in source
    assert "formd: <FormDPage />" in source
    assert "sort=Recently%20detected" in source
    assert "<ClaudeTopPicks />" not in source
    assert "[502, 503, 504].includes(r.status)" in source
    assert '`Meeting · ${meetingCount}`' in source
    assert 'sessionStorage.setItem("landscape_prefill", row.domain)' in source
    assert "Resume all" in source
    assert "Pause all" in source
    assert "Send to Claude" in source
    assert "Queued for Claude" in source
    assert "Claude processing" in source
    assert "claude_queue_status" in source
    assert "Positive diligence signals" in source
    assert "GreenFlagTags" in source
    assert "/api/claude/review/" in source
    assert "sort.key, sort.direction" in source
    assert "Claude calls per day" in source
    assert "claude-tooltip" in source
    assert "Pause CertStream to unlock" in source
    assert "<RunFilters running={Boolean(desiredRunning)} />" in source
    assert source.count("<CompaniesTable />") >= 2
    assert "clamp-text" in source
    assert "Claude’s strongest investment recommendations" in source
    # Fetch enough candidates to backfill the eight-card view after tracked
    # companies are moved out of the recommendation grid.
    assert "/api/claude/top?limit=20" in source
    # Background action refreshes must retain mounted content so the document
    # does not collapse and force the user's scroll position toward the top.
    assert "deals.loading && !deals.data" in source
    assert "result.loading && !result.data" in source
    assert "set((current) => ({" in source
    assert "responseCache" in source
    assert "<DeferredContent minHeight={420}>" in source
    assert "Qualified new deal candidates" in source
    assert "/api/corpus/zone/candidates?limit=25" in source
    assert 'kind === "zone" ? [' in source

    styles = Path("webapp/src/polish.css").read_text(encoding="utf-8")
    assert "fonts.googleapis.com" not in styles
    assert "@font-face" in styles
    assert len(list(Path("webapp/src/fonts").glob("*.woff2"))) == 3


def test_production_entrypoint_is_fastapi():
    railway = Path("railway.json").read_text(encoding="utf-8")
    assert "uvicorn web_api:app" in railway
    assert '"healthcheckPath": "/api/healthz"' in railway


def test_compiled_react_assets_exist():
    assert Path("webapp/dist/index.html").exists()
    assert any(Path("webapp/dist/assets").glob("*.js"))
    assert any(Path("webapp/dist/assets").glob("*.css"))
