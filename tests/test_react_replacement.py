from pathlib import Path


def test_react_app_has_every_primary_page_and_no_streamlit_dependency():
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    for label in (
        "Home",
        "CertStream Scraper",
        "CSV Scraper",
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
    for metric in ("Domains available", "Waiting to scrape", "Searchable pages"):
        assert metric in source
    assert "function GlobalWorkerControls" in source
    assert "<GlobalWorkerControls />" in source
    assert "Resume all" in source
    assert "Pause all" in source
    assert "Send to Claude" in source
    assert "/api/claude/review/" in source
    assert "sort.key, sort.direction" in source
    assert "Claude calls per day" in source
    assert "data-tooltip" in source
    assert "Pause CertStream to unlock" in source
    assert "<RunFilters running={Boolean(desiredRunning)} />" in source
    assert source.count("<CompaniesTable />") >= 2
    assert "clamp-text" in source
    assert "Claude’s strongest investment recommendations" in source
    assert "/api/claude/top?limit=6" in source


def test_production_entrypoint_is_fastapi():
    railway = Path("railway.json").read_text(encoding="utf-8")
    assert "uvicorn web_api:app" in railway
    assert '"healthcheckPath": "/api/healthz"' in railway


def test_compiled_react_assets_exist():
    assert Path("webapp/dist/index.html").exists()
    assert any(Path("webapp/dist/assets").glob("*.js"))
    assert any(Path("webapp/dist/assets").glob("*.css"))
