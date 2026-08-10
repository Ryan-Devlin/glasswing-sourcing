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


def test_production_entrypoint_is_fastapi():
    railway = Path("railway.json").read_text(encoding="utf-8")
    assert "uvicorn web_api:app" in railway
    assert '"healthcheckPath": "/api/healthz"' in railway


def test_compiled_react_assets_exist():
    assert Path("webapp/dist/index.html").exists()
    assert any(Path("webapp/dist/assets").glob("*.js"))
    assert any(Path("webapp/dist/assets").glob("*.css"))
