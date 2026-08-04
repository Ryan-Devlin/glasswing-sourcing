from pathlib import Path


def test_hide_control_is_wired_to_every_company_review_page():
    overview = Path("pages/overview.py").read_text(encoding="utf-8")
    dashboard = Path("dashboard.py").read_text(encoding="utf-8")
    ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
    zone = Path("pages/zone_universe.py").read_text(encoding="utf-8")

    assert "render_hidden_domains_manager" not in overview
    assert "render_hideable_table(" in overview
    assert '"Yes": len(yes_basket)' in overview
    assert '"No": len(no_basket)' in overview
    assert "st.tabs(" in overview
    assert 'key="overview_master_tabs"' in overview
    assert "render_hideable_table(" in dashboard
    assert ai_search.count("render_hideable_table(") >= 2
    assert zone.count("render_hideable_table(") >= 2


def test_main_bucket_table_is_the_default_dense_view():
    dashboard = Path("dashboard.py").read_text(encoding="utf-8")
    assert '["Table", "Cards"]' in dashboard
