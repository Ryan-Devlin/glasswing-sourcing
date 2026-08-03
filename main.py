"""Top-navigation shell for the Glasswing and .AI Search full pages."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from ui_theme import inject_custom_css, render_brand_header

st.set_page_config(
    page_title="Glasswing Sourcing",
    page_icon=str(Path(__file__).resolve().parent / "assets" / "glasswing_mark.png"),
    layout="wide",
    initial_sidebar_state="collapsed",
)
inject_custom_css()
render_brand_header()

navigation = st.navigation(
    [
        st.Page(
            "pages/overview.py",
            title="Overview",
            icon="🏠",
            default=True,
        ),
        st.Page(
            "dashboard.py",
            title="Glasswing Sourcing",
            icon="🪽",
        ),
        st.Page(
            "pages/ai_search.py",
            title=".AI Search",
            icon="🔎",
        ),
        st.Page(
            "pages/zone_universe.py",
            title="Zone Universe",
            icon="🌐",
        ),
        st.Page(
            "pages/system_health.py",
            title="System Health",
            icon="🩺",
        ),
    ],
    position="top",
)
navigation.run()
