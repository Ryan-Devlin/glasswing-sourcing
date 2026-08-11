"""Top-navigation shell for the Glasswing and .AI Search full pages."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from common import sync_training_labels_to_shared
from ui_theme import inject_custom_css, render_brand_header

st.set_page_config(
    page_title="Glasswing Sourcing",
    page_icon=str(Path(__file__).resolve().parent / "assets" / "glasswing_mark.png"),
    layout="wide",
    initial_sidebar_state="collapsed",
)
inject_custom_css()
render_brand_header()


@st.cache_resource(show_spinner=False)
def sync_shared_training_truth_once() -> int:
    return sync_training_labels_to_shared()


try:
    sync_shared_training_truth_once()
except Exception as exc:
    st.warning(
        "Human training labels are safe locally, but their worker sync is "
        f"temporarily unavailable: {type(exc).__name__}."
    )

navigation = st.navigation(
    [
        st.Page(
            "pages/overview.py",
            title="Home",
            default=True,
        ),
        st.Page(
            "dashboard.py",
            title="CertStream Scraper",
        ),
        st.Page(
            "pages/ai_search.py",
            title="Domain Intake",
        ),
        st.Page(
            "pages/zone_universe.py",
            title="Domain Crawler (.com, .app)",
        ),
        st.Page(
            "pages/system_health.py",
            title="System Health",
        ),
    ],
    position="top",
)
navigation.run()
