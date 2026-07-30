"""Top-navigation shell for the Glasswing and .AI Search full pages."""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="Glasswing Sourcing",
    page_icon="🪽",
    layout="wide",
    initial_sidebar_state="collapsed",
)

navigation = st.navigation(
    [
        st.Page(
            "dashboard.py",
            title="Glasswing Sourcing",
            icon="🪽",
            default=True,
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
