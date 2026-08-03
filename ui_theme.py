"""Shared visual polish on top of .streamlit/config.toml's color theme.

Config.toml sets the palette; this handles the handful of things Streamlit's
theming API doesn't expose -- default chrome that reads as "generic AI tool"
(the Streamlit footer/deploy button), and turning st.metric into an actual
bordered KPI tile instead of bare text.
"""

from __future__ import annotations

import streamlit as st

_CSS = """
<style>
/* Streamlit's own footer and the Deploy button are noise on an internal
   tool -- nobody here is deploying from the running app. */
footer, [data-testid="stAppDeployButton"] { display: none; }
#MainMenu { visibility: hidden; }

/* The default top decoration bar is a loud gradient; a hairline in the
   theme's own border color reads as an intentional divider instead. */
div[data-testid="stDecoration"] {
    background: var(--border-color, rgba(11, 11, 11, 0.10));
    height: 1px;
}

/* st.metric ships as bare text; wrapping it as a bordered tile is what
   actually makes a row of metrics read as a KPI row instead of a list. */
div[data-testid="stMetric"] {
    background: var(--secondary-background-color, #f0efec);
    border: 1px solid var(--border-color, rgba(11, 11, 11, 0.10));
    border-radius: 0.5rem;
    padding: 0.9rem 1rem 0.7rem;
}
div[data-testid="stMetricLabel"] {
    font-weight: 500;
    opacity: 0.75;
}

/* Streamlit's default block spacing reads sparse/blog-like at this
   density of widgets; tighten it slightly toward an app-like feel. */
div[data-testid="stVerticalBlock"] { gap: 0.6rem; }

/* Top navigation: give the active page real visual weight instead of the
   default thin underline-only distinction. */
div[data-testid="stTabs"] button[aria-selected="true"] p {
    font-weight: 600;
}
</style>
"""


def inject_custom_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
