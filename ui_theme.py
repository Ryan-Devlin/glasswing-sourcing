"""Shared visual polish on top of .streamlit/config.toml's color theme.

Config.toml sets the palette; this handles the handful of things Streamlit's
theming API doesn't expose -- default chrome that reads as "generic AI tool"
(the Streamlit footer/deploy button), turning st.metric into an actual
bordered KPI tile instead of bare text, the branded header (moth logo), and
the deal-card grid used to display companies/pages across the app.
"""

from __future__ import annotations

import base64
import html
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent
LOGO_SVG = ROOT / "assets" / "glasswing_logo.svg"

_CSS = """
<style>
/* Streamlit's own footer and the Deploy button are noise on an internal
   tool -- nobody here is deploying from the running app. */
footer, [data-testid="stAppDeployButton"] { display: none; }
#MainMenu { visibility: hidden; }

/* Streamlit's top nav (position="top") is a fixed-position overlay, not
   flow content -- it's ~60px tall with a very high z-index. Too little
   top padding here doesn't create a "gap", it renders page content
   (including the logo) underneath that fixed bar, invisible despite
   technically being in the DOM. Too much (Streamlit's own default)
   leaves a large dead zone. This clears the fixed nav with a small
   margin, no more. */
div[data-testid="stMainBlockContainer"] { padding-top: 4.5rem !important; }

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

/* Serif display face for card titles only -- everything else in the app
   stays on the sans-serif body font from config.toml. */
@import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600&display=swap');

.gw-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.25rem 0 0.9rem;
    margin-bottom: 0.75rem;
    border-bottom: 1px solid var(--border-color, rgba(11, 11, 11, 0.10));
}
.gw-header img { height: 28px; display: block; }
.gw-avatar {
    width: 34px;
    height: 34px;
    border-radius: 50%;
    background: #111;
    color: #fff;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.85rem;
    font-weight: 600;
    flex-shrink: 0;
}

.gw-card-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 1rem;
    margin: 0.5rem 0 1.25rem;
}
@media (max-width: 900px) {
    .gw-card-grid { grid-template-columns: 1fr; }
}
.gw-card {
    background: var(--card-background-color, #fcfcfb);
    border: 1px solid var(--border-color, rgba(11, 11, 11, 0.10));
    border-radius: 14px;
    padding: 1.1rem 1.25rem 1rem;
}
.gw-card-top {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.5rem;
}
a.gw-card-title, a.gw-card-title:link, a.gw-card-title:visited,
span.gw-card-title {
    font-family: 'Fraunces', Georgia, serif !important;
    font-weight: 600;
    font-size: 1.55rem;
    line-height: 1.15;
    color: var(--text-color, #0b0b0b) !important;
    text-decoration: none !important;
}
a.gw-card-title:hover { text-decoration: underline !important; }
.gw-card-meta {
    color: var(--primary-color, #2a78d6);
    font-size: 0.88rem;
    margin: 0.3rem 0 0.7rem;
}
.gw-card-tags { margin-bottom: 0.65rem; line-height: 1.9; }
.gw-tag {
    display: inline-block;
    border: 1px solid var(--primary-color, #2a78d6);
    color: var(--primary-color, #2a78d6);
    border-radius: 8px;
    font-size: 0.78rem;
    padding: 0.12rem 0.55rem;
    margin: 0 0.35rem 0.35rem 0;
}
.gw-card-people, .gw-card-roles {
    font-size: 0.85rem;
    opacity: 0.85;
    margin-bottom: 0.35rem;
}
.gw-card-divider {
    border: none;
    border-top: 1px solid var(--border-color, rgba(11, 11, 11, 0.10));
    margin: 0.7rem 0 0.55rem;
}
.gw-activity-row {
    display: flex;
    justify-content: space-between;
    gap: 0.75rem;
    font-size: 0.85rem;
    padding: 0.18rem 0;
    white-space: nowrap;
    overflow: hidden;
}
.gw-activity-row span:first-child {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.gw-activity-time { opacity: 0.55; flex-shrink: 0; }
.gw-card-footer {
    margin-top: 0.6rem;
    font-size: 0.85rem;
    color: var(--primary-color, #2a78d6);
    display: flex;
    gap: 1.1rem;
}
</style>
"""


def _logo_data_uri() -> str:
    svg_bytes = LOGO_SVG.read_bytes()
    encoded = base64.b64encode(svg_bytes).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


def inject_custom_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def render_brand_header(*, avatar_initial: str = "R") -> None:
    """The moth logo + a user-avatar chip, mirroring Glasswing Ventures'
    own deal-board header. Call once near the top of every page."""
    st.markdown(
        f"""
        <div class="gw-header">
            <img src="{_logo_data_uri()}" alt="Glasswing Ventures" />
            <div class="gw-avatar">{html.escape(avatar_initial)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_card_grid(cards: list[dict]) -> None:
    """Render a grid of deal-board-style cards.

    Each card dict may include: title (str), link (str, optional -- makes
    the title clickable), meta (str, optional -- the red subtitle line),
    tags (list[str], optional), people (str, optional), roles
    (list[tuple[str, str]], optional -- (icon, label) pairs), activity
    (list[tuple[str, str]], optional -- (label, timestamp) pairs), footer
    (list[tuple[str, str]], optional -- (icon, label) pairs).
    """
    if not cards:
        return

    parts = ['<div class="gw-card-grid">']
    for card in cards:
        title = html.escape(str(card.get("title", "")))
        link = card.get("link")
        title_html = (
            f'<a class="gw-card-title" href="{html.escape(link)}" target="_blank">{title}</a>'
            if link
            else f'<span class="gw-card-title">{title}</span>'
        )

        section = [f'<div class="gw-card"><div class="gw-card-top">{title_html}</div>']

        if card.get("meta"):
            section.append(f'<div class="gw-card-meta">{html.escape(str(card["meta"]))}</div>')

        tags = card.get("tags") or []
        if tags:
            tag_html = "".join(f'<span class="gw-tag">{html.escape(str(t))}</span>' for t in tags)
            section.append(f'<div class="gw-card-tags">{tag_html}</div>')

        if card.get("people"):
            section.append(f'<div class="gw-card-people">{html.escape(str(card["people"]))}</div>')

        roles = card.get("roles") or []
        if roles:
            role_html = "&nbsp;&nbsp;".join(
                f"{icon} {html.escape(str(label))}" for icon, label in roles
            )
            section.append(f'<div class="gw-card-roles">{role_html}</div>')

        activity = card.get("activity") or []
        if activity:
            section.append('<hr class="gw-card-divider" />')
            for label, timestamp in activity:
                section.append(
                    '<div class="gw-activity-row">'
                    f"<span>{html.escape(str(label))}</span>"
                    f'<span class="gw-activity-time">{html.escape(str(timestamp))}</span>'
                    "</div>"
                )

        footer = card.get("footer") or []
        if footer:
            footer_html = "".join(
                f"<span>{icon} {html.escape(str(label))}</span>" for icon, label in footer
            )
            section.append(f'<div class="gw-card-footer">{footer_html}</div>')

        section.append("</div>")
        parts.append("".join(section))
    parts.append("</div>")

    st.markdown("".join(parts), unsafe_allow_html=True)
