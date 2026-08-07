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

import pandas as pd
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

/* Streamlit dims every element to ~60% opacity while a rerun is pending
   (its own [data-stale="true"] attribute), which on a data-heavy page reads
   as the whole app greying out on every click. The content is still
   correct while stale -- suppress the fade rather than the (useful)
   top-right "running" spinner, which is untouched. */
[data-stale="true"] { opacity: 1 !important; transition: none !important; }

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
    /* A grid item's default min-width is its content's min-content size,
       not 0 -- a long unbroken title (a raw domain has no spaces to wrap
       at) stretches this item past its 1fr track, pushing the whole grid
       (and the card grid's container) past the right edge of the page.
       min-width: 0 lets the track actually hold to 1fr; overflow-wrap
       lets the title itself then wrap instead of overflowing sideways. */
    min-width: 0;
    overflow-wrap: anywhere;
}
.gw-card-top {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.5rem;
    min-width: 0;
}
a.gw-card-title, a.gw-card-title:link, a.gw-card-title:visited,
span.gw-card-title {
    font-family: 'Fraunces', Georgia, serif !important;
    font-weight: 600;
    font-size: 1.55rem;
    line-height: 1.15;
    color: var(--text-color, #0b0b0b) !important;
    text-decoration: none !important;
    min-width: 0;
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


def render_hideable_table(
    frame: pd.DataFrame,
    *,
    domain_column: str,
    key: str,
    column_config: dict | None = None,
    visible_columns: list[str] | None = None,
    editable_columns: list[str] | None = None,
    height: int | None = None,
    empty_message: str = "Everything here is hidden.",
) -> pd.DataFrame:
    """Render persistent Hide and exclusive Mark Yes/Mark No controls.

    Hiding is a pure view preference (classification_store.hide_domains) --
    it never touches the underlying dataset a row came from (crawl results,
    search corpus, classifications, ...); it only stops that domain from
    showing up in *any* table using this helper, across the whole app,
    until unchecked. Toggling "Show hidden" reveals previously-hidden rows
    with the box pre-checked so unchecking it un-hides them.

    Mark Yes/Mark No are training labels only. They never reclassify or remove
    a company from its current list. Clicking either checkbox saves
    immediately, moves the domain out of the opposite basket, and keeps the
    row visible.

    Returns the edited visible rows without the internal action columns.
    Other explicitly editable columns (for example Select) are preserved.

    domain_column must hold the bare domain (what hide_domains/stable_domain
    key on) -- pass visible_columns if the display shouldn't include it
    verbatim (e.g. a separate "Domain" link column already shows a full
    URL derived from it).
    """
    from classification_store import hidden_domain_set, hide_domains, unhide_domains
    from common import (
        NO_BASKET_DATA,
        YES_BASKET_DATA,
        read_basket_data,
        remove_basket_entry,
        save_basket_entries,
        stable_domain,
    )

    if frame.empty:
        st.dataframe(frame, hide_index=True, width="stretch")
        return frame

    domains = frame[domain_column].astype(str).map(stable_domain)
    hidden_now = hidden_domain_set(domains.tolist())
    yes_now = {
        stable_domain(value)
        for value in read_basket_data(YES_BASKET_DATA)["domain_url"]
        if stable_domain(value)
    }
    no_now = {
        stable_domain(value)
        for value in read_basket_data(NO_BASKET_DATA)["domain_url"]
        if stable_domain(value)
    }

    show_hidden = False
    if hidden_now:
        show_hidden = st.checkbox(
            f"Show {len(hidden_now)} hidden row(s)",
            key=f"{key}_show_hidden",
        )

    working = frame.copy() if show_hidden else frame[~domains.isin(hidden_now)].copy()
    if working.empty:
        st.caption(empty_message)
        return working

    display = working.copy()
    display.insert(
        0,
        "Hide",
        display[domain_column].astype(str).map(stable_domain).isin(hidden_now),
    )
    display.insert(
        1,
        "Mark Yes",
        display[domain_column].astype(str).map(stable_domain).isin(yes_now),
    )
    display.insert(
        2,
        "Mark No",
        display[domain_column].astype(str).map(stable_domain).isin(no_now),
    )
    editable = set(editable_columns or [])
    editor_kwargs = {}
    if height is not None:
        editor_kwargs["height"] = height
    edited = st.data_editor(
        display,
        hide_index=True,
        width="stretch",
        column_order=(
            ["Hide", "Mark Yes", "Mark No", *visible_columns]
            if visible_columns is not None
            else None
        ),
        column_config={
            "Hide": st.column_config.CheckboxColumn(
                "Hide",
                help="Hide this row from view -- doesn't delete or change "
                "any underlying data.",
            ),
            "Mark Yes": st.column_config.CheckboxColumn(
                "Mark Yes",
                help="Add to the Yes training basket without moving this row.",
            ),
            "Mark No": st.column_config.CheckboxColumn(
                "Mark No",
                help="Add to the No training basket without moving this row.",
            ),
            **(column_config or {}),
        },
        disabled=[
            column
            for column in display.columns
            if column not in {"Hide", "Mark Yes", "Mark No"}
            and column not in editable
        ],
        key=key,
        **editor_kwargs,
    )
    normalized_edited = edited[domain_column].astype(str).map(stable_domain)
    checked = set(normalized_edited[edited["Hide"]])
    checked_yes = set(normalized_edited[edited["Mark Yes"]])
    checked_no = set(normalized_edited[edited["Mark No"]])
    displayed_domains = set(
        working[domain_column].astype(str).map(stable_domain)
    )
    to_hide = checked - hidden_now
    # Hidden rows are omitted in normal mode, so only interpret an unchecked
    # box as "unhide" when that hidden row was actually displayed to the user.
    to_unhide = (hidden_now & displayed_domains) - checked
    if to_hide:
        hide_domains(to_hide)
    if to_unhide:
        unhide_domains(to_unhide)
    newly_yes = (checked_yes - yes_now) & displayed_domains
    newly_no = (checked_no - no_now) & displayed_domains
    # Switching from an existing label temporarily renders both boxes checked
    # in the widget delta. The newly checked box is the user's intent.
    to_yes = newly_yes
    to_no = newly_no - to_yes
    to_clear = (
        ((yes_now | no_now) & displayed_domains)
        - checked_yes
        - checked_no
    )
    if to_yes:
        save_basket_entries(
            [(domain, 90) for domain in sorted(to_yes)],
            "yes",
            source="human",
            reason="inline_yes_checkbox",
        )
    if to_no:
        save_basket_entries(
            [(domain, 0) for domain in sorted(to_no)],
            "no",
            source="human",
            reason="inline_no_checkbox",
        )
    for domain in sorted(to_clear):
        remove_basket_entry(domain)
    if to_hide or to_unhide or to_yes or to_no or to_clear:
        st.rerun()
    return edited.drop(columns=["Hide", "Mark Yes", "Mark No"])


def render_worker_controls(
    service: str,
    *,
    label: str,
    actual_status: str,
    key: str,
    show_heading: bool = True,
) -> None:
    """Render safe shared start/pause controls on a crawler's own page."""
    from runtime_control import (
        RUNTIME_MODE,
        control_backend_available,
        ensure_supervisor,
        request_services,
        runtime_status,
    )

    if show_heading:
        st.markdown("#### Crawler control")
    message_key = f"{key}_worker_control_message"
    message = st.session_state.pop(message_key, "")
    if message:
        st.success(message)
    if not control_backend_available():
        st.error(
            "Crawler controls are unavailable because the shared runtime "
            "control database is not configured."
        )
        return
    try:
        desired = bool(
            runtime_status()["services"][service]["desired"]
        )
    except Exception as exc:
        st.error(
            "Could not read crawler control state: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    start_col, pause_col, state_col = st.columns([1, 1, 4])
    if start_col.button(
        "Start / resume",
        type="primary",
        disabled=desired,
        width="stretch",
        key=f"{key}_start",
    ):
        request_services(
            {service: True},
            requested_by=f"{key}_page_start",
        )
        if RUNTIME_MODE == "local":
            ensure_supervisor()
        st.session_state[message_key] = (
            f"{label} resume requested. Saved checkpoints are preserved."
        )
        st.rerun()
    if pause_col.button(
        "Pause safely",
        disabled=not desired,
        width="stretch",
        key=f"{key}_pause",
    ):
        request_services(
            {service: False},
            requested_by=f"{key}_page_pause",
        )
        if RUNTIME_MODE == "local":
            ensure_supervisor()
        st.session_state[message_key] = (
            f"{label} pause requested. The active safe operation may finish "
            "before processing becomes idle."
        )
        st.rerun()

    requested_label = "Running" if desired else "Paused"
    actual_label = (actual_status or "unknown").replace("_", " ").title()
    state_col.info(
        f"**Requested: {requested_label}**  \n"
        f"Worker reports: {actual_label}"
    )
