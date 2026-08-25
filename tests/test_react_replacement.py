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
    assert '"/api/ai/domain"' in source
    assert "Run through platform" in source
    assert '"Screening complete"' in Path("web_api.py").read_text(encoding="utf-8")
    assert "source_run_id=source" in Path("web_api.py").read_text(encoding="utf-8")
    assert 'quickBusy ? "Processing…"' in source
    assert '["search", "Search", Search]' not in source
    assert 'search: <GlobalSearchPage />' not in source
    assert "Search everything" not in source
    assert "Search corpus" not in source
    assert "Search basket domains" not in source
    assert "Search indexed website content" not in source
    assert "global-searchbar" not in source
    for metric in ("Domains available", "Waiting to scrape"):
        assert metric in source
    assert "Searchable pages" not in source
    for zone_metric in ("Investable retained", "Discarded", "Fetch failures"):
        # Legacy corpus counters were replaced with deal-funnel metrics. Match
        # the metric declaration rather than the bare word -- "Discarded" is a
        # substring of legitimate copy elsewhere (e.g. the "Discarded by rules"
        # audit page), and a loose check fails on unrelated additions.
        assert f'label="{zone_metric}"' not in source
    # The crawler tiles as the page actually renders them. This list drifted
    # from the UI once already and sat red on main; keep it matching the
    # Metric labels in the !isAi branch of the corpus page.
    for zone_metric in (
        "New domains today",
        # Not "Waiting to check": that read as a backlog when it is an 87-row
        # queue residue, while the number worth showing -- 84% of the delta
        # dropped below the priority ceiling before any request -- had no tile.
        "Skipped — low priority",
        "Domains checked",
        "Passed quick screen",
        "Successfully scraped",
        "Failed to reach",
    ):
        assert zone_metric in source
    assert 'label="Waiting to check"' not in source
    # One funnel. The zone crawl had its own table on this page for exactly
    # one commit; both sources now share the six-step funnel, because below
    # intake they share every step.
    assert "zone_funnel" not in source
    # Two tables now, one per unit: sightings at intake, domains at screening.
    # They are not one chain, and rendering them as one is what required a
    # reconciliation step to stand in as the second-largest "loss".
    assert source.count("<FunnelTable funnel=") == 2
    # Reasons are detail on a step, not steps themselves.
    assert "funnel-reasons" in source
    assert "stage.reasons" in source
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
    assert "<CompaniesTable showRunScope={false} />" in source
    assert "<CompaniesTable showRunScope />" in source
    assert "function FastReviewQueue()" in source
    assert "<FastReviewQueue />" in source
    assert 'event.key.toLowerCase() === "y"' in source
    assert 'event.key.toLowerCase() === "r"' in source
    assert 'event.key.toLowerCase() === "t"' in source
    assert "Decisions save immediately and advance the queue" in source
    assert "Keyboard {keyboardEnabled ? \"on\" : \"off\"}" in source
    assert "if (!keyboardEnabled) return undefined" in source
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
    # Recover quickly after Railway swaps containers instead of leaving the
    # page in stacked loading placeholders for several seconds.
    assert "window.setTimeout(load, 1000)" in source
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


def test_retired_search_ui_is_not_shipped():
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    assert "function GlobalSearchPage()" not in source
    assert "/api/search" not in source


def test_tables_render_only_one_negative_action():
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    table = source.split("function Table(", 1)[1].split("\nfunction ", 1)[0]
    assert "onLabel(row, \"no\")" not in table
    assert "aria-label={`Reject ${row.domain}`}" in table
    assert table.count("onClick={() => onReject(row)}") == 1
    assert table.count(">\n                      Reject\n") == 1


def test_decision_modal_recovers_from_api_failure():
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    modal = source.split("function LabelModal(", 1)[1].split("\nfunction ", 1)[0]
    assert "catch (requestError)" in modal
    assert "setSaving(false)" in modal
    assert 'error && <p className="error inline-notice"' in modal


def test_the_company_table_can_track_a_company():
    """Tracking existed only on the deal cards, so a company found by reading
    the table on the home page could be labelled, rejected or sent to Claude --
    but not tracked, which is the one action that puts it in front of the team.
    """
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    # The table renders it as an action of its own.
    assert "onTrack" in source
    assert source.count("onTrack={() => setRefresh") == 2, (
        "both company tables -- Classifications and the corpus views -- should "
        "offer it"
    )
    # And it uses the in-cell style rather than the card one. card-track is
    # absolutely positioned for a card corner, so in a table the button leaves
    # its cell and the column renders empty.
    assert 'className="track-inline"' in source
    assert 'className="track"' not in source


def test_an_empty_table_still_shows_its_header():
    """The controls live in the header, so dropping it on an empty result took
    away the only way to undo the filter that emptied it. Narrow the date to 24
    hours on a quiet table and it was stuck there permanently -- no sort, no
    filter, no way back without a reload.
    """
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    # No early return that skips the table entirely.
    assert "if (!rows?.length) return" not in source
    # The empty state is a row inside the body instead.
    assert 'className="empty-row"' in source
    assert "const empty = !rows?.length" in source


def test_an_emptied_filter_offers_a_way_back():
    """And the way back is a reset, not another step through the cycle: the
    next window along can be just as empty as the one that emptied it.
    """
    source = Path("webapp/src/main.jsx").read_text(encoding="utf-8")
    assert "onClearAdded" in source
    assert "Show any date" in source
    assert "setAddedWithinDays(0)" in source
