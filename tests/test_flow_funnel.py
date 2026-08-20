"""The funnel follows the code path, not a set of derived buckets.

What this replaces: one chain that mixed two units and two windows. Intake
counts sightings this pass; screening counts domains once each, from rows that
carry a verdict. Chaining them forced an 86,088-row "verdict belongs to an
earlier pass" step to sit as the second-largest loss on an attrition page, to
absorb a mismatch that is a change of unit rather than a loss. And collapsing
route_company's fourteen ordered guards into "never served a usable page" and
"read, not a fit" hid the one thing the page is for: which check is cutting.
"""

from funnel import (
    FLOW_REASONS,
    FLOW_SURVIVOR_REASONS,
    build_flow_funnel,
)

GUARDS = {
    "extension_not_tracked": "Outside tracked extensions",
    "machine_generated": "Machine-generated name",
}

REASONS = {
    "parked_or_for_sale": 36493,
    "generic_hosting_or_registrar_placeholder": 21324,
    "website_never_reachable_after_max_attempts": 47240,
    "predominantly_non_english": 50402,
    "confirmed_non_us": 10565,
    "consumer_b2c_product": 5860,
    "commodity_ai_wrapper_or_service": 11738,
    "no_clear_enterprise_or_glasswing_fit": 90564,
    "claude_reject": 2173,
    "claude_keep": 278,
    "credible_but_requires_human_review": 6383,
}


def build(**overrides):
    kwargs = dict(
        observed=8396798,
        intake_drops={"extension_not_tracked": 8282776, "machine_generated": 861},
        guard_labels=GUARDS,
        awaiting=1019,
        reason_counts=dict(REASONS),
        claude_sent=2451,
    )
    kwargs.update(overrides)
    return build_flow_funnel(**kwargs)


def test_intake_and_screening_are_separate_chains():
    """They do not share a unit or a window, so they cannot share a chain."""
    out = build()
    assert set(out) == {"intake", "screening"}
    # Intake's top line is sightings; screening's is domains with a verdict.
    assert out["intake"].stages[0].entered == 8396798
    assert out["screening"].stages[0].entered < 8396798


def test_every_drop_names_a_reason_the_pipeline_recorded():
    """No derived numbers: each stage's dropped total is the sum of the
    routing_reasons inside it, so a stage can never disagree with the rows
    behind it."""
    for funnel in build().values():
        for stage in funnel.stages:
            if not stage.reasons:
                continue
            assert stage.dropped == sum(n for _, _, n in stage.reasons), stage.label


def test_phases_appear_in_the_order_route_company_evaluates_them():
    out = build()
    labels = [stage.key for stage in out["screening"].stages]
    assert labels[:5] == ["queue", "no_page", "disqualified", "fit", "claude"]

    # And inside a phase, the reasons keep the code's order rather than being
    # sorted by size -- the order is the information: a domain dies at the
    # first branch that claims it.
    disqualified = next(
        s for s in out["screening"].stages if s.key == "disqualified"
    )
    order = [reason for _, reason, _ in FLOW_REASONS]
    seen = [reason for reason, _, _ in disqualified.reasons]
    assert seen == sorted(seen, key=order.index)
    # Non-English precedes non-US precedes consumer, as in the source.
    assert seen.index("predominantly_non_english") < seen.index("confirmed_non_us")


def test_a_surviving_decision_is_not_counted_as_a_drop():
    """"credible_but_requires_human_review" is a live candidate, not a loss."""
    out = build()
    for stage in out["screening"].stages:
        assert "credible_but_requires_human_review" not in {
            reason for reason, _, _ in stage.reasons
        }
    assert "credible_but_requires_human_review" in FLOW_SURVIVOR_REASONS


def test_an_unknown_reason_is_surfaced_rather_than_swallowed():
    """A routing_reason nobody has mapped is exactly the one worth seeing, and
    silently dropping it would make the chain stop reconciling."""
    out = build(reason_counts={**REASONS, "some_brand_new_branch": 42})
    unmapped = [s for s in out["screening"].stages if s.key == "unmapped"]
    assert len(unmapped) == 1
    assert unmapped[0].dropped == 42
    assert unmapped[0].reasons[0][0] == "some_brand_new_branch"


def test_claude_never_sent_is_measured_against_what_reached_it():
    """Claude is a budget applied to survivors, so "never sent" is the gap
    between the pool and what was actually spent -- not a stored count."""
    out = build(claude_sent=0)
    claude = next(s for s in out["screening"].stages if s.key == "claude")
    never = dict((r, n) for r, _, n in claude.reasons)["not_sent"]
    assert never == claude.entered - 2173 or never == claude.entered
    assert never > 0


def test_the_chain_never_drops_more_than_reached_a_stage():
    """Counts come from separate statements and disagree mid-write; a stage
    that removed more than entered it would render a drop rate above 100%."""
    for funnel in build().values():
        for stage in funnel.stages:
            assert 0 <= stage.dropped <= stage.entered, stage.label


def test_a_reason_whose_name_carries_a_status_code_still_finds_its_phase():
    """route_company builds this as f"inaccessible_http_{status}", so the code
    is part of the name and no static map can hold them all. The unmapped
    stage surfaced eight of them charged against the pool that happened to be
    last, rather than against the fetch that produced them.
    """
    out = build(reason_counts={
        **REASONS,
        "inaccessible_http_403": 10,
        "inaccessible_http_525": 3,
    })
    no_page = next(s for s in out["screening"].stages if s.key == "no_page")
    labels = {label for _, label, _ in no_page.reasons}
    assert "Site returned HTTP 403" in labels
    assert "Site returned HTTP 525" in labels
    assert not [s for s in out["screening"].stages if s.key == "unmapped"]


def test_a_reviewer_overruling_the_pipeline_is_counted_apart_from_it():
    """A person is not one of route_company's checks, so charging their
    decision to whichever stage happened to be last misattributes it."""
    out = build(reason_counts={**REASONS, "manual_override_reject": 275})
    manual = next(s for s in out["screening"].stages if s.key == "manual")
    assert manual.dropped == 275
    assert [s.key for s in out["screening"].stages][-1] == "manual"
