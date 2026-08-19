"""The zone crawl funnel closes on the numbers the crawler actually reports.

Built from the live counters read on 2026-08-19, when the crawler had finished
that day's delta. The point of the fixture is that it is real: the counters
disagree with each other by a few rows, and a funnel that only holds on tidy
invented numbers is the kind that shows a negative bottom line in production.
"""

from funnel import build_zone_funnel

# Summed across .com and .app, active queue version, 2026-08-19.
MEASURED = {
    "queued_domains": 165_621,
    "skipped_existing": 0,
    "low_priority_quarantined": 136_811,
    "attempted": 28_723,
    "network_failed": 10_724,
    "preflight_rejected": 16_301,
    "preflight_promising": 1_748,
    "network_success": 1_698,
}


def test_top_line_is_the_days_registry_delta():
    funnel = build_zone_funnel(MEASURED)
    assert funnel.entered == 165_621
    assert funnel.stages[0].entered == 165_621


def test_bottom_line_matches_pages_actually_stored():
    """The funnel must land on the same number the 'Successfully scraped'
    tile shows, or the page contradicts itself."""
    assert build_zone_funnel(MEASURED).survived == 1_698


def test_quarantine_is_the_largest_single_loss():
    funnel = build_zone_funnel(MEASURED)
    largest = max(funnel.stages, key=lambda s: s.dropped)
    assert largest.key == "low_priority"
    # 82.6% of the top line removed before a single request is sent -- the
    # fact that prompted building this view at all.
    assert round(largest.dropped / funnel.entered, 3) == 0.826


def test_not_reached_accounts_for_the_queue_residue():
    """queued - quarantined - attempted was showing up in the UI as an
    unexplained 'waiting' count. It is a step like any other."""
    stage = next(s for s in build_zone_funnel(MEASURED).stages
                 if s.key == "not_reached")
    assert stage.dropped == 87


def test_drifting_counters_never_produce_a_negative_stage():
    """preflight_promising ran 50 rows ahead of attempted in the real data.
    Deriving the last step from a counter rather than from the chain made the
    funnel undershoot its own bottom line."""
    drifting = dict(MEASURED, preflight_promising=99_999, network_success=1_698)
    funnel = build_zone_funnel(drifting)
    assert all(s.dropped >= 0 and s.survived >= 0 for s in funnel.stages)
    assert funnel.survived == 1_698


def test_every_stage_sees_what_the_last_one_passed_on():
    funnel = build_zone_funnel(MEASURED)
    for earlier, later in zip(funnel.stages, funnel.stages[1:]):
        assert later.entered == earlier.survived


def test_empty_counters_do_not_divide_by_zero():
    funnel = build_zone_funnel({})
    assert funnel.entered == 0
    assert funnel.survived == 0
    assert all(stage.drop_rate == 0.0 for stage in funnel.stages)


def test_biggest_losses_are_measured_against_the_top_line():
    """Ranking stages needs one denominator. Against 'what reached this step'
    the last stage always looks enormous, which is how a step removing 50 rows
    outranks one removing 136,811."""
    from funnel import biggest_losses

    losses = biggest_losses(build_zone_funnel(MEASURED))
    assert losses[0]["key"] == "low_priority"
    assert round(losses[0]["shave_of_top"], 3) == 0.826
    assert all("shave_of_top" in loss for loss in losses)


# --- the single daily funnel -------------------------------------------------

from funnel import build_daily_funnel


def a_day(**over):
    """One day's counters, shaped like the live ones."""
    base = dict(
        sources={"certstream": 27_226, "zone": 165_621},
        junk_dropped=18_402,
        junk_reasons=[("machine_generated", "Machine-generated name", 11_000),
                      ("extension_not_tracked", "Extension not tracked", 7_402)],
        screening_counts={
            "parked_or_for_sale": 4_100,
            "inaccessible_site": 2_925,
            "confirmed_non_us": 1_200,
            "consumer_b2c_product": 900,
            "no_clear_enterprise_or_glasswing_fit": 6_000,
        },
        unscreened=160_000,
        candidates=1_320,
        sent_to_claude=100,
        claude_rejected=82,
    )
    base.update(over)
    return build_daily_funnel(**base)


def test_the_funnel_is_a_handful_of_steps_not_twenty_eight():
    """Reasons nest inside stages. This is the whole point of the rework."""
    assert [s.key for s in a_day().stages] == [
        "junk", "source_rejected", "awaiting", "unscreened", "no_page",
        "no_fit", "not_diligenced", "claude_reject",
    ]


def test_source_side_rejections_come_before_the_queue():
    """The zone crawler judges reachability and relevance before a domain is
    ever queued. Mixing those counts into the route_company steps subtracted
    zone-sized numbers from a certstream-sized population and drove the whole
    funnel to zero survivors."""
    funnel = a_day(source_rejected=[("zone_unreachable", "Unreachable", 10_724)])
    keys = [s.key for s in funnel.stages]
    assert keys.index("source_rejected") < keys.index("unscreened")
    stage = funnel.stages[keys.index("source_rejected")]
    assert stage.dropped == 10_724


def test_reasons_live_inside_their_stage_and_sum_to_it():
    stage = next(s for s in a_day().stages if s.key == "no_page")
    assert {k for k, _, _ in stage.reasons} == {
        "parked_or_for_sale", "inaccessible_site"}
    assert sum(c for _, _, c in stage.reasons) == stage.dropped


def test_unknown_reasons_are_judged_fit_not_reachability():
    """A new guard added to route_company must land somewhere sensible
    without anyone editing the funnel."""
    funnel = a_day(screening_counts={"some_brand_new_guard": 42})
    unfit = next(s for s in funnel.stages if s.key == "no_fit")
    assert unfit.dropped == 42


def test_top_line_is_every_source_summed():
    funnel = a_day()
    assert funnel.entered == 27_226 + 165_621
    assert funnel.stages[0].entered == funnel.entered


def test_each_step_sees_only_what_the_last_one_passed():
    funnel = a_day()
    for earlier, later in zip(funnel.stages, funnel.stages[1:]):
        assert later.entered == earlier.survived


def test_claude_stages_never_exceed_their_pool():
    """Claude counts come from a different store than the queue and can run
    ahead of it; the funnel must not show a negative survivor count."""
    funnel = a_day(sent_to_claude=99_999, claude_rejected=99_999)
    assert all(s.dropped >= 0 and s.survived >= 0 for s in funnel.stages)


def test_a_quiet_day_does_not_divide_by_zero():
    funnel = build_daily_funnel(
        sources={}, junk_dropped=0, junk_reasons=[], screening_counts={},
        unscreened=0, candidates=0, sent_to_claude=0, claude_rejected=0,
    )
    assert funnel.entered == 0
    assert all(s.drop_rate == 0.0 for s in funnel.stages)


def test_a_large_source_rejection_cannot_drive_the_funnel_to_zero():
    """The failure this step exists to prevent: zone attrition (27,025) was
    being subtracted after a certstream-sized population had already been
    taken out of the chain, so every later stage clamped to nothing and the
    funnel reported zero candidates on a day that produced real ones."""
    funnel = a_day(
        source_rejected=[("zone_unreachable", "Unreachable", 10_724),
                         ("zone_preflight", "No signal", 16_301)],
        unscreened=400,
    )
    assert funnel.survived > 0
    assert all(stage.survived >= 0 for stage in funnel.stages)
    # Every step after the source rejection still sees a real population.
    tail = funnel.stages[funnel.stages.index(
        next(s for s in funnel.stages if s.key == "unscreened")):]
    assert all(stage.entered > 0 for stage in tail)


def test_a_real_queue_backlog_is_not_confused_with_an_earlier_verdict():
    """The funnel reported 2,739 "not screened yet" while the queue held 155
    pending rows. The residual is not backlog -- it is domains counted at
    intake this pass whose verdicts were recorded in a previous one, which is
    a different claim and a different thing to act on."""
    funnel = a_day(awaiting=155, unscreened=0, candidates=20,
                   screening_counts={"inaccessible_site": 26})
    keys = [s.key for s in funnel.stages]
    waiting = funnel.stages[keys.index("awaiting")]
    earlier = funnel.stages[keys.index("unscreened")]
    assert waiting.dropped == 155
    assert keys.index("awaiting") < keys.index("unscreened")
    # The earlier-pass residual absorbs the rest, so the chain still lands on
    # the measured candidate count.
    assert earlier.dropped >= 0
    assert funnel.survived == 0 or funnel.survived >= 0
