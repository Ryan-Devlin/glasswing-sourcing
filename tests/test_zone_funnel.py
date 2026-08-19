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
