from filter import (
    COVERAGE_EVIDENCE_CAP,
    COVERAGE_FLOOR_MAX,
    COVERAGE_WORD_CAP,
    LOW_VALUE_SCORE_CEILING,
    aiaas_green_flags,
    founder_green_flags,
    founder_pedigree_score,
    high_signal_scoring_text,
    score_text,
    scorecard_from_evidence,
)


def test_aiaas_tags_require_specific_product_evidence():
    flags = aiaas_green_flags(
        "Our GPU as a service uses GPU autoscaling and a model serving API "
        "to lower inference costs for production AI teams."
    )
    labels = {flag["label"] for flag in flags}
    assert labels == {
        "GPU infrastructure", "Model serving", "Inference optimization",
    }
    assert aiaas_green_flags(
        "AI as a service for every business. We build custom AI solutions."
    ) == []


def test_long_tail_keyword_stuffing_does_not_change_score():
    lead = (
        "Acme makes deterministic runtime security for enterprise AI agents. "
        "Its sandbox verifies tool calls and blocks prompt injection."
    )
    neutral_padding = "operating company information " * 900
    stuffed_tail = " ".join([
        "robotics autonomous systems zero trust computer vision digital twin "
        "workflow compliance proprietary platform"
    ] * 500)
    bounded = high_signal_scoring_text(
        "Acme", "Agent security", lead + " " + neutral_padding + stuffed_tail
    )
    concise = high_signal_scoring_text("Acme", "Agent security", lead)
    assert score_text(bounded)[0] == score_text(concise)[0]


def test_customer_logo_does_not_create_founder_pedigree_tag():
    assert founder_green_flags(
        "Trusted by Google, Meta, and Stanford. Integrates with Microsoft."
    ) == []


def test_customer_logos_contribute_no_founder_score():
    text = "Trusted by Google, Meta, Stanford, and MIT customers."
    flags = founder_green_flags(text)
    assert founder_pedigree_score(flags) == 0
    assert score_text(text)[3]["founder_pedigree"] == 0


def test_founder_context_creates_evidence_backed_tags():
    flags = founder_green_flags(
        "Our founding team includes former Google engineers and Stanford graduates."
    )
    labels = {flag["label"] for flag in flags}
    assert "Ex-Google" in labels
    assert "Stanford team" in labels
    assert all(flag["evidence"] and flag["source"] == "website" for flag in flags)
    assert founder_pedigree_score(flags) >= 50


def test_founder_enrichment_extracts_named_founder_and_adjacent_ip():
    flags = founder_green_flags(
        "Acme was founded by Jane Smith. Jane Smith previously worked at "
        "Google and holds patented infrastructure research."
    )
    labels = {flag["label"] for flag in flags}
    assert "Founder · Jane Smith" in labels
    assert "Ex-Google" in labels
    assert "Founder technical IP" in labels


def test_nvidia_inception_membership_is_not_employment_pedigree():
    flags = founder_green_flags(
        "Our team builds optimization software for industrial operations. "
        "AEP Labs is an NVIDIA Inception Member and technology partner."
    )
    assert "Ex-Nvidia" not in {flag["label"] for flag in flags}


def test_actual_former_nvidia_founder_still_gets_pedigree():
    flags = founder_green_flags(
        "Our founding team includes former NVIDIA engineers who built GPU "
        "infrastructure for production AI."
    )
    assert "Ex-Nvidia" in {flag["label"] for flag in flags}


def test_scorecard_is_bounded_and_explainable():
    card = scorecard_from_evidence(
        counts={"defensibility": 9},
        dimensions={"buyer": 2, "technical": 5},
        venture_quality={
            "thesis_score": 80, "product_score": 70, "wedge_score": 60,
            "moat_score": 90, "founder_score": 40,
        },
    )
    assert set(card) == {
        "thesis", "product", "wedge", "moat", "team", "buyer",
        "technical_depth",
    }
    assert all(0 <= value <= 100 for value in card.values())


def test_evidence_coverage_floor_stays_below_the_low_value_fit_ceiling():
    """The floor exists so an extracted page is not confused with a failed
    extraction -- it must never promote a page into a fit band.

    This replaces a `min(coverage_score, 24)` clamp that could never bind,
    because the components cap out at 1 + 8 + 12 = 21. The clamp implied a
    ceiling of 24 that was unreachable, so raising a component cap later would
    have silently crossed the low-value ceiling without anything failing.
    """
    assert COVERAGE_FLOOR_MAX == 1 + COVERAGE_WORD_CAP + COVERAGE_EVIDENCE_CAP
    assert COVERAGE_FLOOR_MAX < LOW_VALUE_SCORE_CEILING


def test_a_long_page_with_no_fit_evidence_scores_within_the_coverage_floor():
    """Lots of text but no thesis, product, or buyer evidence."""
    text = " ".join(["the weather here is pleasant this afternoon"] * 200)
    score = score_text(text)[0]
    assert 0 < score <= COVERAGE_FLOOR_MAX


def test_extraction_failure_is_the_only_way_to_score_zero():
    assert score_text("")[0] == 0
    assert score_text("   ")[0] == 0
