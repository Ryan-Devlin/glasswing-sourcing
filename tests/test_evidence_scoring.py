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


def test_former_role_at_employer_is_read_as_pedigree():
    """The phrasing team pages actually use. The pattern set only matched the
    adjacent forms -- "Ex-Meta", "former Meta" -- so "Former Director of
    Product at Meta", which is how almost every team page writes it, produced
    no flag at all. Real example: verdicore.ai, whose own description named
    two Meta directors and one ex-Googler and scored as having no pedigree.
    """
    flags = founder_green_flags(
        "The Team Behind Verdicore. Co-Founder and CEO Omry Pruzan Former "
        "Director of Product at Meta and Lyft, Ex-Google. Co-Founder and CTO "
        "Miri Pruzan Former Director of Engineering at Meta."
    )
    labels = {flag["label"] for flag in flags}
    assert "Ex-Meta" in labels
    assert "Ex-Google" in labels


def test_employers_founders_actually_come_from_are_in_the_vocabulary():
    """The list predated several of them, most glaringly "facebook" without
    "meta" -- Meta renamed in 2021, so every current team page says Meta."""
    for employer, expected in (
        ("Meta", "Ex-Meta"),
        ("OpenAI", "Ex-Openai"),
        ("Anthropic", "Ex-Anthropic"),
        ("DeepMind", "Ex-Deepmind"),
        ("Databricks", "Ex-Databricks"),
        ("Snowflake", "Ex-Snowflake"),
        ("Waymo", "Ex-Waymo"),
    ):
        flags = founder_green_flags(
            f"Our founder previously worked at {employer} on core "
            "infrastructure before starting this company."
        )
        assert expected in {flag["label"] for flag in flags}, employer


def test_a_relationship_is_not_a_job():
    """The counterweight to reading "former <role> at <employer>": the same
    shape covers "former customers at Meta" and "a former partner at Google
    Ventures", neither of which says anyone worked there. The affiliation
    block already claimed to exclude customer relationships and had no
    pattern for one -- harmless until this phrasing was read as pedigree.
    """
    for text in (
        "Our founder works with former customers at Meta and Google.",
        "Our founder is a former partner at Google Ventures.",
        "Our founding team supports former clients at Snowflake.",
        "Our platform integrates with Snowflake, Databricks and OpenAI.",
        "Built on Databricks. Powered by OpenAI. Our founders love data.",
    ):
        assert founder_green_flags(text) == [], text


def test_words_that_merely_start_with_an_employer_name_are_not_pedigree():
    """"meta" is the most ambiguous name in the vocabulary. Boundary matching
    handles metadata and metaverse; the founder gate has to handle the rest.
    """
    for text in (
        "Our founder built the metadata layer and our team ships metaverse assets.",
        "Our founders published a meta-analysis of clinical outcomes.",
        "Our founding team runs meta-learning experiments at scale.",
    ):
        assert founder_green_flags(text) == [], text


def test_a_school_name_on_its_own_is_not_a_spinout():
    """The branch accepted any school word sitting near team copy, while the
    employer branch beside it had always demanded an employment pattern.
    Measured on the live corpus: of 880 rows carrying a school flag, 869
    (98.8%) do not survive requiring the name to appear as an institution.
    Every case below is drawn from that sample.
    """
    for text in (
        # "brown" is the commonest surname in the corpus, and a colour.
        "Chris Brown CEO & Co-Founder 3x Founder & Product Leader 3 exits",
        "Jessica Brown Founder & CEO I was impressed by the services",
        "Proudly Made By Take Point Marketing brown; our founder built this",
        # nixapublicschools.ai -- a Missouri public school district, which
        # reached Needs Review flagged as an Ivy League spinout.
        "Our team. District Gradecard Gold Star Schools Culture Columbia "
        "Data District Notices District Building Information",
        # "mit" is the licence on every open-source page.
        "Our founder released it under the MIT License on GitHub.",
        # An academic institution list is not a founder's education.
        "Our founding team reads Centro Studi (35) CESifo (57) Chapman "
        "University Columbia (31)",
    ):
        schools = [
            flag for flag in founder_green_flags(text)
            if flag["type"].endswith("_spinout")
        ]
        assert schools == [], f"{text[:48]!r} -> {schools}"


def test_attendance_stated_as_attendance_still_counts():
    for text, expected in (
        ("Our cofounder studied computer science at Brown University.", "Brown team"),
        ("Founded by Jane Doe, an MIT PhD in robotics.", "MIT team"),
        ("Our founder holds a Harvard MBA and scaled two companies.", "Harvard team"),
        ("Our founder graduated from MIT and led perception at Waymo.", "MIT team"),
        ("Our founding team are Stanford alumni building compilers.", "Stanford team"),
        ("Our cofounder earned a PhD at the University of Chicago.", "Chicago team"),
    ):
        labels = {flag["label"] for flag in founder_green_flags(text)}
        assert expected in labels, f"{text!r} -> {labels}"


def test_an_ivy_or_mit_founder_scores_high_on_its_own():
    """Explicit instruction: a founder from MIT, Harvard or any Ivy is a
    high-conviction signal by itself, not a modifier on thesis and product
    evidence. A sparse page is exactly the case it has to survive, since an
    elite team shipping nothing yet is still worth a human's attention.
    """
    from filter import ELITE_SCHOOL_SCORE_FLOOR

    for text in (
        "Founded by Jane Doe, an MIT PhD in robotics. We build perception software.",
        "Our founder holds a Harvard MBA and previously scaled two companies.",
        "Our cofounder studied computer science at Brown University.",
    ):
        assert score_text(text)[0] >= ELITE_SCHOOL_SCORE_FLOOR, text


def test_the_high_score_needs_the_verified_flag_not_the_bare_word():
    """The floor and the gate are one change. On the ungated detector this
    would have lifted a school district and every page naming a founder
    whose surname happens to be Brown straight into the top of the funnel.
    """
    from filter import ELITE_SCHOOL_SCORE_FLOOR

    for text in (
        "Chris Brown CEO & Co-Founder 3x Founder & Product Leader 3 exits",
        "Our founder released it under the MIT License on GitHub.",
        "Our team. District Gradecard Gold Star Schools Culture Columbia Data",
    ):
        assert score_text(text)[0] < ELITE_SCHOOL_SCORE_FLOOR, text


def test_a_strong_but_non_ivy_school_does_not_get_the_floor():
    """The instruction named MIT, Harvard and the Ivy League. A University of
    Chicago PhD is still a real spinout flag and still scores on pedigree; it
    just does not get the floor.
    """
    from filter import ELITE_SCHOOL_SCORE_FLOOR, elite_school_flags

    text = "Our cofounder earned a PhD at the University of Chicago."
    flags = founder_green_flags(text)
    assert any(flag["type"].endswith("_spinout") for flag in flags)
    assert elite_school_flags(flags) == []
    assert score_text(text)[0] < ELITE_SCHOOL_SCORE_FLOOR


def test_a_school_name_inside_another_institution_is_not_that_school():
    """"University of British Columbia" is not Columbia. Found in the
    survivors of the attendance gate, on a row that had just been promoted
    for having an Ivy founder.
    """
    for text in (
        "I earned a Master's degree from the University of British Columbia.",
        "Our founder grew up in Columbia, MO before starting the company.",
    ):
        schools = [
            flag for flag in founder_green_flags(text)
            if flag["type"].endswith("_spinout")
        ]
        assert schools == [], f"{text!r} -> {schools}"


def test_a_keyword_list_is_not_a_sentence():
    """rule_evidence is semicolon-joined keywords, and the attendance
    patterns span "up to 30 characters, no full stop" -- so feeding it in
    manufactured pedigree out of adjacency alone: "researcher; mit;
    stanford" read as an MIT researcher. Only prose reaches the detector now,
    but the detector should not be credulous about it either.
    """
    flags = founder_green_flags(
        "Our founder. Close researcher; mit; stanford physical ai; robotics"
    )
    assert [f for f in flags if f["type"].endswith("_spinout")] == []
