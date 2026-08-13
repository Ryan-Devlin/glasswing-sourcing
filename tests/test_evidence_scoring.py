from filter import (
    founder_green_flags,
    high_signal_scoring_text,
    score_text,
    scorecard_from_evidence,
)


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


def test_founder_context_creates_evidence_backed_tags():
    flags = founder_green_flags(
        "Our founding team includes former Google engineers and Stanford graduates."
    )
    labels = {flag["label"] for flag in flags}
    assert "Ex-Google" in labels
    assert "Stanford team" in labels
    assert all(flag["evidence"] and flag["source"] == "website" for flag in flags)


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
