from form_d import score_filing


def test_form_d_fund_is_hard_denied():
    score, bucket, reason = score_filing({
        "company_name": "Example Venture Fund III LP",
        "industry": "Pooled Investment Fund",
        "entity_type": "Limited Partnership",
    })
    assert score == 0
    assert bucket == "denied"
    assert reason == "pooled_or_investment_fund"


def test_form_d_venture_scale_technology_raise_is_investable():
    score, bucket, reason = score_filing({
        "company_name": "New Robotics Inc",
        "industry": "Other Technology",
        "entity_type": "Corporation",
        "offering_amount": 8_000_000,
        "amount_sold": 2_000_000,
        "investors": 12,
    })
    assert score >= 72
    assert bucket == "investable"
    assert "venture_scale_raise" in reason


def test_form_d_missing_evidence_routes_to_review_not_fake_reject():
    score, bucket, _ = score_filing({
        "company_name": "Sparse New Co",
        "industry": "Other",
        "entity_type": "Corporation",
        "investors": 2,
    })
    assert score >= 0
    assert bucket == "review"


def test_form_d_real_estate_vehicle_is_hard_denied():
    score, bucket, reason = score_filing({
        "company_name": "Example Property DST",
        "industry": "Other Real Estate",
        "entity_type": "Other",
        "offering_amount": 20_000_000,
    })
    assert score == 0
    assert bucket == "denied"
    assert reason == "real_estate_or_investment_vehicle"
