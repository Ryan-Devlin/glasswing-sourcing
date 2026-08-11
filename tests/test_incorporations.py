from incorporations import score_company


def test_delaware_corporation_becomes_candidate():
    score, bucket, reason = score_company({"company_name": "New Robotics Inc", "company_type": "Corporation"})
    assert score == 70
    assert bucket == "review"
    assert reason == "new_delaware_corporation"


def test_fund_and_llc_noise_is_denied():
    assert score_company({"company_name": "Example Venture Fund III", "company_type": "Corporation"})[1] == "denied"
    assert score_company({"company_name": "Example Software", "company_type": "Limited Liability Company"})[1] == "denied"
