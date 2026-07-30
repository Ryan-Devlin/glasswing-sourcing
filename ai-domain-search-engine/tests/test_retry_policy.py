from ai_search.crawler import retry_delay_days


def test_failure_retry_policy_distinguishes_permanent_and_transient_errors():
    assert retry_delay_days("timeout", 1) == 2
    assert retry_delay_days("blocked", 1) == 7
    assert retry_delay_days("http_error", 1) == 14
    assert retry_delay_days("dns_error", 1) == 30


def test_retry_policy_backs_off_repeated_failures_with_a_cap():
    assert retry_delay_days("timeout", 3) == 8
    assert retry_delay_days("dns_error", 99) == 180
