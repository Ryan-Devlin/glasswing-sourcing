from ai_search.domains import normalize_any_domain, normalize_domain


def test_normalizes_ai_domain():
    assert normalize_domain("HTTPS://WWW.Example.AI/path?q=1") == "example.ai"
    assert normalize_domain("*.example.ai.") == ""


def test_rejects_non_ai_and_invalid_hosts():
    assert normalize_domain("example.com") == ""
    assert normalize_domain("127.0.0.1") == ""
    assert normalize_domain("not a domain.ai") == ""


def test_manual_intake_accepts_other_valid_tlds():
    assert normalize_any_domain("https://www.example.com/about") == "example.com"
    assert normalize_any_domain("product.app") == "product.app"
    assert normalize_any_domain("127.0.0.1") == ""
