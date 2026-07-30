import json

from ai_search.discovery import names_from_event, registrable_ai_domains


def test_parses_domains_only_and_full_certstream_events():
    assert names_from_event(json.dumps({
        "message_type": "dns_entries",
        "data": ["www.example.ai"],
    })) == ["www.example.ai"]
    assert names_from_event(json.dumps({
        "message_type": "certificate_update",
        "data": {"leaf_cert": {"all_domains": ["api.company.ai"]}},
    })) == ["api.company.ai"]


def test_reduces_certificate_names_to_ai_registrable_domains():
    assert registrable_ai_domains([
        "*.api.company.ai", "www.example.ai", "ignored.com"
    ]) == {"company.ai", "example.ai"}

