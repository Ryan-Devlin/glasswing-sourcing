from dataclasses import replace

from ai_search.config import load_settings
from ai_search.web_discovery import WebResult, provider_name


def test_provider_selection_prefers_configured_api_keys():
    base = load_settings()
    assert provider_name(replace(
        base, web_provider="auto", brave_search_api_key="key"
    )) == "brave"
    assert provider_name(replace(
        base, web_provider="auto", brave_search_api_key="",
        bing_search_api_key="key"
    )) == "bing"
    assert provider_name(replace(
        base, web_provider="auto", brave_search_api_key="",
        bing_search_api_key=""
    )) == "duckduckgo"
