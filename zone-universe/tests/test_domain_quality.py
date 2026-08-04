from __future__ import annotations

import pytest

from zone_universe.domain_quality import assess_domain, crawl_priority


@pytest.mark.parametrize(
    "domain",
    [
        "openai.com",
        "stripe.com",
        "10xgenomics.com",
        "a5c.app",
        "0x88.app",
        "x.app",
        "material-security.app",
        "xn--bcher-kva.app",
    ],
)
def test_plausible_and_short_brand_domains_are_kept(domain: str) -> None:
    assert assess_domain(domain).accepted is True


@pytest.mark.parametrize(
    "domain",
    [
        "12987choeiusryhfq38boeuirsy.app",
        "1234567890123.com",
        "000079gsb.app",
        "00160c.app",
        "0003.app",
        "1c111.app",
        "176722.app",
        "123p.app",
        "12x7.app",
        "qwertyasdfzxcvbnm.app",
        "aaaaaaaaaaaaaaa.com",
        "pqlkjnmrtvwxzbcdfgh.app",
        "pqlkjnmrtvwxz.com",
        "ab-ab-ab-ab-ab.com",
        "a-b-c-d-e-f-g-h-i.com",
    ],
)
def test_obvious_generated_labels_are_rejected(domain: str) -> None:
    decision = assess_domain(domain)
    assert decision.accepted is False
    assert decision.score >= 70


def test_company_like_domains_are_crawled_before_noisy_safe_names() -> None:
    assert crawl_priority("quantumsecurity.app") == 0
    assert crawl_priority("openai.com") <= 1
    assert crawl_priority("0x88.app") > crawl_priority("openai.com")


@pytest.mark.parametrize(
    "domain",
    [
        "qodo.com",
        "x.com",
        "a5c.app",
        "0x88.app",
        "1password.com",
        "3dscanner.app",
    ],
)
def test_unconventional_but_plausible_brands_remain_active(domain: str) -> None:
    assert crawl_priority(domain) <= 2


@pytest.mark.parametrize(
    "domain",
    [
        "localareashoppingnetworks.com",
        "londonmobiletyreexpress247.com",
        "sfgrt2ht99ar0-xef8a.com",
        "v1x2sdb9ieyecxwd.app",
    ],
)
def test_low_value_names_are_cold_priority(domain: str) -> None:
    assert crawl_priority(domain) == 3


@pytest.mark.parametrize(
    "domain",
    [
        "flowers.com",
        "bakery.com",
        "lawoffice.com",
        "londonplumbing.com",
        "thebestflorist.com",
    ],
)
def test_generic_dotcom_real_words_are_deprioritized(domain: str) -> None:
    # A plain dictionary word or generic small-business name on .com is
    # statistically identical, letter-shape-wise, to an invented startup
    # brand -- .com's scale (100M+) means treating every non-gibberish word
    # as crawl-worthy made the active queue effectively the whole zone
    # file. These require an explicit thesis keyword or a short/brandable
    # name to earn an active tier; a generic real word alone doesn't.
    assert crawl_priority(domain) == 3


@pytest.mark.parametrize(
    "domain",
    [
        "flowers.app",
        "bakery.app",
        "lawoffice.app",
    ],
)
def test_generic_dotapp_real_words_remain_active(domain: str) -> None:
    # .app is a small, inherently startup-signaling universe -- keep its
    # broader recall even for names with no explicit thesis keyword.
    assert crawl_priority(domain) <= 2


def test_digit_stylized_brand_names_stay_active_on_dotcom() -> None:
    # The digit is a deliberate brand signal here (1Password, 3D Scanner),
    # not a random-generation artifact -- these should stay in an active
    # tier even under the stricter .com policy.
    assert crawl_priority("1password.com") == 1
    assert crawl_priority("3dscanner.com") == 1


def test_widened_company_hints_catch_more_thesis_relevant_terms() -> None:
    for domain in (
        "acmerobotics.com",
        "northstarbiotech.com",
        "vertexfintech.com",
        "clearvisionml.com",
    ):
        assert crawl_priority(domain) == 0
