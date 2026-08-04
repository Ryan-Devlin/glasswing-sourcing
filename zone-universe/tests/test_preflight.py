from ai_search.extract import extract_page
from zone_universe.preflight import assess_homepage


def decision(domain: str, html: str):
    return assess_homepage(
        domain,
        extract_page(html, f"https://{domain}"),
    )


def test_clear_ai_infrastructure_company_is_admitted() -> None:
    result = decision(
        "vectorgrid.com",
        """
        <html><head>
        <title>VectorGrid AI Infrastructure</title>
        <meta name="description" content="GPU infrastructure and observability
        software for enterprise artificial intelligence teams.">
        </head><body>
        <h1>Run production AI reliably</h1>
        <p>Our platform helps companies deploy and monitor machine learning
        workloads across private cloud infrastructure.</p>
        <a href="/technology">Technology</a>
        <a href="/product">Product</a>
        </body></html>
        """,
    )
    assert result.promising
    assert result.score >= 60
    assert result.reason == "frontier_or_ai_product_signal"


def test_strong_vertical_ai_company_is_admitted() -> None:
    result = decision(
        "clinicpilot.app",
        """
        <html><head><title>ClinicPilot</title>
        <meta name="description" content="AI agents that automate clinical
        documentation workflows for healthcare teams."></head>
        <body><h1>Clinical workflow automation</h1>
        <p>Our software helps hospitals reduce administrative work.</p></body>
        </html>
        """,
    )
    assert result.promising


def test_parked_domain_stops_before_storage() -> None:
    result = decision(
        "premiumrobotics.com",
        """
        <html><title>PremiumRobotics.com</title><body>
        This domain is for sale. Contact our brokerage team to buy this domain.
        </body></html>
        """,
    )
    assert not result.promising
    assert result.reason == "parked_or_for_sale"


def test_unrelated_shop_stops_before_full_crawl() -> None:
    result = decision(
        "coastalcandles.app",
        """
        <html><head><title>Coastal Candles</title>
        <meta name="description" content="Hand-poured candles and home scents
        shipped from our family workshop."></head>
        <body><h1>Summer candle collection</h1>
        <p>Shop soy candles, gift boxes, and seasonal fragrances.</p></body>
        </html>
        """,
    )
    assert not result.promising
    assert result.reason == "no_clear_technology_company_signal"


def test_thesis_relevant_prelaunch_domain_is_preserved() -> None:
    result = decision(
        "quantumforge.com",
        "<html><title>QuantumForge</title><body>Coming soon</body></html>",
    )
    assert result.promising
    assert result.reason == "promising_prelaunch"
