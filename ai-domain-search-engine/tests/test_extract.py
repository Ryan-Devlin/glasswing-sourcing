from ai_search.extract import (
    extract_page,
    homepage_enrichment_plan,
    rank_internal_links,
    same_site_url,
)


def test_extracts_searchable_content_and_metadata():
    page = extract_page("""
    <html lang="en"><head><title>Robotics Platform</title>
    <meta name="description" content="Automation for factories"></head>
    <body><nav>Noise</nav><h1>Physical AI</h1>
    <p>Our robots improve supply chain operations.</p>
    <script>secretNoise()</script></body></html>
    """, "https://robots.ai")
    assert page.title == "Robotics Platform"
    assert page.description == "Automation for factories"
    assert "supply chain" in page.cleaned_text
    assert "secretNoise" not in page.cleaned_text
    assert "Noise" not in page.cleaned_text


def test_detects_parked_and_coming_soon():
    assert extract_page(
        "<p>This domain is for sale</p>", "https://x.ai"
    ).is_parked
    assert extract_page(
        "<p>Coming soon. Join the waitlist.</p>", "https://x.ai"
    ).is_coming_soon


def test_indexes_searchable_html_metadata_without_dropping_raw_html_support():
    page = extract_page("""
    <html><head>
      <meta property="og:description" content="Quantum data platform">
      <script type="application/ld+json">
        {"name":"Northstar Systems","industry":"Industrial robotics"}
      </script>
    </head><body>
      <img alt="Autonomous warehouse robot">
    </body></html>
    """, "https://northstar.ai")
    assert page.description == "Quantum data platform"
    assert "Northstar Systems" in page.cleaned_text
    assert "Industrial robotics" in page.cleaned_text
    assert "Autonomous warehouse robot" in page.cleaned_text


def test_discovers_high_value_links_before_removing_navigation_noise():
    page = extract_page(
        """
        <html><body>
          <nav>
            <a href="/product">Product</a>
            <a href="/research">Research</a>
            <a href="/founders">Founders</a>
            <a href="/blog">Blog</a>
            <a href="/privacy">Privacy</a>
            <a href="/login">Log in</a>
          </nav>
          <main><h1>Autonomous systems for factories</h1></main>
        </body></html>
        """,
        "https://www.robotics.ai/",
    )

    assert page.internal_links[:3] == (
        "https://www.robotics.ai/research",
        "https://www.robotics.ai/founders",
        "https://www.robotics.ai/product",
    )
    assert all("privacy" not in link for link in page.internal_links)
    assert all("login" not in link for link in page.internal_links)
    assert "Founders" not in page.cleaned_text
    assert "Autonomous systems" in page.cleaned_text


def test_link_ranking_prioritizes_product_then_team_when_no_technical_page():
    ranked = rank_internal_links(
        [
            ("https://example.ai/pricing", "Pricing"),
            ("https://example.ai/product", "Product"),
            ("https://example.ai/team", "Team"),
            ("https://example.ai/solutions/retail", "Retail solution"),
        ]
    )

    assert ranked[:2] == (
        "https://example.ai/product",
        "https://example.ai/team",
    )


def test_first_party_docs_subdomain_is_allowed_but_unrelated_sites_are_not():
    assert same_site_url(
        "https://docs.example.ai/research",
        "https://www.example.ai/",
    )
    assert same_site_url(
        "https://example.ai/about",
        "https://www.example.ai/",
    )
    assert not same_site_url(
        "https://unrelated.ai/research",
        "https://www.example.ai/",
    )


def test_technical_research_post_survives_blog_penalty():
    ranked = rank_internal_links(
        [
            ("https://example.ai/blog", "Blog"),
            (
                "https://example.ai/blog/quantum-research",
                "Quantum research",
            ),
        ]
    )
    assert ranked == (
        "https://example.ai/blog/quantum-research",
    )


def test_homepage_gate_spends_full_budget_on_plausible_company():
    page = extract_page(
        """
        <html><body>
          <nav>
            <a href="/technology">Technology</a>
            <a href="/team">Team</a>
          </nav>
          <main>
            Enterprise robotics platform for autonomous manufacturing.
          </main>
        </body></html>
        """,
        "https://robotics.ai",
    )
    assert homepage_enrichment_plan(page, 2) == (
        2,
        "homepage_plausible_company",
    )


def test_homepage_gate_uses_one_page_for_ambiguous_company():
    page = extract_page(
        """
        <html><body>
          <nav><a href="/about">About</a></nav>
          <main>Building a better future for organizations worldwide.</main>
        </body></html>
        """,
        "https://ambiguous.ai",
    )
    assert homepage_enrichment_plan(page, 2) == (
        1,
        "homepage_ambiguous_one_page_check",
    )


def test_homepage_gate_stops_obvious_non_candidates_after_homepage():
    parked = extract_page(
        """
        <html><body>
          This domain is for sale.
          <a href="/product">Product</a>
        </body></html>
        """,
        "https://forsale.ai",
    )
    prelaunch = extract_page(
        """
        <html><body>
          Coming soon. Join the waitlist.
          <a href="/about">About</a>
        </body></html>
        """,
        "https://prelaunch.ai",
    )
    no_links = extract_page(
        "<html><body>A simple personal homepage.</body></html>",
        "https://personal.ai",
    )

    assert homepage_enrichment_plan(parked, 2)[0] == 0
    assert homepage_enrichment_plan(prelaunch, 2)[0] == 0
    assert homepage_enrichment_plan(no_links, 2) == (
        0,
        "homepage_no_evidence_links",
    )
