"""The founder floor, driven through the real screening path.

The unit tests around pedigree_score_floor pass whether or not the floor is
wired into anything. Two bugs got past them:

* the floor read a variable defined in a different function, which would have
  raised NameError on the first real page the worker screened;
* it then lifted rule_score, the raw keyword measurement, while the tables
  sort and display diligence_rank_score -- so a 95 showed as a 37.

Both are only visible by running ``process`` over a page, which is what this
does. A cached Fetch means no network.
"""

import asyncio

import filter as screening_filter


def page(title, description, text):
    return screening_filter.Fetch(
        url="https://example.ai", status=200, title=title,
        description=description, text=text, pages_scraped=1,
        source="network_fetch",
    )


def screen(fetch):
    async def run():
        return await screening_filter.process(
            {"registrable": "example.ai", "domain": "example.ai"},
            None,
            asyncio.Semaphore(1),
            timeout=5.0,
            homepage_only=True,
            cached_page=fetch,
        )

    return asyncio.run(run())


ELITE_BUT_THIN = page(
    "Perceive AI — Enterprise perception infrastructure",
    "Perception infrastructure for enterprise robotics fleets.",
    "Perceive AI builds perception infrastructure for enterprise robotics "
    "fleets. Our founder Jane Doe is an MIT PhD who previously worked at "
    "Google; her previous company was acquired in 2023. We deploy on-premise "
    "for manufacturing customers with a proprietary sensor-fusion pipeline.",
)

AGENCY_WITH_ELITE_FOUNDER = page(
    "Bright Digital — Marketing agency",
    "We are a full service digital marketing agency for small businesses.",
    "Bright Digital is a full service digital marketing agency. We offer SEO, "
    "paid ads and web design for small businesses. Our founder studied "
    "computer science at Brown University. Contact us for a free quote on "
    "your next campaign. Clients include local restaurants and dentists.",
)


def test_an_exceptional_team_lifts_a_thin_page_through_the_real_path():
    from filter import PEDIGREE_BREADTH_FLOORS

    result = screen(ELITE_BUT_THIN)
    assert result["routing_bucket"] == "review"
    assert result["diligence_rank_score"] >= dict(PEDIGREE_BREADTH_FLOORS)[3]
    assert "founder_pedigree_floor" in result["routing_reason"]


def test_the_lift_reaches_the_score_the_tables_actually_show():
    """rule_score stays the honest keyword measurement -- scorer_benchmark
    holds it to that -- while diligence_rank_score is what the tables sort on.
    Lifting the wrong one showed a 95 as a 37.
    """
    result = screen(ELITE_BUT_THIN)
    assert result["diligence_rank_score"] > result["rule_score"]


def test_an_elite_founder_does_not_rescue_a_disqualified_company():
    """Measured on the live corpus before this was moved behind the router:
    the floor would have promoted a consumer SAT-prep site, a personal
    coaching site and a sports agency into the 80s, all of them already
    refused for a specific factual reason.
    """
    result = screen(AGENCY_WITH_ELITE_FOUNDER)
    assert result["routing_bucket"] == "reject"
    assert result["diligence_rank_score"] < 80
    assert "founder_pedigree_floor" not in result["routing_reason"]
