from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

SPACE = re.compile(r"\s+")
EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
PARKED_SIGNALS = (
    "domain is for sale", "buy this domain", "sedo domain parking",
    "afternic", "hugedomains", "parked free", "this domain may be for sale",
    "godaddy.com/forsale", "namecheap parking",
)
COMING_SOON_SIGNALS = (
    "coming soon", "launching soon", "join the waitlist",
    "request early access", "under construction", "stealth mode",
)
PLACEHOLDER_STOP_SIGNALS = (
    "default web site page",
    "apache2 ubuntu default page",
    "welcome to nginx",
    "powered by cpanel",
    "future home of something quite cool",
    "this domain has been registered",
    "website is under construction",
)
LOW_VALUE_HOMEPAGE_SIGNALS = (
    "personal blog",
    "my personal website",
    "my portfolio",
    "online casino",
    "sports betting picks",
    "coupon codes",
    "link in bio",
    "domain marketplace",
)
COMPANY_HOMEPAGE_SIGNALS = (
    "enterprise",
    "platform",
    "software",
    "infrastructure",
    "technology",
    "product",
    "solution",
    "customers",
    "security",
    "artificial intelligence",
    "machine learning",
    "robotics",
    "autonomous",
    "quantum",
    "developer",
    "api",
    "workflow",
    "industries",
    "business",
    "company",
    "team",
    "founded",
)
TECHNICAL_LINK_SIGNALS = {
    "research": 140,
    "whitepaper": 135,
    "technology": 132,
    "science": 128,
    "architecture": 125,
    "security": 122,
    "trust": 118,
    "engineering": 115,
    "developers": 112,
    "developer": 110,
    "documentation": 108,
    "docs": 106,
    "api": 102,
}
PRODUCT_LINK_SIGNALS = {
    "product": 130,
    "platform": 122,
    "solutions": 115,
    "solution": 112,
    "use-cases": 108,
    "usecases": 108,
    "industries": 98,
    "enterprise": 92,
}
COMPANY_LINK_SIGNALS = {
    "founders": 135,
    "founder": 132,
    "leadership": 130,
    "team": 128,
    "about-us": 118,
    "about": 115,
    "company": 105,
}
TRACTION_LINK_SIGNALS = {
    "case-studies": 105,
    "customers": 102,
    "customer-stories": 102,
    "partners": 85,
}
LOW_VALUE_LINK_SIGNALS = {
    "blog": 35,
    "news": 35,
    "resources": 20,
    "careers": 50,
    "jobs": 55,
    "pricing": 18,
    "contact": 20,
    "events": 18,
}
EXCLUDED_LINK_SIGNALS = (
    "privacy", "terms", "cookies", "legal", "login", "log-in", "signin",
    "sign-in", "signup", "sign-up", "account", "cart", "checkout",
)
EXCLUDED_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".zip",
    ".mp4", ".mp3", ".css", ".js",
)


@dataclass(frozen=True)
class ExtractedPage:
    title: str
    description: str
    headings: str
    body: str
    cleaned_text: str
    canonical_url: str
    language: str
    emails: str
    social_links: str
    json_ld: str
    internal_links: tuple[str, ...]
    word_count: int
    content_hash: str
    is_parked: bool
    is_coming_soon: bool


def _clean(value: str) -> str:
    return SPACE.sub(" ", value or "").strip()


def _site_host(value: str) -> str:
    host = (urlsplit(value).hostname or "").lower().rstrip(".")
    return host.removeprefix("www.")


def same_site_url(first: str, second: str) -> bool:
    """Treat apex/www and first-party subdomains as the same company site."""
    first_host = _site_host(first)
    second_host = _site_host(second)
    if not first_host or not second_host:
        return False
    return bool(
        first_host == second_host
        or first_host.endswith(f".{second_host}")
        or second_host.endswith(f".{first_host}")
    )


def _link_tokens(url: str, label: str) -> set[str]:
    parsed = urlsplit(url)
    value = f"{parsed.path} {label}".lower()
    return {
        token
        for token in re.split(r"[^a-z0-9]+", value)
        if token
    }


def _signal_score(
    tokens: set[str],
    path: str,
    signals: dict[str, int],
) -> int:
    scores = [
        score
        for term, score in signals.items()
        if term in tokens or term in path
    ]
    return max(scores, default=0)


def score_internal_link(url: str, label: str) -> tuple[int, str]:
    """Score a link by the investment evidence it is likely to contain."""
    parsed = urlsplit(url)
    path = parsed.path.lower().rstrip("/")
    tokens = _link_tokens(url, label)
    if (
        not path
        or path == "/"
        or path.endswith(EXCLUDED_EXTENSIONS)
        or any(term in tokens for term in EXCLUDED_LINK_SIGNALS)
    ):
        return 0, ""

    families = {
        "technical": _signal_score(
            tokens,
            path,
            TECHNICAL_LINK_SIGNALS,
        ),
        "product": _signal_score(
            tokens,
            path,
            PRODUCT_LINK_SIGNALS,
        ),
        "company": _signal_score(
            tokens,
            path,
            COMPANY_LINK_SIGNALS,
        ),
        "traction": _signal_score(
            tokens,
            path,
            TRACTION_LINK_SIGNALS,
        ),
    }
    family, score = max(
        families.items(),
        key=lambda item: (item[1], item[0]),
    )
    if score <= 0:
        return 0, ""

    low_value_penalty = max(
        (
            penalty
            for term, penalty in LOW_VALUE_LINK_SIGNALS.items()
            if term in tokens or term in path
        ),
        default=0,
    )
    # Research posts and technical resources remain useful even when nested
    # beneath a blog/resources path.
    if family == "technical" and score >= 120:
        low_value_penalty = min(low_value_penalty, 10)
    depth = len([part for part in path.split("/") if part])
    score -= min(18, max(0, depth - 2) * 4)
    score -= low_value_penalty
    return max(0, score), family


def rank_internal_links(
    candidates: list[tuple[str, str]],
) -> tuple[str, ...]:
    """Rank and diversify pages for wedge, moat, founder, and traction data."""
    best_by_url: dict[str, tuple[int, str, str]] = {}
    for url, label in candidates:
        parsed = urlsplit(url)
        normalized = urlunsplit(
            (
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                parsed.path.rstrip("/") or "/",
                "",
                "",
            )
        )
        score, family = score_internal_link(normalized, label)
        if score <= 0:
            continue
        current = best_by_url.get(normalized)
        candidate = (score, family, normalized)
        if current is None or candidate[:2] > current[:2]:
            best_by_url[normalized] = candidate

    remaining = list(best_by_url.values())
    ranked: list[str] = []
    used_families: set[str] = set()
    while remaining:
        # A diversity bonus prevents two generic product pages from crowding
        # out a strong Team/About or technical-evidence page.
        selected = max(
            remaining,
            key=lambda item: (
                item[0] + (22 if item[1] not in used_families else 0),
                item[0],
                -len(item[2]),
                item[2],
            ),
        )
        remaining.remove(selected)
        ranked.append(selected[2])
        used_families.add(selected[1])
    return tuple(ranked)


def homepage_enrichment_plan(
    page: ExtractedPage,
    maximum_extra_pages: int,
) -> tuple[int, str]:
    """Choose zero, one, or two follow-up pages from homepage evidence.

    This is intentionally conservative: it controls request spend, while the
    full Glasswing classifier still decides the final investment bucket after
    the retained documents are merged.
    """
    maximum = max(0, int(maximum_extra_pages))
    if maximum == 0:
        return 0, "homepage_only_configured"

    lower = page.cleaned_text.lower()
    if page.is_parked:
        return 0, "homepage_parked_or_for_sale"
    if (
        any(signal in lower for signal in COMING_SOON_SIGNALS)
        and page.word_count < 120
    ):
        return 0, "homepage_explicit_prelaunch"
    if any(signal in lower for signal in PLACEHOLDER_STOP_SIGNALS):
        return 0, "homepage_hosting_placeholder"

    language = page.language.lower().replace("_", "-")
    if language and not language.startswith("en"):
        return 0, "homepage_explicit_non_english"
    if not page.internal_links:
        return 0, "homepage_no_evidence_links"

    company_hits = {
        signal
        for signal in COMPANY_HOMEPAGE_SIGNALS
        if signal in lower
    }
    low_value_hits = {
        signal
        for signal in LOW_VALUE_HOMEPAGE_SIGNALS
        if signal in lower
    }
    linked_families = {
        family
        for link in page.internal_links
        for _, family in [score_internal_link(link, "")]
        if family
    }
    strong_evidence_link = bool(
        linked_families.intersection({"technical", "product"})
    )

    if low_value_hits and not strong_evidence_link and len(company_hits) < 2:
        return 0, "homepage_obvious_low_value_site"
    if strong_evidence_link or len(company_hits) >= 2:
        return maximum, "homepage_plausible_company"
    if (
        linked_families.intersection({"company", "traction"})
        or company_hits
        or page.word_count >= 30
    ):
        return min(1, maximum), "homepage_ambiguous_one_page_check"
    return 0, "homepage_no_company_evidence"


def extract_page(html: str, url: str) -> ExtractedPage:
    soup = BeautifulSoup(html or "", "html.parser")
    structured: list[object] = []
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            structured.append(json.loads(node.string or ""))
        except (TypeError, json.JSONDecodeError):
            continue

    # Discover navigation targets before nav/footer removal. Those elements
    # are intentionally excluded from searchable copy, but they contain most
    # links to the product, technology, security, About, and Team pages.
    social: set[str] = set()
    link_candidates: list[tuple[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        absolute = urljoin(url, anchor["href"]).split("#", 1)[0]
        parsed = urlsplit(absolute)
        if any(site in parsed.netloc.lower() for site in (
            "linkedin.com", "twitter.com", "x.com", "github.com",
            "youtube.com", "facebook.com",
        )):
            social.add(absolute)
        if (
            parsed.scheme in {"http", "https"}
            and same_site_url(absolute, url)
        ):
            link_candidates.append(
                (absolute, anchor.get_text(" ", strip=True))
            )
    internal_links = rank_internal_links(link_candidates)

    for node in soup(
        ["script", "style", "svg", "noscript", "template", "iframe"]
    ):
        node.decompose()
    for node in soup.select(
        "[hidden], [aria-hidden='true'], .cookie, .cookie-banner, "
        "#cookie-banner, nav, footer"
    ):
        node.decompose()

    title = _clean(soup.title.get_text(" ", strip=True) if soup.title else "")
    meta = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    if meta is None:
        meta = soup.find(
            "meta",
            attrs={"property": re.compile("^og:description$", re.I)},
        )
    description = _clean(meta.get("content", "") if meta else "")
    headings = _clean(" ".join(
        node.get_text(" ", strip=True) for node in soup.find_all(
            ["h1", "h2", "h3"]
        )
    ))
    body = _clean(soup.get_text(" ", strip=True))
    image_alt_text = _clean(
        " ".join(
            str(node.get("alt", ""))
            for node in soup.find_all("img", alt=True)
        )
    )
    structured_text = _clean(
        json.dumps(structured, ensure_ascii=False)
    )
    cleaned = _clean(
        " ".join(
            filter(
                None,
                [
                    title,
                    description,
                    headings,
                    body,
                    image_alt_text,
                    structured_text,
                ],
            )
        )
    )
    canonical = soup.find("link", attrs={"rel": lambda x: x and "canonical" in x})
    canonical_url = urljoin(url, canonical.get("href", "")) if canonical else ""
    language = _clean(soup.html.get("lang", "") if soup.html else "")
    emails = sorted(set(EMAIL.findall(body)))

    lowered = cleaned.lower()
    words = body.split()
    return ExtractedPage(
        title=title,
        description=description,
        headings=headings,
        body=body,
        cleaned_text=cleaned,
        canonical_url=canonical_url,
        language=language,
        emails=json.dumps(emails),
        social_links=json.dumps(sorted(social)),
        json_ld=json.dumps(structured, ensure_ascii=False),
        internal_links=internal_links,
        word_count=len(words),
        content_hash=hashlib.sha256(cleaned.encode()).hexdigest(),
        is_parked=any(signal in lowered for signal in PARKED_SIGNALS),
        is_coming_soon=(
            any(signal in lowered for signal in COMING_SOON_SIGNALS)
            or (0 < len(words) < 50)
        ),
    )
