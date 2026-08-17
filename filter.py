#!/usr/bin/env python3
"""Strict U.S.-only website enrichment and pre-Claude routing."""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
import pandas as pd
from db_backend import using_postgres
from glasswing_portfolio import GLASSWING_PORTFOLIO_DOMAINS
from bs4 import BeautifulSoup
from label_calibrator import calibrate_text
from classification_store import (
    CLASSIFICATION_DB,
    bootstrap_from_csv,
    count_classifications,
    export_all_screened_csv,
    record_automated_exclusions,
    upsert_classifications,
    using_classification_postgres,
)

from common import (
    AI_SEARCH_DB,
    FILTERED,
    LLM,
    NO_BASKET_DATA,
    RAW,
    ROOT,
    RUN_CONFIG,
    YES_BASKET_DATA,
    ZONE_UNIVERSE_DB,
    atomic_write_csv,
    clean,
    read_basket_data,
    stable_domain,
    upsert_csv,
)

USER_AGENT = "Mozilla/5.0 Glasswing-Sourcing/2.0"

# asyncio.to_thread() uses the event loop's DEFAULT executor, sized at
# min(32, (os.cpu_count() or 1) + 4) -- os.cpu_count() reports the host's
# CPU count, not a container's cgroup quota, so this can look plenty large
# while still being far smaller than a 75-domain batch's peak concurrent
# demand (every domain's scoring, plus one persist call, all wanting a
# thread at once). Queueing time waiting for a free thread doesn't show up
# in any per-operation timer, only in the batch's total wall-clock time --
# a dedicated, generously-sized pool removes that as a variable.
_SCORING_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    # A 128-thread pool plus 100 concurrent scrapers retained nearly 7 GB in
    # the always-on Railway worker. Scoring is CPU-bound; far more threads
    # than the container can execute only adds stacks, allocator arenas, and
    # context switching. Keep this bounded and independently configurable.
    max_workers=max(
        4,
        min(16, int(os.getenv("GLASSWING_SCORING_THREADS", "8"))),
    ),
    thread_name_prefix="glasswing-scoring",
)


def _report_slow_operation(kind: str, **fields: object) -> None:
    """Surface a slow-operation diagnostic on the live dashboard.

    print()-based diagnostics in this module never reached anywhere
    observable -- confirmed live: railway.pipeline-worker.json runs a bare
    "python continuous_pipeline.py" with no log capture wired up, so
    stderr output (even flush=True prints) was invisible for the entire
    investigation, making "no slow operation was ever logged" a false
    negative rather than real evidence. write_status() already reaches the
    live dashboard reliably in both local and cloud (Postgres-backed)
    modes, so route diagnostics through it instead of print().
    """
    try:
        import continuous_pipeline

        continuous_pipeline.write_status(
            **{f"last_slow_{kind}": {**fields, "logged_at": continuous_pipeline.utc_now()}}
        )
    except Exception:
        pass


def crawler_database_paths() -> tuple[Path, Path]:
    """Resolve portable overrides while keeping ROOT injectable in tests."""
    ai_path = (
        AI_SEARCH_DB
        if os.getenv("AI_SEARCH_DB")
        else ROOT / "ai-domain-search-engine" / "data" / "search.sqlite3"
    )
    zone_path = (
        ZONE_UNIVERSE_DB
        if os.getenv("ZONE_UNIVERSE_DB")
        else ROOT / "zone-universe" / "data" / "universe.sqlite3"
    )
    return ai_path, zone_path


def persist_crawled_corpus_views(
    all_screened_master: pd.DataFrame,
) -> dict[str, int]:
    """Maintain dedicated .AI and Zone Universe compatibility datasets."""
    search_db, zone_db = crawler_database_paths()
    if all_screened_master.empty:
        return {}

    ai_domains: set[str] = set()
    if search_db.exists():
        with sqlite3.connect(search_db, timeout=30) as conn:
            ai_domains = {
                str(row[0])
                for row in conn.execute("""
                    SELECT domain FROM domains
                    WHERE last_crawled_at IS NOT NULL
                """)
            }
    zone_domains: set[str] = set()
    if zone_db.exists():
        with sqlite3.connect(zone_db, timeout=30) as conn:
            zone_domains = {
                str(row[0])
                for row in conn.execute("SELECT domain FROM pages")
            }

    def datasets(
        domains: set[str],
        *,
        prefix: str,
    ) -> dict[str, pd.DataFrame]:
        corpus = all_screened_master[
            all_screened_master["domain"].map(stable_domain).isin(domains)
        ].copy()
        buckets = corpus.get(
            "routing_bucket",
            pd.Series("", index=corpus.index),
        ).astype(str)
        return {
            f"{prefix}all_screened.csv": corpus,
            f"{prefix}investable.csv": corpus[buckets.eq("priority")],
            # "coming_soon"/"insufficient"/"ghost"/"screened_out" are legacy
            # stored values from before 2026-08-08 -- route_company only
            # produces "review" or "reject" now. Coming Soon and Insufficient
            # Evidence are no longer separate outcomes.
            f"{prefix}needs_review.csv": corpus[
                buckets.isin([
                    "review", "coming_soon", "insufficient", "ghost",
                    "screened_out",
                ])
            ],
            f"{prefix}potential_comp.csv": corpus[buckets.eq("comp")],
            f"{prefix}hard_denied.csv": corpus[buckets.eq("reject")],
        }

    outputs = {
        **datasets(ai_domains, prefix="crawled_"),
        **datasets(zone_domains, prefix="zone_crawled_"),
    }
    for filename, frame in outputs.items():
        atomic_write_csv(FILTERED / filename, frame)
    return {
        filename.removesuffix(".csv"): len(frame)
        for filename, frame in outputs.items()
    }


def persist_claude_decision_views() -> dict[str, int]:
    """Master + per-source Investable/Hard Denied, keyed on Claude's own
    decision rather than the Python routing_bucket.

    "If a domain is ever passed through Claude and accepted, it appends
    straight to the master investable and the local investable" -- KEEP is
    the only decision that counts as accepted. There is no MAYBE: every
    Claude call resolves to KEEP or REJECT, never an in-between outcome.
    llm_approved_master.csv/llm_denied_master.csv (written
    by claude_classifier.py's reconcile_masters via upsert_csv) already
    accumulate permanently, by domain, across every run and every source --
    this just derives the master/local KEEP-only and REJECT-only views from
    them. Deliberately NOT gated behind bulk_compatibility_exports_enabled():
    these are small, Claude-reviewed-only subsets, not full-corpus mirrors,
    and are the only durable record of Claude's own decision.
    """
    if using_classification_postgres():
        return {"database_backed": 1}
    approved_path = LLM / "llm_approved_master.csv"
    denied_path = LLM / "llm_denied_master.csv"
    approved = (
        pd.read_csv(approved_path, dtype=str).fillna("")
        if approved_path.exists() and approved_path.stat().st_size
        else pd.DataFrame()
    )
    denied = (
        pd.read_csv(denied_path, dtype=str).fillna("")
        if denied_path.exists() and denied_path.stat().st_size
        else pd.DataFrame()
    )
    investable = (
        approved[approved["llm_decision"].eq("KEEP")].copy()
        if not approved.empty and "llm_decision" in approved.columns
        else pd.DataFrame()
    )
    hard_denied = (
        denied[denied["llm_decision"].eq("REJECT")].copy()
        if not denied.empty and "llm_decision" in denied.columns
        else pd.DataFrame()
    )

    search_db, zone_db = crawler_database_paths()
    ai_domains: set[str] = set()
    if search_db.exists():
        with sqlite3.connect(search_db, timeout=30) as conn:
            ai_domains = {
                str(row[0])
                for row in conn.execute("""
                    SELECT domain FROM domains
                    WHERE last_crawled_at IS NOT NULL
                """)
            }
    zone_domains: set[str] = set()
    if zone_db.exists():
        with sqlite3.connect(zone_db, timeout=30) as conn:
            zone_domains = {
                str(row[0]) for row in conn.execute("SELECT domain FROM pages")
            }

    def local_view(frame: pd.DataFrame, domains: set[str]) -> pd.DataFrame:
        if frame.empty or "domain" not in frame.columns:
            return frame
        return frame[frame["domain"].map(stable_domain).isin(domains)]

    def certstream_view(frame: pd.DataFrame) -> pd.DataFrame:
        # CertStream has no dedicated corpus database of its own to query
        # membership from -- its "local" view is simply whatever wasn't
        # discovered through the other two crawlers.
        if frame.empty or "domain" not in frame.columns:
            return frame
        crawled = ai_domains | zone_domains
        return frame[~frame["domain"].map(stable_domain).isin(crawled)]

    outputs = {
        "master_investable_claude.csv": investable,
        "master_hard_denied_claude.csv": hard_denied,
        "crawled_investable_claude.csv": local_view(investable, ai_domains),
        "crawled_hard_denied_claude.csv": local_view(hard_denied, ai_domains),
        "zone_crawled_investable_claude.csv": local_view(investable, zone_domains),
        "zone_crawled_hard_denied_claude.csv": local_view(hard_denied, zone_domains),
        "certstream_investable_claude.csv": certstream_view(investable),
        "certstream_hard_denied_claude.csv": certstream_view(hard_denied),
    }
    for filename, frame in outputs.items():
        atomic_write_csv(FILTERED / filename, frame)
    return {
        filename.removesuffix(".csv"): len(frame)
        for filename, frame in outputs.items()
    }

# Exact Fund III focus areas, translated into website-language signals.
THESIS = {
    "vertical_ai": [
        "industry-specific", "vertical ai", "workflow automation",
        "healthcare ai", "legal ai", "insurance ai", "manufacturing ai",
        "logistics ai", "financial services ai", "life sciences ai",
        "construction ai", "supply chain ai", "procurement ai",
        "medical interpretation", "clinical documentation",
        "clinical workflow", "healthcare workflow", "industry operating system",
        "entire workflow", "end-to-end workflow",
        "recipe formulation", "food innovation", "delivery dispatch",
        "delivery dispatching", "delivery management", "engagement intelligence",
        "recognition and performance", "pcb layout", "electronics design",
    ],
    "physical_ai": [
        "physical ai", "robotics", "computer vision", "machine vision",
        "autonomous system", "autonomous systems", "sensor fusion",
        "predictive maintenance", "industrial automation", "digital twin",
        "edge ai", "robot perception", "real-world environment",
        "telemetry", "emergency response", "physical engineering",
        "application-layer robotics", "robotics evaluation",
        "self-propelled vehicle", "satellite imagery", "aerial imagery",
        "geospatial intelligence", "autonomous reliability",
    ],
    "ai_adaptive_infrastructure": [
        "ai-adaptive infrastructure", "reinforcement learning",
        "self-improving", "model evaluation", "model monitoring",
        "mlops", "llmops", "inference infrastructure", "model serving",
        "agent infrastructure", "agent orchestration", "training data",
        "synthetic data", "gpu orchestration", "ai observability",
        "adaptive model", "continuous learning", "agent runtime",
        "ai runtime", "ai governance", "model governance",
        "databricks", "finops", "cost intelligence", "customer context layer",
        "ai gateway", "prompt management", "model distillation",
        "purpose-built models", "evaluation infrastructure",
        "reproducible evaluation", "workload reliability",
        "ai infrastructure as a service", "ai infrastructure service",
        "gpu as a service", "inference as a service",
        "serverless inference", "managed inference", "model serving api",
        "training as a service", "ai compute platform",
    ],
    "intelligent_enterprise_defense": [
        "intelligent enterprise defense", "cybersecurity", "threat detection",
        "autonomous remediation", "security operations", "identity security",
        "cloud security", "application security", "endpoint security",
        "data security", "fraud detection", "threat intelligence",
        "security automation", "attack surface", "zero-trust", "zero trust",
        "prompt injection", "data leakage", "adversarial attack",
        "dynamic masking", "security proxy", "protocol-aware",
        "ai security", "agent security", "security and compliance",
        "attack corpus", "audit-ready", "physical security",
        "video security", "agent security protocol",
        "programmable guardrails", "mpc co-signing",
    ],
    "collaborative_intelligence": [
        "collaborative intelligence", "ai teammate", "copilot",
        "human-in-the-loop", "human ai collaboration", "enterprise search",
        "knowledge management", "document intelligence",
        "decision intelligence", "embedded workflow", "agentic workflow",
        "work alongside", "augment employees"
    ],
    "next_gen_compute": [
        "next-gen compute", "next generation compute", "distributed computing",
        "distributed architecture", "quantum computing", "quantum",
        "semiconductor", "ai chip", "accelerator", "photonics",
        "neuromorphic", "reservoir computing", "novel compute",
        "compute architecture", "high-performance computing"
    ],
}

ENTERPRISE = [
    "enterprise", "b2b", "businesses", "organizations", "companies",
    "teams", "developers", "manufacturers", "banks", "insurers",
    "security teams", "data teams", "operations teams", "enterprises",
    "finance teams", "engineering teams", "it teams", "hr teams",
    "security leaders", "ciso", "cio", "cto", "cfo", "government agencies",
    "industrial operators", "health systems", "healthcare",
    "emergency services", "accounting firms",
    "request a demo", "book a demo", "contact sales", "talk to sales"
]

PRODUCT = [
    "platform", "software", "api", "product", "integration", "integrations",
    "documentation", "docs", "pricing", "deploy", "deployment",
    "solution", "solutions", "dashboard", "customers", "case studies",
    "request a demo", "book a demo", "contact sales", "enterprise-grade",
    "workflow", "infrastructure", "security", "on-premise", "private cloud",
    "ai agent", "ai agents", "control plane", "proxy"
]

AI = [
    "artificial intelligence", "machine learning", "generative ai",
    "ai-native", "agentic", "ai agent", "ai agents", "llm",
    "computer vision", "deep learning", "foundation model",
    "neural network", "reinforcement learning"
]

COMING_SOON = [
    "coming soon", "launching soon", "we're building", "we are building",
    "building something", "currently building", "in stealth", "stealth mode",
    "under construction", "join the waitlist", "get notified when we launch",
    "get notified when it's ready", "be the first to know",
    "early access coming soon", "private beta coming soon"
]

PARKED = [
    "domain for sale", "buy this domain", "afternic", "sedo", "hugedomains",
    "parkingcrew", "make an offer", "this domain is parked", "dan.com",
    "this domain is registered at dynadot", "registered at dynadot",
    "domain name registration", "this domain may be for sale",
    "premium domain available for purchase", "verified premium domain",
    "secure checkout and quick transfer", "submit your offer today",
    "available for acquisition", "pay in installments",
    "spaceship. secure checkout", "sudos.com", "unstoppable domains",
    "domain availability and pricing", "internet domain registration",
    # Confirmed live (2026-08-08): real registrar parking pages use
    # phrasing that didn't match the exact strings above -- Porkbun's
    # "porkbun.com | parked domain", Gandi's "It is currently parked by
    # the owner", GoDaddy's own for-sale marketplace redirect copy.
    "parked domain", "parked by the owner", "is currently parked",
    "parked free", "domain is parked", "may be for sale on the",
    "speeder marketplace", "browse other businesses for sale",
    # Generic ad-supported domain-parking/SEO-filler templates -- the
    # actual site name gets interpolated into the template, but the
    # surrounding boilerplate phrase itself is a fixed, real signal.
    "your first and best source for information about",
    "find the best information and most relevant links",
    "here you will also find topics relating to issues of general interest",
]

DOMAIN_SALE_PATTERNS = [
    (
        "domain_for_sale",
        re.compile(
            r"\bdomains?(?:\s+(?:name|names|pair|portfolio|collection|"
            r"assets?)){0,3}\s+(?:is\s+|are\s+)?for sale\b"
        ),
    ),
    (
        "domain_acquisition_pitch",
        re.compile(
            r"\binterested in (?:this|the|a) domain\b|"
            r"\bdomain(?: name)? available for acquisition\b"
        ),
    ),
    (
        "domain_brokerage",
        re.compile(
            r"\bcontact (?:our )?(?:domain )?brokerage team\b|"
            r"\bdomain (?:brokerage|broker|acquisition) services?\b"
        ),
    ),
    (
        "domain_escrow_transfer",
        re.compile(
            r"\bsecure escrow and transfer services?\b|"
            r"\bdomain escrow\b"
        ),
    ),
    (
        "domain_asset_collection",
        re.compile(
            r"\bdomain asset collections?\b|"
            r"\bpremium digital assets?\b"
        ),
    ),
    (
        "domain_marketplace",
        re.compile(
            r"\bmarketplace on (?:un)?stoppable domains\b|"
            r"\bvisit our domain marketplace\b"
        ),
    ),
    (
        "fictional_product_preview",
        re.compile(
            r"\bconcept(?:ual)? (?:interface|product) "
            r"(?:preview|visualization)\b|"
            r"\bnot (?:a )?screenshot of existing software\b|"
            r"\bnot a currently available application\b"
        ),
    ),
]

AGENCY = [
    "marketing agency", "seo agency", "development agency",
    "consulting agency", "ai consulting", "automation agency",
    "we build websites", "digital agency", "creative agency",
    "consulting services", "custom software development services"
]

B2C_AUDIENCE = [
    "for job seekers", "job seekers", "for students", "students",
    "for parents", "parents", "for children", "children", "kids",
    "for creators", "creators", "individuals", "personal use",
    "homeowners", "gamers", "fans", "couples", "freelancers",
    "your resume", "your career", "your child", "your family"
]

B2C_USE_CASE = [
    "practice interviews", "mock interview", "interview prep",
    "resume checker", "resume review", "resume writer", "resume fit",
    "fit & readiness", "fit and readiness", "career coaching",
    "land your next job", "job search assistant", "homework helper",
    "essay writer", "ai girlfriend", "dating assistant",
    "caption generator", "logo generator", "avatar generator",
    "social media generator", "prompt marketplace", "personalized stories",
    "illustrated stories", "stories for children", "bedtime stories",
    "chapter books", "kdp-ready", "kdp ready", "art styles",
    "photo generator", "headshot generator", "personal productivity",
    "fitness coach", "wellness coach", "personal finance assistant",
    "sports betting picks", "betting picks", "fan loyalty game",
    "loyalty arena", "personal trading assistant",
    # Confirmed live (2026-08-08): real consumer apps with none of the
    # vocabulary above -- habit tracking, personal money transfer, a
    # reading/news personalization app, a cannabis lifestyle guide, and a
    # message-tone checker -- all scored 0 and were indistinguishable from
    # a real B2B product with this list as it stood.
    "habit tracker", "habit tracking", "track your habits",
    "build better habits", "earn xp", "track habits",
    "instant funds transfer", "send money to friends",
    "money transfer app", "peer-to-peer payments",
    "read smarter, not harder", "personalized reading",
    "cannabis guide", "dispensary finder", "strain guide",
    "how your message sounds", "message tone checker",
]

MEDIA_NEWSLETTER = [
    "newsletter", "daily newsletter", "weekly newsletter", "subscribe",
    "ai news", "latest news", "news and research", "research and tools",
    "summarized daily", "digest", "publication", "media company",
    "editorial", "read the latest", "top stories", "industry news",
    "curated news", "ranked and summarized", "stay on top of ai"
]

# Commodity AI products and service businesses that are outside the target thesis.
COMMODITY_WRAPPER = [
    "answering service", "ai answering service", "virtual receptionist",
    "ai receptionist", "phone answering", "answer every call",
    "appointment booking", "appointment scheduling", "missed calls",
    "inbound calls", "outbound calls", "call center", "contact center agent",
    "lead generation agency", "appointment setting agency", "seo services",
    "social media agency", "recruiting agency", "staffing agency",
    "outsourcing services", "done-for-you automation", "white label agency",
    "ai content writer", "blog generator", "social post generator",
    "image generator", "headshot generator", "resume builder",
    "interview coach", "homework helper", "travel planner", "meal planner",
    "personal assistant", "dating assistant", "horoscope", "astrology",
    "trading signals", "crypto signals", "casino", "gambling"
]

# Common English function words. This is deliberately conservative: a site is
# rejected only when its main text strongly appears non-English.
ENGLISH_COMMON_WORDS = {
    "the", "and", "for", "with", "that", "this", "from", "your", "our",
    "you", "we", "are", "is", "to", "of", "in", "on", "by", "as", "at",
    "an", "a", "or", "be", "can", "help", "build", "business", "company",
    "platform", "software", "security", "data", "enterprise", "teams"
}

NON_ENGLISH_COMMON_WORDS = {
    # Spanish / Portuguese
    "nuestra", "nuestro", "para", "empresas", "empresa", "ayuda", "mejorar",
    "gestionar", "clientes", "solicita", "demostración", "funciones", "conocer",
    "todas", "procesos", "productividad", "inteligencia", "negocios", "soluções",
    "clientes", "dados", "segurança", "plataforma", "automatizar",
    # French
    "notre", "votre", "entreprise", "entreprises", "avec", "pour", "nous",
    "vous", "solution", "solutions", "demandez", "démo", "sécurité", "données",
    # German / Dutch / Italian
    "unternehmen", "unsere", "ihre", "lösung", "lösungen", "sicherheit", "daten",
    "bedrijf", "bedrijven", "onze", "voor", "gegevens", "sicurezza", "azienda",
    "aziende", "nostra", "dati", "richiedi", "demo"
}


US_LOCATION_TERMS = [
    "united states", "u.s.", "u.s.a.", "usa",
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia"
]

STRONG_US_SCHOOLS = ["harvard", "mit", "stanford", "yale", "princeton", "columbia",
                     "caltech", "chicago", "upenn", "cornell", "duke", "northwestern", 
                     "johns hopkins", "brown", "dartmouth", "washington university",
                       "university of california", "university of michigan"]
MID_US_SCHOOLS = ["penn state", "university of florida", "university of texas",
                  "university of wisconsin", "university of washington", "university of illinois",
                  "university of minnesota", "university of arizona", "university of colorado",
                  "university of oregon", "university of utah", "university of nevada",
                  "university of kansas", "university of missouri", "university of tennessee",
                  "university of georgia", "university of north carolina", "university of virginia",
                  "university of maryland", "university of massachusetts", "university of connecticut"]
BAD_US_SCHOOLS = ["university of phoenix", "devry university", "strayer university",
                  "capella university", "walden university", "grand canyon university",
                  "keiser university", "american intercontinental university", "argosy university",
                  "chamberlain university", "herzing university", "independence university",
                  "national american university", "robert morris university", "south university",
                  "trident university international", "westwood college"] 

STRONG_WORK_EXPERIENCE = ["google", "microsoft", "amazon", "facebook", "apple", "netflix",
                          "linkedin", "twitter", "uber", "airbnb", "salesforce",
                          "adobe", "intel", "nvidia", "ibm", "oracle", "dell",
                          "hp", "cisco", "vmware", "sap", "qualcomm", "tesla",
                          "space x", "palantir", "stripe", "square", "snap",
                          "reddit", "dropbox", "slack", "zoom"]

NON_US_COUNTRIES = [
    "afghanistan", "albania", "algeria", "andorra", "angola",
    "antigua and barbuda", "argentina", "armenia", "australia", "austria",
    "azerbaijan", "bahamas", "bahrain", "bangladesh", "barbados",
    "belarus", "belgium", "belize", "benin", "bhutan", "bolivia",
    "bosnia and herzegovina", "botswana", "brazil", "brunei", "bulgaria",
    "burkina faso", "burundi", "cabo verde", "cambodia", "cameroon",
    "canada", "central african republic", "chad", "chile", "china",
    "colombia", "comoros", "costa rica", "croatia", "cuba", "cyprus",
    "czech republic", "czechia", "democratic republic of the congo",
    "denmark", "djibouti", "dominica", "dominican republic", "ecuador",
    "egypt", "el salvador", "equatorial guinea", "eritrea", "estonia",
    "eswatini", "ethiopia", "fiji", "finland", "france", "gabon",
    "gambia", "germany", "ghana", "greece", "grenada", "guatemala",
    "guinea", "guinea-bissau", "guyana", "haiti", "honduras", "hungary",
    "iceland", "india", "indonesia", "iran", "iraq", "ireland", "israel",
    "italy", "ivory coast", "jamaica", "japan", "jordan", "kazakhstan",
    "kenya", "kiribati", "kuwait", "kyrgyzstan", "laos", "latvia",
    "lebanon", "lesotho", "liberia", "libya", "liechtenstein",
    "lithuania", "luxembourg", "madagascar", "malawi", "malaysia",
    "maldives", "mali", "malta", "marshall islands", "mauritania",
    "mauritius", "mexico", "micronesia", "moldova", "monaco", "mongolia",
    "montenegro", "morocco", "mozambique", "myanmar", "namibia", "nauru",
    "nepal", "netherlands", "new zealand", "nicaragua", "niger", "nigeria",
    "north korea", "north macedonia", "norway", "oman", "pakistan",
    "palau", "panama", "papua new guinea", "paraguay", "peru",
    "philippines", "poland", "portugal", "qatar", "republic of the congo",
    "romania", "russia", "rwanda", "saint kitts and nevis", "saint lucia",
    "saint vincent and the grenadines", "samoa", "san marino",
    "sao tome and principe", "saudi arabia", "senegal", "serbia",
    "seychelles", "sierra leone", "singapore", "slovakia", "slovenia",
    "solomon islands", "somalia", "south africa", "south korea",
    "south sudan", "spain", "sri lanka", "sudan", "suriname", "sweden",
    "switzerland", "syria", "taiwan", "tajikistan", "tanzania", "thailand",
    "timor-leste", "togo", "tonga", "trinidad and tobago", "tunisia",
    "turkey", "turkmenistan", "tuvalu", "uganda", "ukraine",
    "united arab emirates", "united kingdom", "uruguay", "uzbekistan",
    "vanuatu", "vatican city", "venezuela", "vietnam", "yemen", "zambia",
    "zimbabwe",
]

NON_US_ALIASES = [
    "uae", "u.a.e.", "uk", "u.k.", "great britain", "england", "scotland",
    "wales", "hong kong", "macau", "dubai", "abu dhabi", "riyadh",
    "doha", "london", "berlin", "paris", "toronto", "vancouver",
    "montreal", "sydney", "melbourne", "bengaluru", "bangalore",
    "mumbai", "delhi", "tel aviv", "zurich", "amsterdam", "singapore city",
]

LOCATION_CONTEXT_TERMS = [
    "headquartered", "headquarters", "hq", "based in", "located in",
    "founded in", "our office", "offices in", "office in",
    "registered office", "company address", "mailing address",
    "contact us", "visit us", "incorporated in", "registered in",
]

SELF_SERVE_CONSUMER = [
    "start free", "free to try", "no credit card required",
    "try for free", "sign up free", "download the app",
    "create your", "upload your", "get started free"
]

BUSINESS_VALUE = [
    "reduce downtime", "reduce costs", "lower costs", "increase revenue",
    "improve yield", "reduce fraud", "reduce risk", "automate compliance",
    "improve throughput", "forecast accuracy", "operational efficiency",
    "security posture", "remediation time", "breach recovery",
    "quality control", "labor savings", "productivity gains",
    "return on investment", "roi", "cost savings", "reduce ramp time",
    "investigate faster", "cost optimization", "surface optimizations",
    "coordinate response", "real-time feedback", "trusted decisions",
    "cuts costs", "cut costs", "boost productivity", "boosts productivity",
    "fabrication-ready", "in hours, not weeks", "faster iteration",
    "reproducible results", "gpu utilization", "reduce inference costs",
    "lower inference costs", "inference throughput", "training throughput",
    "cold start latency",
]

WORKFLOW_DEPTH = [
    "end-to-end", "entire workflow", "mission-critical", "system of record",
    "embedded workflow", "autonomous remediation", "continuous monitoring",
    "decision intelligence", "workflow automation", "orchestration",
    "closed loop", "real-time operations", "production environment",
    "delivery dispatch", "delivery dispatching", "smarter dispatching",
    "recipe formulation", "pcb layout",
    "prompt management", "evaluation pipeline", "policy evaluation",
]

TECHNICAL_DEPTH = [
    "proprietary model", "proprietary data", "novel architecture",
    "foundation model", "reinforcement learning", "computer vision",
    "sensor fusion", "inference infrastructure", "model serving",
    "distributed architecture", "quantum computing", "semiconductor",
    "photonics", "neuromorphic", "synthetic data", "continuous learning",
    "autonomous system", "edge ai", "security research", "zero-trust",
    "zero trust", "dynamic masking", "real-time policies", "attack corpus",
    "prompt injection", "protocol-aware", "reverse proxy", "telemetry",
    "simulations", "ambient clinical documentation", "federated access",
    "cost anomaly", "runtime control", "model governance", "video security",
    "threat detection", "secure access", "physical security",
    "model distillation", "purpose-built model", "mpc co-signing",
    "programmable guardrails", "application-layer robotics",
    "satellite imagery", "aerial imagery", "geospatial model",
    "physics-driven ai", "fabrication-ready", "recipe optimization",
    "automated dispatch", "dispatch optimization", "smarter dispatching",
    # Data-derived: each term below was measured across 253 Claude
    # KEEPs and 600 REJECTs and appears at >=2.4x lift toward KEEP.
    "mcp server",
    "mcp tool",
    "model context protocol",
    "agent runtime",
    "runtime verification",
    "sandboxed execution",
    "egress control",
    "kubernetes",
    "observability",
    "vector database",
    "embeddings",
    "fine-tuning",
    "retrieval-augmented",
    "inference latency",
    "gpu cluster",
    "multi-agent orchestration",
    "tool calling",
    "eval harness",
    "guardrails",
    "pre-trained",
    "model provider",
    "deterministic execution",
    "ai infrastructure as a service",
    "gpu as a service",
    "inference as a service",
    "serverless inference",
    "managed inference",
    "model serving api",
    "training as a service",
    "ai compute platform",
    "gpu autoscaling",
    "inference optimization",
]


DEFENSIBILITY_SIGNALS = [
    "proprietary data", "proprietary dataset", "unique data",
    "data network effect", "novel architecture", "patent pending",
    "patented", "research breakthrough", "state of the art",
    "technical moat", "hard-to-replicate", "hard to replicate",
    "purpose-built model", "domain-specific model", "custom silicon",
    "novel hardware", "new compute architecture", "exclusive data",
    "closed-loop learning", "continuous learning", "simulation engine",
    "digital twin", "sensor fusion", "autonomous control", "attack corpus",
    "protocol-aware", "domain-specific workflow", "real-time context",
    "audit-ready evidence", "federated access", "physics-driven ai",
    "purpose-built models", "mpc co-signing", "proprietary world model",
    "reproducible evaluation",
    # Data-derived: each term below was measured across 253 Claude
    # KEEPs and 600 REJECTs and appears at >=2.4x lift toward KEEP.
    "deterministic",
    "verifiable",
    "provable",
    "cryptographically signed",
    "signed attestation",
    "tamper-proof",
    "reproducible",
    "policy engine",
    "governance policy",
    "system of record",
    "audit-ready",
]

IMPORTANT_PROBLEM_SIGNALS = [
    "mission-critical", "critical infrastructure", "production downtime",
    "cyber attack", "data breach", "fraud", "supply chain",
    "manufacturing defect", "quality inspection", "clinical workflow",
    "drug discovery", "defense", "national security", "grid reliability",
    "semiconductor", "model inference", "model deployment",
    "compliance burden", "operational bottleneck", "prompt injection",
    "data leakage", "adversarial attack", "zero-trust access",
    "emergency response", "medical interpretation", "clinical documentation",
    "cost anomalies", "physical security", "security and compliance",
    "electronics design", "pcb layout", "agricultural autonomy",
    "food formulation", "delivery operations", "workload reliability",
    "agent security", "physical engineering",
    # Data-derived: each term below was measured across 253 Claude
    # KEEPs and 600 REJECTs and appears at >=2.4x lift toward KEEP.
    "agent sprawl",
    "agent governance",
    "shadow ai",
    "model risk",
    "hallucination",
    "non-deterministic",
    "enterprise compliance",
    "regulatory reporting",
    "data residency",
]

GENERIC_AI_PHRASES = [
    "ai-powered", "powered by ai", "using artificial intelligence",
    "leverage ai", "harness the power of ai", "revolutionize",
    "transform your business", "next-generation ai solution",
]

GENERIC_BOILERPLATE = [
    # Measured, not guessed: each of these appears far more often in companies
    # Claude REJECTED than in ones it KEPT, across 253 KEEPs and 600 REJECTs.
    # The pattern is consistent -- a company that describes a concrete
    # mechanism gets kept, one that describes itself in marketing or academic
    # register gets rejected. Note that the field's own umbrella terms are on
    # this list: "machine learning" appeared in 22.0% of REJECTs against 5.5%
    # of KEEPs, and "artificial" in 20.0% against 7.1%. Naming the category is
    # not evidence of building in it.
    "machine learning", "artificial intelligence", "algorithms",
    "leverage", "strategies", "excellence", "professional services",
    "businesses of all sizes", "cutting-edge technology", "seamlessly",
    "empower", "empowering", "unlock the power", "state-of-the-art solutions",
    "digital transformation", "innovative solutions", "best practices",
    "tailored solutions", "end-to-end solutions", "holistic approach",
]

SERVICE_BUSINESS_SCORING_SIGNALS = [
    "ai development company", "custom ai solutions", "custom ai agents",
    "custom software", "bespoke ai", "bespoke systems", "we build bespoke",
    "we build custom", "consultancy", "consulting firm", "free consultation",
    "technical partnership", "we join your team", "for hire",
]

LOW_VALUE_SCORING_SIGNALS = [
    "job board", "coupon codes", "discount codes", "personal portfolio",
    "portfolio of", "seeking entry-level", "cold calling",
    "appointment scheduling", "ai agency", "automation consultancy",
]

PLACEHOLDER = [
    "default web site page", "welcome to nginx",
    "apache2 ubuntu default page", "vercel deployment",
    "generated by create next app", "next.js app with clerk authentication",
    "lovable generated project", "premium bootstrap 5 landing page template",
    "future home of something quite cool", "site is currently unavailable",
    "create a website with squarespace", "powered by squarespace",
    "website coming soon - dynadot", "website coming soon. dynadot",
    "this domain is registered at dynadot.com",
    "coming soon - godaddy", "godaddy parked page",
    "namecheap parking page", "wix website builder",
]

HOSTING_TEMPLATE_PHRASES = [
    "we're under construction. please check back for an update soon",
    "we are under construction. please check back for an update soon",
    "under construction. please check back for an update soon",
    "this domain is registered at dynadot.com. website coming soon",
    "website coming soon. this domain is registered",
    "future home of something quite cool",
    "this site is currently unavailable",
    "generated by create next app",
    "lovable generated project",
    "premium bootstrap 5 landing page template",
    "default web site page",
    "welcome to nginx",
    "apache2 ubuntu default page",
]

FOUNDER_PRELAUNCH_PHRASES = [
    "we're building", "we are building", "currently building",
    "building something", "in stealth", "stealth mode",
    "private beta", "launching soon",
]

PRODUCT_REALITY_SIGNALS = [
    "customers", "customer", "case study", "case studies", "deployed",
    "in production", "production deployment", "documentation", "docs",
    "api reference", "integrations", "integration", "request a demo",
    "book a demo", "pilot", "pilots", "private beta", "general availability",
    "used by", "trusted by", "open source", "github",
    # Data-derived: each term below was measured across 253 Claude
    # KEEPs and 600 REJECTs and appears at >=2.4x lift toward KEEP.
    "read the docs",
    "self-serve",
    "sdk",
    "quickstart",
    "api key",
    "changelog",
    "status page",
    "single sign-on",
    # SOC/SOC 2 language is ubiquitous procurement boilerplate. It may be
    # displayed as context, but it is not evidence that a product is real or
    # differentiated and must never increase the score.
    "approval workflows",
    "audit log",
    "audit logs",
]

WEDGE_SIGNALS = [
    "purpose-built for", "built specifically for", "designed specifically for",
    "replaces", "eliminates", "automates", "end-to-end", "entire workflow",
    "system of record", "mission-critical", "embedded workflow",
    "reduce downtime", "reduce costs", "increase revenue", "improve yield",
    "reduce fraud", "reduce risk", "automate compliance",
    "improve throughput", "hours, not weeks", "minutes instead of",
]

MOAT_SIGNALS = [
    "proprietary", "patent pending", "patented", "novel architecture",
    "research breakthrough", "custom silicon", "novel hardware",
    "exclusive data", "data network effect", "network effect",
    "closed-loop learning", "continuous learning", "simulation engine",
    "domain-specific model", "purpose-built model", "foundation model",
    "vertically integrated", "full-stack hardware", "deep integration",
    "embedded integration", "regulatory approval", "fda cleared",
    "peer-reviewed", "published research",
]

FOUNDER_VISIBILITY_SIGNALS = [
    "founded by", "co-founded by", "cofounder", "co-founder",
    "our founders", "founding team", "leadership team", "meet the team",
]

FOUNDER_CREDIBILITY_SIGNALS = [
    "phd", "ph.d.", "professor", "research scientist", "researcher",
    "former founder", "repeat founder", "previously founded",
    "ex-google", "ex-meta", "ex-openai", "ex-anthropic", "ex-nvidia",
    "former google", "former meta", "former openai", "former anthropic",
    "former nvidia", "carnegie mellon",
    # "mit"/"stanford" moved to the tiered STRONG/MID/BAD_US_SCHOOLS check
    # below -- that list is a more precise, dedicated treatment of school
    # pedigree than a couple of bare names mixed into a generic
    # credibility list, and this avoids double-counting the same signal.
    "published research", "peer-reviewed", "patent holder",
]

AI_SLOP_SIGNALS = [
    "unlock the power of ai", "harness the power of ai",
    "revolutionize your business", "transform your business",
    "cutting-edge ai solutions", "innovative ai solutions",
    "ai solutions for everyone", "all-in-one ai platform",
    "next-generation ai solution", "seamlessly integrate ai",
]

HOSTING_PROVIDER_SIGNALS = [
    "web hosting", "wordpress hosting", "shared hosting", "reseller hosting",
    "vps hosting", "dedicated hosting", "domain management",
    "domain registration", "register a domain", "website builder",
]

NON_OPERATING_INVESTOR_SIGNALS = [
    "venture studio", "venture capital", "investment firm",
    "we invest in", "portfolio companies", "our portfolio",
    "venture creation", "company builder", "startup studio",
    "operating thesis", "capital and ai",
]

PRELAUNCH_ACTION_SIGNALS = [
    "join the waitlist", "join waitlist", "request early access",
    "get early access", "sign up for updates", "get notified",
    "notify me", "be the first to know", "join our beta",
    "request access", "contact us", "email us", "follow us",
]

GENERIC_PRELAUNCH_ONLY = [
    "coming soon", "website coming soon", "under construction",
    "check back soon", "please check back",
]

MATURE_COMPANY_SIGNALS = [
    "nasdaq", "nyse", "publicly traded", "annual report",
    "investor relations", "fortune 500", "global leader",
    "over 1,000 employees", "over 1000 employees", "10,000 employees",
    "10000 employees", "offices worldwide", "serving millions",
    "millions of customers", "series c", "series d", "series e",
    "series f", "unicorn", "acquired by", "a subsidiary of"
]

MATURE_CONTEXT_SIGNALS = [
    "investor relations", "global offices", "our history",
    "board of directors", "annual revenue", "shareholders"
]

US_STATES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia"
]

US_CITIES = [
    "boston", "cambridge", "new york", "san francisco", "palo alto",
    "mountain view", "seattle", "austin", "chicago", "los angeles",
    "san diego", "denver", "atlanta", "miami", "washington dc",
    "washington, dc", "philadelphia", "raleigh", "durham", "charlotte",
    "pittsburgh", "salt lake city", "portland", "boulder", "houston",
    "dallas", "minneapolis", "detroit", "nashville"
]

US_PATTERNS = [
    r"\bunited states\b", r"\bu\.s\.\b", r"\busa\b",
    r"\bheadquartered in the us\b", r"\bbased in the us\b",
    r"\bdelaware corporation\b", r"\bdelaware c-corp\b",
    r"\b[0-9]{5}(?:-[0-9]{4})?\b",
]

FOREIGN_COUNTRIES = [
    "indonesia", "singapore", "india", "pakistan", "bangladesh",
    "china", "hong kong", "japan", "south korea", "australia",
    "new zealand", "canada", "mexico", "brazil", "argentina",
    "united kingdom", "england", "scotland", "ireland", "france",
    "germany", "spain", "italy", "netherlands", "belgium",
    "switzerland", "sweden", "norway", "denmark", "finland",
    "poland", "portugal", "estonia", "latvia", "lithuania",
    "israel", "united arab emirates", "uae", "saudi arabia",
    "qatar", "south africa", "nigeria", "kenya", "vietnam",
    "thailand", "malaysia", "philippines", "taiwan"
]

RELEVANT_LINK_TERMS = [
    "about", "company", "contact", "team", "careers", "location",
    "product", "platform", "solutions", "technology", "security",
    "industries", "customers", "case-studies", "case studies",
    "use-cases", "use cases", "docs", "documentation"
]


@dataclass
class Fetch:
    url: str = ""
    status: int | None = None
    title: str = ""
    description: str = ""
    text: str = ""
    error: str = ""
    pages_scraped: int = 0
    meaningful_internal_links: int = 0
    source: str = "network_fetch"
    crawl_status: str = ""
    crawled_at: str = ""


def load_crawler_fetches(domains: list[str]) -> dict[str, Fetch]:
    """Reuse content already collected by either local crawler.

    Raw HTML remains in its source archive. Screening consumes the derived
    text and metadata so it never issues a contradictory second network
    request for the same completed crawl.
    """
    search_db, zone_db = crawler_database_paths()
    normalized = list(
        dict.fromkeys(
            stable_domain(domain)
            for domain in domains
            if stable_domain(domain)
        )
    )
    if not normalized:
        return {}

    results: dict[str, Fetch] = {}
    conn = None
    try:
        if using_postgres("AI_SEARCH_DATABASE_URL"):
            # The intake crawler and screening worker are separate Railway
            # services. In production the completed crawl lives in the
            # shared ai_search Postgres schema, not in this worker's local
            # SQLite path. Missing this branch made every manually submitted
            # domain get fetched a second time; a transient timeout then left
            # an otherwise-good intake permanently without a score.
            from ai_search.database import connect as connect_ai_search

            conn = connect_ai_search(search_db, read_only=True)
        else:
            if not search_db.exists():
                raise sqlite3.OperationalError("AI search database is absent")
            conn = sqlite3.connect(
                f"file:{search_db}?mode=ro",
                uri=True,
                timeout=30,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
        for start in range(0, len(normalized), 800):
            batch = normalized[start : start + 800]
            marks = ",".join("?" for _ in batch)
            domain_rows = conn.execute(
                f"""
                SELECT id,domain,latest_crawl_status,last_crawled_at
                FROM domains
                WHERE last_crawled_at IS NOT NULL
                  AND domain IN ({marks})
                """,
                batch,
            ).fetchall()
            for domain_row in domain_rows:
                pages = conn.execute(
                    """
                    SELECT final_url,title,meta_description,cleaned_text,
                           http_status,crawled_at,word_count
                    FROM pages
                    WHERE domain_id=?
                    ORDER BY crawled_at DESC, word_count DESC
                    LIMIT 3
                    """,
                    (domain_row["id"],),
                ).fetchall()
                crawl_status = str(
                    domain_row["latest_crawl_status"] or ""
                )
                crawled_at = str(domain_row["last_crawled_at"] or "")
                if pages:
                    primary = pages[0]
                    texts = [
                        clean(page["cleaned_text"])
                        for page in pages
                        if clean(page["cleaned_text"])
                    ]
                    latest_failed = crawl_status not in {"", "success"}
                    results[str(domain_row["domain"])] = Fetch(
                        url=str(primary["final_url"] or ""),
                        status=primary["http_status"],
                        title=clean(primary["title"]),
                        description=clean(primary["meta_description"]),
                        text=clean(" ".join(texts))[:60000],
                        error=(
                            f"crawler_latest_status:{crawl_status}"
                            if latest_failed else ""
                        ),
                        pages_scraped=len(pages),
                        source=(
                            "crawler_cache_stale"
                            if latest_failed else "crawler_cache"
                        ),
                        crawl_status=crawl_status,
                        crawled_at=crawled_at,
                    )
                else:
                    results[str(domain_row["domain"])] = Fetch(
                        url=f"https://{domain_row['domain']}",
                        error=f"crawler_{crawl_status or 'empty'}",
                        source="crawler_cache",
                        crawl_status=crawl_status,
                        crawled_at=crawled_at,
                    )
    except Exception:
        # Cache reuse is an optimization for ordinary crawler traffic. A
        # missing/unavailable cache still falls back to the network fetch.
        pass
    finally:
        if conn is not None:
            conn.close()

    if zone_db.exists() or using_postgres("ZONE_UNIVERSE_DATABASE_URL"):
        conn = None
        try:
            if using_postgres("ZONE_UNIVERSE_DATABASE_URL"):
                from zone_universe.database import connect as connect_zone

                conn = connect_zone(read_only=True)
            else:
                conn = sqlite3.connect(
                    f"file:{zone_db}?mode=ro",
                    uri=True,
                    timeout=30,
                )
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA query_only=ON")
            for start in range(0, len(normalized), 800):
                batch = normalized[start : start + 800]
                marks = ",".join("?" for _ in batch)
                rows = conn.execute(
                    f"""
                    SELECT domain,final_url,title,meta_description,
                           cleaned_text,http_status,crawled_at,
                           word_count,document_count
                    FROM pages
                    WHERE domain IN ({marks})
                    """,
                    batch,
                ).fetchall()
                for row in rows:
                    domain = str(row["domain"])
                    # A completed Zone Universe fetch is newer and more
                    # directly relevant for .app/.com than any stale overlap.
                    results[domain] = Fetch(
                        url=str(row["final_url"] or f"https://{domain}"),
                        status=row["http_status"],
                        title=clean(row["title"]),
                        description=clean(row["meta_description"]),
                        text=clean(row["cleaned_text"])[:60000],
                        pages_scraped=max(
                            1,
                            int(row["document_count"] or 1),
                        ),
                        source="zone_universe_cache",
                        crawl_status="success",
                        crawled_at=str(row["crawled_at"] or ""),
                    )
        except Exception:
            # Cache reuse is an optimization, not a reason to abort the
            # classifier. A missing/unavailable corpus falls back to the
            # normal network fetch path.
            pass
        finally:
            if conn is not None:
                conn.close()
    return results


@lru_cache(maxsize=96)
def _compiled_phrase_pattern(terms: tuple[str, ...]) -> re.Pattern[str]:
    alternatives = "|".join(
        re.escape(term)
        for term in sorted(
            {term.lower() for term in terms},
            key=len,
            reverse=True,
        )
    )
    return re.compile(
        rf"(?<![a-z0-9])(?:{alternatives})(?![a-z0-9])",
        flags=re.I,
    )


def occurrences(text: str, terms: list[str]) -> list[str]:
    """Find boundary-safe phrases with one compiled scan per lexicon."""
    if not text or not terms:
        return []
    canonical = {term.lower(): term for term in terms}
    matched = {
        match.group(0).lower()
        for match in _compiled_phrase_pattern(tuple(terms)).finditer(text)
    }
    return [
        term
        for key, term in canonical.items()
        if key in matched
    ]


# Enough for a normal homepage's hero, product, buyer, and technical sections,
# while still excluding long documentation/blog/legal tails. Production-label
# calibration showed 6k cut away too much substantive evidence (AUC 0.509).
SCORING_LEAD_CHARS = 15000


def high_signal_scoring_text(
    title: str,
    description: str,
    body: str,
    *,
    limit: int = SCORING_LEAD_CHARS,
) -> str:
    """Bound cheap screening to the page's highest-signal copy.

    Long docs, blogs, legal pages, and keyword-heavy footers used to expose far
    more vocabulary than a concise startup homepage.  That rewarded page
    volume instead of company quality.  Title and metadata are retained once,
    followed by only the leading visible copy needed to identify the product.
    """
    lead = clean(body)[: max(0, int(limit))]
    return clean(" ".join(part for part in (title, description, lead) if part))


def founder_green_flags(text: str) -> list[dict[str, str]]:
    """Return conservative, evidence-backed pedigree tags from team copy."""
    cleaned = clean(text)
    if not cleaned:
        return []
    sentences = [
        clean(item)
        for item in re.split(r"(?<=[.!?])\s+|\s*[|•]\s*", cleaned)
        if clean(item)
    ]
    founder_terms = tuple(FOUNDER_VISIBILITY_SIGNALS + [
        "founder", "cofounder", "co-founder", "our team", "leadership",
        "previously at", "formerly at", "alumni", "graduate",
    ])
    founder_indexes = [
        index for index, sentence in enumerate(sentences)
        if any(term in sentence.lower() for term in founder_terms)
    ]
    # Team pages commonly put the name, prior employer, school, and research
    # in adjacent sentences. Keep a one-sentence neighborhood around explicit
    # founder language while still excluding unrelated logos and testimonials.
    context_indexes = sorted({
        nearby
        for index in founder_indexes[:12]
        for nearby in range(max(0, index - 1), min(len(sentences), index + 2))
    })
    contexts = [sentences[index] for index in context_indexes]
    # Keep the evidence close to founder/team language. This prevents customer
    # logos and integration lists from creating fake pedigree tags.
    context = " ".join(contexts[:12])
    if not context:
        return []

    flags: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(kind: str, label: str, evidence: str) -> None:
        key = (kind, label.lower())
        if key in seen or len(flags) >= 4:
            return
        seen.add(key)
        flags.append({
            "type": kind,
            "label": label,
            "evidence": clean(evidence)[:220],
            "source": "website",
        })

    # Only accept names attached to explicit founder grammar. Avoid guessing
    # from page headings, customer quotes, or generic team-member lists.
    founder_patterns = [
        r"(?:founded|co-founded) by\s+([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,2})",
        r"([A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,2})\s*,?\s+(?:co-?founder|founder)(?:\s|[,.])",
    ]
    for sentence in contexts:
        for pattern in founder_patterns:
            match = re.search(pattern, sentence)
            if match:
                founder_name = match.group(1).rstrip(".,")
                add("founder_identity", f"Founder · {founder_name}", sentence)
                break
        if any(flag["type"] == "founder_identity" for flag in flags):
            break

    school_hits = occurrences(context, STRONG_US_SCHOOLS)
    ivy = {
        "harvard", "yale", "princeton", "columbia",
        "upenn", "cornell", "brown", "dartmouth",
    }
    for school in school_hits[:2]:
        kind = (
            "ivy_league_spinout"
            if school.lower() in ivy
            else "top_university_spinout"
        )
        evidence = next(
            (s for s in contexts if school.lower() in s.lower()), context
        )
        school_label = school.upper() if school.lower() == "mit" else school.title()
        add(kind, f"{school_label} team", evidence)

    employer_hits = occurrences(context, STRONG_WORK_EXPERIENCE)
    for employer in employer_hits[:2]:
        evidence = next((s for s in contexts if employer.lower() in s.lower()), context)
        add("ex_big_tech", f"Ex-{employer.title()}", evidence)

    if occurrences(context, ["repeat founder", "previously founded", "former founder"]):
        evidence = next((s for s in contexts if "found" in s.lower()), context)
        add("repeat_founder", "Repeat founder", evidence)
    if occurrences(context, [
        "previous exit", "prior exit", "acquired his company",
        "acquired her company", "company was acquired",
    ]):
        evidence = next(
            (s for s in contexts if "acquir" in s.lower() or "exit" in s.lower()),
            context,
        )
        add("prior_exit", "Prior founder exit", evidence)
    if occurrences(context, [
        "patent holder", "patent pending", "patented", "published research",
        "peer-reviewed", "research breakthrough",
    ]):
        evidence = next(
            (s for s in contexts if any(term in s.lower() for term in (
                "patent", "published research", "peer-reviewed",
                "research breakthrough",
            ))),
            context,
        )
        add("deep_tech_ip", "Founder technical IP", evidence)
    return flags


def founder_pedigree_score(flags: list[dict[str, str]]) -> int:
    """Score only verified, founder-context pedigree evidence.

    Pedigree is intentionally meaningful for sparse early-stage companies,
    but customer logos, integration lists, and generic team claims contribute
    nothing because ``founder_green_flags`` excludes them.
    """
    labels_by_type: dict[str, set[str]] = {}
    for flag in flags:
        kind = str(flag.get("type", "")).strip()
        label = str(flag.get("label", "")).strip().lower()
        if kind and label:
            labels_by_type.setdefault(kind, set()).add(label)

    score = 0
    if labels_by_type.get("founder_identity"):
        score += 10
    school_count = sum(
        len(labels_by_type.get(kind, set()))
        for kind in ("ivy_league_spinout", "top_university_spinout")
    )
    if school_count:
        score += 28 + min(school_count - 1, 1) * 8
    employer_count = len(labels_by_type.get("ex_big_tech", set()))
    if employer_count:
        score += 28 + min(employer_count - 1, 1) * 8
    if labels_by_type.get("repeat_founder"):
        score += 30
    if labels_by_type.get("prior_exit"):
        score += 40
    if labels_by_type.get("deep_tech_ip"):
        score += 25
    return min(100, score)


def aiaas_green_flags(text: str) -> list[dict[str, str]]:
    """Return evidence-backed infrastructure-as-a-service deal tags."""
    cleaned = clean(text)
    if not cleaned:
        return []
    sentences = [
        clean(item)
        for item in re.split(r"(?<=[.!?])\s+|\s*[|•]\s*", cleaned)
        if clean(item)
    ]
    groups = (
        ("gpu_infrastructure", "GPU infrastructure", (
            "gpu as a service", "gpu cloud", "gpu cluster",
            "gpu orchestration", "ai compute platform",
        )),
        ("serverless_inference", "Serverless inference", (
            "serverless inference", "managed inference",
            "inference as a service",
        )),
        ("model_serving", "Model serving", (
            "model serving", "model serving api", "inference api",
        )),
        ("training_infrastructure", "Training infrastructure", (
            "training as a service", "distributed training",
            "training infrastructure", "training cluster",
        )),
        ("inference_optimization", "Inference optimization", (
            "inference optimization", "inference throughput",
            "reduce inference costs", "lower inference costs",
            "gpu autoscaling", "gpu utilization",
        )),
    )
    flags: list[dict[str, str]] = []
    for kind, label, terms in groups:
        evidence = next(
            (sentence for sentence in sentences if any(
                term in sentence.lower() for term in terms
            )),
            "",
        )
        if evidence:
            flags.append({
                "type": kind,
                "label": label,
                "evidence": evidence[:220],
                "source": "website",
            })
    return flags[:4]


def deal_green_flags(text: str) -> list[dict[str, str]]:
    """Combine founder pedigree and high-value market/product signals."""
    combined = [*founder_green_flags(text), *aiaas_green_flags(text)]
    seen: set[str] = set()
    flags: list[dict[str, str]] = []
    for flag in combined:
        if flag["type"] in seen:
            continue
        seen.add(flag["type"])
        flags.append(flag)
        if len(flags) == 6:
            break
    return flags


def scorecard_from_evidence(
    *,
    counts: dict[str, int],
    dimensions: dict[str, int],
    venture_quality: dict[str, object],
) -> dict[str, int]:
    """Stable, bounded explanation for the displayed 0-100 score."""
    return {
        "thesis": int(venture_quality.get("thesis_score", 0) or 0),
        "product": int(venture_quality.get("product_score", 0) or 0),
        "wedge": int(venture_quality.get("wedge_score", 0) or 0),
        "moat": int(venture_quality.get("moat_score", 0) or 0),
        "team": int(venture_quality.get("founder_score", 0) or 0),
        "buyer": min(100, int(dimensions.get("buyer", 0)) * 35),
        "technical_depth": min(
            100,
            int(dimensions.get("technical", 0)) * 22
            + int(counts.get("defensibility", 0)) * 18,
        ),
    }


def score_text(text: str) -> tuple[int, str, list[str], dict[str, int]]:
    """Conservative website-only Glasswing score.

    Scores in the 90s require multiple independent evidence dimensions, not
    repeated keyword mentions. Generic AI language contributes almost nothing.
    """
    text = clean(text)
    pillar_hits = {name: occurrences(text, terms) for name, terms in THESIS.items()}
    best = max(pillar_hits, key=lambda name: len(set(pillar_hits[name])))

    ai_hits = occurrences(text, AI)
    enterprise_hits = occurrences(text, ENTERPRISE)
    product_hits = occurrences(text, PRODUCT)
    technical_hits = occurrences(text, TECHNICAL_DEPTH)
    workflow_hits = occurrences(text, WORKFLOW_DEPTH)
    value_hits = occurrences(text, BUSINESS_VALUE)
    defensibility_hits = occurrences(text, DEFENSIBILITY_SIGNALS)
    important_problem_hits = occurrences(text, IMPORTANT_PROBLEM_SIGNALS)
    generic_hits = occurrences(text, GENERIC_AI_PHRASES)
    boilerplate_hits = occurrences(text, GENERIC_BOILERPLATE)
    service_hits = occurrences(text, SERVICE_BUSINESS_SCORING_SIGNALS)
    low_value_hits = occurrences(text, LOW_VALUE_SCORING_SIGNALS)
    founder_flags = founder_green_flags(text)
    pedigree_score = founder_pedigree_score(founder_flags)

    unique_pillar = len(set(pillar_hits[best]))
    unique_ai = len(set(ai_hits))
    unique_enterprise = len(set(enterprise_hits))
    unique_product = len(set(product_hits))
    unique_technical = len(set(technical_hits))
    unique_workflow = len(set(workflow_hits))
    unique_value = len(set(value_hits))
    unique_defensibility = len(set(defensibility_hits))
    unique_problem = len(set(important_problem_hits))

    counts = {
        "pillar": unique_pillar,
        "ai": unique_ai,
        "enterprise": unique_enterprise,
        "product": unique_product,
        "technical": unique_technical,
        "workflow": unique_workflow,
        "business_value": unique_value,
        "defensibility": unique_defensibility,
        "important_problem": unique_problem,
        "generic_ai": len(set(generic_hits)),
        "boilerplate": len(set(boilerplate_hits)),
        "service_business": len(set(service_hits)),
        "low_value": len(set(low_value_hits)),
        "founder_pedigree": pedigree_score,
    }

    # Dimension caps prevent keyword repetition from saturating the score.
    thesis_points = min(unique_pillar, 3) * 6          # max 18
    product_points = min(unique_product, 2) * 5        # max 10
    buyer_points = min(unique_enterprise, 2) * 5       # max 10
    technical_points = min(unique_technical, 3) * 6    # max 18
    workflow_points = min(unique_workflow, 2) * 5      # max 10
    value_points = min(unique_value, 2) * 4            # max 8
    defensibility_points = min(unique_defensibility, 2) * 7  # max 14
    problem_points = min(unique_problem, 2) * 4         # max 8

    raw = (
        thesis_points
        + product_points
        + buyer_points
        + technical_points
        + workflow_points
        + value_points
        + defensibility_points
        + problem_points
    )

    # Reward intersecting Glasswing evidence rather than isolated keywords.
    if (
        unique_pillar >= 2
        and unique_product >= 1
        and (unique_technical >= 1 or unique_problem >= 1)
    ):
        raw += 10
    if (
        unique_enterprise >= 1
        and unique_product >= 1
        and unique_technical >= 1
        and (unique_workflow >= 1 or unique_problem >= 1)
    ):
        raw += 8
    supporting_dimensions = sum([
        unique_product >= 1,
        unique_enterprise >= 1,
        unique_technical >= 1,
        unique_workflow >= 1,
        unique_value >= 1,
        unique_problem >= 1,
    ])
    if unique_pillar >= 2 and supporting_dimensions >= 3:
        raw += 20
    elif unique_pillar >= 2 and supporting_dimensions >= 2:
        raw += 14

    # A verified founder pedigree is a major early-stage signal. Keep the
    # website score lift bounded and require both thesis and product evidence,
    # so pedigree cannot rescue a personal page, holding company, or vaporware.
    if pedigree_score and unique_pillar >= 1 and unique_product >= 1:
        raw += round(pedigree_score * 0.25)

    # Generic branding without substance is actively penalized.
    if generic_hits and unique_technical == 0 and unique_workflow == 0:
        raw -= 15
    # Boilerplate density, scaled. A page can legitimately use one of these
    # words, so a single hit costs nothing; leaning on the register repeatedly
    # while showing little concrete mechanism is the signal. Capped so it
    # dampens rather than dominates, and softened when the page has already
    # demonstrated real technical depth.
    unique_boilerplate = len(set(boilerplate_hits))
    if unique_boilerplate >= 2:
        concrete = unique_technical + unique_defensibility
        raw -= min(18, 4 * (unique_boilerplate - 1)) if concrete <= 1 else min(
            8, 2 * (unique_boilerplate - 1)
        )
    if unique_enterprise == 0:
        raw -= 10
    if unique_product == 0:
        raw -= 12
    if unique_technical == 0 and unique_workflow == 0:
        raw -= 12
    if unique_defensibility == 0:
        raw -= 7
    if service_hits:
        raw -= 25
    if low_value_hits:
        raw -= 30

    # Score ceilings enforce selectivity.
    score = max(0, raw)
    if unique_pillar == 0:
        score = min(score, 40)
    if service_hits:
        score = min(score, 42)
    if low_value_hits:
        score = min(score, 24)
    if unique_technical == 0:
        score = min(score, 68)
    if unique_defensibility == 0:
        score = min(score, 79)
    if unique_problem == 0 or unique_value == 0:
        score = min(score, 84)

    # 90+ requires unusually complete website-level evidence.
    exceptional_dimensions = sum([
        unique_pillar >= 2,
        unique_product >= 1,
        unique_enterprise >= 1,
        unique_technical >= 2,
        unique_workflow >= 1,
        unique_value >= 1,
        unique_defensibility >= 1,
        unique_problem >= 1,
    ])
    if exceptional_dimensions < 7:
        score = min(score, 89)
    if exceptional_dimensions == 7:
        score = min(score, 93)
    if exceptional_dimensions == 8:
        score = min(score, 96)

    # A successfully extracted website receives a small, differentiated
    # evidence-coverage score even when it has no Glasswing fit. Zero is
    # reserved for pages where no usable text was extracted.
    if text:
        word_count = len(text.split())
        observed_evidence = (
            unique_pillar
            + unique_ai
            + unique_enterprise
            + unique_product
            + unique_technical
            + unique_workflow
            + unique_value
            + unique_defensibility
            + unique_problem
        )
        coverage_score = (
            1
            + min(word_count // 40, 8)
            + min(observed_evidence, 12)
        )
        score = max(score, min(coverage_score, 24))

    evidence = list(dict.fromkeys(
        pillar_hits[best]
        + product_hits
        + enterprise_hits
        + technical_hits
        + workflow_hits
        + value_hits
        + defensibility_hits
        + important_problem_hits
        + [str(flag.get("label", "")) for flag in founder_flags]
    ))

    return (
        int(max(0, min(score, 96))),
        best if pillar_hits[best] else "unclear",
        evidence,
        counts,
    )


def investment_eligibility_adjusted_score(
    score: object,
    *,
    predominantly_non_english: bool = False,
    location_status: str = "",
    parked: bool = False,
    placeholder: bool = False,
    agency: bool = False,
    consumer: bool = False,
    media: bool = False,
    commodity_ai_service: bool = False,
    mature_or_funded: bool = False,
) -> tuple[int, list[str]]:
    """Apply Glasswing eligibility caps to the website thesis-fit score."""
    adjusted = int(pd.to_numeric(score, errors="coerce") or 0)
    applied: list[str] = []
    caps = (
        (parked, 1, "parked_or_for_sale"),
        (placeholder and not parked, 3, "generic_placeholder"),
        (predominantly_non_english, 5, "predominantly_non_english"),
        (str(location_status).strip().upper() == "NON_US", 10, "confirmed_non_us"),
        (consumer, 15, "consumer_b2c_product"),
        (media, 15, "media_newsletter_or_aggregator"),
        (agency, 20, "agency_or_consultancy"),
        (commodity_ai_service, 25, "commodity_ai_wrapper_or_service"),
        (mature_or_funded, 45, "mature_or_funded_company"),
    )
    for active, cap, reason in caps:
        if active:
            adjusted = min(adjusted, cap)
            applied.append(f"{reason}:max_{cap}")
    return max(0, min(adjusted, 100)), applied


def agency_assessment(text: str) -> tuple[bool, list[str]]:
    lower = clean(text).lower()
    leading_copy = lower[:700]
    hits = [term for term in AGENCY if term in lower]

    service_identity = [
        "consulting firm",
        "consulting practice",
        "consulting services",
        "ai consulting",
        "ai consultancy",
        "automation consultancy",
        "ai consultant",
        "technology consultant",
        "independent consultant",
        "fractional cto",
        "fractional ai",
        "advisory firm",
        "advisory services",
        "executive ai consulting",
        "custom ai solutions",
        "ai development company",
        "it services company",
        "ai-driven it services",
    ]
    service_signals = [
        "book a consultation",
        "schedule a consultation",
        "our team of experts",
        "client projects",
        "staff augmentation",
        "providing the necessary resources",
        "pool of highly skilled",
        "design and verification services",
        "engineering services",
        "bespoke development",
        "custom solutions for your business",
        "we build custom",
        "implementation services",
        "professional services",
        "fractional ai",
        "done-for-you",
        "for clients",
        "client work",
        "client projects",
        # A small senior/boutique team billed per project ("the
        # engagement") advising clients is a consultancy regardless of
        # industry -- confirmed live: a boutique offensive-security shop
        # ("small, senior team... advise leaders on AI strategy... the
        # capability that outlasts the engagement") scored as a top
        # cybersecurity candidate because none of these exact phrases
        # were previously matched anywhere.
        "advise leaders",
        "advises leaders",
        "outlasts the engagement",
        "crossed into ai security",
        "handed off to your team",
        "handed off to you",
    ]
    product_signals = [
        "platform",
        "software",
        "subscription",
        "self-serve",
        "dashboard",
        "api",
        "integrations",
        "product",
        "free trial",
        "pricing",
    ]

    service_hits = [term for term in service_signals if term in lower]
    # Confirmed live: a naive substring check matched "product" inside
    # "production" and would equally match "api" inside "rapid" --
    # occurrences() already exists precisely for boundary-safe phrase
    # matching everywhere else in this file.
    product_hits = occurrences(lower, product_signals)
    identity_hits = [
        term for term in service_identity if term in leading_copy
    ]
    studio_identity = bool(
        any(term in leading_copy for term in (
            "product studio",
            "ai studio",
            "development studio",
            "digital product studio",
        ))
        and any(term in lower for term in (
            "for clients",
            "client work",
            "client projects",
            "we build",
            "custom solutions",
        ))
    )
    if studio_identity:
        identity_hits.append("client-services studio")
    engineering_services_identity = bool(
        any(term in leading_copy for term in (
            "our services",
            "we offer",
            "engineering services",
        ))
        and sum(term in lower for term in (
            "design services",
            "verification services",
            "providing the necessary resources",
            "pool of highly skilled",
            "scale your design",
            "staff augmentation",
            "client projects",
        )) >= 2
    )
    if engineering_services_identity:
        identity_hits.append("engineering-services provider")
    service_identity_bundles = [
        (
            ["software development"],
            ["providing", "services", "for clients", "custom solutions"],
            "software-development provider",
        ),
        (
            ["cybersecurity services"],
            ["consulting", "managed services", "technical services"],
            "cybersecurity-services provider",
        ),
        (
            ["consulting"],
            ["strategic services", "technical services", "managed services"],
            "consulting/services provider",
        ),
        (
            ["attestation services", "audit firm"],
            ["audit", "compliance", "assessment"],
            "audit/attestation firm",
        ),
        (
            ["design engineering"],
            ["rapid prototyping", "final assembly"],
            "design/prototyping services",
        ),
        (
            ["guiding enterprises through"],
            ["transformation journey", "ai transformation"],
            "transformation consultancy",
        ),
        (
            ["helps businesses integrate ai", "integrate ai into real workflows"],
            ["advisory", "consulting", "implementation", "validate what matters"],
            "ai-integration services",
        ),
        (
            ["we help small and mid-sized businesses", "we help smbs"],
            ["put ai to work", "implementation", "managed service", "msp"],
            "smb-ai services",
        ),
        (
            ["builds"],
            ["digital products for clients", "software for clients"],
            "client product development",
        ),
        (
            ["application development", "we architect and build"],
            ["we deliver production solutions", "build high-performance applications"],
            "application-development services",
        ),
        (
            # Confirmed live: "A precision approach to AI implementation.
            # Building AI systems for regulated industries with
            # enterprise-grade controls" scored as a top candidate --
            # bespoke, per-client "building AI systems for X" is a
            # services model, not a repeatable product, even when the
            # copy uses enterprise/product-sounding language.
            ["ai implementation", "building ai systems for"],
            ["regulated industries", "enterprise-grade controls", "precision approach"],
            "bespoke-ai-implementation services",
        ),
    ]
    for identity_terms, confirming_terms, label in service_identity_bundles:
        if (
            any(term in leading_copy for term in identity_terms)
            and any(term in lower for term in confirming_terms)
        ):
            identity_hits.append(label)

    explicit_agency = any(term in lower for term in (
        "marketing agency",
        "seo agency",
        "development agency",
        "digital agency",
        "creative agency",
        "automation agency",
    ))

    # Stay strict on actual agencies, but a single generic consulting phrase
    # cannot override strong product evidence.
    is_agency = (
        explicit_agency
        or bool(identity_hits)
        or (
            len(hits) + len(service_hits) >= 2
            and len(product_hits) <= 1
        )
        or (
            len(service_hits) >= 3
        )
    )
    evidence = list(dict.fromkeys(identity_hits + hits + service_hits))[:10]
    return is_agency, evidence


def media_newsletter_assessment(text: str) -> tuple[bool, list[str]]:
    lower = clean(text).lower()
    leading_copy = lower[:900]
    hits = occurrences(lower, MEDIA_NEWSLETTER)

    strong_media = [
        "daily newsletter",
        "weekly newsletter",
        "weekly report",
        "weeklyreport",
        "ai newsletter",
        "news publication",
        "daily brief",
        "curated news",
        "latest headlines",
        "join our readers",
        "subscribers",
        "editorial team",
        "read the latest stories",
        "stay on top of ai",
        "ai news, research, and tools",
        "summarized daily",
        "curated ai news",
        "news site",
        "news website",
        "news publication",
    ]
    product_signals = [
        "platform",
        "software",
        "api",
        "dashboard",
        "integrations",
        "workflow",
        "deploy",
        "enterprise",
        "data platform",
        "automation",
        "infrastructure",
    ]

    strong_hits = [term for term in strong_media if term in lower]
    # Confirmed live: a naive substring check matched "product" inside
    # "production" and would equally match "api" inside "rapid" --
    # occurrences() already exists precisely for boundary-safe phrase
    # matching everywhere else in this file.
    product_hits = occurrences(lower, product_signals)

    education_identity_hits: list[str] = []
    education_patterns = (
        (
            ["online course", "online program", "online ai program",
             "training program",
             "course coaching", "academy", "ai training"],
            ["enroll", "curriculum", "students", "coaching", "certification"],
        ),
        (
            ["conference", "summit"],
            ["speakers", "agenda", "tickets", "workshops",
             "reserve my seat", "register", "take place"],
        ),
        (
            ["webinar", "live webinar", "on-demand webinar", "virtual event",
             "free webinar"],
            ["register", "rsvp", "save your seat", "reserve your seat",
             "agenda", "speakers", "sign up to attend",
             "watch now", "watch on demand", "webinar recording",
             "hosted by", "save the date"],
        ),
    )
    for identity_terms, confirming_terms in education_patterns:
        identity = [term for term in identity_terms if term in leading_copy]
        confirming = [
            term for term in confirming_terms if term in leading_copy
        ]
        if identity and confirming:
            education_identity_hits.extend(identity + confirming[:2])
    # Invite copy just as often reads "Join <company> for a live webinar" as
    # it does "Register now" -- a reported case used only this phrasing and
    # slipped through undetected since it shares no words with the flat
    # confirming-term list above.
    if re.search(r"\bjoin\b[^.]{0,60}\bwebinar\b", leading_copy):
        education_identity_hits.append("join ... for a webinar")

    directory_identity_hits: list[str] = []
    directory_identity = [
        "ai tools directory",
        "tools directory",
        "comparison and analysis",
        "independent benchmarks",
        "top providers compared",
        "providers compared",
        "best providers compared",
    ]
    directory_confirming = [
        "discovering", "reviewing", "comparing", "rankings",
        "providers", "leaderboard", "reviews",
    ]
    directory_identity_hits.extend(
        term for term in directory_identity if term in leading_copy
    )
    if (
        "directory" in leading_copy
        and sum(term in lower for term in directory_confirming) >= 2
    ):
        directory_identity_hits.append("review/comparison directory")
    if (
        re.search(r"\bbest\b.{0,80}\bproviders?\b", leading_copy)
        and any(term in leading_copy for term in (
            "compared", "comparison", "top providers", "pricing",
        ))
    ):
        directory_identity_hits.append("provider-comparison article")
    # A curated list of academic papers ("The best AI papers of 2025",
    # citation counts, abstracts) is content, not a company -- a reported
    # case (bestpapers.ai) scored 81 and reached Investable because none of
    # the directory/comparison terms above are about research literature.
    if (
        re.search(r"\bbest\b.{0,40}\bpapers?\b", leading_copy)
        and any(term in lower for term in ("citations", "arxiv", "abstract"))
    ):
        directory_identity_hits.append("curated research paper list")

    newsletter_identity = bool(
        any(term in leading_copy for term in (
            "ai weekly report",
            "weekly report",
            "daily ai intelligence briefing",
            "ai news world",
        ))
        and any(term in lower for term in (
            "news", "briefing", "newsletter", "subscribe", "latest",
        ))
    )
    if newsletter_identity:
        strong_hits.append("newsletter/news identity")

    # Reference libraries and personal futurist pages can be extremely dense
    # with thesis vocabulary while having no operating company or product.
    # Keep these patterns narrow and require the absence of commercial company
    # framing so a real regulatory/compliance product is not suppressed.
    reference_identity = bool(
        any(term in leading_copy for term in (
            "working reference for",
            "regulatory playbook",
            "compliance playbook",
            "reference guide for",
        ))
        and any(term in lower for term in (
            "regulation", "regulatory", "standards", "framework", "guide",
        ))
        and not any(term in lower for term in (
            "our platform", "our product", "book a demo", "request a demo",
            "customers", "pricing",
        ))
    )
    if reference_identity:
        strong_hits.append("reference/playbook identity")

    personal_exploration_identity = bool(
        re.search(r"\bexploring\b.{0,80}\b(?:technology|innovation|the future)\b", leading_copy)
        and not any(term in lower for term in (
            "our platform", "our product", "our company", "our customers",
            "book a demo", "request a demo", "pricing",
        ))
    )
    if personal_exploration_identity:
        strong_hits.append("personal technology exploration page")

    # A personal site/portfolio, not an operating company -- distinct from
    # the media/newsletter case above but the same underlying problem: no
    # company to invest in. Requires an explicit personal-site
    # self-identification near the top of the page AND the total absence of
    # any company-ish language, so a solo founder's legitimate startup site
    # (which almost always mentions a team, customers, or pricing somewhere)
    # isn't caught by this.
    personal_site_terms = [
        "my portfolio", "personal portfolio", "personal website",
        "welcome to my website", "welcome to my personal site",
        "this is my personal site", "i'm a freelance", "i am a freelance",
        "freelance portfolio", "my resume", "hire me",
        "thanks for visiting my site",
        # A hobby open-source project, not a company to invest in --
        # confirmed live: "A modern open-source CRM" (an OSS client for an
        # existing project) reached Needs Review with nothing to evaluate.
        # An open-core company still survives this: it pairs the same
        # phrase with real company_signals (pricing/enterprise/customers).
        "open-source", "open source project", "fully open-source",
        "is open source", "an open-source alternative",
        # An academic CV. Confirmed live: a senior lecturer's homepage reached
        # the candidate list scoring 61 -- higher than kerno.io, a real
        # AI-agent infrastructure company, at 23. A verbose academic page
        # incidentally hits many lexicon terms ("machine learning", "data
        # science", "cloud computing") without being a company at all, which
        # is exactly how keyword volume outranks a concise real pitch.
        "curriculum vitae", "publications", "conference papers",
        "journal articles", "research interests", "senior lecturer",
        "associate professor", "phd student", "phd candidate",
        "google scholar", "orcid", "my students", "courses i teach",
        "teaching experience", "peer-reviewed",
        # A personal portfolio that never says "portfolio". Confirmed live:
        # an analytics engineer's site scored 42.
        "about me", "my work", "my projects", "my writing", "my blog",
        "get in touch", "years of experience", "i'm a developer",
        "i am a developer", "i'm an engineer", "i am an engineer",
        "creative technologist", "based in",
    ]
    company_signals = [
        "our platform", "our product", "our customers", "our team",
        "pricing", "book a demo", "request a demo", "enterprise",
        "case studies", "customers include", "our mission",
    ]
    personal_site_hits = [
        term for term in personal_site_terms if term in leading_copy
    ]
    company_hits = [term for term in company_signals if term in lower]

    # Terse personal-bio openers (a bare job title as the entire identity,
    # no explicit "my portfolio"/"hire me" self-identification at all) --
    # confirmed live: "Full stack engineer. Builder of systems that run."
    # and "Frontend engineer building scalable platforms. Currently at
    # Dream11, serving 150M+ users." are real examples the checks above
    # missed. Gated on very short overall content so a real careers page
    # (much longer -- describes the company, role, and benefits) can't
    # accidentally match.
    word_count = len(lower.split())
    job_title_bio = bool(
        re.search(
            r"\b(?:senior |lead |staff |principal |junior )?"
            r"(?:full[\s-]?stack|frontend|front-end|backend|back-end|"
            r"software|mobile|ios|android|devops|platform|systems?)"
            r" engineer(?:ing consultant)?\b",
            leading_copy,
        )
        and word_count <= 40
        and not company_hits
    )
    if job_title_bio:
        personal_site_hits.append("bare job title bio, no company framing")

    is_personal_site = (
        bool(personal_site_hits) or job_title_bio
    ) and not company_hits

    is_media = (
        bool(education_identity_hits)
        or bool(directory_identity_hits)
        or newsletter_identity
        or reference_identity
        or personal_exploration_identity
        or is_personal_site
        or
        (
            len(strong_hits) >= 2
            and len(product_hits) <= 2
        )
        or len(strong_hits) >= 3
        # The narrow strong_media list can miss real newsletters entirely;
        # a broad hit count this high across the full MEDIA_NEWSLETTER
        # lexicon is unlikely on a genuine product/company page.
        or len(hits) >= 5
    )
    evidence = list(dict.fromkeys(
        education_identity_hits
        + directory_identity_hits
        + strong_hits
        + personal_site_hits
        + hits
    ))[:10]
    return is_media, evidence


def _location_segments(text: str) -> list[str]:
    """Return only sentences/lines likely to describe the company location."""
    normalized = text.replace("\r", "\n")
    pieces = re.split(r"(?<=[.!?])\s+|\n+", normalized)
    segments: list[str] = []

    address_words = (
        "street", "st.", "avenue", "ave.", "road", "rd.", "boulevard",
        "blvd", "suite", "floor", "building", "postal", "postcode",
    )
    us_zip = re.compile(
        r"\b(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|"
        r"ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|"
        r"PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\s+\d{5}(?:-\d{4})?\b",
        re.IGNORECASE,
    )

    for piece in pieces:
        cleaned = clean(piece)
        lower = cleaned.lower()
        if not cleaned:
            continue

        has_context = any(marker in lower for marker in LOCATION_CONTEXT_TERMS)
        looks_like_address = (
            bool(us_zip.search(cleaned))
            or (
                any(word in lower for word in address_words)
                and bool(re.search(r"\d", cleaned))
            )
        )

        if has_context or looks_like_address:
            segments.append(cleaned)

    return segments


def company_location_assessment(text: str) -> tuple[str, list[str]]:
    """Infer company location from HQ, office, registration, or address text."""
    segments = _location_segments(text)
    non_us_hits: list[str] = []
    us_hits: list[str] = []

    us_zip = re.compile(
        r"\b(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|"
        r"ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|"
        r"PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\s+\d{5}(?:-\d{4})?\b",
        re.IGNORECASE,
    )

    for segment in segments:
        lower = segment.lower()

        for term in NON_US_COUNTRIES + NON_US_ALIASES:
            if re.search(rf"\b{re.escape(term)}\b", lower):
                non_us_hits.append(term)

        for term in US_LOCATION_TERMS:
            if re.search(rf"\b{re.escape(term)}\b", lower):
                us_hits.append(term)

        if us_zip.search(segment):
            us_hits.append("us_state_zip")

    non_us_hits = list(dict.fromkeys(non_us_hits))
    us_hits = list(dict.fromkeys(us_hits))

    if non_us_hits and not us_hits:
        return "NON_US", non_us_hits
    if us_hits and not non_us_hits:
        return "US", us_hits
    if non_us_hits and us_hits:
        return "UNKNOWN", [
            "conflicting_location_evidence",
            *non_us_hits,
            *us_hits,
        ]
    return "UNKNOWN", []


def explicit_non_us_identity_assessment(text: str) -> tuple[bool, list[str]]:
    """Detect geography that explicitly describes the company or target market.

    This intentionally scans only title/description copy supplied by the
    caller, avoiding country names from customer logos, blogs, and footers.
    """
    leading = clean(text).lower()
    patterns = [
        r"\b(?:born|based|headquartered|founded|incorporated)\s+in\s+"
        r"(?:saudi(?: arabia)?|singapore|canada|india|europe|africa)\b",
        r"\b(?:saudi|singaporean|canadian|indian|european|african)\s+"
        r"(?:ai\s+)?(?:company|startup|platform|healthcare|logistics|"
        r"powerhouse)\b",
        r"\b(?:platform|agent|software|infrastructure)\s+for\s+"
        r"(?:europe|india|canada|saudi arabia|africa)\b",
        r"\bbuilt\s+for\s+(?:indian|european|canadian|saudi|african)\b",
        r"\|\s*(?:singapore|canada|india|saudi arabia)\b",
    ]
    hits = [
        match.group(0)
        for pattern in patterns
        for match in re.finditer(pattern, leading, flags=re.I)
    ]
    return bool(hits), list(dict.fromkeys(hits))[:6]


def category_is_valid(pillar: str, text: str) -> bool:
    lower = text.lower()
    evidence = {
        "next_gen_compute": [
            "semiconductor", "chip", "accelerator", "gpu", "asic",
            "inference infrastructure", "model serving", "compute platform",
            "distributed compute", "quantum computing", "photonics",
            "neuromorphic", "memory bandwidth", "data center"
        ],
        "intelligent_enterprise_defense": [
            "cybersecurity", "security operations", "threat detection",
            "incident response", "soc", "ciso", "malware", "vulnerability",
            "identity security", "cloud security", "defense",
            "agent security", "security protocol", "programmable guardrails",
            "wallet security",
        ],
        "physical_ai": [
            "robotics", "robot", "computer vision", "industrial automation",
            "manufacturing", "autonomous system", "sensor", "factory",
            "warehouse", "physical world"
        ],
        "vertical_ai": [
            "workflow", "industry", "vertical", "operations", "finance",
            "insurance", "healthcare", "legal", "construction",
            "logistics", "manufacturing"
        ],
        "ai_adaptive_infrastructure": [
            "data infrastructure", "ml infrastructure", "model monitoring",
            "observability", "data pipeline", "vector database",
            "orchestration", "deployment", "inference", "developer platform",
            "ai infrastructure as a service", "gpu as a service",
            "inference as a service", "serverless inference",
            "managed inference", "model serving api", "ai compute platform"
        ],
        "collaborative_intelligence": [
            "copilot", "collaboration", "human-in-the-loop",
            "decision support", "team workflow", "knowledge worker"
        ],
    }
    required = evidence.get(pillar, [])
    return any(term in lower for term in required)

def glasswing_dimensions(text: str) -> dict[str, int]:
    return {
        "buyer": len(occurrences(text, ENTERPRISE)),
        "workflow": len(occurrences(text, WORKFLOW_DEPTH)),
        "technical": len(occurrences(text, TECHNICAL_DEPTH)),
        "business_value": len(occurrences(text, BUSINESS_VALUE)),
    }


ENTERPRISE_RESCUE_BUYERS = [
    "enterprise", "businesses", "business content", "your team", "teams",
    "manager's job", "auditor", "biopharma", "researcher", "developers",
    "software engineering", "title companies", "security teams", "systems",
]

ENTERPRISE_RESCUE_WORKFLOWS = [
    "workflow", "workflows", "operating system", "control plane",
    "runtime control", "governance", "compliance", "audit evidence",
    "accrual", "forecast automation", "budgeting", "qualify leads",
    "hand conversations", "messaging", "pipeline", "orchestration",
    "data quality", "lifecycle",
    "search", "title search", "secure access", "identity enforcement",
]

ENTERPRISE_RESCUE_TECHNICAL = [
    "agent", "agents", "autonomous", "api", "mcp", "model in production",
    "runtime", "policy enforcement", "mathematically verified",
    "formal proof", "real-time prediction", "infrastructure",
    "ai-powered operating system",
    "pipelines", "identity", "zero-knowledge proofs",
]

ENTERPRISE_RESCUE_PRODUCTS = [
    "platform", "software", "workspace", "operating system", "control plane",
    "saas", "system", "inbox", "automation", "generator", "agent",
    "stack", "gateway",
]

ENTERPRISE_RESCUE_STRONG_CONCEPTS = [
    "enterprise ai governance", "ai governance", "control plane",
    "runtime control", "runtime integrity", "grc", "iso 27001",
    "iso 42001", "nis2", "research operating system", "formal proof",
    "mathematically verified", "mcp tools", "agent orchestration",
    "agent workspace", "autonomous systems", "software engineering",
    "r&d accrual", "forecast automation", "biopharma", "title companies",
    "ai-powered operating system", "supplier verification",
    "claims risk detection", "real-time prediction",
    "ai search", "identity enforcement", "zero-knowledge proofs",
]


def phrase_occurrences(text: str, terms: list[str]) -> list[str]:
    """Match rescue phrases on token boundaries to avoid agent/agency bugs."""
    return occurrences(text, terms)


def phrase_pattern(terms: list[str]) -> str:
    alternatives = "|".join(re.escape(term) for term in terms)
    return rf"(?<![a-z0-9])(?:{alternatives})(?![a-z0-9])"


def enterprise_rescue_mask(texts: pd.Series) -> pd.Series:
    """Vectorized equivalent of the rescue gate for historical rows."""
    cleaned = texts.fillna("").astype(str).str.lower()
    buyers = cleaned.str.contains(
        phrase_pattern(ENTERPRISE_RESCUE_BUYERS),
        regex=True,
        na=False,
    )
    workflow_count = cleaned.str.count(
        phrase_pattern(ENTERPRISE_RESCUE_WORKFLOWS),
    )
    technical = cleaned.str.contains(
        phrase_pattern(ENTERPRISE_RESCUE_TECHNICAL),
        regex=True,
        na=False,
    )
    products = cleaned.str.contains(
        phrase_pattern(ENTERPRISE_RESCUE_PRODUCTS),
        regex=True,
        na=False,
    )
    strong = cleaned.str.contains(
        phrase_pattern(ENTERPRISE_RESCUE_STRONG_CONCEPTS),
        regex=True,
        na=False,
    )
    return (
        cleaned.str.count(r"\S+").ge(8)
        & strong
        & products
        & (buyers | workflow_count.ge(2))
        & (technical | workflow_count.ge(3))
    )


def enterprise_rescue_assessment(text: str) -> tuple[bool, list[str]]:
    """Identify concise but credible enterprise products for human review.

    This is deliberately a review rescue, never an investable promotion.
    It requires a product plus an enterprise buyer/workflow and substantive
    technical or high-value workflow evidence.
    """
    cleaned = clean(text).lower()
    if len(cleaned.split()) < 8:
        return False, []

    buyers = phrase_occurrences(cleaned, ENTERPRISE_RESCUE_BUYERS)
    workflows = phrase_occurrences(cleaned, ENTERPRISE_RESCUE_WORKFLOWS)
    technical = phrase_occurrences(cleaned, ENTERPRISE_RESCUE_TECHNICAL)
    products = phrase_occurrences(cleaned, ENTERPRISE_RESCUE_PRODUCTS)
    strong_concepts = phrase_occurrences(
        cleaned,
        ENTERPRISE_RESCUE_STRONG_CONCEPTS,
    )

    rescued = bool(
        strong_concepts
        and products
        and (buyers or len(workflows) >= 2)
        and workflows
        and (technical or len(workflows) >= 3)
    )
    evidence = list(dict.fromkeys(
        strong_concepts + products + buyers + workflows + technical
    ))[:12]
    return rescued, evidence


def high_conviction_fit_assessment(text: str) -> tuple[bool, list[str]]:
    """Recognize specific technical/product bundles supported by human Yeses.

    Each bundle requires multiple related phrases. Generic words such as
    platform, API, security, workflow, or AI agent never qualify on their own.
    """
    leading = clean(text).lower()[:3000]
    # Every bundle has an anchor phrase plus a minimum number of total hits.
    # Requiring the anchor prevents generic combinations such as
    # "observability + guardrails + governance" from becoming Investable.
    bundles = [
        (
            ["functional safety"],
            ["functional safety", "certifiable safety"],
            2,
        ),
        (
            ["evaluation infrastructure"],
            ["evaluation infrastructure", "continuous evals",
             "reproducible evaluation"],
            2,
        ),
        (
            ["satellite and aerial imagery", "structured world models"],
            ["satellite and aerial imagery", "structured world models",
             "geospatial intelligence"],
            2,
        ),
        (
            ["physics-driven ai", "pcb layout"],
            ["physics-driven ai", "pcb layout", "fabrication-ready"],
            2,
        ),
        (
            ["ai gateway"],
            ["ai gateway", "observability", "guardrails",
             "prompt management", "governance"],
            3,
        ),
        (
            ["ai infrastructure and workloads", "ai factories"],
            ["ai infrastructure and workloads", "reliability",
             "ai factories", "gpus", "accelerators", "uptime",
             "utilization"],
            3,
        ),
        (
            ["ai infrastructure as a service", "gpu as a service",
             "inference as a service", "serverless inference"],
            ["ai infrastructure as a service", "gpu as a service",
             "inference as a service", "serverless inference",
             "managed inference", "model serving api", "gpu autoscaling",
             "inference optimization", "gpu utilization",
             "inference throughput", "training throughput"],
            3,
        ),
        (
            ["distill", "distills", "model distillation"],
            ["distill", "distills", "model distillation", "private",
             "purpose-built models", "own environment"],
            3,
        ),
        (
            ["ai agents build", "prompt your ideas"],
            ["build websites", "web apps", "end-to-end software",
             "prompt your ideas", "ai agents build"],
            3,
        ),
        (
            ["engagement intelligence"],
            ["engagement intelligence", "recognition and rewards",
             "goals and reviews", "hris", "performance"],
            3,
        ),
        (
            ["recipe formulation"],
            ["recipe formulation", "food innovation",
             "product development", "r&d teams", "consumer acceptance"],
            3,
        ),
        (
            ["delivery management"],
            ["delivery management", "dispatching", "delivery dispatch",
             "reputation management", "customer experience"],
            3,
        ),
        (
            ["production line monitoring", "visual ai agents"],
            ["production line monitoring", "visual ai agents",
             "root cause analysis", "overall equipment effectiveness",
             "continuous vision", "quality inspection"],
            3,
        ),
    ]
    for anchors, terms, minimum in bundles:
        if not phrase_occurrences(leading, anchors):
            continue
        hits = phrase_occurrences(leading, terms)
        if len(hits) >= minimum:
            return True, hits
    return False, []


def venture_quality_assessment(
    text: str,
    *,
    pillar: str,
    counts: dict[str, int],
    dimensions: dict[str, int],
    high_conviction_fit: bool = False,
) -> dict[str, object]:
    """Measure venture quality separately from raw Glasswing keywords.

    A company can be thesis-relevant without yet being investable. This layer
    asks whether the website also establishes a product, wedge, and plausible
    moat. Founder evidence is a bonus, never a mandatory website field.
    """
    cleaned = clean(text)
    product_evidence = occurrences(cleaned, PRODUCT_REALITY_SIGNALS)
    wedge_evidence = occurrences(cleaned, WEDGE_SIGNALS)
    moat_evidence = occurrences(cleaned, MOAT_SIGNALS)
    founder_visibility = occurrences(
        cleaned,
        FOUNDER_VISIBILITY_SIGNALS,
    )
    founder_credibility = occurrences(
        cleaned,
        FOUNDER_CREDIBILITY_SIGNALS,
    )
    strong_school_hits = occurrences(cleaned, STRONG_US_SCHOOLS)
    mid_school_hits = occurrences(cleaned, MID_US_SCHOOLS)
    bad_school_hits = occurrences(cleaned, BAD_US_SCHOOLS)
    strong_work_experience_hits = occurrences(cleaned, STRONG_WORK_EXPERIENCE)
    verified_founder_flags = founder_green_flags(cleaned)
    pedigree_score = founder_pedigree_score(verified_founder_flags)
    slop_evidence = occurrences(cleaned, AI_SLOP_SIGNALS)

    pillar_count = int(counts.get("pillar", 0))
    product_count = int(counts.get("product", 0))
    technical_count = int(dimensions.get("technical", 0))
    workflow_count = int(dimensions.get("workflow", 0))
    buyer_count = int(dimensions.get("buyer", 0))
    value_count = int(dimensions.get("business_value", 0))
    defensibility_count = int(counts.get("defensibility", 0))
    problem_count = int(counts.get("important_problem", 0))
    generic_count = int(counts.get("generic_ai", 0))

    category_valid = bool(
        pillar
        and pillar != "unclear"
        and category_is_valid(pillar, cleaned)
    )
    thesis_score = min(
        100,
        pillar_count * 22
        + (18 if category_valid else 0)
        + min(technical_count, 2) * 8,
    )
    if (
        pillar == "collaborative_intelligence"
        and workflow_count == 0
        and technical_count == 0
    ):
        thesis_score = min(thesis_score, 25)

    product_score = min(
        100,
        product_count * 12
        + min(len(set(product_evidence)), 3) * 16
        + (10 if workflow_count else 0),
    )
    wedge_score = min(
        100,
        min(buyer_count, 2) * 15
        + min(workflow_count, 2) * 18
        + min(problem_count, 2) * 15
        + min(value_count, 2) * 12
        + min(len(set(wedge_evidence)), 2) * 12,
    )
    moat_score = min(
        100,
        min(defensibility_count, 2) * 28
        + min(technical_count, 3) * 12
        + min(len(set(moat_evidence)), 2) * 16,
    )
    # Pedigree is heavily weighted only when it appears in explicit founder or
    # leadership context. Whole-page school/employer mentions remain in the
    # evidence trail for debugging but add no score (customer logos are common).
    school_adjustment = (
        min(len(set(mid_school_hits)), 2) * 3
        - min(len(set(bad_school_hits)), 2) * 8
    )
    founder_score = min(
        100,
        max(
            0,
            pedigree_score
            + min(len(set(founder_visibility)), 2) * 8
            + min(len(set(founder_credibility)), 2) * 8
            + school_adjustment,
        ),
    )

    thesis_fit = thesis_score >= 38
    product_real = product_score >= 30
    wedge_present = wedge_score >= 42
    moat_present = bool(
        moat_score >= 32
        and (
            defensibility_count >= 1
            or len(set(moat_evidence)) >= 1
            or technical_count >= 4
        )
    )
    founder_strength = founder_score >= 35
    frontier_pillar = pillar in {
        "physical_ai",
        "ai_adaptive_infrastructure",
        "intelligent_enterprise_defense",
        "next_gen_compute",
    }
    slop_risk = bool(
        (
            len(set(slop_evidence)) >= 2
            or generic_count >= 2
        )
        and moat_score < 32
        and wedge_score < 42
    )

    overall_score = round(
        thesis_score * 0.22
        + product_score * 0.15
        + wedge_score * 0.20
        + moat_score * 0.18
        + founder_score * 0.25
    )
    # Frontier companies often explain the technical edge in detail while
    # omitting the marketing language that our generic wedge detector expects.
    # A demonstrated moat can substitute for that wording, but only for a real
    # product in a frontier pillar with a strong combined quality score.
    frontier_technical_wedge = bool(
        frontier_pillar
        and moat_present
        and technical_count >= 1
        and overall_score >= 48
    )
    investable_ready = bool(
        thesis_fit
        and product_real
        and (wedge_present or frontier_technical_wedge)
        and (
            moat_present
            or high_conviction_fit
            or (
                founder_score >= 55
                and frontier_pillar
                and technical_count >= 1
            )
        )
        and overall_score >= 48
        and not slop_risk
    )
    # Review is expensive human attention. A product must show more than a
    # thesis keyword and generic enterprise positioning: preserve candidates
    # with a moat/founder signal, a curated learned-positive pattern, or a
    # concrete wedge supported by an additional technical/problem signal.
    review_quality_signal = bool(
        high_conviction_fit
        or moat_present
        or founder_strength
        or (
            wedge_present
            and (
                technical_count >= 2
                or defensibility_count >= 1
                or problem_count >= 1
            )
        )
    )
    reviewable = bool(
        thesis_fit
        and product_real
        and not slop_risk
        and review_quality_signal
    )

    return {
        "thesis_score": thesis_score,
        "product_score": product_score,
        "wedge_score": wedge_score,
        "moat_score": moat_score,
        "founder_score": founder_score,
        "overall_score": max(0, min(overall_score, 100)),
        "thesis_fit": thesis_fit,
        "product_real": product_real,
        "wedge_present": wedge_present,
        "moat_present": moat_present,
        "founder_strength": founder_strength,
        "slop_risk": slop_risk,
        "investable_ready": investable_ready,
        "reviewable": reviewable,
        "frontier_technical_wedge": frontier_technical_wedge,
        "review_quality_signal": review_quality_signal,
        "product_evidence": product_evidence[:10],
        "wedge_evidence": wedge_evidence[:10],
        "moat_evidence": list(dict.fromkeys(
            moat_evidence
            + occurrences(cleaned, DEFENSIBILITY_SIGNALS)
        ))[:10],
        "founder_evidence": list(dict.fromkeys(
            [str(flag.get("label", "")) for flag in verified_founder_flags]
            + founder_visibility
            + founder_credibility
            + strong_school_hits
            + mid_school_hits
            + bad_school_hits
            + strong_work_experience_hits
        ))[:10],
        "slop_evidence": slop_evidence[:10],
    }


def b2c_assessment(text: str) -> tuple[bool, int, list[str]]:
    """Reject obvious consumer products without punishing enterprise software."""
    audience = occurrences(text, B2C_AUDIENCE)
    use_case = occurrences(text, B2C_USE_CASE)
    self_serve = occurrences(text, SELF_SERVE_CONSUMER)
    enterprise = occurrences(text, ENTERPRISE)

    score = min(len(audience), 2) * 2 + min(len(use_case), 3) * 3 + min(len(self_serve), 2)
    obvious = (
        (len(use_case) >= 1 and len(audience) >= 1)
        or (len(use_case) >= 2)
        or (len(audience) >= 1 and len(self_serve) >= 1)
    )
    is_b2c = obvious and len(enterprise) < 2
    evidence = list(dict.fromkeys(audience + use_case + self_serve))
    return is_b2c, score, evidence




def only_generic_coming_soon_content(text: str) -> bool:
    """Detect pages with no meaningful content beyond launch boilerplate."""
    cleaned = clean(text).lower()
    cleaned = re.sub(r"https?://\S+", " ", cleaned)
    cleaned = re.sub(r"\b(?:www\.)?[a-z0-9-]+(?:\.[a-z]{2,})+\b", " ", cleaned)

    generic_phrases = [
        "our website is coming soon",
        "new website coming soon",
        "website coming soon",
        "site coming soon",
        "coming soon",
        "under construction",
        "please check back for an update soon",
        "check back for an update soon",
        "please check back soon",
        "check back soon",
        "stay tuned",
    ]
    for phrase in generic_phrases:
        cleaned = cleaned.replace(phrase, " ")

    tokens = re.findall(r"[a-z0-9]+", cleaned)
    ignorable = {
        "the", "a", "an", "website", "site", "official", "home",
        "page", "welcome", "new", "our", "is", "will", "be",
    }
    meaningful = [token for token in tokens if token not in ignorable]
    return len(meaningful) == 0

def placeholder_assessment(
    text: str,
    *,
    word_count: int,
) -> tuple[bool, str, list[str]]:
    """Detect blank, registrar, and generic hosting-template pages."""
    cleaned = clean(text)
    lower = cleaned.lower()
    evidence: list[str] = []

    if not cleaned or word_count == 0:
        return False, "ghost_or_inaccessible_site", ["no_visible_content"]

    anti_bot_hits = [
        phrase for phrase in (
            "checking your browser before accessing",
            "checking your browser before accessing. just a moment",
            "please wait for up to 5 seconds",
            "verify you are human to continue",
        )
        if phrase in lower
    ]
    if anti_bot_hits:
        return (
            True,
            "anti_bot_interstitial_no_company_content",
            anti_bot_hits,
        )

    if only_generic_coming_soon_content(cleaned):
        return (
            True,
            "generic_coming_soon_only",
            ["no_original_company_or_product_content"],
        )

    if (
        ("page not found" in lower or "site not found" in lower)
        and any(term in lower for term in (
            "doesn't exist",
            "does not exist",
            "has been moved",
            "404",
            "site not found",
        ))
    ):
        return (
            True,
            "generic_hosting_or_registrar_placeholder",
            ["soft_404_page"],
        )

    # Confirmed live (2026-08-08): this used to run after the word_count<=2
    # return below, so a minimal-text parking page (many real ones are
    # just a couple of words) never even reached the parked check --
    # it returned "insufficient_visible_content" (not parked) instead of
    # being correctly flagged, and fell through to Needs Review.
    parked_hits = [term for term in PARKED if term in lower]
    parked_hits.extend(
        label
        for label, pattern in DOMAIN_SALE_PATTERNS
        if pattern.search(lower)
    )
    if parked_hits:
        return (
            True,
            "parked_or_for_sale",
            list(dict.fromkeys(parked_hits))[:8],
        )

    if word_count <= 2:
        return False, "insufficient_visible_content", ["under_3_words"]

    template_hits = [
        phrase for phrase in HOSTING_TEMPLATE_PHRASES
        if phrase in lower
    ]
    provider_hits = [
        term for term in PLACEHOLDER
        if term in lower
    ]

    squarespace_like = (
        "under construction" in lower
        and "check back for an update soon" in lower
    )
    if squarespace_like:
        template_hits.append("generic_squarespace_under_construction")

    if template_hits or provider_hits:
        evidence.extend(template_hits)
        evidence.extend(provider_hits)
        return (
            True,
            "generic_hosting_or_registrar_placeholder",
            list(dict.fromkeys(evidence))[:10],
        )

    return False, "", []


def prelaunch_assessment(
    *,
    text: str,
    pages_scraped: int,
    meaningful_internal_links: int,
    word_count: int,
    pillar_count: int,
    ai_count: int,
    technical_count: int,
    buyer_count: int,
) -> tuple[bool, int, list[str]]:
    """Recognize only genuine founder-controlled prelaunch pages.

    A generic "Coming Soon" or hosting-provider template is not enough.
    """
    cleaned = clean(text)
    lower = cleaned.lower()
    evidence: list[str] = []

    is_placeholder, placeholder_reason, placeholder_evidence = (
        placeholder_assessment(cleaned, word_count=word_count)
    )
    if is_placeholder:
        return False, 0, [
            placeholder_reason,
            *placeholder_evidence,
        ]

    founder_hits = [
        phrase for phrase in FOUNDER_PRELAUNCH_PHRASES
        if phrase in lower
    ]
    action_hits = [
        phrase for phrase in PRELAUNCH_ACTION_SIGNALS
        if phrase in lower
    ]
    generic_hits = [
        phrase for phrase in GENERIC_PRELAUNCH_ONLY
        if phrase in lower
    ]

    # Original company-controlled copy beyond launch boilerplate. This can be
    # a product hint even when the startup is too early to use thesis keywords.
    stripped_original = lower
    for phrase in (
        FOUNDER_PRELAUNCH_PHRASES
        + PRELAUNCH_ACTION_SIGNALS
        + GENERIC_PRELAUNCH_ONLY
    ):
        stripped_original = stripped_original.replace(phrase, " ")
    original_words = [
        word for word in re.findall(r"[a-zA-Z][a-zA-Z0-9-]+", stripped_original)
        if word not in {
            "the", "a", "an", "and", "or", "for", "to", "of", "in",
            "on", "our", "your", "we", "us", "soon", "website",
        }
        and not word.endswith(".ai")
    ]
    has_original_copy = len(original_words) >= 3

    original_signal_count = (
        pillar_count
        + ai_count
        + technical_count
        + buyer_count
    )

    if pages_scraped <= 1:
        evidence.append("single_page_site")
    if meaningful_internal_links <= 2:
        evidence.append("minimal_navigation")
    if 4 <= word_count < 50:
        evidence.append("under_50_words")
    evidence.extend(founder_hits)
    evidence.extend(action_hits)

    # Valid paths:
    # 1) Founder-controlled language plus an action or product/market hint.
    # 2) Generic "coming soon" plus BOTH an action and original product evidence.
    founder_controlled = bool(founder_hits) and (
        bool(action_hits)
        or original_signal_count >= 1
        or has_original_copy
    )
    generic_but_original = (
        bool(generic_hits)
        and bool(action_hits)
        and original_signal_count >= 1
    )

    is_prelaunch = (
        4 <= word_count < 50
        and pages_scraped <= 1
        and meaningful_internal_links <= 2
        and (founder_controlled or generic_but_original)
    )

    structure_score = (
        (2 if pages_scraped <= 1 else 0)
        + (2 if meaningful_internal_links <= 2 else 0)
        + (2 if 4 <= word_count < 50 else 0)
        + (3 if founder_controlled else 0)
        + (2 if generic_but_original else 0)
    )

    if generic_hits and not is_prelaunch:
        evidence.append("generic_prelaunch_copy_without_original_evidence")

    return (
        is_prelaunch,
        structure_score,
        list(dict.fromkeys(evidence)),
    )


def language_assessment(text: str) -> tuple[bool, list[str]]:
    """Reject pages whose meaningful content is not primarily English.

    This deliberately favors precision for a U.S.-focused sourcing workflow.
    Chinese, Japanese, Korean, Cyrillic, Arabic, Hebrew, Thai, and Devanagari
    scripts are immediately rejected when they form meaningful page content.
    """
    cleaned = clean(text)
    if not cleaned:
        return False, []

    evidence: list[str] = []

    script_patterns = {
        "Chinese": r"[\u3400-\u4DBF\u4E00-\u9FFF]",
        "Japanese": r"[\u3040-\u30FF]",
        "Korean": r"[\uAC00-\uD7AF]",
        "Cyrillic": r"[\u0400-\u04FF]",
        "Arabic": r"[\u0600-\u06FF]",
        "Hebrew": r"[\u0590-\u05FF]",
        "Thai": r"[\u0E00-\u0E7F]",
        "Devanagari": r"[\u0900-\u097F]",
        "Greek": r"[\u0370-\u03FF]",
    }
    letters = [char for char in cleaned if char.isalpha()]
    total_letters = max(len(letters), 1)

    for language, pattern in script_patterns.items():
        count = len(re.findall(pattern, cleaned))
        ratio = count / total_letters
        # A handful of foreign characters may come from a footer, embedded
        # widget, customer name, or localization menu. Require both meaningful
        # volume and concentration before hard-denying an otherwise English page.
        if (count >= 24 and ratio >= 0.12) or ratio >= 0.25:
            evidence.append(f"detected_language={language}")
            evidence.append(f"foreign_script_ratio={ratio:.2f}")
            return True, evidence

    words = re.findall(r"[A-Za-zÀ-ÿ']+", cleaned.lower())
    if len(words) < 8:
        return False, []

    english_hits = sum(word in ENGLISH_COMMON_WORDS for word in words)
    foreign_hits = sum(word in NON_ENGLISH_COMMON_WORDS for word in words)

    expanded_foreign_markers = {
        # Spanish / Portuguese
        "el", "la", "los", "las", "una", "uno", "del", "como", "más",
        "somos", "servicios", "solución", "soluciones", "tecnología",
        "negocio", "empresas", "clientes", "dados", "soluções", "você",
        "para", "com", "uma", "não", "mais", "gestão",
        # French
        "le", "les", "des", "une", "est", "sont", "dans", "sur",
        "entreprise", "entreprises", "données", "sécurité", "avec",
        # German / Dutch
        "der", "die", "das", "und", "ist", "für", "mit", "wir",
        "unternehmen", "lösung", "daten", "veiligheid", "voor", "met",
        # Italian
        "gli", "della", "delle", "con", "per", "azienda", "aziende",
        "soluzione", "sicurezza", "dati",
        # Turkish / Indonesian
        "ve", "bir", "için", "ile", "şirket", "çözüm",
        "dan", "yang", "untuk", "dengan", "perusahaan", "kami",
    }
    expanded_hits = sum(word in expanded_foreign_markers for word in words)
    foreign_hits += expanded_hits

    english_ratio = english_hits / len(words)
    foreign_ratio = foreign_hits / len(words)

    if foreign_hits >= 10 and foreign_hits > english_hits * 2.0:
        evidence.append("detected_language=non_English_Latin")
        evidence.append(f"foreign_marker_ratio={foreign_ratio:.2f}")
        return True, evidence

    if len(words) >= 40 and english_hits <= 2 and foreign_hits >= 8:
        evidence.append("detected_language=non_English_Latin")
        evidence.append(f"english_marker_ratio={english_ratio:.2f}")
        return True, evidence

    # Do not hard-deny solely because a long page contains few English
    # function words. Product copy, code, and technical terms can produce that
    # pattern. Explicit foreign-language markers above are still enforced.

    return False, evidence


def commodity_wrapper_assessment(text: str) -> tuple[bool, list[str]]:
    lower = clean(text).lower()
    leading_copy = lower[:900]
    hits = occurrences(lower, COMMODITY_WRAPPER)

    strong = [
        "ai receptionist",
        "virtual receptionist",
        "ai answering service",
        "answers every call",
        "answer every call",
        "never miss a call",
        "missed calls",
        "books appointments",
        "book appointments",
        "appointment booking",
        "appointment scheduling",
        "white-label voice agent",
    ]
    vertical = [
        "salon",
        "dentist",
        "dental",
        "home services",
        "plumber",
        "hvac",
        "real estate agent",
        "funeral home",
        "restaurant",
    ]
    platform_signals = [
        "agent orchestration",
        "infrastructure",
        "developer platform",
        "api-first",
        "governance",
        "enterprise platform",
        "multi-agent",
    ]

    # Generic agent wrappers are a separate failure mode from the obvious
    # receptionist/content products above.  A homepage that promises a
    # catch-all agent or an "AI employee" without a defensible platform,
    # infrastructure, security, or technical system is not Glasswing-fit.
    generic_agent_signals = [
        "ai employee", "digital employee", "autonomous employee",
        "general purpose agent", "general-purpose agent", "any task",
        "automate any", "build your own agent", "custom agent builder",
        "sales agent", "marketing agent", "customer support agent",
        "ai assistant for everyone", "all-in-one ai assistant",
        "your ai workforce", "ai workforce",
        "enterprise ai agent platform", "enterprise agent platform",
        "agentic ai platform", "multi-agent platform",
        "build test and run ai agents", "build, test, and run ai agents",
        "deploy autonomous ai agents", "custom llm solutions",
        "custom ai solutions", "ai integration services",
        "executive ai mentor", "ai mentor", "ai executive coach",
        "ai leadership coach", "ai business coach", "ai career coach",
    ]

    strong_hits = [term for term in strong if term in lower]
    vertical_hits = [term for term in vertical if term in lower]
    platform_hits = [term for term in platform_signals if term in lower]
    generic_agent_hits = [
        term for term in generic_agent_signals if term in lower
    ]
    generic_agent_platform = bool(
        generic_agent_hits
        and any(term in generic_agent_hits for term in (
            "enterprise ai agent platform", "enterprise agent platform",
            "agentic ai platform", "multi-agent platform",
            "build test and run ai agents", "build, test, and run ai agents",
            "deploy autonomous ai agents", "custom llm solutions",
            "custom ai solutions", "ai integration services",
            "executive ai mentor", "ai mentor", "ai executive coach",
            "ai leadership coach", "ai business coach", "ai career coach",
        ))
        and not any(term in lower for term in (
            "manufacturing", "factory", "industrial", "robotics",
            "satellite imagery", "agriculture", "crop", "semiconductor",
            "pcb", "clinical", "drug discovery", "insurance claims",
            "food cost", "inventory management",
        ))
    )
    # Cybersecurity is an exceptionally crowded category. Broad category
    # claims are not a venture signal; retain a security company only when the
    # page names a concrete mechanism, a narrow operational workflow, or a
    # defensible technical asset. This deliberately does not punish a single
    # incidental use of "security" elsewhere on a company's site.
    generic_cyber_signals = [
        "ai cybersecurity platform", "ai cyber security platform",
        "ai-powered cybersecurity", "ai powered cybersecurity",
        "ai-powered security platform", "cybersecurity platform",
        "cyber security platform", "next-generation cybersecurity",
        "next generation cybersecurity", "advanced threat protection",
        "protect against evolving threats", "protects against evolving threats",
        "comprehensive security solution", "end-to-end cybersecurity",
        "ai governance platform", "agent governance platform",
        "ai agent governance", "non-human identity governance",
        "ai compliance platform", "ai-native grc", "ai native grc",
        "compliance automation", "compliance-as-code",
        "compliance as code", "automated compliance",
        "ai red teaming", "automated red teaming",
        "autonomous penetration testing", "generative penetration testing",
        "ai pentest", "ai pentesting", "pentest appliance",
        "xdr", "siem", "soar", "soc analyst",
        "security awareness training", "phishing defense program",
        "prompt management", "ai dlp",
    ]
    concrete_cyber_signals = [
        "prompt injection", "runtime verification", "runtime control",
        "protocol-aware", "dynamic masking", "zero-knowledge proof",
        "zero-knowledge proofs", "mpc co-signing", "sandboxed execution",
        "egress control", "identity enforcement", "attack corpus",
        "malware reverse engineering", "memory forensics",
        "software supply chain", "sbom", "secrets detection",
        "cloud misconfiguration", "lateral movement", "data exfiltration",
        "autonomous remediation", "incident response", "threat hunting",
        "vulnerability prioritization", "policy engine", "security protocol",
        "programmable guardrails", "model firewall", "secure access",
    ]
    cyber_workflow_signals = [
        "security operations center", "security operations", "soc analyst",
        "identity and access", "application security", "cloud security",
        "endpoint security", "email security", "data security",
        "detection engineering", "incident response workflow",
        "vulnerability management", "security engineering",
    ]
    generic_cyber_hits = [
        term for term in generic_cyber_signals if term in leading_copy
    ]
    concrete_cyber_hits = [
        term for term in concrete_cyber_signals if term in lower
    ]
    cyber_workflow_hits = [
        term for term in cyber_workflow_signals if term in lower
    ]
    generic_cyber_slop = bool(
        generic_cyber_hits
        and (
            # These are explicit user-rejected crowded categories even when
            # the site supplies technical detail. They are not current
            # sourcing targets.
            any(term in generic_cyber_hits for term in (
                "ai governance platform", "agent governance platform",
                "ai agent governance", "non-human identity governance",
                "ai compliance platform", "ai-native grc", "ai native grc",
                "compliance automation", "compliance-as-code",
                "compliance as code", "automated compliance",
                "ai red teaming", "automated red teaming",
                "autonomous penetration testing",
                "generative penetration testing", "ai pentest",
                "ai pentesting", "pentest appliance", "xdr", "siem",
                "soar", "soc analyst", "security awareness training",
                "phishing defense program", "prompt management", "ai dlp",
            ))
            or (not concrete_cyber_hits and not cyber_workflow_hits)
        )
    )
    explicit_low_value_hits = [
        term for term in (
            "ai-powered cold email",
            "cold email",
            "sports betting picks",
            "ai sports betting",
            "sports betting",
            "online casino",
            "casino",
            "gambling",
            "sportsbook",
            "linkedin lead gen",
            "automated b2b lead gen",
            "ai ad agency",
        )
        if term in leading_copy
    ]

    unmistakable = (
        any(term in lower for term in (
            "ai receptionist",
            "virtual receptionist",
            "ai answering service",
        ))
        and len(strong_hits) >= 2
    )
    is_commodity = (
        bool(explicit_low_value_hits)
        or generic_cyber_slop
        or generic_agent_platform
        or unmistakable
        or (
            len(strong_hits) >= 2
            and len(vertical_hits) >= 1
            and not platform_hits
        )
        or (
            len(strong_hits) >= 3
            and not platform_hits
        )
        or (
            len(generic_agent_hits) >= 1
            and len(strong_hits) + len(generic_agent_hits) >= 2
            and not platform_hits
        )
    )
    evidence = list(
        dict.fromkeys(
            strong_hits + generic_agent_hits + vertical_hits + hits
            + explicit_low_value_hits + generic_cyber_hits
            + concrete_cyber_hits + cyber_workflow_hits
        )
    )[:10]
    return is_commodity, evidence


def hosting_provider_assessment(text: str) -> tuple[bool, list[str]]:
    """Detect conventional web-hosting and domain-registration businesses."""
    leading = clean(text).lower()[:1500]
    hits = occurrences(leading, HOSTING_PROVIDER_SIGNALS)
    identity = any(term in leading for term in (
        "web hosting company",
        "web hosting & domains",
        "web hosting and domains",
        "domain registrar",
        "hosting provider",
    ))
    detected = bool(identity or len(set(hits)) >= 3)
    return detected, list(dict.fromkeys(hits))[:10]


def non_operating_investor_assessment(
    text: str,
) -> tuple[bool, list[str]]:
    """Exclude investors and venture studios from the company pipeline."""
    leading = clean(text).lower()[:1800]
    hits = occurrences(leading, NON_OPERATING_INVESTOR_SIGNALS)
    detected = bool(
        len(set(hits)) >= 2
        or any(term in leading for term in (
            "venture capital firm",
            "venture studio",
            "startup studio",
        ))
    )
    return detected, list(dict.fromkeys(hits))[:10]


def maturity_score(text: str) -> tuple[int, list[str]]:
    lower = text.lower()
    evidence = [term for term in MATURE_COMPANY_SIGNALS if term in lower]
    context = [term for term in MATURE_CONTEXT_SIGNALS if term in lower]

    employee_match = re.search(
        r"\b(?:over|more than|approximately|about)?\s*([1-9][0-9,]{2,})\+?\s+employees\b",
        lower,
    )
    if employee_match:
        try:
            employees = int(employee_match.group(1).replace(",", ""))
            if employees >= 500:
                evidence.append(f"{employees} employees")
        except ValueError:
            pass

    score = len(evidence) * 25 + len(context) * 8
    return min(score, 100), list(dict.fromkeys(evidence + context))



def investment_profile_label(
    text: str,
    maturity_risk: int,
) -> tuple[str, str, float | None]:
    """Label a thesis-relevant company as a candidate or useful comp."""
    lower = text.lower()
    # Company identity and financing facts should appear in the title,
    # description, or opening company copy. Blog/customer/news mentions later
    # on a page are not evidence that the screened company itself is mature.
    leading_copy = lower[:2500]
    reasons: list[str] = []
    funding_millions: float | None = None

    if any(signal in leading_copy for signal in (
        "acquired by", "a subsidiary of", "subsidiary of", "merged with",
    )):
        reasons.append("Acquired / subsidiary")

    if any(signal in leading_copy for signal in (
        "nasdaq", "nyse", "publicly traded", "investor relations",
    )):
        reasons.append("Public company")

    series_round = re.search(
        r"\b(?:raised|raising|funding|financing|round|led by|backed by)"
        r"\b.{0,100}\bseries\s+[a-f]\b"
        r"|\bseries\s+[a-f]\b.{0,100}"
        r"\b(?:raised|raising|funding|financing|round|led by|backed by)\b",
        leading_copy,
        flags=re.I | re.S,
    )
    if series_round or "growth round" in leading_copy:
        reasons.append("Series A+")

    funding_patterns = [
        r"(?:raised|funding|financing|secured)[^$]{0,45}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(billion|bn|million|m)\b",
        r"\$\s*([0-9]+(?:\.[0-9]+)?)\s*(billion|bn|million|m)\b[^.]{0,45}(?:raised|funding|financing|round)",
    ]
    amounts: list[float] = []
    for pattern in funding_patterns:
        for amount, unit in re.findall(pattern, leading_copy, flags=re.I):
            value = float(amount)
            if unit.lower() in {"billion", "bn"}:
                value *= 1000
            amounts.append(value)

    if amounts:
        funding_millions = max(amounts)
        if funding_millions > 8:
            reasons.append(f"Overfunded (${funding_millions:g}M disclosed)")

    scale_patterns = [
        (r"\b([1-9][0-9,]{2,})\+?\s+(?:customers|clients|sites|locations|deployments)\b", 500),
        (r"\b(?:across|in)\s+([2-9][0-9]|[1-9][0-9]{2,})\+?\s+countries\b", 20),
        (r"\b([1-9][0-9,]{2,})\+?\s+employees\b", 200),
    ]
    for pattern, threshold in scale_patterns:
        match = re.search(pattern, lower)
        if match:
            try:
                value = int(match.group(1).replace(",", ""))
            except ValueError:
                continue
            if value >= threshold:
                reasons.append("Too mature / scaled")
                break

    years_on_market = re.search(
        r"\b([1-9][0-9]?)\+?\s+years?\s+on\s+(?:the\s+)?market\b",
        leading_copy,
    )
    if years_on_market and int(years_on_market.group(1)) >= 7:
        reasons.append("Too mature / established")

    if maturity_risk >= 50 and not reasons:
        reasons.append("Too mature / established")

    reasons = list(dict.fromkeys(reasons))
    if reasons:
        return "Potential Comp", " · ".join(reasons), funding_millions

    return "Investable Candidate", "Needs Claude diligence", funding_millions

def location_assessment(text: str) -> tuple[str, list[str]]:
    lower = text.lower()
    us_evidence: list[str] = []
    foreign_evidence: list[str] = []

    for pattern in US_PATTERNS:
        if re.search(pattern, lower):
            us_evidence.append(pattern.strip(r"\b").replace("\\", ""))

    for state in US_STATES:
        if state in lower:
            us_evidence.append(state)

    for city in US_CITIES:
        if city in lower:
            us_evidence.append(city)

    # Common state abbreviation in an address: Boston, MA 02116
    if re.search(r"\b[A-Z][a-z .'-]+,\s*(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\s+\d{5}\b", text):
        us_evidence.append("US city/state/ZIP address")

    for country in FOREIGN_COUNTRIES:
        if country in lower:
            foreign_evidence.append(country)

    us_evidence = list(dict.fromkeys(us_evidence))
    foreign_evidence = list(dict.fromkeys(foreign_evidence))

    if foreign_evidence and not us_evidence:
        return "NON_US", foreign_evidence
    if us_evidence:
        return "US", us_evidence
    return "UNKNOWN", []


def parse_html(html: str, final_url: str) -> tuple[Fetch, list[str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    base_host = urlparse(final_url).netloc.lower().removeprefix("www.")
    for anchor in soup.find_all("a", href=True):
        label = clean(anchor.get_text(" ", strip=True)).lower()
        href = anchor.get("href", "")
        absolute = urljoin(final_url, href)
        parsed = urlparse(absolute)
        host = parsed.netloc.lower().removeprefix("www.")
        if (
            parsed.scheme in {"http", "https"}
            and host == base_host
            and parsed.path not in {"", "/"}
            and not parsed.path.lower().endswith((".pdf", ".jpg", ".jpeg", ".png", ".svg"))
        ):
            if any(term in label or term in href.lower() for term in RELEVANT_LINK_TERMS):
                links.append(absolute)

    for tag in soup(["script", "style", "noscript", "svg", "template"]):
        tag.decompose()

    title = clean(soup.title.get_text(" ", strip=True) if soup.title else "")
    meta = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    description = clean(meta.get("content", "")) if meta else ""
    text = clean(soup.get_text(" ", strip=True))[:45000]

    return (
        Fetch(
            url=final_url,
            title=title,
            description=description,
            text=text,
            pages_scraped=1,
            meaningful_internal_links=len(set(links)),
        ),
        list(dict.fromkeys(links)),
    )


_FETCH_MAX_RESPONSE_BYTES = max(
    100_000,
    min(
        2_000_000,
        int(os.getenv("GLASSWING_MAX_HTML_BYTES", "500000")),
    ),
)


async def _read_html_capped(
    response: aiohttp.ClientResponse,
    maximum_bytes: int,
) -> str:
    """Read at most maximum_bytes of a response body.

    response.text() with no cap (the previous implementation) downloads and
    decodes the entire body regardless of size, then hands all of it to
    BeautifulSoup -- a synchronous, CPU-bound parse -- before the 45,000-char
    truncation downstream ever applies. Confirmed live: a batch's outer
    deadline fired 40s late (220.5s against a 180s ceiling), which
    asyncio.wait_for can only do if the event loop itself was blocked by
    synchronous work for that long. A single pathologically large or
    slow-trickling page could stall BeautifulSoup's parse for many seconds,
    blocking every other concurrent domain in the same batch along with it.
    """
    chunks: list[bytes] = []
    size = 0
    async for chunk in response.content.iter_chunked(32_768):
        remaining = maximum_bytes - size
        if remaining <= 0:
            break
        selected = chunk[:remaining]
        chunks.append(selected)
        size += len(selected)
        if len(selected) < len(chunk) or size >= maximum_bytes:
            break
    return b"".join(chunks).decode(
        response.charset or "utf-8",
        errors="replace",
    )


async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float,
) -> tuple[Fetch, list[str]]:
    try:
        async with session.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            max_redirects=5,
        ) as response:
            content_type = response.headers.get("content-type", "")
            if "html" not in content_type.lower():
                return Fetch(
                    url=str(response.url),
                    status=response.status,
                    error="non_html",
                ), []

            html = await _read_html_capped(response, _FETCH_MAX_RESPONSE_BYTES)
            # BeautifulSoup parsing is synchronous CPU work. Keeping it on
            # the event loop made one ugly/large page stall every concurrent
            # network request in the batch, collapsing CertStream throughput.
            result, links = await asyncio.get_running_loop().run_in_executor(
                _SCORING_EXECUTOR,
                parse_html,
                html,
                str(response.url),
            )
            result.status = response.status
            return result, links
    except Exception as exc:
        return Fetch(error=f"{type(exc).__name__}: {exc}"), []


async def _fetch_candidate_group(
    session: aiohttp.ClientSession,
    urls: list[str],
    timeout: float,
) -> tuple[Fetch | None, list[str], Fetch | None, list[str]]:
    """Race apex and www and stop as soon as either yields a real page."""
    tasks = [asyncio.create_task(fetch_html(session, url, timeout)) for url in urls]
    completed: list[tuple[Fetch, list[str]]] = []
    try:
        for future in asyncio.as_completed(tasks):
            result, links = await future
            completed.append((result, links))
            if result.text:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                return result, links, result, links
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if not completed:
        return None, [], None, []
    best_result, best_links = min(
        completed,
        key=lambda item: (
            item[0].status is None,
            int(item[0].status or 999),
        ),
    )
    return None, [], best_result, best_links


async def fetch(
    session: aiohttp.ClientSession,
    domain: str,
    timeout: float,
    homepage_only: bool = False,
) -> Fetch:
    https_urls = [
        f"https://{domain}",
        f"https://www.{domain}",
    ]
    http_urls = [
        f"http://{domain}",
        f"http://www.{domain}",
    ]

    homepage, links, best_result, best_links = (
        await _fetch_candidate_group(
            session,
            https_urls,
            timeout,
        )
    )

    if homepage is None:
        # If both HTTPS variants consumed the full timeout, repeating the same
        # wait over plain HTTP rarely recovers a modern site and doubles batch
        # latency. Immediate DNS/connect/HTTP failures still get HTTP fallback.
        https_timed_out = (
            best_result is not None
            and "TimeoutError" in str(best_result.error or "")
            and best_result.status is None
        )
        if https_timed_out:
            return best_result
        http_page, http_links, http_best, http_best_links = (
            await _fetch_candidate_group(
                session,
                http_urls,
                timeout,
            )
        )
        if http_page is not None:
            homepage = http_page
            links = http_links
        else:
            candidates = [
                (best_result, best_links),
                (http_best, http_best_links),
            ]
            candidates = [item for item in candidates if item[0] is not None]
            if candidates:
                homepage, links = min(
                    candidates,
                    key=lambda item: int(item[0].status or 999),
                )
            else:
                homepage = Fetch(
                    url=f"https://{domain}",
                    error="homepage_fetch_failed",
                )
                links = []

    if not homepage.url:
        homepage.url = f"https://{domain}"

    if not homepage.text or homepage_only:
        return homepage

    base_host = urlparse(homepage.url).netloc.lower().removeprefix("www.")
    selected: list[str] = []

    def evidence_link_rank(link: str) -> tuple[int, int]:
        path = urlparse(link).path.lower().strip("/")
        if any(term in path for term in ("team", "founder", "leadership")):
            return (0, len(path))
        if any(term in path for term in ("about", "company")):
            return (1, len(path))
        if any(term in path for term in ("product", "technology", "platform")):
            return (2, len(path))
        return (3, len(path))

    for link in sorted(links, key=evidence_link_rank):
        parsed = urlparse(link)
        link_host = parsed.netloc.lower().removeprefix("www.")
        if parsed.scheme not in {"http", "https"}:
            continue
        if link_host != base_host:
            continue
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if normalized not in selected and normalized != homepage.url:
            selected.append(normalized)
        if len(selected) >= 2:
            break

    if selected:
        extra_results = await asyncio.gather(
            *[fetch_html(session, link, timeout) for link in selected]
        )
        additional_text = [
            page.text
            for page, _ in extra_results
            if page.text
        ]
        if additional_text:
            homepage.text = clean(
                homepage.text + " " + " ".join(additional_text)
            )[:60000]
            homepage.pages_scraped += len(additional_text)

    return homepage


def route_company(
    *,
    is_parked: bool,
    is_coming: bool,
    is_agency: bool,
    is_consumer_junk: bool,
    is_media_newsletter: bool = False,
    is_non_english: bool = False,
    is_commodity_wrapper: bool = False,
    is_hosting_provider: bool = False,
    is_non_operating_investor: bool = False,
    is_placeholder: bool = False,
    page_error: str,
    page_status: int | None,
    location_status: str,
    maturity_risk: int,
    qualified_for_llm: bool,
    score: int,
    potential_glasswing_fit: bool = True,
    high_conviction_fit: bool = False,
    strong_multidimensional_fit: bool = False,
    venture_reviewable: bool = False,
    substantive_no_fit_evidence: bool = False,
) -> tuple[str, str]:
    """Route by viability: exactly two outcomes, Hard Denied or Needs Review.

    Hard Denied requires a specific disqualifying fact. Low score, sparse copy,
    unknown location, or missing technical evidence never cause denial alone.

    Confirmed live (2026-08-08): a Python rule-based scorer cannot be
    trusted to auto-declare "Investable" or auto-dismiss "Insufficient
    Evidence" on its own -- real companies scored 1-5/100 on the same
    keyword engine that also let boilerplate through. Investable and
    Coming Soon are no longer routing outcomes this function can produce;
    they are labels Claude or a human applies on top of a Needs Review
    company (see visible_classification_frame in dashboard.py, and the
    manual-override / Claude-decision paths). Every company that isn't a
    specific, factual disqualification goes to Needs Review -- there is no
    more low-attention "insufficient evidence" limbo bucket that a
    thesis-fit signal quietly falls into and is never looked at again.
    """
    # High-confidence website validity failures.
    if is_parked:
        return "reject", "parked_or_for_sale"
    if is_placeholder:
        return "reject", "generic_hosting_or_registrar_placeholder"

    # An inaccessible site is not investment evidence. Route it to "ghost",
    # which is hidden and purged -- NOT "review", which is literally the
    # Needs Review bucket and is protected from purging. Confirmed live: an
    # error page that renders text (a Cloudflare "525 SSL handshake failed"
    # interstitial, an "nginx 404 Not Found") is not blank, so it never fell
    # through to the blank-page ghost check either, and sat permanently in
    # Needs Review scoring 1-5.
    if page_error:
        return "ghost", "inaccessible_site"
    if page_status and int(page_status) >= 400:
        return "ghost", f"inaccessible_http_{page_status}"

    # Explicit scope exclusions.
    if is_non_english:
        return "reject", "predominantly_non_english"
    if location_status == "NON_US":
        return "reject", "confirmed_non_us"
    if is_consumer_junk:
        return "reject", "consumer_b2c_product"
    if is_agency:
        return "reject", "agency_or_consultancy"
    if is_media_newsletter:
        return "reject", "media_newsletter_or_aggregator"
    if is_hosting_provider:
        return "reject", "hosting_or_domain_registration_business"
    if is_non_operating_investor:
        return "reject", "investor_or_venture_studio"
    if is_commodity_wrapper:
        return "reject", "commodity_ai_wrapper_or_service"
    # A legitimate founder-controlled minimal site is preserved separately.
    if is_coming:
        return "review", "verified_prelaunch_watch"

    # These narrow, multi-phrase product bundles come from trusted human Yes
    # examples. Explicit exclusions above still win, but once a clean site
    # matches one of them the router should not discard the signal.
    if high_conviction_fit:
        return "review", "trusted_high_conviction_product_fit"

    # A substantive site with no thesis keyword still gets one chance on score.
    # This rule sits ahead of every score-based review path, so a real company
    # scoring in the 40s was discarded before it could reach them. Measured over
    # 3 hours of live traffic: 86.5% of discards scored 0-9 (the feed genuinely
    # is that low-signal, so the rule is right in bulk), but the top of the
    # discard pile was xfactor.io at 50, trialetics.io 48, nexorum.io 47,
    # detaillabs.ai 45 -- all deleted permanently. Roughly 100/day clear this
    # floor, which is real deal flow rather than a flood.
    if substantive_no_fit_evidence:
        if score >= SUBSTANTIVE_REVIEW_FLOOR:
            return "review", "substantive_site_scored_for_human_review"
        return "screened_out", "substantive_site_no_glasswing_fit_signals"

    # A credible product, wedge, and moat/technical edge -- still routed to
    # human/Claude review rather than auto-declared Investable.
    if strong_multidimensional_fit and score >= 45:
        return "review", "venture_quality_investable"
    if qualified_for_llm and score >= 55:
        return "review", "strong_investable_candidate"

    # Reserved for credible thesis-aligned products that show at least two
    # quality dimensions but are missing full conviction.
    if venture_reviewable or qualified_for_llm:
        return "review", "credible_venture_candidate_missing_one_dimension"
    if potential_glasswing_fit:
        return "review", "thesis_fit_missing_wedge_or_moat"

    # No factual disqualifier means this is not Hard Denied. But a page with
    # no venture/product evidence is not a review candidate either; discard
    # its content so the retained tables stay useful.
    return "screened_out", "insufficient_venture_quality_evidence"


def diligence_rank_score(
    *,
    rule_score: int,
    counts: dict[str, int],
    dimensions: dict[str, int],
    location_status: str,
    pillar: str,
    word_count: int,
) -> int:
    """Rank candidates without allowing keyword saturation."""
    base = int(rule_score)

    # Small bonuses only; the evidence-heavy rule score carries the ranking.
    bonus = 0
    if location_status == "US":
        bonus += 3
    if pillar and pillar != "unclear":
        bonus += 2
    if word_count >= 40:
        bonus += 1
    if word_count >= 120:
        bonus += 1

    # A company must have complete evidence to enter the 90s.
    complete_dimensions = sum([
        counts.get("pillar", 0) >= 2,
        counts.get("product", 0) >= 1,
        dimensions.get("buyer", 0) >= 1,
        dimensions.get("technical", 0) >= 2,
        dimensions.get("workflow", 0) >= 1,
        dimensions.get("business_value", 0) >= 1,
        counts.get("defensibility", 0) >= 1,
        counts.get("important_problem", 0) >= 1,
    ])

    score = base + bonus
    if complete_dimensions < 6:
        score = min(score, 84)
    elif complete_dimensions == 6:
        score = min(score, 89)
    elif complete_dimensions == 7:
        score = min(score, 93)
    else:
        score = min(score, 96)

    return max(0, int(score))


# HTTP statuses that mean "we could not read this site", never "this is not a
# company". 403 is the single most common failure in live CertStream traffic --
# that is a Cloudflare/WAF challenge aimed at our scraper, not a verdict on the
# business. 401/407 are auth walls, 408/429 are rate limiting, and every 5xx
# (including Cloudflare's own 520-530 origin errors) is a server-side fault on
# a site that is very often mid-deploy. Certificates are issued *before* a site
# goes live, so these are disproportionately the freshest startups in the feed.
# Treat them as unknown and retry with backoff instead of tombstoning them as
# permanently excluded.
RETRYABLE_FETCH_STATUSES = frozenset({401, 402, 403, 407, 408, 409, 425, 429})


def is_retryable_inaccessible_reason(reason: object) -> bool:
    """Whether a routing reason means "unreadable now", not "not a company"."""
    text = str(reason or "")
    prefix = "inaccessible_http_"
    if not text.startswith(prefix):
        return False
    try:
        status = int(text[len(prefix):])
    except ValueError:
        return False
    return status in RETRYABLE_FETCH_STATUSES or 500 <= status <= 599


# A substantive site scoring at or above this still reaches Needs Review even
# with no explicit thesis keyword match.
#
# Set to 10 from direct inspection, not intuition. Sampling live discards in the
# 12-24 band and re-reading their homepages found roughly 40% were real,
# thesis-relevant companies: kerno.io at 23 ("the self-healing loop for AI coding
# agents... independent runtime verification layer"), axfolio.io at 24
# ("portfolio intelligence for RIA firms"), pulseupdates.dev at 23 (signed
# self-hosted OTA infrastructure). The rest were personal portfolios and
# marketing consultancies, which have their own detectors.
#
# The underlying problem is that score_text cannot rank this traffic at all.
# Reproduced in isolation: kerno.io's real pitch scores 3, axfolio.io's scores 1
# with zero signals detected, and a lecturer's personal homepage also scores 3.
# The lexicon has no vocabulary for how modern AI companies write -- "runtime
# verification", "AI coding agents", "portfolio intelligence", "registered
# investment advisors" all contribute nothing -- while keyword-dense security
# copy clears 60 comfortably. No threshold can fix that; a floor can only stop
# the scorer deleting what it failed to understand.
#
# So the floor sits just above the band that is genuinely empty rather than
# merely unrecognised: discards scoring 0-9 average 1.3 and are real junk (84.5%
# of all discards, still dropped), while everything above that is a substantive,
# reachable, non-disqualified website whose only fault is vocabulary the lexicon
# does not know. Those now reach Needs Review, where a human or Claude can judge
# them. Raise GLASSWING_SUBSTANTIVE_REVIEW_FLOOR if the queue gets unmanageable.
SUBSTANTIVE_REVIEW_FLOOR = max(
    0, min(100, int(os.getenv("GLASSWING_SUBSTANTIVE_REVIEW_FLOOR", "10")))
)


HARD_DENIAL_REASONS = {
    "parked_or_for_sale",
    "generic_hosting_or_registrar_placeholder",
    "predominantly_non_english",
    "confirmed_non_us",
    "consumer_b2c_product",
    "agency_or_consultancy",
    "media_newsletter_or_aggregator",
    "commodity_ai_wrapper_or_service",
    "hosting_or_domain_registration_business",
    "investor_or_venture_studio",
    "substantive_site_no_glasswing_fit_signals",
}

def denial_audit_fields(
    *,
    bucket: str,
    reason: str,
    placeholder_evidence: list[str],
    language_evidence: list[str],
    b2c_evidence: list[str],
    media_evidence: list[str],
    commodity_evidence: list[str],
    location_evidence: list[str] | str,
    page_error: str,
    http_status: int | None,
    combined: str,
) -> tuple[str, str, str]:
    """Return confidence, exact evidence, and rule for dashboard auditing."""
    if bucket != "reject":
        return "", "", ""

    if reason.startswith("http_"):
        return "High", f"HTTP status {http_status}", "WEBSITE_HTTP_FAILURE"
    if reason == "website_unavailable":
        return "High", page_error or "Website could not be fetched", "WEBSITE_UNAVAILABLE"
    if reason in {"parked_or_for_sale", "generic_hosting_or_registrar_placeholder"}:
        evidence = placeholder_evidence
        rule = "PARKED_OR_TEMPLATE"
    elif reason == "predominantly_non_english":
        evidence = language_evidence
        rule = "NON_ENGLISH_CONTENT"
    elif reason == "confirmed_non_us":
        evidence = (
            [location_evidence]
            if isinstance(location_evidence, str)
            else location_evidence
        )
        rule = "CONFIRMED_NON_US"
    elif reason == "consumer_b2c_product":
        evidence = b2c_evidence
        rule = "CLEAR_CONSUMER_PRODUCT"
    elif reason == "agency_or_consultancy":
        evidence = ["agency_or_consultancy_phrase_detected"]
        rule = "CLEAR_SERVICES_BUSINESS"
    elif reason == "media_newsletter_or_aggregator":
        evidence = media_evidence
        rule = "MEDIA_OR_AGGREGATOR"
    elif reason == "commodity_ai_wrapper_or_service":
        evidence = commodity_evidence
        rule = "COMMODITY_AI_WRAPPER"
    elif reason == "hosting_or_domain_registration_business":
        evidence = commodity_evidence
        rule = "HOSTING_OR_DOMAIN_REGISTRAR"
    elif reason == "investor_or_venture_studio":
        evidence = commodity_evidence
        rule = "INVESTOR_OR_VENTURE_STUDIO"
    elif reason == "substantive_site_no_glasswing_fit_signals":
        evidence = [
            "Readable website with 80+ words",
            "did not meet the score-25 borderline Glasswing review gate",
            "no qualifying enterprise, technical, workflow, business-value, "
            "clear-company, or conservative rescue signal",
        ]
        rule = "SUBSTANTIVE_SITE_NO_GLASSWING_FIT"
    else:
        evidence = []
        rule = "EXPLICIT_DISQUALIFIER"

    excerpt = "; ".join(str(item) for item in evidence if str(item).strip())
    if not excerpt:
        excerpt = clean(combined)[:350]
    return "High", excerpt[:1000], rule

def viability_label(bucket: str) -> str:
    return {
        "priority": "Investable Candidate",
        "review": "Review Candidate",
        "comp": "Potential Comp",
        "coming_soon": "Verified Prelaunch",
        "reject": "Hard Denied",
        "ghost": "Ghost / Inaccessible",
    }.get(bucket, "Review Candidate")

async def process(
    row: dict,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    timeout: float,
    homepage_only: bool = False,
    cached_page: Fetch | None = None,
) -> dict:
    domain = stable_domain(row.get("registrable", row.get("domain", "")))
    cached_page = cached_page or row.get("_cached_page")

    if cached_page is not None:
        page = cached_page
    else:
        async with sem:
            page = await fetch(
                session,
                domain,
                timeout,
                homepage_only=homepage_only,
            )

    def _score_and_bucket_domain():
        # The live CertStream pass only needs enough homepage evidence to
        # route a company. Scoring 40 x 45k-character pages concurrently is
        # pure GIL contention and previously stopped the event loop itself.
        # Cheap rejection checks use only the highest-signal leading copy.
        # Final scoring still gets the complete substantive evidence: the
        # production Claude-labelled benchmark shows that cutting it off loses
        # real product differentiation.
        early_text = high_signal_scoring_text(
            page.title,
            page.description,
            page.text,
        )
        scoring_text = page.text[:15000] if homepage_only else page.text
        combined = clean(f"{page.title} {page.description} {scoring_text}")
        lower = combined.lower()
        _scoring_started = time.monotonic()
        scoring_copy = combined

        word_count = len(combined.split())
        (
            is_placeholder,
            placeholder_reason,
            placeholder_evidence,
        ) = placeholder_assessment(early_text, word_count=len(early_text.split()))
        is_parked = placeholder_reason == "parked_or_for_sale"
        is_agency, agency_evidence = agency_assessment(early_text)

        score, pillar, evidence, counts = score_text(scoring_copy)
        dimensions = glasswing_dimensions(scoring_copy)
        enterprise_rescue_signal, enterprise_rescue_evidence = (
            enterprise_rescue_assessment(combined)
        )
        high_conviction_fit, high_conviction_evidence = (
            high_conviction_fit_assessment(combined)
        )
        is_consumer_junk, b2c_score, b2c_evidence = b2c_assessment(early_text)
        is_media_newsletter, media_evidence = media_newsletter_assessment(early_text)
        is_non_english, language_evidence = language_assessment(early_text)
        is_commodity_wrapper, commodity_evidence = commodity_wrapper_assessment(early_text)
        is_hosting_provider, hosting_evidence = hosting_provider_assessment(
            early_text
        )
        (
            is_non_operating_investor,
            investor_evidence,
        ) = non_operating_investor_assessment(early_text)
        maturity_risk, maturity_evidence = maturity_score(combined)
        investment_label, investment_label_reason, disclosed_funding_m = (
            investment_profile_label(combined, maturity_risk)
        )
        location_status, location_evidence = company_location_assessment(combined)
        explicit_non_us, explicit_non_us_evidence = (
            explicit_non_us_identity_assessment(
                f"{page.title} {page.description}"
            )
        )
        if explicit_non_us and location_status != "US":
            location_status = "NON_US"
            location_evidence = list(dict.fromkeys(
                list(location_evidence) + explicit_non_us_evidence
            ))

        if pillar and not category_is_valid(pillar, scoring_copy):
            pillar = ""
            score = min(score, 55)

        score, score_eligibility_caps = investment_eligibility_adjusted_score(
            score,
            predominantly_non_english=is_non_english,
            location_status=location_status,
            parked=is_parked,
            placeholder=is_placeholder,
            agency=is_agency,
            consumer=is_consumer_junk,
            media=is_media_newsletter,
            commodity_ai_service=(
                is_commodity_wrapper
                or is_hosting_provider
                or is_non_operating_investor
            ),
            mature_or_funded=investment_label == "Potential Comp",
        )

        venture_quality = venture_quality_assessment(
            combined,
            pillar=pillar,
            counts=counts,
            dimensions=dimensions,
            high_conviction_fit=high_conviction_fit,
        )
        website_green_flags = deal_green_flags(page.text[:20000])
        scorecard = scorecard_from_evidence(
            counts=counts,
            dimensions=dimensions,
            venture_quality=venture_quality,
        )
        potential_glasswing_fit = bool(venture_quality["thesis_fit"])

        is_coming, prelaunch_score, prelaunch_evidence = prelaunch_assessment(
            text=combined,
            pages_scraped=page.pages_scraped,
            meaningful_internal_links=page.meaningful_internal_links,
            word_count=word_count,
            pillar_count=counts["pillar"],
            ai_count=counts["ai"],
            technical_count=dimensions["technical"],
            buyer_count=dimensions["buyer"],
        )

        # Thesis-first, not website-polish-first. Sparse early-stage sites can
        # qualify when the underlying idea is clearly relevant and non-commodity.
        # Generic "AI-powered" copy alone is never enough.
        deep_tech_pillars = {
            "physical_ai",
            "ai_adaptive_infrastructure",
            "intelligent_enterprise_defense",
            "next_gen_compute",
        }

        clear_enterprise_idea = (
            counts["pillar"] >= 1
            and counts["product"] >= 1
            and dimensions["buyer"] >= 1
            and (
                dimensions["technical"] >= 1
                or dimensions["workflow"] >= 1
                or dimensions["business_value"] >= 1
            )
        )

        sparse_deep_tech_idea = (
            pillar in deep_tech_pillars
            and counts["product"] >= 1
            and dimensions["technical"] >= 1
            and (
                counts["ai"] >= 1
                or counts["pillar"] >= 2
                or dimensions["business_value"] >= 1
            )
        )

        sparse_vertical_workflow_idea = (
            pillar in {"vertical_ai", "collaborative_intelligence"}
            and counts["product"] >= 1
            and dimensions["workflow"] >= 1
            and (
                dimensions["buyer"] >= 1
                or dimensions["business_value"] >= 1
            )
        )

        plausible_company = (
            word_count >= 12
            and bool(venture_quality["reviewable"])
            and not is_commodity_wrapper
            and not is_hosting_provider
            and not is_non_operating_investor
            and not is_non_english
        )
        independent_signal_dimensions = sum([
            dimensions["buyer"] >= 1,
            dimensions["technical"] >= 1,
            dimensions["workflow"] >= 1,
            dimensions["business_value"] >= 1,
            counts["defensibility"] >= 1,
            counts["important_problem"] >= 1,
        ])
        generic_strong_fit = bool(
            score >= 70
            and counts["pillar"] >= 2
            and counts["product"] >= 1
            and dimensions["buyer"] >= 1
            and independent_signal_dimensions >= 4
            and (
                counts["defensibility"] >= 1
                or counts["important_problem"] >= 1
            )
        )

        rank_score = diligence_rank_score(
            rule_score=score,
            counts=counts,
            dimensions=dimensions,
            location_status=location_status,
            pillar=pillar,
            word_count=word_count,
        )
        rank_score = round(
            rank_score * 0.60
            + int(venture_quality["overall_score"]) * 0.40
        )
        rank_score, _ = investment_eligibility_adjusted_score(
            rank_score,
            predominantly_non_english=is_non_english,
            location_status=location_status,
            parked=is_parked,
            placeholder=is_placeholder,
            agency=is_agency,
            consumer=is_consumer_junk,
            media=is_media_newsletter,
            commodity_ai_service=(
                is_commodity_wrapper
                or is_hosting_provider
                or is_non_operating_investor
            ),
            mature_or_funded=investment_label == "Potential Comp",
        )

        # Investable is now a venture-quality judgment, not a keyword threshold.
        qualified_for_llm = (
            word_count >= 12
            and location_status in {"US", "UNKNOWN"}
            and investment_label == "Investable Candidate"
            and not is_consumer_junk
            and not is_media_newsletter
            and not is_agency
            and not is_non_english
            and not is_commodity_wrapper
            and not is_hosting_provider
            and bool(venture_quality["investable_ready"])
            and not is_coming
        )

        calibration_text = "\n".join([
            f"Company: {clean(page.title)[:100]}",
            f"Website description: {clean(page.description)[:300]}",
            f"Website evidence: {clean(page.text)[:800]}",
            f"Glasswing category evidence: {clean('; '.join(evidence))[:220]}",
            f"Location: {location_status}; "
            f"{clean('; '.join(location_evidence))[:100]}",
            f"Maturity: {clean('; '.join(maturity_evidence))[:120]}",
        ])[:1800]
        preference = calibrate_text(calibration_text)
        preference_yes_probability = (
            preference.probability_yes if preference is not None else None
        )
        preference_confidence = (
            preference.confidence if preference is not None else None
        )
        preference_evidence = (
            "; ".join(preference.evidence) if preference is not None else ""
        )
        preference_model_ready = bool(
            preference is not None and preference.training_ready
        )
        semantic_score = (
            round(float(preference_yes_probability) * 100)
            if preference_yes_probability is not None
            else None
        )
        # Human-trained semantics can rank viable companies only after its
        # validation gate passes. It never creates a hard denial.
        if preference_model_ready and semantic_score is not None:
            rank_score = round(rank_score * 0.45 + semantic_score * 0.55)

        # The learned layer may promote a strongly matching, otherwise plausible
        # enterprise company. It may not invent a Hard Denial: the current
        # holdout set supports positive recognition, but is not yet large enough
        # to prove that an inferred negative is safe.
        learned_positive_fit = bool(
            preference_yes_probability is not None
            and preference_model_ready
            and preference_yes_probability >= 0.90
            and word_count >= 20
            and location_status in {"US", "UNKNOWN"}
            and investment_label == "Investable Candidate"
            and bool(venture_quality["reviewable"])
            and not any([
                is_placeholder,
                is_parked,
                is_consumer_junk,
                is_media_newsletter,
                is_agency,
                is_non_english,
                is_commodity_wrapper,
                is_hosting_provider,
                is_non_operating_investor,
                is_coming,
            ])
        )
        if learned_positive_fit:
            # Training data can reorder an already credible candidate, but it
            # cannot manufacture a wedge or moat that the website never showed.
            rank_score = min(100, rank_score + 3)

        enterprise_rescue_signal = bool(
            enterprise_rescue_signal
            and not is_placeholder
            and not is_parked
            and not is_consumer_junk
            and not is_media_newsletter
            and not is_agency
            and not is_non_english
            and not is_commodity_wrapper
            and not is_hosting_provider
            and not is_non_operating_investor
            and location_status != "NON_US"
            and not page.error
            and (page.status is None or int(page.status) < 400)
            and bool(venture_quality["thesis_fit"])
            and bool(venture_quality["product_real"])
        )

        borderline_glasswing_fit = bool(
            bool(venture_quality["reviewable"])
            or enterprise_rescue_signal
        )

        substantive_no_fit_evidence = bool(
            domain not in {
                stable_domain(item) for item in GLASSWING_PORTFOLIO_DOMAINS
            }
            and
            not page.error
            and page.status is not None
            and 200 <= int(page.status) < 400
            and word_count >= 80
            and not borderline_glasswing_fit
        )
        strong_multidimensional_fit = bool(
            investment_label == "Investable Candidate"
            and bool(venture_quality["investable_ready"])
            and location_status in {"US", "UNKNOWN"}
            and not any([
                is_placeholder,
                is_parked,
                is_consumer_junk,
                is_media_newsletter,
                is_agency,
                is_non_english,
                is_commodity_wrapper,
                is_hosting_provider,
                is_non_operating_investor,
                is_coming,
            ])
        )

        bucket, reason = route_company(
            is_parked=is_parked,
            is_coming=is_coming,
            is_agency=is_agency,
            is_consumer_junk=is_consumer_junk,
            is_media_newsletter=is_media_newsletter,
            is_non_english=is_non_english,
            is_commodity_wrapper=is_commodity_wrapper,
            is_hosting_provider=is_hosting_provider,
            is_non_operating_investor=is_non_operating_investor,
            is_placeholder=is_placeholder,
            page_error=page.error if not combined else "",
            page_status=page.status,
            location_status=location_status,
            maturity_risk=maturity_risk,
            qualified_for_llm=qualified_for_llm,
            score=score,
            potential_glasswing_fit=potential_glasswing_fit,
            high_conviction_fit=high_conviction_fit,
            strong_multidimensional_fit=strong_multidimensional_fit,
            venture_reviewable=bool(venture_quality["reviewable"]),
            substantive_no_fit_evidence=substantive_no_fit_evidence,
        )

        if not combined.strip():
            bucket = "ghost"
            reason = "ghost_or_inaccessible_site"

        hard_rejection = bucket == "reject"
        if (
            investment_label == "Potential Comp"
            and potential_glasswing_fit
            and not hard_rejection
            and bucket != "coming_soon"
        ):
            bucket = "comp"
            reason = "relevant_but_mature_or_funded"

        (
            denial_confidence,
            denial_evidence,
            denial_rule,
        ) = denial_audit_fields(
            bucket=bucket,
            reason=reason,
            placeholder_evidence=placeholder_evidence,
            language_evidence=language_evidence,
            b2c_evidence=b2c_evidence,
            media_evidence=media_evidence,
            commodity_evidence=list(dict.fromkeys(
                commodity_evidence + hosting_evidence + investor_evidence
            )),
            location_evidence=location_evidence,
            page_error=page.error,
            http_status=page.status,
            combined=combined,
        )

        scoring_elapsed = time.monotonic() - _scoring_started
        if scoring_elapsed > 2.0:
            _report_slow_operation(
                "scoring",
                domain=domain,
                seconds=round(scoring_elapsed, 2),
                text_chars=len(combined),
            )

        source_row = {
            key: value
            for key, value in row.items()
            if key != "_cached_page"
        }
        return {
            **source_row,
            "domain": domain,
            "final_url": page.url or f"https://{domain}",
            "website_attempted": page.url or f"https://{domain}",
            "http_status": page.status or "",
            "page_title": page.title,
            "meta_description": page.description,
            "homepage_excerpt": page.text[:12000],
            "pages_scraped": page.pages_scraped,
            "meaningful_internal_links": page.meaningful_internal_links,
            "content_source": page.source,
            "crawl_status": page.crawl_status,
            "crawler_crawled_at": page.crawled_at,
            "rule_score": score,
            "glasswing_rating": rank_score,
            "diligence_rank_score": rank_score,
            "score_eligibility_caps": "; ".join(score_eligibility_caps),
            "score_defensibility_signals": counts.get("defensibility", 0),
            "score_important_problem_signals": counts.get("important_problem", 0),
            "score_technical_signals": counts.get("technical", 0),
            "score_workflow_signals": counts.get("workflow", 0),
            "score_business_value_signals": counts.get("business_value", 0),
            "venture_quality_score": venture_quality["overall_score"],
            "venture_thesis_score": venture_quality["thesis_score"],
            "venture_product_score": venture_quality["product_score"],
            "venture_wedge_score": venture_quality["wedge_score"],
            "venture_moat_score": venture_quality["moat_score"],
            "venture_founder_score": venture_quality["founder_score"],
            "venture_investable_ready": venture_quality["investable_ready"],
            "venture_reviewable": venture_quality["reviewable"],
            "venture_frontier_technical_wedge": venture_quality[
                "frontier_technical_wedge"
            ],
            "venture_review_quality_signal": venture_quality[
                "review_quality_signal"
            ],
            "venture_wedge_present": venture_quality["wedge_present"],
            "venture_moat_present": venture_quality["moat_present"],
            "venture_founder_strength": venture_quality["founder_strength"],
            "venture_slop_risk": venture_quality["slop_risk"],
            "venture_product_evidence": "; ".join(
                venture_quality["product_evidence"]
            ),
            "venture_wedge_evidence": "; ".join(
                venture_quality["wedge_evidence"]
            ),
            "venture_moat_evidence": "; ".join(
                venture_quality["moat_evidence"]
            ),
            "venture_founder_evidence": "; ".join(
                venture_quality["founder_evidence"]
            ),
            "website_green_flags": website_green_flags,
            "scorecard": scorecard,
            "venture_slop_evidence": "; ".join(
                venture_quality["slop_evidence"]
            ),
            "rule_thesis_pillar": pillar,
            "rule_evidence": "; ".join(evidence[:15]),
            "word_count": word_count,
            "pillar_hit_count": counts["pillar"],
            "ai_hit_count": counts["ai"],
            "enterprise_hit_count": counts["enterprise"],
            "product_hit_count": counts["product"],
            "buyer_signal_count": dimensions["buyer"],
            "workflow_depth_count": dimensions["workflow"],
            "technical_depth_count": dimensions["technical"],
            "business_value_count": dimensions["business_value"],
            "b2c_score": b2c_score,
            "b2c_evidence": "; ".join(b2c_evidence[:12]),
            "media_newsletter": is_media_newsletter,
            "media_evidence": "; ".join(media_evidence[:12]),
            "placeholder_detected": is_placeholder,
            "placeholder_reason": placeholder_reason,
            "placeholder_evidence": "; ".join(placeholder_evidence[:12]),
            "predominantly_non_english": is_non_english,
            "language_evidence": "; ".join(language_evidence[:12]),
            "commodity_wrapper": is_commodity_wrapper,
            "commodity_evidence": "; ".join(commodity_evidence[:12]),
            "hosting_provider": is_hosting_provider,
            "hosting_evidence": "; ".join(hosting_evidence[:12]),
            "non_operating_investor": is_non_operating_investor,
            "investor_evidence": "; ".join(investor_evidence[:12]),
            "clear_enterprise_idea": clear_enterprise_idea,
            "sparse_deep_tech_idea": sparse_deep_tech_idea,
            "sparse_vertical_workflow_idea": sparse_vertical_workflow_idea,
            "contextual_location_evidence": (
                location_evidence
                if isinstance(location_evidence, str)
                else "; ".join(location_evidence[:12])
            ),
            "prelaunch_score": prelaunch_score,
            "prelaunch_evidence": "; ".join(prelaunch_evidence[:12]),
            "potential_glasswing_fit": potential_glasswing_fit,
            "enterprise_rescue_signal": enterprise_rescue_signal,
            "enterprise_rescue_evidence": "; ".join(
                enterprise_rescue_evidence
            ),
            "high_conviction_fit": high_conviction_fit,
            "high_conviction_evidence": "; ".join(
                high_conviction_evidence
            ),
            "generic_strong_fit": generic_strong_fit,
            "independent_signal_dimensions": independent_signal_dimensions,
            "location_status": location_status,
            "location_evidence": "; ".join(location_evidence[:10]),
            "maturity_risk_score": maturity_risk,
            "maturity_evidence": "; ".join(maturity_evidence[:10]),
            "investment_classification": investment_label,
            "classification_reason": investment_label_reason,
            "website_disclosed_funding_m": (
                disclosed_funding_m if disclosed_funding_m is not None else ""
            ),
            "qualified_for_llm": qualified_for_llm,
            "preference_yes_probability": (
                preference_yes_probability
                if preference_yes_probability is not None else ""
            ),
            "preference_confidence": (
                preference_confidence
                if preference_confidence is not None else ""
            ),
            "preference_evidence": preference_evidence,
            "preference_model_ready": preference_model_ready,
            "semantic_score": (
                semantic_score if semantic_score is not None else ""
            ),
            "semantic_scoring_mode": (
                "active_human_model" if preference_model_ready else "shadow_only"
            ),
            "learned_positive_fit": learned_positive_fit,
            "routing_bucket": bucket,
            "routing_reason": reason,
            "viability_label": viability_label(bucket),
            "denial_confidence": denial_confidence,
            "denial_rule": denial_rule,
            "denial_evidence": denial_evidence,
            "fetch_error": page.error,
        }

    return await asyncio.get_running_loop().run_in_executor(
        _SCORING_EXECUTOR, _score_and_bucket_domain
    )


INDUSTRY_LABELS = {
    "Vertical AI": "vertical_ai",
    "Physical AI": "physical_ai",
    "AI Infrastructure": "ai_adaptive_infrastructure",
    "Cybersecurity": "intelligent_enterprise_defense",
    "Collaborative Intelligence": "collaborative_intelligence",
    "Next-Generation Compute": "next_gen_compute",
}


def load_run_preferences() -> dict:
    defaults = {
        "selected_industries": list(INDUSTRY_LABELS.values()),
        "include_keywords": [],
        "exclude_keywords": [],
        "keyword_mode": "any",
        "minimum_score": 55,
        "minimum_signal_dimensions": 2,
        "require_us_evidence": False,
        "include_coming_soon": True,
    }
    if not RUN_CONFIG.exists():
        return defaults
    try:
        loaded = json.loads(RUN_CONFIG.read_text(encoding="utf-8"))
    except Exception:
        return defaults
    defaults.update({
        key: value
        for key, value in loaded.items()
        if key in defaults
    })
    return defaults


def keyword_list(value: object) -> list[str]:
    if isinstance(value, list):
        items = value
    else:
        items = re.split(r"[,;\n]+", str(value or ""))
    return [
        clean(str(item)).lower()
        for item in items
        if clean(str(item))
    ]


def apply_run_preferences(
    output: pd.DataFrame,
    preferences: dict,
) -> pd.DataFrame:
    """Apply user-selected run filters without turning uncertainty into denial."""
    if output.empty:
        return output

    result = output.copy()
    selected_industries = set(
        str(item)
        for item in preferences.get("selected_industries", [])
        if str(item)
    )
    include_keywords = keyword_list(
        preferences.get("include_keywords", [])
    )
    exclude_keywords = keyword_list(
        preferences.get("exclude_keywords", [])
    )
    keyword_mode = str(
        preferences.get("keyword_mode", "any")
    ).lower()
    minimum_score = int(preferences.get("minimum_score", 55))
    minimum_dimensions = int(
        preferences.get("minimum_signal_dimensions", 2)
    )
    require_us = bool(
        preferences.get("require_us_evidence", False)
    )

    screened_reasons: list[str] = []

    for index, row in result.iterrows():
        bucket = str(row.get("routing_bucket", ""))
        reasons: list[str] = []

        # Uncertain companies are retained, but no longer mislabeled investable.
        if bucket == "review":
            reasons.append("insufficient_conviction_for_investable")

        if bucket in {"priority", "review"}:
            pillar = str(row.get("rule_thesis_pillar", ""))
            if selected_industries and pillar not in selected_industries:
                reasons.append("outside_selected_industries")

            high_conviction = (
                str(row.get("high_conviction_fit", ""))
                .strip()
                .lower()
                in {"true", "1", "yes"}
            )
            venture_investable = (
                str(row.get("venture_investable_ready", ""))
                .strip()
                .lower()
                in {"true", "1", "yes"}
            )
            score = int(
                pd.to_numeric(
                    row.get("diligence_rank_score", 0),
                    errors="coerce",
                )
                or 0
            )
            if (
                score < minimum_score
                and not high_conviction
                and not venture_investable
            ):
                reasons.append(
                    f"below_run_minimum_score_{minimum_score}"
                )

            signal_columns = (
                "score_defensibility_signals",
                "score_important_problem_signals",
                "score_technical_signals",
                "workflow_depth_count",
                "business_value_count",
                "buyer_signal_count",
            )
            dimensions_present = sum(
                1
                for column in signal_columns
                if float(
                    pd.to_numeric(
                        row.get(column, 0),
                        errors="coerce",
                    )
                    or 0
                ) > 0
            )
            if (
                dimensions_present < minimum_dimensions
                and not high_conviction
                and not venture_investable
            ):
                reasons.append(
                    "insufficient_signal_dimensions_"
                    f"{dimensions_present}_of_{minimum_dimensions}"
                )

            searchable = clean(
                " ".join([
                    str(row.get("page_title", "")),
                    str(row.get("meta_description", "")),
                    str(row.get("homepage_excerpt", "")),
                    str(row.get("rule_evidence", "")),
                ])
            ).lower()

            if include_keywords:
                matches = [
                    term
                    for term in include_keywords
                    if term in searchable
                ]
                include_ok = (
                    len(matches) == len(include_keywords)
                    if keyword_mode == "all"
                    else bool(matches)
                )
                if not include_ok:
                    reasons.append("missing_required_keywords")

            excluded_hits = [
                term
                for term in exclude_keywords
                if term in searchable
            ]
            if excluded_hits:
                reasons.append(
                    "matched_excluded_keywords:"
                    + "|".join(excluded_hits[:8])
                )

            if require_us and str(
                row.get("location_status", "")
            ) != "US_VERIFIED":
                reasons.append("explicit_us_evidence_required")

        if reasons and bucket != "reject":
            score_value = int(
                pd.to_numeric(
                    row.get(
                        "diligence_rank_score",
                        row.get("rule_score", 0),
                    ),
                    errors="coerce",
                )
                or 0
            )
            signal_columns = (
                "score_defensibility_signals",
                "score_important_problem_signals",
                "score_technical_signals",
                "workflow_depth_count",
                "business_value_count",
                "buyer_signal_count",
            )
            signal_dimensions = sum(
                1
                for column in signal_columns
                if float(
                    pd.to_numeric(
                        row.get(column, 0),
                        errors="coerce",
                    )
                    or 0
                ) > 0
            )
            clear_enterprise_signal = any(
                str(row.get(column, "")).strip().lower()
                in {"true", "1", "yes"}
                for column in (
                    "clear_enterprise_idea",
                    "sparse_deep_tech_idea",
                    "sparse_vertical_workflow_idea",
                    "potential_glasswing_fit",
                )
            )
            enterprise_rescue_signal = (
                str(row.get("enterprise_rescue_signal", ""))
                .strip()
                .lower()
                in {"true", "1", "yes"}
            )
            venture_reviewable_text = str(
                row.get("venture_reviewable", "")
            ).strip().lower()
            venture_reviewable = (
                venture_reviewable_text in {"true", "1", "yes"}
            )
            has_venture_framework = venture_reviewable_text != ""
            review_floor = 25
            review_eligible = (
                bucket == "comp"
                or venture_reviewable
                or (
                    not has_venture_framework
                    and
                    score_value >= review_floor
                    and (
                        signal_dimensions >= 1
                        or clear_enterprise_signal
                        or enterprise_rescue_signal
                    )
                )
            )

            if review_eligible:
                result.at[index, "routing_bucket"] = "review"
                result.at[index, "viability_label"] = "Needs Review"
                result.at[index, "routing_reason"] = (
                    "enterprise_rescue_requires_human_review"
                    if enterprise_rescue_signal
                    else "credible_but_requires_human_review"
                )
                result.at[index, "review_reasons"] = "; ".join(reasons)
                result.at[index, "screened_out_reasons"] = ""
            else:
                page_error = str(row.get("fetch_error", "")).strip()
                status_text = str(row.get("http_status", "")).strip()
                word_count = int(
                    pd.to_numeric(
                        row.get("word_count", 0),
                        errors="coerce",
                    )
                    or 0
                )
                inaccessible = (
                    bool(page_error)
                    or status_text.startswith(("4", "5"))
                    or word_count < 3
                )
                # A reachable-but-unconvincing page is still Needs Review --
                # there is no bucket left that a real, factual disqualifier
                # doesn't route to. But an unreachable one is not a weaker
                # candidate, it is not a company page at all, so it must go
                # to ghost (hidden and purged) rather than sit in Needs
                # Review forever.
                result.at[index, "routing_bucket"] = (
                    "ghost" if inaccessible else "review"
                )
                result.at[index, "viability_label"] = (
                    "Ghost / Inaccessible" if inaccessible else "Needs Review"
                )
                result.at[index, "routing_reason"] = (
                    "website_unavailable_or_no_usable_company_evidence"
                    if inaccessible
                    else "no_clear_enterprise_or_glasswing_fit"
                )
                result.at[index, "denial_rule"] = ""
                result.at[index, "denial_evidence"] = ""
                result.at[index, "review_reasons"] = "; ".join(reasons[:8])
                result.at[index, "screened_out_reasons"] = ""
        else:
            result.at[index, "review_reasons"] = ""
            result.at[index, "screened_out_reasons"] = ""

    return result



def apply_persistent_overrides(output: pd.DataFrame) -> pd.DataFrame:
    """Apply classification overrides and attach independent training labels."""
    path = ROOT / "data" / "manual" / "domain_overrides.csv"
    if output.empty:
        return output

    overrides = pd.DataFrame()
    if path.exists():
        try:
            overrides = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            overrides = pd.DataFrame()
    if not overrides.empty and "domain" in overrides.columns:
        overrides["domain"] = overrides["domain"].map(stable_domain)
        overrides = overrides.drop_duplicates("domain", keep="last")
        mapping = overrides.set_index("domain").to_dict("index")
    else:
        mapping = {}

    target_to_bucket = {
        "Investable Candidate": (
            "priority",
            "manual_override_investable",
        ),
        "Review Candidate": (
            "review",
            "manual_override_review",
        ),
        "Potential Comp": (
            "comp",
            "manual_override_comp",
        ),
        "Hard Denied": (
            "reject",
            "manual_override_hard_denied",
        ),
        "Ignore Permanently": (
            "screened_out",
            "manual_override_ignore",
        ),
    }

    result = output.copy()
    for idx, row in result.iterrows():
        domain = stable_domain(row.get("domain", ""))
        override = mapping.get(domain)
        if not override:
            continue
        target = str(override.get("override_target", "")).strip()
        if target not in target_to_bucket:
            continue

        bucket, reason = target_to_bucket[target]
        result.at[idx, "routing_bucket"] = bucket
        result.at[idx, "routing_reason"] = reason
        result.at[idx, "viability_label"] = target
        result.at[idx, "manual_override"] = True
        result.at[idx, "manual_override_target"] = target
        result.at[idx, "manual_override_note"] = str(
            override.get("override_note", "")
        )
        result.at[idx, "manual_override_updated_at"] = str(
            override.get("updated_at", "")
        )

    basket_targets: dict[str, tuple[str, int]] = {}
    for basket_path, basket in (
        (YES_BASKET_DATA, "yes"),
        (NO_BASKET_DATA, "no"),
    ):
        examples = read_basket_data(basket_path)
        for _, example in examples.iterrows():
            basket_targets[stable_domain(example.get("domain_url", ""))] = (
                basket,
                int(example.get("rating", 0)),
            )

    for idx, row in result.iterrows():
        domain = stable_domain(row.get("domain", ""))
        basket_entry = basket_targets.get(domain)
        if not basket_entry:
            continue
        basket, rating = basket_entry
        result.at[idx, "training_basket"] = basket
        result.at[idx, "human_rating"] = rating

    return result


def retention_minimum_rating() -> int:
    """Minimum final Glasswing rating worth retaining as searchable data."""
    try:
        configured = int(os.getenv("GLASSWING_RETENTION_MIN_SCORE", "25"))
    except ValueError:
        configured = 25
    return max(0, min(configured, 100))


def bulk_compatibility_exports_enabled() -> bool:
    """Keep enormous CSV mirrors out of cloud worker volumes by default."""
    configured = os.getenv("GLASSWING_BULK_CSV_EXPORTS", "").strip().lower()
    if configured:
        return configured not in {"0", "false", "no", "off"}
    return not using_classification_postgres()


def automated_discard_mask(output: pd.DataFrame) -> pd.Series:
    """Select disposable model outcomes while preserving useful ambiguity."""
    buckets = output.get(
        "routing_bucket", pd.Series("", index=output.index)
    ).astype(str).str.lower()
    ratings = pd.to_numeric(
        output.get(
            "glasswing_rating",
            output.get(
                "diligence_rank_score",
                output.get("rule_score", pd.Series(0, index=output.index)),
            ),
        ),
        errors="coerce",
    ).fillna(0)
    slop = output.get(
        "venture_slop_risk", pd.Series(False, index=output.index)
    ).astype(str).str.lower().isin({"true", "1", "yes"})
    # route_company/apply_run_preferences only ever produce "review" or
    # "reject" now (2026-08-08: Investable/Coming Soon/Insufficient Evidence
    # collapsed into a single Needs Review outcome -- see route_company's
    # docstring). "priority"/"coming_soon"/"comp"/"insufficient" only reach
    # routing_bucket via a manual override now, which is already separately
    # protected via manually_protected below -- kept here too as a second,
    # harmless line of defense against ever silently purging one.
    #
    # "reject" is protected too (2026-08-08): there are only 3 buckets --
    # Investable, Needs Review, Hard Denied. If Python rejects a domain, it
    # must actually show up as Hard Denied, not get purged down to a
    # tombstone while a separate "auto-rejected" counter tries to account
    # for it after the fact. Only "ghost" (a genuinely blank/unreachable
    # page -- nothing to review either way) still gets purged.
    protected_buckets = buckets.isin(
        {"priority", "coming_soon", "review", "comp", "insufficient", "reject"}
    )
    return (
        buckets.isin({"ghost", "screened_out"})
        | slop
        | ((ratings < retention_minimum_rating()) & ~protected_buckets)
    )


def persist_screening_output(output: pd.DataFrame) -> dict[str, int]:
    """Persist one in-process screening batch to all master datasets."""
    if output.empty:
        # Confirmed live: screen_records now excludes timed-out rows from
        # output before this is ever called, so a batch where every domain
        # timed out reaches here with zero rows. .get() on a genuinely
        # empty frame returns the fallback scalar instead of an empty
        # Series (e.g. output.get("routing_bucket", "") == "priority" is
        # just False, not a Series), which crashes .sum() below.
        return {
            "batch_total": 0,
            "investable": 0,
            "coming_soon": 0,
            "hard_denied": 0,
            "screened_out": 0,
        }
    output = apply_run_preferences(output, load_run_preferences())
    output = apply_persistent_overrides(output)
    manual_override = (
        output.get("manual_override", pd.Series(False, index=output.index))
        .astype(str).str.lower().isin({"true", "1", "yes"})
    )
    manual_reason = (
        output.get("routing_reason", pd.Series("", index=output.index))
        .astype(str).str.startswith(
            ("manual_override_", "human_training_")
        )
    )
    manual_basket = output.get(
        "training_basket", pd.Series("", index=output.index)
    ).astype(str).str.lower().isin({"yes", "no"})
    from training_truth import protected_training_domains

    protected_domains = protected_training_domains()
    shared_training_truth = output.get(
        "domain", pd.Series("", index=output.index)
    ).map(stable_domain).isin(protected_domains)
    manually_protected = (
        manual_override | manual_reason | manual_basket | shared_training_truth
    )
    denied_mask = output["routing_bucket"].eq("reject")
    manual_denied = output[denied_mask & manually_protected].copy()
    # An error page is not a company and must never end up in a bucket -- not
    # Needs Review, not Ghost, not anything. A site we could not read tells us
    # nothing, so it produces no classification row at all: it is not
    # persisted (so it can never be bucketed or counted as a retained
    # candidate) and not tombstoned (so a domain that is merely blocked or
    # mid-deploy is not written off forever, and an existing good record for it
    # is never deleted by a single bad fetch).
    #
    # Retry state lives in the pipeline queue instead, which is where it
    # belongs: blocked/5xx domains are requeued with backoff, and anything
    # still unreadable is closed out by the queue's own attempt limit rather
    # than by deleting classification data.
    unreadable = output["routing_bucket"].eq("ghost")
    retryable_inaccessible = output.get(
        "routing_reason", pd.Series("", index=output.index)
    ).map(is_retryable_inaccessible_reason)
    disposable = (
        automated_discard_mask(output)
        & ~manually_protected
        & ~unreadable
    )
    automated_discarded = output[disposable].copy()
    retry_domains = [
        stable_domain(domain)
        for domain in output.loc[retryable_inaccessible, "domain"].tolist()
        if stable_domain(domain)
    ]
    persistable_output = output.loc[
        ~output.index.isin(automated_discarded.index)
        & ~unreadable
    ].copy()
    if count_classifications() == 0:
        bootstrap_from_csv()
    upsert_classifications(persistable_output)

    tombstoned = 0
    tombstone_verified = 0
    if not automated_discarded.empty:
        # Keep only a minimal cross-crawler tombstone. All website text,
        # search rows, retry data, and full classification payloads go away.
        from rejection_cleanup import purge_automated_rejections

        tombstoned = record_automated_exclusions(
            automated_discarded.to_dict(orient="records")
        )
        # Verify the write actually landed. record_automated_exclusions returns
        # the number of rows it *intended* to write, so a store that accepts the
        # statement without persisting anything is indistinguishable from a
        # successful write. Confirmed live: it reported 12-17 rows per batch
        # while automated_exclusions grew by zero over 100 seconds, and reads
        # from the same store returned correct counts -- so only a read-back can
        # tell the difference.
        try:
            from classification_store import excluded_domain_set

            tombstone_verified = len(
                excluded_domain_set(list(automated_discarded["domain"]))
            )
        except Exception as exc:
            tombstone_verified = -1
            print(
                f"tombstone verification failed: {type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
        if tombstone_verified == 0 and tombstoned > 0:
            print(
                "CLASSIFICATION STORE IS NOT PERSISTING: wrote "
                f"{tombstoned} exclusions, read back 0.",
                file=sys.stderr,
                flush=True,
            )
        purge_automated_rejections(
            automated_discarded["domain"],
            clean_csv=False,
        )

    approved = persistable_output[
        persistable_output["routing_bucket"].isin(["priority", "coming_soon"])
    ].copy()
    denied = persistable_output[
        persistable_output["routing_bucket"] == "reject"
    ].copy()
    coming_soon = persistable_output[
        persistable_output["routing_bucket"] == "coming_soon"
    ].copy()
    candidates = persistable_output[
        persistable_output["routing_bucket"] == "priority"
    ].copy()
    screened_out = persistable_output[
        persistable_output["routing_bucket"].isin(
            ["screened_out", "review", "comp", "insufficient"]
        )
    ].copy()
    ghost = persistable_output[
        persistable_output["routing_bucket"] == "ghost"
    ].copy()
    insufficient = persistable_output[
        persistable_output["routing_bucket"] == "insufficient"
    ].copy()

    shortlist_master = upsert_csv(
        FILTERED / "python_shortlist_master.csv",
        candidates,
        key="domain",
    )
    approved_master = upsert_csv(
        FILTERED / "python_approved_master.csv",
        approved,
        key="domain",
    )
    denied_master = upsert_csv(
        FILTERED / "python_denied_master.csv",
        manual_denied,
        key="domain",
    )

    if not approved.empty and not denied_master.empty:
        denied_master = denied_master[
            ~denied_master["domain"].isin(approved["domain"])
        ]
        atomic_write_csv(
            FILTERED / "python_denied_master.csv",
            denied_master,
        )

    if not denied.empty and not approved_master.empty:
        approved_master = approved_master[
            ~approved_master["domain"].isin(denied["domain"])
        ]
        atomic_write_csv(
            FILTERED / "python_approved_master.csv",
            approved_master,
        )

    upsert_csv(
        FILTERED / "investable_candidates_master.csv",
        candidates,
        key="domain",
    )
    if bulk_compatibility_exports_enabled():
        upsert_csv(
            FILTERED / "screened_out_master.csv",
            screened_out,
            key="domain",
        )
        upsert_csv(
            FILTERED / "ghost_inaccessible_master.csv",
            ghost,
            key="domain",
        )
        # Local SQLite mode retains compatibility exports for old scripts.
        exported_master = export_all_screened_csv(
            minimum_interval_seconds=300,
        )
        if exported_master is not None:
            crawled_view_counts = persist_crawled_corpus_views(
                exported_master
            )
        else:
            crawled_view_counts = {}
    else:
        crawled_view_counts = {}
    classification_total = count_classifications()

    atomic_write_csv(
        FILTERED / "candidates_master.csv",
        candidates,
    )
    atomic_write_csv(
        FILTERED / "coming_soon_master.csv",
        coming_soon,
    )
    atomic_write_csv(
        FILTERED / "rejected_master.csv",
        manual_denied,
    )

    return {
        "batch_total": len(output),
        "investable": len(candidates),
        "coming_soon": len(coming_soon),
        "hard_denied": int(denied_mask.sum()),
        "insufficient_evidence": int(
            output["routing_bucket"].isin(["insufficient", "ghost"]).sum()
        ),
        "screened_out": int(output["routing_bucket"].isin(
            ["screened_out", "review", "comp", "insufficient"]
        ).sum()),
        "ghost": int(output["routing_bucket"].eq("ghost").sum()),
        "storage_discarded": len(automated_discarded),
        # Per-batch write accounting. Without this, a batch that computed
        # discards but wrote nothing to the shared store was indistinguishable
        # from a batch with nothing to write -- and this worker has no log
        # capture, so there was no other way to tell them apart.
        "rows_in": int(len(output)),
        "rows_persisted": int(len(persistable_output)),
        "rows_unreadable": int(unreadable.sum()),
        "rows_tombstoned": int(tombstoned),
        "rows_tombstone_verified": int(tombstone_verified),
        "retry_domains": retry_domains,
        "blocked_or_erroring": len(retry_domains),
        "shortlist_master": len(shortlist_master),
        "approved_master": len(approved_master),
        "denied_master": len(denied_master),
        "all_screened_master": classification_total,
        **crawled_view_counts,
    }


async def screen_records(
    records: list[dict],
    *,
    workers: int,
    timeout: float,
    homepage_only: bool = False,
    retry_timeouts: bool = True,
    persist: bool = True,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Screen records directly inside the caller's Python process."""
    if not records:
        return pd.DataFrame(), {
            "batch_total": 0,
            "investable": 0,
            "coming_soon": 0,
            "hard_denied": 0,
            "screened_out": 0,
        }

    domains = [
        stable_domain(
            record.get("registrable", record.get("domain", ""))
        )
        for record in records
    ]
    cached_pages = await asyncio.to_thread(
        load_crawler_fetches,
        domains,
    )

    async def scrape_rows(
        selected_records: list[dict],
        selected_workers: int,
        selected_timeout: float,
    ) -> list[dict]:
        connector = aiohttp.TCPConnector(
            # Each domain probes apex and www concurrently.
            limit=max(2, int(selected_workers) * 2),
            ttl_dns_cache=300,
            # The real bug (confirmed live: a whole 75-domain batch hung
            # 600+ seconds with zero exception, twice in a row, even after
            # every per-request HTTP timeout below was already correctly
            # bounded). aiohttp's default resolver runs socket.getaddrinfo()
            # in the loop's thread-pool executor -- cancelling the asyncio
            # Task waiting on it (our outer asyncio.wait_for, or any of the
            # timeouts here) does NOT stop that underlying thread. A domain
            # whose DNS never resolves and never errors (a black-holed
            # query, a broken/non-responding authoritative nameserver) burns
            # one executor thread forever. Enough of those, accumulated
            # across repeated batches, exhausts the whole pool -- every
            # future getaddrinfo() call then queues behind zombie threads
            # that will never finish, hanging every subsequent batch too,
            # regardless of any of the timeout values below. aiodns's
            # AsyncResolver does DNS natively on the event loop instead, so
            # a cancelled lookup is actually cancelled.
            resolver=aiohttp.AsyncResolver(),
        )
        client_timeout = aiohttp.ClientTimeout(
            # total=None was the real bug here (confirmed live: an entire
            # 75-domain batch hung for 600+ seconds with zero exception,
            # zero progress, until an outer deadline in continuous_pipeline
            # finally force-failed it). connect/sock_connect/sock_read each
            # only bound one phase -- sock_read in particular resets on
            # every successful chunk, so a server that trickles bytes just
            # under that interval (a slow-loris response, a chunked stream
            # that never closes) can keep "making progress" forever without
            # ever tripping any of them. Every request in the batch runs
            # inside one asyncio.gather(), so a single pathological site
            # stalled the whole batch, not just its own row. A real ceiling
            # on the request as a whole is the only thing that bounds that.
            total=selected_timeout * 2,
            connect=selected_timeout,
            sock_connect=selected_timeout,
            sock_read=selected_timeout,
        )
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=client_timeout,
            headers={"User-Agent": USER_AGENT},
        ) as session:
            sem = asyncio.Semaphore(max(1, int(selected_workers)))
            return await asyncio.gather(*[
                process(
                    {
                        **row,
                        "_cached_page": cached_pages.get(
                            stable_domain(
                                row.get(
                                    "registrable",
                                    row.get("domain", ""),
                                )
                            )
                        ),
                    },
                    session,
                    sem,
                    float(selected_timeout),
                    homepage_only=homepage_only,
                )
                for row in selected_records
            ])

    results = await scrape_rows(records, int(workers), float(timeout))

    # A saturated connector or brief network stall can otherwise make a whole
    # batch look like dead websites. Retry timeout-only records before
    # classification or persistence; real HTTP failures are left untouched.
    retry_indexes = [
        index
        for index, result in enumerate(results)
        if "TimeoutError" in str(result.get("fetch_error", ""))
    ]
    # If most of the batch timed out on the first pass, that's a systemic
    # condition (dead egress, saturated connector), not a handful of slow
    # sites -- retrying it at deliberately low concurrency (below) would
    # only multiply the wait. Confirmed live: a 75-domain batch with a near-
    # total first-pass timeout rate, retried at a fixed 2 workers, took long
    # enough to blow through the caller's outer deadline before ever
    # finishing, surfacing as a bare TimeoutError with zero progress. Skip
    # straight to the systemic-timeout check below instead of retrying.
    mostly_timed_out = (
        len(records) >= 5 and len(retry_indexes) / len(records) >= 0.5
    )
    if retry_timeouts and retry_indexes and not mostly_timed_out:
        # A fixed cap of 8 was tuned for small (~75-domain) batches --
        # confirmed live: once batch sizes scaled up with the worker
        # count (batch_size == workers, so every domain gets its own
        # concurrent slot), this same fixed cap would have made the retry
        # pass for a large batch queue behind just 8 slots regardless of
        # how many workers the main pass used, recreating the exact
        # queueing bottleneck that motivated matching batch size to worker
        # count in the first place. Scale with the batch's own worker
        # count instead of a constant.
        retry_records = [records[index] for index in retry_indexes]
        retry_results = await scrape_rows(
            retry_records,
            max(2, int(workers) // 2),
            max(20.0, float(timeout)),
        )
        for index, retry_result in zip(retry_indexes, retry_results):
            results[index] = retry_result

    # Confirmed live: this used to be all-or-nothing -- if 80%+ of a batch
    # timed out (common for a batch skewed toward brand-new, not-yet-live
    # CertStream domains, regardless of TLD), the whole batch was discarded
    # via a raised exception, throwing away the real, successfully-fetched
    # companies in it too (100 domains genuinely succeeded in one batch,
    # then the next 100-domain batch's real successes were discarded
    # alongside its timeouts). Persist whatever genuinely succeeded either
    # way; only the timed-out rows get excluded here and handed back for
    # the caller's normal retry-with-backoff path instead of being
    # silently lost.
    timed_out_rows = [
        result
        for result in results
        if "TimeoutError" in str(result.get("fetch_error", ""))
    ]
    succeeded_rows = [
        result for result in results if result not in timed_out_rows
    ]
    timed_out_domains = [
        str(result.get("domain", "")) for result in timed_out_rows if result.get("domain")
    ]

    output = pd.DataFrame(succeeded_rows)
    _persist_started = time.monotonic()
    stats = (
        await asyncio.get_running_loop().run_in_executor(
            _SCORING_EXECUTOR, persist_screening_output, output
        )
        if persist
        else (
            {
                "batch_total": 0,
                "investable": 0,
                "coming_soon": 0,
                "hard_denied": 0,
                "screened_out": 0,
            }
            if output.empty
            else {
                "batch_total": len(output),
                "investable": int(
                    (output.get("routing_bucket", "") == "priority").sum()
                ),
                "coming_soon": int(
                    (output.get("routing_bucket", "") == "coming_soon").sum()
                ),
                "hard_denied": int(
                    (output.get("routing_bucket", "") == "reject").sum()
                ),
                "screened_out": int(
                    output.get("routing_bucket", "").isin(
                        ["screened_out", "review", "comp"]
                    ).sum()
                ),
            }
        )
    )
    persist_elapsed = time.monotonic() - _persist_started
    if persist_elapsed > 2.0:
        _report_slow_operation(
            "persist", rows=len(output), seconds=round(persist_elapsed, 2)
        )
    stats["timed_out_domains"] = timed_out_domains
    return output, stats


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(RAW / "domains_master.csv"),
    )
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


async def run() -> None:
    options = args()
    path = Path(options.csv_path).expanduser()

    if not path.exists():
        raise SystemExit(f"Input CSV not found: {path}")

    frame = pd.read_csv(path, dtype=str).fillna("")
    if options.limit:
        frame = frame.head(options.limit)

    output, stats = await screen_records(
        frame.to_dict("records"),
        workers=options.workers,
        timeout=options.timeout,
        persist=True,
    )

    if not output.empty:
        print(
            output["routing_bucket"]
            .value_counts(dropna=False)
            .to_string()
        )
    print(f"Investable candidates this run: {stats['investable']}")
    print(f"Coming soon this run: {stats['coming_soon']}")
    print(f"Hard denied this run: {stats['hard_denied']}")
    print(f"Screened out this run: {stats['screened_out']}")
    print(f"All screened master: {stats.get('all_screened_master', 0)}")
    print(f"Saved outputs in {FILTERED}")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
