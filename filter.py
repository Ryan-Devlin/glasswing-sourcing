#!/usr/bin/env python3
"""Strict U.S.-only website enrichment and pre-Claude routing."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
import pandas as pd
from glasswing_portfolio import GLASSWING_PORTFOLIO_DOMAINS
from bs4 import BeautifulSoup
from label_calibrator import calibrate_text
from classification_store import (
    CLASSIFICATION_DB,
    bootstrap_from_csv,
    count_classifications,
    export_all_screened_csv,
    upsert_classifications,
)

from common import (
    AI_SEARCH_DB,
    FILTERED,
    NO_BASKET_DATA,
    RAW,
    ROOT,
    RUN_CONFIG,
    YES_BASKET_DATA,
    ZONE_UNIVERSE_DB,
    atomic_write_csv,
    clean,
    read_label_metadata,
    read_basket_data,
    stable_domain,
    upsert_csv,
)

USER_AGENT = "Mozilla/5.0 Glasswing-Sourcing/2.0"


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
            f"{prefix}coming_soon.csv": corpus[buckets.eq("coming_soon")],
            f"{prefix}needs_review.csv": corpus[buckets.eq("review")],
            f"{prefix}potential_comp.csv": corpus[buckets.eq("comp")],
            f"{prefix}insufficient_evidence.csv": corpus[
                buckets.isin(["insufficient", "ghost", "screened_out"])
            ],
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
        "telemetry", "physical security",
        "video security", "emergency response", "physical engineering",
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
        "agent security protocol", "programmable guardrails",
        "mpc co-signing",
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
    "west virginia", "wisconsin", "wyoming", "district of columbia",
]

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
    "reproducible results",
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
]

GENERIC_AI_PHRASES = [
    "ai-powered", "powered by ai", "using artificial intelligence",
    "leverage ai", "harness the power of ai", "revolutionize",
    "transform your business", "next-generation ai solution",
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
    "former nvidia", "mit", "stanford", "carnegie mellon", "caltech",
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
    try:
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
        conn.close()
    except sqlite3.Error:
        pass

    if zone_db.exists():
        try:
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
            conn.close()
        except sqlite3.Error:
            pass
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
    service_hits = occurrences(text, SERVICE_BUSINESS_SCORING_SIGNALS)
    low_value_hits = occurrences(text, LOW_VALUE_SCORING_SIGNALS)

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
        "service_business": len(set(service_hits)),
        "low_value": len(set(low_value_hits)),
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

    # Generic branding without substance is actively penalized.
    if generic_hits and unique_technical == 0 and unique_workflow == 0:
        raw -= 15
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
    product_hits = [term for term in product_signals if term in lower]
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
    product_hits = [term for term in product_signals if term in lower]

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
    )
    for identity_terms, confirming_terms in education_patterns:
        identity = [term for term in identity_terms if term in leading_copy]
        confirming = [
            term for term in confirming_terms if term in leading_copy
        ]
        if identity and confirming:
            education_identity_hits.extend(identity + confirming[:2])

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

    is_media = (
        bool(education_identity_hits)
        or bool(directory_identity_hits)
        or newsletter_identity
        or
        (
            len(strong_hits) >= 2
            and len(product_hits) <= 1
        )
        or len(strong_hits) >= 3
    )
    evidence = list(dict.fromkeys(
        education_identity_hits
        + directory_identity_hits
        + strong_hits
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
            "orchestration", "deployment", "inference", "developer platform"
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
    "runtime control", "runtime integrity", "grc", "soc 2", "iso 27001",
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
    bundles = [
        (
            ["functional safety", "certifiable safety"],
            2,
        ),
        (
            ["evaluation infrastructure", "continuous evals",
             "reproducible evaluation"],
            2,
        ),
        (
            ["satellite and aerial imagery", "structured world models",
             "geospatial intelligence"],
            2,
        ),
        (
            ["physics-driven ai", "pcb layout", "fabrication-ready"],
            2,
        ),
        (
            ["ai gateway", "observability", "guardrails",
             "prompt management", "governance"],
            3,
        ),
        (
            ["ai infrastructure and workloads", "reliability",
             "ai factories", "gpus", "accelerators", "uptime",
             "utilization"],
            3,
        ),
        (
            ["distill", "private", "purpose-built models",
             "own environment"],
            3,
        ),
        (
            ["build websites", "web apps", "end-to-end software",
             "prompt your ideas", "ai agents build"],
            3,
        ),
        (
            ["engagement intelligence", "recognition and rewards",
             "goals and reviews", "hris", "performance"],
            3,
        ),
        (
            ["recipe formulation", "food innovation",
             "product development", "r&d teams", "consumer acceptance"],
            3,
        ),
        (
            ["delivery management", "dispatching", "delivery dispatch",
             "reputation management", "customer experience"],
            3,
        ),
    ]
    for terms, minimum in bundles:
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
    founder_score = min(
        100,
        min(len(set(founder_visibility)), 2) * 18
        + min(len(set(founder_credibility)), 3) * 20,
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
        thesis_score * 0.25
        + product_score * 0.15
        + wedge_score * 0.25
        + moat_score * 0.25
        + founder_score * 0.10
    )
    investable_ready = bool(
        thesis_fit
        and product_real
        and wedge_present
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
    reviewable = bool(
        thesis_fit
        and product_real
        and not slop_risk
        and (
            wedge_score >= 25
            or moat_present
            or founder_strength
            or high_conviction_fit
        )
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
        "product_evidence": product_evidence[:10],
        "wedge_evidence": wedge_evidence[:10],
        "moat_evidence": list(dict.fromkeys(
            moat_evidence
            + occurrences(cleaned, DEFENSIBILITY_SIGNALS)
        ))[:10],
        "founder_evidence": list(dict.fromkeys(
            founder_visibility + founder_credibility
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

    if only_generic_coming_soon_content(cleaned):
        return (
            True,
            "generic_coming_soon_only",
            ["no_original_company_or_product_content"],
        )

    if (
        "page not found" in lower
        and any(term in lower for term in (
            "doesn't exist",
            "does not exist",
            "has been moved",
            "404",
        ))
    ):
        return (
            True,
            "generic_hosting_or_registrar_placeholder",
            ["soft_404_page"],
        )

    if word_count <= 2:
        return False, "insufficient_visible_content", ["under_3_words"]

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
    ]

    strong_hits = [term for term in strong if term in lower]
    vertical_hits = [term for term in vertical if term in lower]
    platform_hits = [term for term in platform_signals if term in lower]
    generic_agent_hits = [
        term for term in generic_agent_signals if term in lower
    ]
    explicit_low_value_hits = [
        term for term in (
            "ai-powered cold email",
            "cold email",
            "sports betting picks",
            "ai sports betting",
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
            + explicit_low_value_hits
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

            html = await response.text(errors="ignore")
            result, links = parse_html(html, str(response.url))
            result.status = response.status
            return result, links
    except Exception as exc:
        return Fetch(error=f"{type(exc).__name__}: {exc}"), []


async def _fetch_candidate_group(
    session: aiohttp.ClientSession,
    urls: list[str],
    timeout: float,
) -> tuple[Fetch | None, list[str], Fetch | None, list[str]]:
    """Try apex first; only spend another connection when it is unusable."""
    best_result: Fetch | None = None
    best_links: list[str] = []
    for url in urls:
        result, links = await fetch_html(session, url, timeout)
        if not result.url:
            result.url = url
        if result.text:
            return result, links, result, links
        if (
            best_result is None
            or best_result.status is None
            or (
                result.status is not None
                and int(result.status) < int(best_result.status or 999)
            )
        ):
            best_result = result
            best_links = links
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

    for link in links:
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
    """Route by viability.

    Hard Denied requires a specific disqualifying fact. Low score, sparse copy,
    unknown location, or missing technical evidence never cause denial alone.
    """
    # High-confidence website validity failures.
    if is_parked:
        return "reject", "parked_or_for_sale"
    if is_placeholder:
        return "reject", "generic_hosting_or_registrar_placeholder"

    # An inaccessible site is not investment evidence. Keep it hidden for
    # retry/recheck rather than presenting it as a hard denial.
    if page_error:
        return "review", "inaccessible_site"
    if page_status and int(page_status) >= 400:
        return "review", f"inaccessible_http_{page_status}"

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
    if substantive_no_fit_evidence:
        return "insufficient", "substantive_site_no_glasswing_fit_signals"

    # A legitimate founder-controlled minimal site is preserved separately.
    if is_coming:
        return "coming_soon", "verified_prelaunch_watch"

    # Investable requires a credible product, wedge, and moat/technical edge.
    if strong_multidimensional_fit and score >= 45:
        return "priority", "venture_quality_investable"
    if qualified_for_llm and score >= 55:
        return "priority", "strong_investable_candidate"

    # Needs Review is reserved for credible thesis-aligned products that show
    # at least two quality dimensions but are missing full conviction.
    if venture_reviewable or qualified_for_llm:
        return "review", "credible_venture_candidate_missing_one_dimension"
    if potential_glasswing_fit:
        return "insufficient", "thesis_fit_missing_wedge_or_moat"

    # A readable company without venture evidence is not a Hard Denial, but it
    # should not consume the human-review queue.
    return "insufficient", "insufficient_venture_quality_evidence"


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
        "insufficient": "Insufficient Evidence",
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

    combined = clean(f"{page.title} {page.description} {page.text}")
    lower = combined.lower()
    scoring_copy = combined

    word_count = len(combined.split())
    (
        is_placeholder,
        placeholder_reason,
        placeholder_evidence,
    ) = placeholder_assessment(combined, word_count=word_count)
    is_parked = placeholder_reason == "parked_or_for_sale"
    is_agency, agency_evidence = agency_assessment(combined)

    score, pillar, evidence, counts = score_text(scoring_copy)
    dimensions = glasswing_dimensions(scoring_copy)
    enterprise_rescue_signal, enterprise_rescue_evidence = (
        enterprise_rescue_assessment(combined)
    )
    high_conviction_fit, high_conviction_evidence = (
        high_conviction_fit_assessment(combined)
    )
    is_consumer_junk, b2c_score, b2c_evidence = b2c_assessment(combined)
    is_media_newsletter, media_evidence = media_newsletter_assessment(combined)
    is_non_english, language_evidence = language_assessment(combined)
    is_commodity_wrapper, commodity_evidence = commodity_wrapper_assessment(combined)
    is_hosting_provider, hosting_evidence = hosting_provider_assessment(
        combined
    )
    (
        is_non_operating_investor,
        investor_evidence,
    ) = non_operating_investor_assessment(combined)
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
        domain not in set(GLASSWING_PORTFOLIO_DOMAINS)
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
        "learned_positive_fit": learned_positive_fit,
        "routing_bucket": bucket,
        "routing_reason": reason,
        "viability_label": viability_label(bucket),
        "denial_confidence": denial_confidence,
        "denial_rule": denial_rule,
        "denial_evidence": denial_evidence,
        "fetch_error": page.error,
    }


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
    include_coming_soon = bool(
        preferences.get("include_coming_soon", True)
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

        if bucket == "coming_soon":
            if not include_coming_soon:
                reasons.append("coming_soon_disabled_for_run")
            pillar = str(row.get("rule_thesis_pillar", ""))
            if selected_industries and pillar not in selected_industries:
                reasons.append("outside_selected_industries")

            searchable = clean(
                " ".join([
                    str(row.get("page_title", "")),
                    str(row.get("meta_description", "")),
                    str(row.get("homepage_excerpt", "")),
                    str(row.get("prelaunch_evidence", "")),
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
                result.at[index, "routing_bucket"] = "insufficient"
                result.at[index, "viability_label"] = "Insufficient Evidence"
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
    """Apply manual decisions and human training labels as final authority."""
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
        "Coming Soon": (
            "coming_soon",
            "manual_override_coming_soon",
        ),
        "Insufficient Evidence": (
            "insufficient",
            "manual_override_insufficient",
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

    basket_targets: dict[str, tuple[str, str, int]] = {}
    for basket_path, basket, target in (
        (YES_BASKET_DATA, "yes", "Investable Candidate"),
        (NO_BASKET_DATA, "no", "Hard Denied"),
    ):
        examples = read_basket_data(basket_path)
        for _, example in examples.iterrows():
            basket_targets[stable_domain(example.get("domain_url", ""))] = (
                basket,
                target,
                int(example.get("rating", 0)),
            )

    label_metadata = read_label_metadata()
    trusted_label_domains = {
        stable_domain(row.get("domain", ""))
        for _, row in label_metadata.iterrows()
        if str(row.get("source", "")).lower()
        in {"human", "manual", "glasswing_portfolio"}
    }
    trusted_label_domains.update(
        stable_domain(domain) for domain in GLASSWING_PORTFOLIO_DOMAINS
    )

    for idx, row in result.iterrows():
        domain = stable_domain(row.get("domain", ""))
        basket_entry = basket_targets.get(domain)
        if not basket_entry:
            continue
        basket, target, rating = basket_entry
        result.at[idx, "training_basket"] = basket
        result.at[idx, "human_rating"] = rating
        if domain not in trusted_label_domains:
            continue
        if basket == "yes":
            result.at[idx, "routing_bucket"] = "priority"
            result.at[idx, "routing_reason"] = "trusted_human_training_yes"
            result.at[idx, "viability_label"] = "Investable Candidate"
        elif basket == "no":
            result.at[idx, "routing_bucket"] = "reject"
            result.at[idx, "routing_reason"] = "human_training_basket_no"
            result.at[idx, "viability_label"] = target

    return result


def persist_screening_output(output: pd.DataFrame) -> dict[str, int]:
    """Persist one in-process screening batch to all master datasets."""
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
    denied_mask = output["routing_bucket"].eq("reject")
    manual_denied = output[denied_mask & (manual_override | manual_reason)].copy()
    automated_denied = output[
        denied_mask & ~(manual_override | manual_reason)
    ].copy()
    persistable_output = output.loc[
        ~output.index.isin(automated_denied.index)
    ].copy()
    if count_classifications() == 0:
        bootstrap_from_csv()
    upsert_classifications(persistable_output)

    if not automated_denied.empty:
        # Keep only a minimal queue dedupe marker. All derived content and
        # classification payloads are deleted immediately.
        from rejection_cleanup import purge_automated_rejections

        purge_automated_rejections(
            automated_denied["domain"],
            clean_csv=False,
        )

    approved = output[
        output["routing_bucket"].isin(["priority", "coming_soon"])
    ].copy()
    denied = output[output["routing_bucket"] == "reject"].copy()
    coming_soon = output[
        output["routing_bucket"] == "coming_soon"
    ].copy()
    candidates = output[
        output["routing_bucket"] == "priority"
    ].copy()
    screened_out = output[
        output["routing_bucket"].isin(
            ["screened_out", "review", "comp", "insufficient"]
        )
    ].copy()
    ghost = output[
        output["routing_bucket"] == "ghost"
    ].copy()
    insufficient = output[
        output["routing_bucket"] == "insufficient"
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
    # SQLite is canonical. The legacy CSV is refreshed at a controlled cadence
    # for older scripts instead of rereading and rewriting 60+ MB every batch.
    exported_master = export_all_screened_csv(
        minimum_interval_seconds=300,
    )
    if exported_master is not None:
        crawled_view_counts = persist_crawled_corpus_views(
            exported_master
        )
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
        "hard_denied": len(denied),
        "insufficient_evidence": len(insufficient) + len(ghost),
        "screened_out": len(screened_out),
        "ghost": len(ghost),
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
        )
        client_timeout = aiohttp.ClientTimeout(
            total=None,
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
    # batch look like dead websites. Retry timeout-only records slowly before
    # classification or persistence; real HTTP failures are left untouched.
    retry_indexes = [
        index
        for index, result in enumerate(results)
        if "TimeoutError" in str(result.get("fetch_error", ""))
    ]
    if retry_indexes:
        retry_records = [records[index] for index in retry_indexes]
        retry_results = await scrape_rows(
            retry_records,
            min(2, max(1, int(workers))),
            max(25.0, float(timeout)),
        )
        for index, retry_result in zip(retry_indexes, retry_results):
            results[index] = retry_result

    remaining_timeouts = sum(
        "TimeoutError" in str(result.get("fetch_error", ""))
        for result in results
    )
    if len(results) >= 5 and remaining_timeouts / len(results) >= 0.8:
        raise RuntimeError(
            "systemic_scraper_timeout: refusing to persist unreliable batch"
        )

    output = pd.DataFrame(results)
    stats = (
        persist_screening_output(output)
        if persist
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
