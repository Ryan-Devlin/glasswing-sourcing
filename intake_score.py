#!/usr/bin/env python3
"""Score a certificate at intake, before anything is fetched.

CertStream delivers one certificate per frame as its full SAN list. The
pipeline used to reduce that to a set of bare apexes and throw the rest away,
then decide what to crawl by recency (``ORDER BY first_seen DESC``). Recency is
uncorrelated with whether a domain is a company, and arrival runs about four
times faster than screening capacity -- so which quarter of arrivals gets
fetched was effectively arbitrary, and that choice determines everything
downstream.

The certificate is the best free predictor available at time zero:

- **Subdomain structure is build maturity.** ``foo.ai`` + ``www.foo.ai`` means
  somebody registered a domain. ``foo.ai`` + ``app.foo.ai`` + ``api.foo.ai``
  means somebody has engineers shipping an application. That is the difference
  between a parked name and a company, visible before a single fetch.
- **Distinct apexes on one certificate is platform detection.** Forty
  unrelated registrable domains on one cert is a hosting provider's shared
  certificate, not a company -- and every apex on it used to be queued as an
  equal peer.
- **TLD is a prior, not a gate.** ``.ai`` and ``.com`` are not equally dense.

**Scores are meant to be re-applied on every observation, taking the maximum.**
A domain that is a bare registration today scores low; when its founders ship
and that provisions ``app.`` and ``api.`` subdomains, the next certificate
scores higher and the domain climbs the queue on its own. The ranking tracks
what companies actually do, which is why nothing here needs a revisit timer.

Zero I/O and no database: the whole ranking policy is a pure function of one
certificate's hostname list.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from certstream_filter import normalize_hostname, to_registrable

# How dense a TLD is in real companies, as a starting score. Not a gate --
# the extension allow-list still decides what is tracked at all; this only
# decides what gets looked at first when capacity is short.
TLD_WEIGHTS = {
    ".ai": 30, ".dev": 24, ".io": 22, ".app": 20,
    ".tech": 14, ".xyz": 12, ".co": 10, ".com": 8,
}
DEFAULT_TLD_WEIGHT = 8

# Subdomains that mean somebody built something. An application, an API, a
# dashboard or an auth flow is engineering work; it does not appear on a
# domain that was bought and parked.
PRODUCT_LABELS = frozenset({
    "app", "api", "dashboard", "console", "portal", "platform", "studio",
    "admin", "auth", "account", "accounts", "login", "dev", "staging",
    "stage", "beta", "demo", "docs", "developer", "developers", "sandbox",
    "gateway", "graphql", "ws", "socket", "webhook", "webhooks",
})

# Present on essentially every certificate and therefore worth nothing as a
# discriminator. Listed explicitly so they are never mistaken for product
# structure, and so a cert of pure infrastructure scores as the bare
# registration it is.
NEUTRAL_LABELS = frozenset({
    "www", "mail", "email", "smtp", "imap", "mx", "cdn", "static", "assets",
    "img", "images", "media", "ns", "ns1", "ns2", "autodiscover",
    "autoconfig", "_acme-challenge", "_dmarc", "_domainkey",
})

# Above this many distinct registrable domains on a single certificate, the
# certificate belongs to a platform rather than a company. Deliberately
# generous: a real company legitimately holds a handful of domains (.com plus
# .ai plus a redirect), and dropping at intake is irreversible -- an apex
# never queued cannot be recovered later.
MAX_APEXES_PER_CERT = max(2, int(os.getenv("INTAKE_MAX_APEXES_PER_CERT", "10")))

_PRODUCT_POINTS = 8
_PRODUCT_CAP = 32
_BREADTH_POINTS = 2
_BREADTH_CAP = 12
_WILDCARD_POINTS = 6


@dataclass(frozen=True)
class ScoredDomain:
    domain: str
    score: int
    evidence: str = ""
    labels: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CertificateReading:
    """What one certificate says about the domains on it."""

    scored: list[ScoredDomain] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    drop_reason: str = ""
    # Hostnames the junk/TLD filter refused. Counted here so the caller does
    # not have to run the filter a second time just to tally intake attrition.
    filtered_hostnames: int = 0

    @property
    def domains(self) -> list[str]:
        return [entry.domain for entry in self.scored]


def tld_weight(domain: str) -> int:
    suffix = domain.rsplit(".", 1)[-1] if "." in domain else ""
    return TLD_WEIGHTS.get(f".{suffix}", DEFAULT_TLD_WEIGHT)


def subdomain_labels(hostname: str, registrable: str) -> tuple[str, ...]:
    """The labels sitting in front of the registrable domain, lowercased.

    ``api.v2.foo.ai`` against ``foo.ai`` yields ``("api", "v2")``. A wildcard
    entry yields ``("*",)`` so callers can tell it apart from a bare apex.
    """
    host = normalize_hostname(hostname)
    raw = (hostname or "").strip().lower().rstrip(".")
    if raw.startswith("*."):
        return ("*",)
    if not host or not registrable or host == registrable:
        return ()
    if not host.endswith("." + registrable):
        return ()
    return tuple(part for part in host[: -(len(registrable) + 1)].split(".") if part)


def read_certificate(
    hostnames: list[str],
    allowed_extensions: set[str] | None = None,
    *,
    max_apexes: int = MAX_APEXES_PER_CERT,
) -> CertificateReading:
    """Score every tracked domain on one certificate, or drop the whole cert.

    Returns a reading rather than a bare mapping so the caller can record why
    something was refused. Junk filtering stays in ``certstream_filter``; this
    only ranks what survives it and refuses platform certificates.
    """
    grouped: dict[str, list[str]] = {}
    filtered = 0
    for hostname in hostnames or []:
        registrable = to_registrable(hostname, allowed_extensions)
        if registrable:
            grouped.setdefault(registrable, []).append(hostname)
        else:
            filtered += 1

    if not grouped:
        return CertificateReading(filtered_hostnames=filtered)

    if len(grouped) > max_apexes:
        # A platform's shared certificate. Every apex on it used to be queued
        # as an equal peer, which is a large and invisible noise source at the
        # very top of the funnel.
        return CertificateReading(
            dropped=sorted(grouped),
            drop_reason=f"shared_cert:{len(grouped)}_apexes",
            filtered_hostnames=filtered,
        )

    scored: list[ScoredDomain] = []
    for registrable, hosts in sorted(grouped.items()):
        labels: set[str] = set()
        has_wildcard = False
        for host in hosts:
            parts = subdomain_labels(host, registrable)
            if parts == ("*",):
                has_wildcard = True
                continue
            labels.update(parts)

        product = sorted(labels & PRODUCT_LABELS)
        # Anything that is neither a known product label nor boilerplate still
        # counts as structure -- a company's own naming ("tenant", "eu-west")
        # is evidence of a build even when it is not on the list.
        custom = sorted(labels - PRODUCT_LABELS - NEUTRAL_LABELS)

        score = tld_weight(registrable)
        reasons = [f"tld:{score}"]

        product_points = min(len(product) * _PRODUCT_POINTS, _PRODUCT_CAP)
        if product_points:
            score += product_points
            reasons.append(f"product[{','.join(product)}]:+{product_points}")

        custom_points = min(len(custom) * (_PRODUCT_POINTS // 2), _PRODUCT_CAP // 2)
        if custom_points:
            score += custom_points
            reasons.append(f"custom[{','.join(custom[:4])}]:+{custom_points}")

        breadth = min(len(hosts) * _BREADTH_POINTS, _BREADTH_CAP)
        if breadth:
            score += breadth
            reasons.append(f"sans:{len(hosts)}:+{breadth}")

        if has_wildcard:
            score += _WILDCARD_POINTS
            reasons.append(f"wildcard:+{_WILDCARD_POINTS}")

        if not product and not custom:
            # Apex plus boilerplate only: a domain somebody registered, with
            # no evidence anyone has built on it yet. Kept, ranked last -- it
            # is the population this funnel exists to watch, and a later
            # certificate will re-score it the moment that changes.
            reasons.append("bare_registration")

        scored.append(
            ScoredDomain(
                domain=registrable,
                score=max(0, score),
                evidence=" ".join(reasons)[:200],
                labels=tuple(sorted(labels)),
            )
        )

    return CertificateReading(scored=scored, filtered_hostnames=filtered)
