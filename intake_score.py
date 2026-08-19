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

from certstream_filter import (
    guard_registrable,
    is_domain_too_long,
    normalize_hostname,
    reduce_hostname,
    reduce_normalized,
)

# How dense a TLD is in real companies, as a starting score. Not a gate --
# the extension allow-list still decides what is tracked at all; this only
# decides what gets looked at first when capacity is short.
# Nearly flat, and small next to the evidence terms below. An earlier version
# hand-guessed a 8..30 spread and was wrong: measured on this corpus, .com
# yields 2.02% candidates against .dev's 1.00% and .app's 0.64%, yet .com had
# been given the lowest weight of all eight and .dev the second highest.
#
# The .app number turned out not to be about .app at all -- that TLD is where
# a bulk-registration farm lives (000026js.app, 004936gg.app), so the weight
# was laundering a garbage-filtering gap into a fake fact about extensions.
# The filter now drops those names outright, which is the right place to fix
# it.
#
# What survives is a small .ai bonus, because its yield gap (7.90%) is far too
# large to be explained by noise alone -- though even that is confounded, since
# .ai arrives via CertStream while .com and .app arrive via the zone crawler,
# so the two are different acquisition channels rather than a clean comparison.
# Kept deliberately small: the TLD is a proxy, evidence of a build is a direct
# observation, and a proxy must never outweigh an observation.
TLD_WEIGHTS = {".ai": 16}
DEFAULT_TLD_WEIGHT = 10

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

# Platform certificates are ranked to the floor, not discarded. Every *name*
# rule in ``certstream_filter`` was validated against the 1,911 domains this
# corpus had already judged real candidates, and required zero losses -- a
# test that rejected three earlier versions. This rule could get no such test,
# because certificate composition was never stored, so there is no way to check
# whether a real candidate ever arrived on a cert carrying eleven apexes.
#
# An untested irreversible rule is not worth its own certainty. Sorting below
# a never-scored row (which reads as -1) means these are never fetched in
# practice, which is the same outcome a drop would produce -- but the row
# survives, ``intake_evidence`` records why, and the rule can be checked
# against real outcomes later instead of being trusted forever.
SHARED_CERT_SCORE = int(os.getenv("INTAKE_SHARED_CERT_SCORE", "-50"))

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
    # Domains ranked to the floor rather than refused. They are still in
    # ``scored`` -- this is the audit trail, not a separate outcome.
    penalized: list[str] = field(default_factory=list)
    penalty_reason: str = ""
    # Hostnames the junk/TLD filter refused. Counted here so the caller does
    # not have to run the filter a second time just to tally intake attrition.
    # Every distinct registrable domain this certificate showed us, before
    # any judgement -- the true top of the funnel.
    domains_observed: int = 0
    # Hostnames that never reduced to a domain at all (malformed, over-length,
    # bare public suffix). Not part of the domain-unit population.
    unreducible_hostnames: int = 0
    # Domains refused, by guard, in the order the guards run.
    dropped_by_guard: dict[str, int] = field(default_factory=dict)

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
    """Score every tracked domain on one certificate, counted in domains.

    Reduction happens first and guarding second, so ``domains_observed`` is
    the true top of the funnel: every distinct registrable domain this
    certificate showed us, before any judgement. Guards then run once per
    domain rather than once per hostname -- charging a domain that appears on
    ten hostnames ten times is what made the guard tallies impossible to
    reconcile against anything downstream, which counts domains.
    """
    # Normalize once, then reduce each *distinct* host once. A SAN list is
    # mostly aliases of the same name -- foo.ai, www.foo.ai and *.foo.ai all
    # normalize to foo.ai -- and the old loop paid for a full Public Suffix
    # List extraction on every one of them. Measured live, the feed delivered
    # 189,514 hostnames against 109,516 distinct domains in a single run.
    #
    # Exactly equivalent to reducing every hostname separately: the same hosts
    # produce the same registrables, and duplicates contribute their full count
    # to unreducible, so domains_observed and the guard tallies are unchanged.
    normalized: dict[str, list[str]] = {}
    unreducible = 0
    for hostname in hostnames or []:
        host = normalize_hostname(hostname)
        if not host or is_domain_too_long(host):
            # Malformed or over-length: never a domain at all, so it is not
            # part of the domain-unit population below.
            unreducible += 1
            continue
        normalized.setdefault(host, []).append(hostname)

    grouped: dict[str, list[str]] = {}
    for host, originals in normalized.items():
        registrable = reduce_normalized(host)
        if registrable:
            grouped.setdefault(registrable, []).extend(originals)
        else:
            # A bare public suffix (com.ai) has no registrable label.
            unreducible += len(originals)

    observed = len(grouped)
    if not grouped:
        return CertificateReading(
            domains_observed=0, unreducible_hostnames=unreducible
        )

    drop_counts: dict[str, int] = {}
    kept: dict[str, list[str]] = {}
    for registrable, hosts in grouped.items():
        reason = guard_registrable(registrable, allowed_extensions)
        if reason:
            drop_counts[reason] = drop_counts.get(reason, 0) + 1
        else:
            kept[registrable] = hosts

    if len(kept) > max_apexes:
        reason = f"shared_cert:{len(kept)}_apexes"
        return CertificateReading(
            scored=[
                ScoredDomain(domain=d, score=SHARED_CERT_SCORE, evidence=reason)
                for d in sorted(kept)
            ],
            penalized=sorted(kept),
            penalty_reason=reason,
            domains_observed=observed,
            unreducible_hostnames=unreducible,
            dropped_by_guard=drop_counts,
        )

    scored: list[ScoredDomain] = []
    for registrable, hosts in sorted(kept.items()):
        labels: set[str] = set()
        has_wildcard = False
        for host in hosts:
            parts = subdomain_labels(host, registrable)
            if parts == ("*",):
                has_wildcard = True
                continue
            labels.update(parts)

        product = sorted(labels & PRODUCT_LABELS)
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
            reasons.append("bare_registration")

        scored.append(ScoredDomain(
            domain=registrable,
            score=max(0, score),
            evidence=" ".join(reasons)[:200],
            labels=tuple(sorted(labels)),
        ))

    return CertificateReading(
        scored=scored,
        domains_observed=observed,
        unreducible_hostnames=unreducible,
        dropped_by_guard=drop_counts,
    )
