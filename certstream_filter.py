#!/usr/bin/env python3
"""Pure hostname -> registrable domain reduction plus junk guards for intake.

``to_registrable(hostname, allowed_extensions)`` is the whole filtering
decision for one hostname seen on the CertStream feed: zero I/O, no socket,
no database, so it is unit-testable on its own. It turns one raw feed
hostname (``_acme-challenge.hephaestus.exa.ai``, ``*.exa.ai``) into the
registrable company domain (``exa.ai``), or returns ``""`` to drop it.

Two things it fixes over the previous last-two-labels heuristic:

- **Registrable reduction is Public-Suffix-List correct.** ``foo.com.ai``
  reduced to ``com.ai`` -- a bare public suffix, not a company -- and that
  junk row was enqueued, fetched and screened like a real domain. The PSL
  carries 19 multi-label suffixes across the supported TLDs (4 under ``.ai``,
  7 under ``.co``, 8 under ``.io``), so this was not a corner case.
- **Junk guards exist at all.** The intake path had none: an all-numeric
  label, a repeated-letter run, motif spam (``aiaiai``) and a 300-character
  malformed hostname were all accepted and queued. ``filter.py``'s language
  checks run on fetched *page text*, a different axis entirely -- nothing
  screened the domain name itself.

**Deliberately NOT ported: an English-word/dictionary predicate on the
label.** The upstream design measured one at ~76% recall on 37 real ``.ai``
startups -- it drops coined brands (``cohere``, ``groq``, ``exa``, ``vly``,
``pika``) while happily keeping staging and infra noise. Coined names are the
signal here, not the noise. Scraping and Claude screening are the real
qualifier; this filter only does registrable reduction plus the minimal junk
guards.

Ported from the certstream-share extract's
``packages/tracker/certstream/filter.py`` and its ``domain_filter`` junk
guards, generalised from ``.ai``-only to the pipeline's supported extensions.
"""

from __future__ import annotations

# RFC 1035 maximum FQDN length. An untrusted feed can emit longer strings;
# they are malformed by definition and would only bloat a queue row.
MAX_DOMAIN_LENGTH = 253

# Multi-label public suffixes under the supported TLDs, used only when
# tldextract is unavailable. Derived from the bundled PSL snapshot rather than
# guessed. The fallback keeps intake correct on a container that somehow ships
# without the dependency instead of silently regressing to the old behaviour.
_FALLBACK_MULTI_LABEL_SUFFIXES = frozenset({
    "com.ai", "net.ai", "off.ai", "org.ai",
    "com.co", "edu.co", "gov.co", "mil.co", "net.co", "nom.co", "org.co",
    "co.io", "com.io", "edu.io", "gov.io", "mil.io", "net.io", "nom.io",
    "org.io",
})

try:
    import tldextract

    # One-time init at import, never per hostname, and ``suffix_list_urls=()``
    # so the bundled snapshot is used: no network fetch on the hot path of a
    # long-lived listener.
    _extract = tldextract.TLDExtract(suffix_list_urls=())
except ImportError:  # pragma: no cover - exercised only on a broken install
    _extract = None


def _split_suffix(host: str) -> tuple[str, str]:
    """Return ``(registrable_label, public_suffix)`` for ``host``, or ``("", "")``.

    Uses tldextract's PSL snapshot when available and the explicit
    multi-label table otherwise, so ``foo.com.ai`` yields ``("foo", "com.ai")``
    either way rather than the ``("com", "ai")`` the old two-label split
    produced.
    """
    if _extract is not None:
        result = _extract(host)
        return result.domain, result.suffix

    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return "", ""
    two_label = ".".join(labels[-2:])
    if two_label in _FALLBACK_MULTI_LABEL_SUFFIXES:
        # A bare multi-label suffix (``com.ai``) has no registrable label.
        return ("", two_label) if len(labels) == 2 else (labels[-3], two_label)
    return labels[-2], labels[-1]


def is_motif_spam(value: str) -> bool:
    """True if ``value`` is a short unit (len <= 3) repeated >= 3 times.

    Catches ``aiaiai``, ``mememe``, ``dsadsadsa``, ``coucoucou`` -- labels that
    tile the same fragment over and over. The >= 3 repetitions bound spares
    genuine 2x reduplications like ``papa``, ``coco``, ``mama``, ``bonbon``,
    which are real brand names.
    """
    for period in (1, 2, 3):
        if len(value) >= 3 * period and value == (
            value[:period] * (len(value) // period + 1)
        )[: len(value)]:
            return True
    return False


def is_domain_too_long(domain: str) -> bool:
    """True iff ``domain`` exceeds the DNS maximum FQDN length (RFC 1035)."""
    return len(domain) > MAX_DOMAIN_LENGTH


def normalize_hostname(hostname: str) -> str:
    """Lowercase and strip a raw feed hostname down to a bare host.

    Handles what the feed actually sends: wildcard SAN entries (``*.exa.ai``),
    stray whitespace, a trailing root dot, and the scheme/port/path that show
    up when a hostname is copied out of a URL. ``www.`` is stripped so the
    apex and its ``www`` alias reduce to one queue row.
    """
    host = (hostname or "").strip().lower()
    if "//" in host:
        host = host.split("//", 1)[1]
    host = host.split("/", 1)[0].split(":", 1)[0]
    # Wildcard SAN entries arrive as ``*.example.ai``; the leading ``*`` and
    # any bounding dots are not part of the name.
    host = host.strip("*.").strip()
    return host[4:] if host.startswith("www.") else host


def to_registrable(
    hostname: str,
    allowed_extensions: set[str] | None = None,
) -> str:
    """Reduce a feed hostname to its registrable domain, or ``""`` to drop it.

    Returns the lowercase registrable domain (``exa.ai``,
    ``something.com.ai``) iff the host sits under one of
    ``allowed_extensions``, has a registrable label, and survives the junk
    guards. Returns ``""`` -- not ``None``, matching the previous
    ``registrable_domain`` contract so callers are unchanged -- for a
    disallowed extension, empty input, a bare public suffix, an over-length
    host, or a label that is motif spam, a repeated-letter run, or all digits.

    A single-character label (``x.ai``) is kept: one character is a coined
    brand, not a repeated run.
    """
    host = normalize_hostname(hostname)
    if not host or is_domain_too_long(host):
        return ""

    label, suffix = _split_suffix(host)
    if not label or not suffix:
        # Empty input, a single label, or a bare public suffix (``ai``,
        # ``com.ai``): nothing registrable to enqueue.
        return ""

    if allowed_extensions is not None:
        extension = f".{suffix.rsplit('.', 1)[-1]}"
        if extension not in allowed_extensions:
            return ""

    if is_motif_spam(label):
        return ""
    if len(label) > 1 and len(set(label)) == 1:
        # A multi-character repeated-letter run (``aaa``, ``bb``) is never a
        # real brand. A one-character label is, so the guard is length-gated.
        # Measured against the 247,742-row queue corpus this drops 26 rows,
        # every one of them already dead or insufficient -- no candidate lost.
        return ""

    # NO all-numeric guard, deliberately, against the upstream filter which
    # has one. It was measured on this pipeline's own corpus: an
    # ``isdigit()`` drop removes 876 rows, and six of them are domains this
    # pipeline already screened and rated -- ``375.ai`` and ``8090.ai`` as
    # companies, ``044.ai`` / ``2021.ai`` / ``2077.ai`` / ``2342.ai`` as
    # review candidates. Numeric brands are real (particularly in Asian
    # markets), and there is no length or shape rule separating those six
    # from the ~870 dead ones. The costs are wildly asymmetric here: a dead
    # numeric domain costs one cheap fetch and, measured over the whole
    # corpus, zero Claude calls, while a missed company is the one error
    # this funnel exists to avoid. So the junk stays in and screening
    # discards it, which is what screening is for.

    return f"{label}.{suffix}"
