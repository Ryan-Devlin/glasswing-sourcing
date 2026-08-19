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

import re
from functools import lru_cache

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


def tld_of(host: str) -> str:
    """The extension a host will be judged on, without touching the PSL.

    Exactly what :func:`guard_registrable` derives from the PSL result: it
    takes the last label of the public suffix, and the last label of a public
    suffix is always the last label of the host. ``foo.com.ai`` gives ``.ai``
    either way. Having it as a bare string operation is what lets the
    extension gate run before the PSL lookup rather than after it.
    """
    # A dotless label has no extension at all: rpartition puts the whole
    # string in the tail when there is no separator, so ``bare`` would
    # otherwise read as the extension ``.bare``.
    _, separator, last = (host or "").rpartition(".")
    return f".{last}" if separator and last else ""


# The PSL split is a pure function of the host, and the feed repeats hosts
# constantly -- certificate renewals, multi-SAN certs, and the same domain
# reappearing across frames. Measured live, 283 of 483 hostnames surviving the
# extension gate in one run were domains already seen. Caching the split turns
# those into a dict hit.
#
# Bounded on purpose: this runs inside a long-lived listener with an RSS
# recycle threshold, and an unbounded cache on an unbounded input stream is a
# memory leak with a slow fuse.
@lru_cache(maxsize=65536)
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


# Machine-generated labels. Measured on the 247,742-row queue, 7.83% of rows
# were bulk-registration noise that every guard above waved through --
# 000026js.app, 0003939pgl1.app, 004936gg.app, 00097lottery.app -- almost all
# of it on one TLD, which was quietly making that TLD look unproductive.
#
# Dropping at intake is irreversible, so these two rules are deliberately
# narrow and each needs real length before it fires:
#
#   * Digit-dominated labels of six characters or more. The length floor is
#     what keeps real numeric brands: 375.ai and 8090.ai are both companies
#     in this corpus, and an earlier all-numeric guard would have discarded
#     them, so short digit labels stay.
#   * Long labels with no pronounceable structure -- a vowel-starved run or a
#     six-consonant pile-up in a name of ten characters or more. Coined
#     brands are short by design (groq, exa, vly, qdrant), so the floor keeps
#     them well clear of this.
_VOWELS = frozenset("aeiouy")
_MIN_DIGIT_HEAVY_LEN = 6
_MAX_DIGIT_RATIO = 0.5
_MAX_DIGIT_RUN = 5
_MIN_UNPRONOUNCEABLE_LEN = 10
_MAX_CONSONANT_RUN = 6
_MIN_VOWEL_RATIO = 0.18


def is_machine_generated(label: str) -> bool:
    """True for labels that read as bulk-registration output, not a name."""
    core = re.sub(r"[^a-z0-9]", "", (label or "").lower())
    if len(core) < _MIN_DIGIT_HEAVY_LEN:
        # Too short to judge, and short is where the real coined brands and
        # numeric brands live.
        return False

    # A plain digit ratio is the wrong test. Checked against this corpus, a
    # 0.4 ratio discarded seven domains the pipeline had already judged real
    # candidates -- 387labs.ai, eco360.ai, 101gen.ai, gpt7prompts.ai -- because
    # pairing a small number with a word is ordinary branding. The farm names
    # differ structurally, not statistically: they carry leading-zero counters
    # (000026js, 004936gg, 0003939pgl1) or long digit runs, which no brand
    # does.
    if re.match(r"0\d{2,}", core):
        return True
    if max((len(run) for run in re.findall(r"\d+", core)), default=0) >= _MAX_DIGIT_RUN:
        return True
    digits = sum(character.isdigit() for character in core)
    if len(core) >= _MIN_UNPRONOUNCEABLE_LEN and digits / len(core) >= _MAX_DIGIT_RATIO:
        return True

    if len(core) < _MIN_UNPRONOUNCEABLE_LEN:
        return False

    # No vowel-ratio rule. It was redundant -- every random string it caught
    # (x7k2mqp9wzvnbhdlrs, kjhgfdsazxcvbnm) is already caught by the
    # consonant-run test below -- and it was the one heuristic producing a
    # false positive against a real candidate: gpt7prompts.ai reduces to
    # "gptprompts", one vowel in ten letters. Acronym-dense names are exactly
    # what this sector produces, so vowel scarcity is not evidence of a machine.
    runs = re.findall(r"[^aeiouy0-9]+", core)
    return max((len(run) for run in runs), default=0) >= _MAX_CONSONANT_RUN


# A hyphen-delimited number sitting between two words is a template, not a
# name: auth-8510-system.ai, private-1971-system.ai, admin-7840-system.ai,
# api-2821-system.ai -- one generator, thousands of domains, all of it on the
# best-yielding TLD in the corpus.
#
# Matched structurally rather than by entropy, because entropy cannot separate
# this farm from real names. Loosening the vowel and consonant thresholds far
# enough to catch it also discarded gr8rthan.ai ("greater than"),
# apachealthgroup.com ("APA Health Group") and trendsspotter.ai. Requiring the
# digits to be delimited by hyphens on *both* sides is what makes it precise:
# a brand may contain digits (gr8rthan, a16z, web3auth, n8n) but essentially
# never as a standalone hyphen-fenced number between two words.
_TEMPLATED_NUMBER = re.compile(r"(?:^|-)\d{2,6}-(?=[a-z])")


def is_templated_bulk_name(label: str) -> bool:
    """True for names built by a generator from a word/number/word template."""
    text = (label or "").lower()
    if "-" not in text:
        return False
    return bool(_TEMPLATED_NUMBER.search(text))


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


# The guards, in evaluation order. Each one only ever sees what the previous
# guards passed, so counting them in this order gives a real filter funnel
# rather than a set of overlapping tallies.
DROP_REASONS = (
    "empty",
    "too_long",
    "no_registrable_label",
    "extension_not_tracked",
    "motif_spam",
    "repeated_run",
    "machine_generated",
    "templated_bulk",
)


def reduce_hostname(hostname: str) -> str:
    """Hostname to its registrable domain, with no judgement applied.

    Separate from the guards on purpose. Everything downstream counts
    domains, so the guards must count domains too -- applied per hostname
    they charge a domain appearing on ten certificates ten times, and the
    funnel's top can never reconcile with its stages.
    """
    host = normalize_hostname(hostname)
    if not host or is_domain_too_long(host):
        return ""
    return reduce_normalized(host)


def reduce_normalized(host: str) -> str:
    """Reduce a host that has already been normalized and length-checked.

    Split out so the callers that have to normalize anyway -- to read the
    extension, or to check the length -- do not pay for a second pass over the
    same string. ``classify_hostname`` used to normalize, then call
    ``reduce_hostname``, which normalized again.
    """
    label, suffix = _split_suffix(host)
    if not label or not suffix:
        return ""
    return f"{label}.{suffix}"


def guard_registrable(
    domain: str,
    allowed_extensions: set[str] | None = None,
) -> str:
    """Return the guard that refuses ``domain``, or ``""`` if it passes.

    Guards in the order they run; each only ever sees what the previous
    passed, which is what makes the tally a funnel rather than a set of
    overlapping counts.
    """
    if not domain:
        return "empty"
    label, _, suffix = domain.partition(".")
    if not suffix:
        return "no_registrable_label"

    if allowed_extensions is not None:
        extension = f".{suffix.rsplit('.', 1)[-1]}"
        if extension not in allowed_extensions:
            return "extension_not_tracked"

    if is_motif_spam(label):
        return "motif_spam"
    if len(label) > 1 and len(set(label)) == 1:
        return "repeated_run"
    if is_machine_generated(label):
        return "machine_generated"
    if is_templated_bulk_name(label):
        return "templated_bulk"
    return ""


def classify_hostname(
    hostname: str,
    allowed_extensions: set[str] | None = None,
) -> tuple[str, str]:
    """Reduce a hostname and say which guard refused it, if one did.

    Kept as the single-hostname entry point; callers that need domain-unit
    counting should reduce first and guard the distinct domains.
    """
    host = normalize_hostname(hostname)
    if not host:
        return "", "empty"
    if is_domain_too_long(host):
        return "", "too_long"
    # Deliberately NOT gated on the extension here, though tld_of would let
    # it skip the Public Suffix List for 99% of the feed. Doing so reorders
    # the guards: a bare public suffix on an untracked extension would report
    # extension_not_tracked where it now reports no_registrable_label, and
    # DROP_REASONS is the order the funnel renders. The saving belongs in
    # read_certificate, which is what the listener actually calls and which
    # counts in domains rather than hostnames.
    domain = reduce_normalized(host)
    if not domain:
        return "", "no_registrable_label"
    reason = guard_registrable(domain, allowed_extensions)
    return ("", reason) if reason else (domain, "")


def to_registrable(
    hostname: str,
    allowed_extensions: set[str] | None = None,
) -> str:
    """Reduce a feed hostname to its registrable domain, or ``""`` to drop it.

    Returns the lowercase registrable domain (``exa.ai``,
    ``something.com.ai``) iff the host sits under one of
    ``allowed_extensions``, has a registrable label, and survives every guard.
    Returns ``""`` -- not ``None``, matching the previous ``registrable_domain``
    contract so callers are unchanged. See :func:`classify_hostname` for which
    guard refused it.

    A single-character label (``x.ai``) is kept: one character is a coined
    brand, not a repeated run.
    """
    return classify_hostname(hostname, allowed_extensions)[0]
