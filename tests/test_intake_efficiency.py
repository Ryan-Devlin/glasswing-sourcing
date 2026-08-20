"""The intake hot path does the same work with less of it.

Every change here has to be output-identical. This is the code the listener
runs inside its exception handler, where anything that throws is caught as a
connection fault and turns into a silent reconnect storm -- that has happened
three times in this pipeline. So the tests are about equivalence first and
saved work second.
"""

import certstream_filter as cf
from intake_score import read_certificate

TRACKED = {".ai", ".app"}


def test_tld_of_matches_what_the_guard_derives():
    """tld_of has to agree with guard_registrable's own extension, including
    on multi-label public suffixes, or gating on it would silently change
    which domains are tracked."""
    for host, expected in (
        ("exa.ai", ".ai"),
        ("foo.com.ai", ".ai"),
        ("thing.co.uk", ".uk"),
        ("bare", ""),
        ("", ""),
    ):
        assert cf.tld_of(host) == expected, host

    for domain in ("exa.ai", "foo.com.ai", "thing.app"):
        blocked = cf.guard_registrable(domain, {".nope"})
        allowed = cf.guard_registrable(domain, {cf.tld_of(domain)})
        assert blocked == "extension_not_tracked", domain
        assert allowed != "extension_not_tracked", domain


def test_reduce_normalized_matches_reduce_hostname():
    for hostname in (
        "*.exa.ai", "www.exa.ai", "api.foo.com.ai", "_acme-challenge.a.b.app",
        "EXA.AI", "https://exa.ai/path", "exa.ai:443", "com.ai", "x.ai",
    ):
        host = cf.normalize_hostname(hostname)
        direct = cf.reduce_hostname(hostname)
        if not host or cf.is_domain_too_long(host):
            assert direct == ""
            continue
        assert cf.reduce_normalized(host) == direct, hostname


def test_deduplicating_a_san_list_changes_nothing():
    """A certificate listing the same name many ways must read identically to
    one listing it once -- same domains_observed, same guard tally."""
    one = read_certificate(["foo.ai"], TRACKED)
    many = read_certificate(
        ["foo.ai", "www.foo.ai", "*.foo.ai", "FOO.AI", "foo.ai."], TRACKED,
    )
    assert many.domains_observed == one.domains_observed == 1
    assert many.dropped_by_guard == one.dropped_by_guard
    assert [s.domain for s in many.scored] == [s.domain for s in one.scored]


def test_unreducible_still_counts_every_hostname_not_just_distinct_ones():
    """The dedup must not quietly turn three junk hostnames into one. The
    count is of hostnames, and the funnel reconciles against it."""
    reading = read_certificate(["com.ai", "com.ai", "net.ai"], TRACKED)
    assert reading.unreducible_hostnames == 3
    assert reading.domains_observed == 0


def test_over_length_hostnames_are_still_unreducible():
    too_long = ("a" * 60 + ".") * 5 + "ai"
    reading = read_certificate([too_long, "foo.ai"], TRACKED)
    assert reading.unreducible_hostnames == 1
    assert reading.domains_observed == 1


def test_the_suffix_split_is_cached_and_bounded():
    """Unbounded caching on an unbounded input stream is a memory leak with a
    slow fuse, inside a worker that already has an RSS recycle threshold."""
    info = cf._split_suffix.cache_info()
    assert info.maxsize is not None and info.maxsize > 0
    cf._split_suffix("cache-probe.ai")
    before = cf._split_suffix.cache_info().hits
    cf._split_suffix("cache-probe.ai")
    assert cf._split_suffix.cache_info().hits == before + 1


def test_repeated_hosts_do_not_re_enter_the_suffix_list():
    """The saving this exists for: a SAN list of aliases costs one PSL
    lookup, not one per alias."""
    cf._split_suffix.cache_clear()
    read_certificate(
        ["a.dedup-probe.ai", "b.dedup-probe.ai", "c.dedup-probe.ai",
         "dedup-probe.ai", "www.dedup-probe.ai"],
        TRACKED,
    )
    misses = cf._split_suffix.cache_info().misses
    # Four distinct normalized hosts (www collapses onto the apex), so at most
    # four trips to the PSL for five hostnames.
    assert misses <= 4


def test_guard_tallies_stay_in_domain_units():
    """Two hostnames on one untracked domain is one drop, not two -- the
    invariant that lets the funnel's top line reconcile with its stages."""
    reading = read_certificate(
        ["shop.example.com", "www.example.com", "example.com"], TRACKED,
    )
    assert reading.domains_observed == 1
    assert reading.dropped_by_guard == {"extension_not_tracked": 1}


def test_a_tripled_letter_beside_a_counter_is_refused():
    """Observed live as a sequential run of .app registrations reaching Needs
    Review: zzzb1085, zzzb1086, zzzb1087, zzzb1089 ... zzzb1099, plus
    zzzbvip764, pppp128, kppp698, heee555. None of the existing guards saw
    them -- the repeated-run test needs the *whole* label to be one character,
    and a four-digit counter sits under the five-digit run threshold.
    """
    for domain in (
        "zzzb1653.app", "zzzbvip764.app", "zzzb1085.app", "zzzb1099.app",
        "pppp128.xyz", "kppp698.xyz", "heee555.app", "ycyyy888.xyz",
    ):
        assert cf.guard_registrable(
            domain, {".app", ".ai", ".xyz"}
        ) == "machine_generated", domain


def test_it_takes_both_halves_to_be_a_farm_name():
    """The reason this is safe. A tripled letter alone would refuse a
    plausible coined brand; a counter alone is ordinary branding, and refusing
    it is the mistake this module already made once and fixed. Checked against
    the live corpus, zero of the 28,762 rows scoring 25 or better are refused.
    """
    for domain in (
        "buzzz.ai",       # tripled letter, no counter
        "eco360.ai",      # counter, no tripled letter
        "387labs.ai",
        "101gen.ai",
        "gpt7prompts.ai",
        "figma.ai",
    ):
        assert cf.guard_registrable(domain, {".app", ".ai"}) == "", domain
