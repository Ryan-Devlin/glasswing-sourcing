"""Behaviour of the pure CertStream intake filter.

No socket, no database: ``to_registrable`` is zero-I/O, so every intake
filtering decision is asserted directly here rather than inferred from a live
run.
"""

import unittest

from certstream_filter import (
    is_domain_too_long,
    is_motif_spam,
    normalize_hostname,
    to_registrable,
)
from collector import SUPPORTED_EXTENSIONS, registrable_domain

ALL = SUPPORTED_EXTENSIONS


class RegistrableReductionTests(unittest.TestCase):
    def test_reduces_infra_subdomains_to_the_company_domain(self):
        for hostname, expected in {
            "_acme-challenge.hephaestus.exa.ai": "exa.ai",
            "*.exa.ai": "exa.ai",
            "pr-preview-123.staging.exa.ai": "exa.ai",
            "EXA.AI": "exa.ai",
            "  exa.ai  ": "exa.ai",
            "exa.ai.": "exa.ai",
            "www.example.ai": "example.ai",
            "https://api.thing.io/some/path": "thing.io",
        }.items():
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), expected)

    def test_multi_label_public_suffixes_keep_their_registrable_label(self):
        # The regression this port exists for: the old last-two-labels split
        # reduced these to the bare public suffix (``com.ai``), enqueuing a
        # non-company row that was then fetched and screened like a startup.
        for hostname, expected in {
            "foo.com.ai": "foo.com.ai",
            "api.my-startup.net.ai": "my-startup.net.ai",
            "shop.brand.com.co": "brand.com.co",
            "deep.sub.co.io": "sub.co.io",
        }.items():
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), expected)

    def test_bare_public_suffix_is_dropped(self):
        for hostname in ["ai", "com.ai", "net.ai", "co.io", "com.co", "io"]:
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), "")


class CoinedBrandTests(unittest.TestCase):
    def test_coined_brands_survive(self):
        # The upstream design measured an English-word predicate at ~76%
        # recall on real .ai startups precisely because it discards these.
        # Nothing in this filter may reject a label for not being a word.
        for brand in [
            "exa.ai", "vly.ai", "groq.ai", "cohere.ai", "pika.ai",
            "qdrant.io", "modal.com", "vercel.app",
        ]:
            with self.subTest(brand=brand):
                self.assertEqual(to_registrable(brand, ALL), brand)

    def test_single_character_label_is_kept(self):
        # x.ai is xAI. One character is a coined brand, not a repeated run,
        # so the repeated-letter guard must be length-gated.
        self.assertEqual(to_registrable("x.ai", ALL), "x.ai")
        self.assertEqual(to_registrable("api.x.ai", ALL), "x.ai")

    def test_two_fold_reduplication_is_kept(self):
        # papa/coco/bonbon are real names; only >= 3 repetitions is spam.
        for brand in ["papa.ai", "coco.ai", "bonbon.ai"]:
            with self.subTest(brand=brand):
                self.assertEqual(to_registrable(brand, ALL), brand)


class JunkGuardTests(unittest.TestCase):
    def test_motif_spam_is_dropped(self):
        for hostname in ["aiaiai.ai", "mememe.ai", "dsadsadsa.ai", "coucoucou.ai"]:
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), "")

    def test_repeated_letter_runs_are_dropped(self):
        for hostname in ["aaa.ai", "bb.ai", "zzzzzz.io"]:
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), "")

    def test_all_numeric_labels_are_kept(self):
        # Deliberately divergent from the upstream filter. Measured on this
        # pipeline's 247k-row queue corpus, an all-numeric drop would have
        # discarded six domains it had already screened and rated -- 375.ai
        # and 8090.ai as companies, 044.ai/2021.ai/2077.ai/2342.ai as review
        # candidates -- to save ~870 dead rows that cost zero Claude calls.
        for hostname in ["375.ai", "8090.ai", "2021.ai", "12345.ai", "007.io"]:
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), hostname)

    def test_repeated_digit_runs_are_still_dropped(self):
        # 00.ai / 11.ai / 88.io are all dead or insufficient in the corpus,
        # so the repeated-run guard stays even though it is digit-shaped.
        for hostname in ["00.ai", "11.ai", "88.io"]:
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), "")

    def test_over_length_hostnames_are_dropped(self):
        self.assertTrue(is_domain_too_long("a" * 254))
        self.assertFalse(is_domain_too_long("a" * 253))
        self.assertEqual(to_registrable("a" * 300 + ".ai", ALL), "")

    def test_empty_and_single_label_input_is_dropped(self):
        for hostname in ["", "   ", "localhost", ".", "*."]:
            with self.subTest(hostname=hostname):
                self.assertEqual(to_registrable(hostname, ALL), "")

    def test_motif_spam_bounds(self):
        self.assertTrue(is_motif_spam("aiaiai"))
        self.assertTrue(is_motif_spam("aaa"))
        self.assertFalse(is_motif_spam("papa"))
        self.assertFalse(is_motif_spam("exa"))


class ExtensionGateTests(unittest.TestCase):
    def test_disallowed_extension_is_dropped(self):
        self.assertEqual(to_registrable("example.com", {".ai"}), "")
        self.assertEqual(to_registrable("www.bluewater.co.uk", ALL), "")
        self.assertEqual(to_registrable("thing.net", ALL), "")

    def test_gate_reads_the_public_suffix_not_the_last_label(self):
        # ``foo.com.ai`` is a .ai domain; gating on the last label of the
        # *suffix* keeps it, and keeps ``foo.com.co`` out of a .com-only run.
        self.assertEqual(to_registrable("foo.com.ai", {".ai"}), "foo.com.ai")
        self.assertEqual(to_registrable("foo.com.co", {".com"}), "")
        self.assertEqual(to_registrable("foo.com.co", {".co"}), "foo.com.co")

    def test_none_allows_every_extension(self):
        self.assertEqual(to_registrable("example.net", None), "example.net")


class NormalizeTests(unittest.TestCase):
    def test_strips_wildcard_scheme_port_and_www(self):
        self.assertEqual(normalize_hostname("*.Exa.AI"), "exa.ai")
        self.assertEqual(normalize_hostname("https://www.exa.ai:443/x"), "exa.ai")
        self.assertEqual(normalize_hostname("exa.ai."), "exa.ai")


class CollectorContractTests(unittest.TestCase):
    def test_registrable_domain_still_returns_empty_string_on_drop(self):
        # Both callers test the result for truthiness rather than None, so
        # the delegate must keep returning "".
        self.assertEqual(registrable_domain("com.ai"), "")
        self.assertIsInstance(registrable_domain("com.ai"), str)

    def test_registrable_domain_defaults_to_supported_extensions(self):
        self.assertEqual(registrable_domain("exa.ai"), "exa.ai")
        self.assertEqual(registrable_domain("thing.net"), "")


if __name__ == "__main__":
    unittest.main()
