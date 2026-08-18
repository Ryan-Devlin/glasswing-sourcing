"""Intake ranking policy — pure, from one certificate's hostname list.

This decides which quarter of arrivals ever gets fetched, so it is the single
highest-leverage policy in the pipeline. Asserted directly rather than
inferred from a live run.
"""

import unittest

from intake_score import (
    MAX_APEXES_PER_CERT,
    read_certificate,
    subdomain_labels,
    tld_weight,
)

ALL = {".ai", ".io", ".app", ".dev", ".co", ".xyz", ".tech", ".com"}


def score_of(hostnames, domain, **kw):
    reading = read_certificate(hostnames, ALL, **kw)
    return next(s.score for s in reading.scored if s.domain == domain)


class BuildMaturityTests(unittest.TestCase):
    def test_shipping_an_app_outranks_a_bare_registration(self):
        # The whole point: engineering structure on the certificate is the
        # difference between a company and a parked name, and it is visible
        # before anything is fetched.
        bare = score_of(["foo.ai", "www.foo.ai"], "foo.ai")
        built = score_of(
            ["foo.ai", "www.foo.ai", "app.foo.ai", "api.foo.ai"], "foo.ai"
        )
        self.assertGreater(built, bare)

    def test_boilerplate_subdomains_do_not_count_as_structure(self):
        # mail/cdn/www appear on essentially every certificate. If they scored
        # as "built", the ranking would be noise.
        bare = score_of(["foo.ai", "www.foo.ai"], "foo.ai")
        infra = score_of(
            ["foo.ai", "www.foo.ai", "mail.foo.ai", "cdn.foo.ai", "mx.foo.ai"],
            "foo.ai",
        )
        self.assertLess(infra - bare, 8)
        evidence = read_certificate(
            ["foo.ai", "www.foo.ai", "mail.foo.ai"], ALL
        ).scored[0].evidence
        self.assertIn("bare_registration", evidence)

    def test_unrecognised_subdomains_still_count_as_a_build(self):
        # A company's own naming is evidence of engineering even when it is
        # not on the known-label list; only boilerplate is discounted.
        bare = score_of(["foo.ai", "www.foo.ai"], "foo.ai")
        custom = score_of(["foo.ai", "tenant-eu.foo.ai"], "foo.ai")
        self.assertGreater(custom, bare)

    def test_wildcard_reads_as_provisioned_infrastructure(self):
        plain = score_of(["foo.ai", "app.foo.ai"], "foo.ai")
        wild = score_of(["*.foo.ai", "foo.ai", "app.foo.ai"], "foo.ai")
        self.assertGreater(wild, plain)

    def test_a_bare_registration_is_ranked_not_discarded(self):
        # This is the population the funnel exists to watch. It must survive
        # intake and simply sort last, because the next certificate is what
        # re-scores it.
        reading = read_certificate(["foo.ai"], ALL)
        self.assertEqual(reading.domains, ["foo.ai"])
        self.assertGreater(reading.scored[0].score, 0)


class RescoringTests(unittest.TestCase):
    def test_a_later_certificate_scores_the_same_domain_higher(self):
        # The property that removes the need for a revisit scheduler: when a
        # company ships, the certificate that provisions its app subdomains
        # re-scores the domain and it climbs the queue on its own.
        day_one = score_of(["foo.ai", "www.foo.ai"], "foo.ai")
        after_launch = score_of(
            ["foo.ai", "www.foo.ai", "app.foo.ai", "api.foo.ai", "dashboard.foo.ai"],
            "foo.ai",
        )
        self.assertGreater(after_launch, day_one)

    def test_scoring_is_deterministic(self):
        hosts = ["foo.ai", "app.foo.ai", "api.foo.ai"]
        self.assertEqual(score_of(hosts, "foo.ai"), score_of(hosts, "foo.ai"))

    def test_hostname_order_does_not_change_the_score(self):
        a = score_of(["foo.ai", "app.foo.ai", "api.foo.ai"], "foo.ai")
        b = score_of(["api.foo.ai", "foo.ai", "app.foo.ai"], "foo.ai")
        self.assertEqual(a, b)


class PlatformCertificateTests(unittest.TestCase):
    def test_a_shared_certificate_is_dropped_whole(self):
        hosts = [f"company{i}.ai" for i in range(MAX_APEXES_PER_CERT + 1)]
        reading = read_certificate(hosts, ALL)
        self.assertEqual(reading.scored, [])
        self.assertEqual(len(reading.dropped), MAX_APEXES_PER_CERT + 1)
        self.assertIn("shared_cert", reading.drop_reason)

    def test_a_company_holding_a_few_domains_is_kept(self):
        # Dropping at intake is irreversible, so the threshold is generous:
        # a real company legitimately holds .com plus .ai plus a redirect.
        reading = read_certificate(["foo.ai", "foo.com", "foo.io"], ALL)
        self.assertEqual(len(reading.scored), 3)
        self.assertEqual(reading.dropped, [])

    def test_the_threshold_is_on_distinct_apexes_not_hostnames(self):
        # One company with many subdomains is the best kind of candidate and
        # must never be mistaken for a platform certificate.
        hosts = ["foo.ai"] + [f"svc{i}.foo.ai" for i in range(40)]
        reading = read_certificate(hosts, ALL)
        self.assertEqual(reading.domains, ["foo.ai"])


class TldPriorTests(unittest.TestCase):
    def test_denser_tlds_start_higher(self):
        self.assertGreater(tld_weight("foo.ai"), tld_weight("foo.com"))
        self.assertGreater(tld_weight("foo.dev"), tld_weight("foo.co"))

    def test_an_untracked_suffix_falls_back_rather_than_raising(self):
        self.assertGreater(tld_weight("foo.example"), 0)

    def test_tld_is_a_prior_not_a_gate(self):
        # The extension allow-list decides what is tracked; this only decides
        # what is looked at first.
        reading = read_certificate(["foo.com", "app.foo.com"], {".com"})
        self.assertEqual(reading.domains, ["foo.com"])


class SubdomainLabelTests(unittest.TestCase):
    def test_labels_are_extracted_relative_to_the_registrable_domain(self):
        self.assertEqual(subdomain_labels("api.v2.foo.ai", "foo.ai"), ("api", "v2"))
        self.assertEqual(subdomain_labels("foo.ai", "foo.ai"), ())

    def test_wildcards_are_distinguishable_from_a_bare_apex(self):
        self.assertEqual(subdomain_labels("*.foo.ai", "foo.ai"), ("*",))

    def test_a_mismatched_host_yields_no_labels(self):
        self.assertEqual(subdomain_labels("api.other.ai", "foo.ai"), ())


class RobustnessTests(unittest.TestCase):
    def test_empty_and_junk_input_yields_an_empty_reading(self):
        for hosts in ([], None, ["", "   "], ["com.ai"], ["not-a-domain"]):
            with self.subTest(hosts=hosts):
                reading = read_certificate(hosts, ALL)
                self.assertEqual(reading.scored, [])
                self.assertEqual(reading.dropped, [])

    def test_untracked_extensions_are_filtered_before_scoring(self):
        reading = read_certificate(["foo.ai", "bar.net"], {".ai"})
        self.assertEqual(reading.domains, ["foo.ai"])

    def test_scores_are_non_negative_integers(self):
        for s in read_certificate(["foo.xyz", "www.foo.xyz"], ALL).scored:
            self.assertIsInstance(s.score, int)
            self.assertGreaterEqual(s.score, 0)

    def test_evidence_is_recorded_and_bounded(self):
        hosts = ["foo.ai"] + [f"label{i}.foo.ai" for i in range(60)]
        entry = read_certificate(hosts, ALL).scored[0]
        self.assertTrue(entry.evidence)
        self.assertLessEqual(len(entry.evidence), 200)


if __name__ == "__main__":
    unittest.main()
