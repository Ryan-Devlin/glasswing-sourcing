import unittest

from filter import enterprise_rescue_assessment


class EnterpriseRescueTests(unittest.TestCase):
    def assertRescued(self, text: str) -> None:
        rescued, evidence = enterprise_rescue_assessment(text)
        self.assertTrue(rescued)
        self.assertTrue(evidence)

    def test_rescues_concise_glasswing_candidates(self):
        examples = [
            (
                "Trussed AI is an enterprise AI governance control plane "
                "enforcing policy and capturing audit evidence across every "
                "model in production."
            ),
            (
                "An AI agent that does the GRC manager's job. Compliance "
                "for SOC 2 and whatever else your auditor checks."
            ),
            (
                "A research operating system platform spanning the whole "
                "lifecycle with data quality, formal proof, SaaS, and MCP "
                "tools for researchers."
            ),
            (
                "Observable autonomous software systems for software "
                "engineering orchestration workflows powered by Claude Code."
            ),
            (
                "A platform for R&D accrual and forecast automation, "
                "budgeting, and financial planning for biopharma teams."
            ),
            (
                "An AI search stack for enterprise site search with secure "
                "pipelines that teams can scale."
            ),
            (
                "Identity enforcement gateway providing secure access to "
                "enterprise systems without VPNs."
            ),
        ]
        for example in examples:
            with self.subTest(example=example):
                self.assertRescued(example)

    def test_does_not_rescue_noise(self):
        examples = [
            "",
            "Welcome to our website.",
            "Online dance classes for beginners with a monthly subscription.",
            "Marketing agency offering content services for growing brands.",
            "Buy cheap card dumps and anonymous crypto services online.",
        ]
        for example in examples:
            with self.subTest(example=example):
                rescued, _ = enterprise_rescue_assessment(example)
                self.assertFalse(rescued)


if __name__ == "__main__":
    unittest.main()
