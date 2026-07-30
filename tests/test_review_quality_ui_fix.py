import unittest
from pathlib import Path


class ReviewQualityUiFixTests(unittest.TestCase):
    def setUp(self):
        self.dashboard = Path("dashboard.py").read_text(encoding="utf-8")
        self.filter = Path("filter.py").read_text(encoding="utf-8")

    def test_review_floor_is_enforced(self):
        self.assertIn("review_floor = 25", self.dashboard)
        self.assertIn("review_floor = 25", self.filter)
        self.assertIn("signal_dimensions >= 1", self.filter)

    def test_low_signal_is_not_hard_denied(self):
        self.assertIn(
            "no_clear_enterprise_or_glasswing_fit",
            self.dashboard,
        )
        self.assertIn(
            'result.at[index, "routing_bucket"] = "insufficient"',
            self.filter,
        )
        self.assertIn('"Insufficient Evidence"', self.dashboard)
        self.assertIn(
            'routing.eq("ghost")] = "insufficient_evidence"',
            self.dashboard,
        )

    def test_hard_denied_requires_explicit_evidence(self):
        self.assertNotIn(
            'visible.loc[routing.eq("reject")] = "hard_denied"',
            self.dashboard,
        )
        self.assertIn(
            'visible.loc[explicit_denial] = "hard_denied"',
            self.dashboard,
        )

    def test_enterprise_rescue_still_requires_objective_score_floor(self):
        self.assertIn("enterprise_rescue_mask", self.dashboard)
        self.assertIn("scores.ge(review_floor)", self.dashboard)
        self.assertIn("| enterprise_rescue", self.dashboard)
        self.assertIn("enterprise_rescue_signal", self.filter)

    def test_dashboard_uses_revised_fit_score_without_rewriting_history(self):
        self.assertIn('result["_fit_score_v2"]', self.dashboard)
        self.assertIn('"_fit_pillar_v2"', self.dashboard)
        self.assertIn('"homepage_excerpt"', self.dashboard)
        self.assertIn("website_copy.map(score_text)", self.dashboard)
        self.assertIn('website_copy_is_usable = website_copy.ne("")', self.dashboard)

    def test_compact_review_ui(self):
        self.assertIn(
            '"Select and move companies"',
            self.dashboard,
        )
        self.assertIn('"Move selected to"', self.dashboard)
        self.assertIn('"Move selected"', self.dashboard)
        self.assertNotIn('cols = st.columns(5)', self.dashboard)
        self.assertIn("@st.fragment", self.dashboard)
        self.assertIn('st.rerun(scope="fragment")', self.dashboard)
        self.assertIn('"Select"] = False', self.dashboard)
        self.assertIn('batch["Website"]', self.dashboard)
        self.assertIn(
            '"Open website"',
            self.dashboard,
        )
        self.assertIn(
            "_batch_message",
            self.dashboard,
        )

    def test_needs_review_caption_explains_gate(self):
        self.assertIn(
            "score 25+ plus",
            self.dashboard,
        )

    def test_needs_review_is_not_automatically_positive_training_data(self):
        self.assertIn(
            "positive_training = investable_candidates.copy()",
            self.dashboard,
        )
        self.assertNotIn(
            'if decision in {"Needs Review", "Investable Candidate"}:',
            self.dashboard,
        )
        self.assertIn(
            'if target == "Investable Candidate":',
            self.dashboard,
        )

    def test_required_keyword_miss_cannot_be_promoted_to_review(self):
        self.assertIn("missing_required_keywords", self.dashboard)
        self.assertIn('visible.loc[keyword_failed] = "insufficient_evidence"', self.dashboard)

    def test_classification_quality_diagnostics(self):
        self.assertIn('"#### Classification quality"', self.dashboard)
        self.assertIn("classification_reason_summary", self.dashboard)
        self.assertIn("Hard-denial reasons", self.dashboard)
        self.assertIn("Needs-review reasons", self.dashboard)

    def test_claude_rejections_have_a_separate_audit_tab(self):
        self.assertIn('"Rejected by Claude"', self.dashboard)
        self.assertIn("claude_rejected = hard_denied", self.dashboard)
        self.assertIn('rejected["Claude Reasoning"]', self.dashboard)
        self.assertIn('rejected["Claude Concerns"]', self.dashboard)
        self.assertIn("claude_reject_overrides.eq(\"\")", self.dashboard)
        self.assertIn("with tabs[6]:", self.dashboard)

    def test_human_baskets_have_live_editable_tables(self):
        self.assertIn('"Yes Basket"', self.dashboard)
        self.assertIn('"No Basket"', self.dashboard)
        self.assertIn('st.data_editor(', self.dashboard)
        self.assertIn('"Save basket edits"', self.dashboard)
        self.assertIn('"Save table edits"', self.dashboard)
        self.assertIn("save_basket_entry", self.dashboard)
        self.assertIn("with tabs[8]:", self.dashboard)

    def test_manual_hard_denial_atomically_moves_company_to_no(self):
        self.assertIn(
            'elif target == "Hard Denied":',
            self.dashboard,
        )
        self.assertIn(
            '"manual_hard_denial_classification"',
            self.dashboard,
        )
        self.assertIn(
            'objective_score=int(selected_item["Score"])',
            self.dashboard,
        )

    def test_investable_and_review_support_bulk_hard_denial(self):
        self.assertIn("save_bulk_hard_denials", self.dashboard)
        self.assertEqual(
            self.dashboard.count(
                "allow_move_all_to_hard_denied=True"
            ),
            2,
        )
        self.assertIn(
            '"bulk_move_all_to_hard_denied"',
            self.dashboard,
        )

    def test_streamlit_width_api_is_current(self):
        self.assertNotIn("use_container_width", self.dashboard)
        self.assertIn('width="stretch"', self.dashboard)

    def test_dashboard_keeps_stable_data_during_refreshes(self):
        self.assertIn(
            "@st.cache_data(show_spinner=False, max_entries=32)",
            self.dashboard,
        )
        self.assertIn("csv_snapshot::", self.dashboard)
        self.assertIn(
            "@st.cache_data(show_spinner=False, max_entries=4)",
            self.dashboard,
        )


if __name__ == "__main__":
    unittest.main()
