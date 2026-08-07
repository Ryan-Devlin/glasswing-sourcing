import unittest
from pathlib import Path


class ReviewQualityUiFixTests(unittest.TestCase):
    def setUp(self):
        self.dashboard = Path("dashboard.py").read_text(encoding="utf-8")
        self.filter = Path("filter.py").read_text(encoding="utf-8")
        self.ui_theme = Path("ui_theme.py").read_text(encoding="utf-8")
        self.ai_search = Path("pages/ai_search.py").read_text(encoding="utf-8")
        self.zone_universe = Path("pages/zone_universe.py").read_text(encoding="utf-8")

    def test_review_floor_is_enforced(self):
        self.assertIn("review_floor = 25", self.dashboard)
        self.assertIn("review_floor = 25", self.filter)
        self.assertIn("signal_dimensions >= 1", self.filter)

    def test_low_signal_is_not_hard_denied(self):
        # 2026-08-08: there is no more separate "insufficient" limbo bucket
        # -- a low-signal, non-disqualified company is Needs Review, the
        # same as everything else that isn't a factual denial.
        self.assertIn(
            "no_clear_enterprise_or_glasswing_fit",
            self.dashboard,
        )
        self.assertIn(
            'result.at[index, "routing_bucket"] = "review"',
            self.filter,
        )
        self.assertNotIn('"Insufficient Evidence"', self.dashboard)

    def test_hard_denied_requires_explicit_evidence(self):
        # route_company's "reject" bucket is itself already gated on a
        # specific factual disqualifier (see its docstring) -- the display
        # layer trusting it directly is no longer a second, separate
        # regex-reconstruction of "is this really a denial".
        self.assertIn(
            'visible.loc[routing.eq("reject")] = "hard_denied"',
            self.dashboard,
        )
        self.assertIn(
            'visible.loc[explicit_denial] = "hard_denied"',
            self.dashboard,
        )

    def test_enterprise_rescue_signal_is_still_computed(self):
        # Enterprise rescue no longer gates promotion into Needs Review --
        # everything not a factual denial is Needs Review regardless. It's
        # kept only as an informational reason attached to the row.
        self.assertIn("enterprise_rescue_mask", self.dashboard)
        self.assertIn("rescue_promoted = enterprise_rescue", self.dashboard)
        self.assertIn("enterprise_rescue_signal", self.filter)

    def test_dashboard_uses_revised_fit_score_without_rewriting_history(self):
        self.assertIn('result["_fit_score_v2"]', self.dashboard)
        self.assertIn('"_fit_pillar_v2"', self.dashboard)
        self.assertIn('"homepage_excerpt"', self.dashboard)
        self.assertIn("website_copy.map(score_text)", self.dashboard)
        self.assertIn('website_copy_is_usable = website_copy.ne("")', self.dashboard)

    def test_inline_review_controls_replace_move_ui(self):
        self.assertNotIn('"Move selected to"', self.dashboard)
        self.assertNotIn('"Move selected"', self.dashboard)
        self.assertNotIn('"Move selected"', self.ai_search)
        self.assertNotIn('"Move selected"', self.zone_universe)
        self.assertIn('"Mark Yes": st.column_config.CheckboxColumn(', self.ui_theme)
        self.assertIn('"Mark No": st.column_config.CheckboxColumn(', self.ui_theme)
        self.assertIn('reason="inline_yes_checkbox"', self.ui_theme)
        self.assertIn('reason="inline_no_checkbox"', self.ui_theme)
        self.assertNotIn('cols = st.columns(5)', self.dashboard)
        self.assertIn("render_hideable_table(", self.dashboard)

    def test_needs_review_caption_explains_gate(self):
        # The bucketing redesign made Needs Review the catch-all for
        # anything without a factual disqualifier, regardless of score --
        # a stale caption claiming a 25+ score gate misled the user into
        # thinking a filter existed that the routing logic never applied.
        self.assertIn(
            "regardless of score",
            self.dashboard,
        )
        self.assertNotIn("score 25+", self.dashboard)

    def test_needs_review_is_not_automatically_positive_training_data(self):
        self.assertIn(
            "positive_training = investable_candidates.copy()",
            self.dashboard,
        )
        self.assertNotIn(
            'if decision in {"Needs Review", "Investable Candidate"}:',
            self.dashboard,
        )
        self.assertNotIn("no_basket_targets", self.dashboard)
        self.assertNotIn("trusted_human_training_yes", self.filter)
        self.assertNotIn("human_training_basket_no", self.filter)
        self.assertIn('result.at[idx, "training_basket"]', self.filter)

    def test_required_keyword_miss_is_recorded_as_a_screened_out_reason(self):
        # 2026-08-08: a keyword miss no longer hides a company in a separate
        # limbo bucket -- there is none left. It still shows up as a
        # persisted reason for context, but the company remains visible in
        # Needs Review like anything else that isn't a factual denial.
        self.assertIn("missing_required_keywords", self.filter)

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
        self.assertIn("with tabs[3]:", self.dashboard)

    def test_human_baskets_have_live_editable_tables(self):
        self.assertIn('"Yes Basket"', self.dashboard)
        self.assertIn('"No Basket"', self.dashboard)
        self.assertIn('st.data_editor(', self.dashboard)
        self.assertIn('"Save table edits"', self.dashboard)
        self.assertIn("save_basket_entry", self.dashboard)
        self.assertIn("save_basket_entries", self.ui_theme)
        self.assertIn("with tabs[4]:", self.dashboard)

    def test_training_labels_do_not_create_manual_classification_moves(self):
        self.assertNotIn("record_manual_move", self.dashboard)
        self.assertNotIn("save_bulk_hard_denials", self.dashboard)
        self.assertNotIn("allow_move_all_to_hard_denied", self.dashboard)
        self.assertIn(
            "Mark Yes/Mark No are training labels only. They never reclassify",
            self.ui_theme,
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

    def test_websites_checked_is_the_three_bucket_sum_only(self):
        # 2026-08-08: no separate "auto-rejected" concept -- a Python
        # "reject" is retained and counted as real Hard Denied now (see
        # automated_discard_mask in filter.py), so the three visible
        # buckets are the complete picture again on their own.
        self.assertIn(
            "funnel_screened = (\n"
            "    len(investable_candidates) + len(needs_review) + len(hard_denied)\n"
            ")",
            self.dashboard,
        )
        self.assertNotIn("auto_rejected", self.dashboard)
        self.assertNotIn("Auto-rejected", self.dashboard)
        # The old duplicate "Classified" metric (a separate worker-side
        # counter that could show a different number than the funnel's
        # own "Websites checked") is gone -- one definition, not two.
        self.assertNotIn('.metric(\n    "Classified"', self.dashboard)
        self.assertNotIn('.metric("Classified"', self.dashboard)


if __name__ == "__main__":
    unittest.main()
