import unittest
from unittest.mock import patch

import pandas as pd

import filter as screening_filter


class RejectionPersistenceTests(unittest.TestCase):
    def test_storage_gate_keeps_credible_ambiguity_and_drops_junk(self):
        frame = pd.DataFrame([
            {"routing_bucket": "priority", "glasswing_rating": 10},
            {"routing_bucket": "coming_soon", "glasswing_rating": 0},
            {"routing_bucket": "review", "glasswing_rating": 20},
            {"routing_bucket": "comp", "glasswing_rating": 10},
            {"routing_bucket": "insufficient", "glasswing_rating": 24},
            {"routing_bucket": "insufficient", "glasswing_rating": 25},
            {"routing_bucket": "screened_out", "glasswing_rating": 5},
            {"routing_bucket": "ghost", "glasswing_rating": 90},
            {"routing_bucket": "reject", "glasswing_rating": 90},
            {
                "routing_bucket": "priority",
                "glasswing_rating": 80,
                "venture_slop_risk": True,
            },
        ])
        # "insufficient" is the primary catch-all for real, substantive
        # sites that just lack enough evidence yet -- it must be protected
        # like review/comp regardless of score, or almost every ambiguous
        # domain silently vanishes before reaching the Insufficient
        # Evidence tab (confirmed live: a 600-domain batch retained zero).
        #
        # "reject" is protected too (2026-08-08): three buckets only, and
        # a Python rejection must actually show up as Hard Denied, not get
        # purged. venture_slop_risk still overrides protection regardless
        # of bucket, including "reject" -- that's a quality signal, not
        # something any bucket should shield a row from.
        self.assertEqual(
            screening_filter.automated_discard_mask(frame).tolist(),
            [False, False, False, False, False, False, True, True, False, True],
        )

    def test_all_rejections_persist_now_not_just_manual_ones(self):
        # 2026-08-08: three buckets only -- "if something is rejected it
        # should fall under hard denied", full stop. An automatic Python
        # rejection and a manually-protected one now persist identically;
        # neither gets purged down to a tombstone.
        output = pd.DataFrame([
            {
                "domain": "automatic.ai",
                "routing_bucket": "reject",
                "routing_reason": "domain_for_sale",
                "manual_override": False,
            },
            {
                "domain": "human.ai",
                "routing_bucket": "reject",
                "routing_reason": "manual_override_hard_denied",
                "manual_override": True,
            },
        ])
        saved_frames = []

        def capture(frame):
            saved_frames.append(frame.copy())
            return len(frame)

        with (
            patch.object(
                screening_filter, "apply_run_preferences",
                side_effect=lambda frame, _preferences: frame,
            ),
            patch.object(
                screening_filter, "apply_persistent_overrides",
                side_effect=lambda frame: frame,
            ),
            patch.object(screening_filter, "load_run_preferences", return_value={}),
            patch(
                "training_truth.protected_training_domains",
                return_value=set(),
            ),
            patch.object(screening_filter, "count_classifications", return_value=1),
            patch.object(
                screening_filter, "upsert_classifications",
                side_effect=capture,
            ),
            patch.object(
                screening_filter, "upsert_csv",
                side_effect=lambda _path, frame, key="domain": frame,
            ),
            patch.object(
                screening_filter, "export_all_screened_csv",
                return_value=None,
            ),
            patch.object(screening_filter, "atomic_write_csv"),
            patch(
                "rejection_cleanup.purge_automated_rejections",
                return_value={"purged_domains": 0},
            ) as purge,
            patch.object(
                screening_filter,
                "record_automated_exclusions",
                return_value=0,
            ) as tombstone,
        ):
            screening_filter.persist_screening_output(output)

        self.assertEqual(
            sorted(saved_frames[0]["domain"].tolist()),
            ["automatic.ai", "human.ai"],
        )
        purge.assert_not_called()
        tombstone.assert_not_called()


if __name__ == "__main__":
    unittest.main()
