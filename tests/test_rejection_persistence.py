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
        self.assertEqual(
            screening_filter.automated_discard_mask(frame).tolist(),
            [False, False, False, False, False, False, True, True, True, True],
        )

    def test_only_manual_denial_is_persisted(self):
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
                return_value={"purged_domains": 1},
            ) as purge,
            patch.object(
                screening_filter,
                "record_automated_exclusions",
                return_value=1,
            ) as tombstone,
        ):
            screening_filter.persist_screening_output(output)

        self.assertEqual(saved_frames[0]["domain"].tolist(), ["human.ai"])
        self.assertEqual(
            list(purge.call_args.args[0]),
            ["automatic.ai"],
        )
        self.assertFalse(purge.call_args.kwargs["clean_csv"])
        tombstone.assert_called_once()


if __name__ == "__main__":
    unittest.main()
